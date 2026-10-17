package gears.asyncio.uring

import gears.asyncio.Address
import gears.asyncio.HandleSlot
import gears.asyncio.Interest
import gears.asyncio.Repeatable
import gears.asyncio.Slot
import gears.asyncio.unsafe.PosixSockets

import java.io.FileNotFoundException
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.scalanative.memory.PointerBufferOps.given
import scala.scalanative.posix.errno
import scala.scalanative.posix.string
import scala.scalanative.posix.sys.socket
import scala.scalanative.posix.unistd
import scala.scalanative.runtime.ByteArray
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import uring.*
import uringStructOps.*

private[uring] object Ring {
  export LinuxConstants.{ETIME, POLLIN, POLLOUT, SOCK_CLOEXEC, SOCK_NONBLOCK}

  val currentPosition: __u64 = (-1L).toULong

  def transient(res: Int): Boolean = res == -errno.EINTR || res == -errno.EAGAIN

  def failure(what: String, res: Int): IOException =
    new IOException(s"$what: ${fromCString(string.strerror(-res))}")

  def pollMask(interest: Interest): CUnsignedInt =
    (if interest == Interest.Read then POLLIN else POLLOUT).toUInt
}

import Ring.*

// Heap buffers are handed to the kernel in place, which relies on Scala Native's GC never moving objects.
private[uring] final class BufferRegion(buf: ByteBuffer) {
  private var scratch: Array[Byte] | Null = null
  private var copied = false

  private def inPlace(): Ptr[Byte] | Null =
    if buf.hasArray() then {
      val index = buf.arrayOffset() + buf.position()
      if index < buf.array().length then buf.array().asInstanceOf[ByteArray].at(index) else null
    } else if buf.hasPointer() then buf.pointer() + buf.position()
    else null

  private def scratchOf(size: Int): Ptr[Byte] = {
    if scratch == null || scratch.nn.length < size then scratch = new Array[Byte](math.max(size, 1))
    scratch.nn.asInstanceOf[ByteArray].at(0)
  }

  def validateFill(): Unit =
    require(!buf.isReadOnly, "Read buffers must be writable")

  def toFill(): Ptr[Byte] = {
    validateFill()
    val direct = inPlace()
    copied = direct == null && buf.remaining() > 0
    if copied then scratchOf(buf.remaining()) else direct.asInstanceOf[Ptr[Byte]]
  }

  def toDrain(): Ptr[Byte] = {
    val direct = inPlace()
    copied = direct == null && buf.remaining() > 0
    if copied then {
      val ptr = scratchOf(buf.remaining())
      buf.duplicate().get(scratch.nn, 0, buf.remaining())
      ptr
    } else direct.asInstanceOf[Ptr[Byte]]
  }

  def filled(n: Int): Unit =
    if copied then buf.put(scratch.nn, 0, n) else buf.position(buf.position() + n)

  def drained(n: Int): Unit = buf.position(buf.position() + n)
}

private[uring] final class VectorBufferRegion(bufs: Array[ByteBuffer]) {
  require(bufs.length <= 1024, "A vector I/O operation may contain at most 1024 buffers")
  private val vectors = new Array[Byte](math.max(1, sizeof[iovec].toInt * bufs.length))
  private val regions = bufs.map(new BufferRegion(_))
  private var filling = false

  def prepare(fill: Boolean): Ptr[iovec] =
    filling = fill
    val base = vectors.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[iovec]]
    var i = 0
    while i < bufs.length do
      val vec = base + i
      vec.iov_base = if fill then regions(i).toFill() else regions(i).toDrain()
      vec.iov_len = bufs(i).remaining().toUSize
      i += 1
    base

  def advance(count: Int): Unit =
    var left = count
    var i = 0
    while i < bufs.length && left > 0 do
      val moved = math.min(left, bufs(i).remaining())
      if filling then regions(i).filled(moved) else regions(i).drained(moved)
      left -= moved
      i += 1
    if left != 0 then throw new IOException(s"Vector I/O returned $count bytes beyond the supplied buffers")
}

private[uring] final class MessageHeader {
  private val header = new Array[Byte](sizeof[msghdr].toInt)
  private val vector = new Array[Byte](sizeof[iovec].toInt)
  val name: Array[Byte] = SocketAddresses.storage()

  def msg: Ptr[msghdr] = header.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[msghdr]]

  def set(data: Ptr[Byte], length: Int, nameLength: Int): Unit = {
    val iov = vector.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[iovec]]
    iov.iov_base = data
    iov.iov_len = length.toCSize
    setIovecs(iov, 1, nameLength)
  }

  def setIovecs(iov: Ptr[iovec], count: Int, nameLength: Int): Unit = {
    val m = uringStructOps.msghdrOps(msg)
    m.msg_name = if nameLength > 0 then SocketAddresses.pointer(name).asInstanceOf[Ptr[Byte]] else null
    m.msg_namelen = nameLength.toUInt
    m.msg_iov = iov
    m.msg_iovlen = count.toCSize
    m.msg_control = null
    m.msg_controllen = 0.toCSize
    m.msg_flags = 0
  }
}

final class Read[Owner](val fd: Int, buf: ByteBuffer, offset: Long = -1L) extends RingOp[Owner] with Repeatable {
  override protected def readinessFd: Int = fd
  override protected def readinessEvents: Int = POLLIN
  private val region = new BufferRegion(buf)

  private[uring] def prepare(sqe: Ptr[io_uring_sqe]): Unit =
    io_uring_prep_read(sqe, fd, region.toFill(), buf.remaining().toUInt, offset.toULong)

  override private[uring] def keepProgress(res: Int): Unit = region.filled(res)

  private[uring] def finish(res: Int): Boolean =
    if transient(res) then false
    else if res < 0 then throw failure(s"Failed to read from descriptor $fd", res)
    else {
      region.filled(res)
      true
    }
}

final class ReadVector[Owner](val fd: Int, bufs: Array[ByteBuffer]) extends RingOp[Owner] with Repeatable {
  override protected def readinessFd: Int = fd
  override protected def readinessEvents: Int = POLLIN
  require(bufs.forall(buf => !buf.isReadOnly), "Read buffers must be writable")
  private val region = new VectorBufferRegion(bufs)

  private[uring] def prepare(sqe: Ptr[io_uring_sqe]): Unit =
    io_uring_prep_readv(sqe, fd, region.prepare(fill = true), bufs.length.toUInt, currentPosition)

  override private[uring] def keepProgress(res: Int): Unit = region.advance(res)

  private[uring] def finish(res: Int): Boolean =
    if transient(res) then false
    else if res < 0 then throw failure(s"Failed to read from descriptor $fd", res)
    else {
      region.advance(res)
      true
    }
}

final class Write[Owner](val fd: Int, buf: ByteBuffer, private var plain: Boolean, offset: Long = -1L)
    extends RingOp[Owner]
    with Repeatable {
  override protected def readinessFd: Int = fd
  override protected def readinessEvents: Int = POLLOUT
  private val region = new BufferRegion(buf)

  private[uring] def prepare(sqe: Ptr[io_uring_sqe]): Unit = {
    val data = region.toDrain()
    if plain then io_uring_prep_write(sqe, fd, data, buf.remaining().toUInt, offset.toULong)
    else io_uring_prep_send(sqe, fd, data, buf.remaining().toCSize, socket.MSG_NOSIGNAL)
  }

  override private[uring] def keepProgress(res: Int): Unit = region.drained(res)

  private[uring] def finish(res: Int): Boolean =
    if res == -errno.ENOTSOCK && !plain then {
      plain = true
      false
    } else if transient(res) then false
    else if res < 0 then throw failure(s"Failed to write to descriptor $fd", res)
    else if res == 0 && buf.hasRemaining() then throw new IOException(s"Write to descriptor $fd made no progress")
    else {
      region.drained(res)
      true
    }
}

final class WriteVector[Owner](val fd: Int, bufs: Array[ByteBuffer]) extends RingOp[Owner] with Repeatable {
  override protected def readinessFd: Int = fd
  override protected def readinessEvents: Int = POLLOUT
  private val region = new VectorBufferRegion(bufs)
  private val header = new MessageHeader
  private var plain = false

  private[uring] def prepare(sqe: Ptr[io_uring_sqe]): Unit = {
    val vectors = region.prepare(fill = false)
    if plain then io_uring_prep_writev(sqe, fd, vectors, bufs.length.toUInt, currentPosition)
    else {
      header.setIovecs(vectors, bufs.length, 0)
      io_uring_prep_sendmsg(sqe, fd, header.msg, socket.MSG_NOSIGNAL)
    }
  }

  override private[uring] def keepProgress(res: Int): Unit = region.advance(res)

  private[uring] def finish(res: Int): Boolean =
    if res == -errno.ENOTSOCK && !plain then {
      plain = true
      false
    } else if transient(res) then false
    else if res < 0 then throw failure(s"Failed to write to descriptor $fd", res)
    else if res == 0 && bufs.exists(_.hasRemaining()) then
      throw new IOException(s"Vector write to descriptor $fd made no progress")
    else {
      region.advance(res)
      true
    }
}

final class Accept[Owner](val fd: Int, into: HandleSlot) extends RingOp[Owner] with Repeatable {
  override protected def readinessFd: Int = fd
  override protected def readinessEvents: Int = POLLIN
  private[uring] def prepare(sqe: Ptr[io_uring_sqe]): Unit =
    io_uring_prep_accept(sqe, fd, null, null, SOCK_NONBLOCK | SOCK_CLOEXEC)

  private[uring] def finish(res: Int): Boolean =
    if transient(res) || res == -errno.ECONNABORTED then false
    else if res < 0 then throw failure("Failed to accept connection", res)
    else {
      into.set(res)
      true
    }

  override private[uring] def discard(res: Int): Unit = if res >= 0 then unistd.close(res)
}

final class Connect[Owner](val fd: Int) extends RingOp[Owner] {
  private[uring] def prepare(sqe: Ptr[io_uring_sqe]): Unit = io_uring_prep_poll_add(sqe, fd, POLLOUT.toUInt)

  private[uring] def finish(res: Int): Boolean =
    if transient(res) then false
    else if res < 0 then throw failure("Failed to wait for connection", res)
    else {
      PosixSockets.checkConnect(fd)
      true
    }
}

final class WhenReady[Owner](val fd: Int, val interest: Interest, body: () => Boolean)
    extends RingOp[Owner]
    with Repeatable {
  private[uring] def prepare(sqe: Ptr[io_uring_sqe]): Unit = io_uring_prep_poll_add(sqe, fd, pollMask(interest))

  private[uring] def finish(res: Int): Boolean =
    if transient(res) then false
    else if res < 0 then throw failure(s"Failed to wait for descriptor $fd", res)
    else body()
}

final class Receive[Owner](val fd: Int, buf: ByteBuffer, from: Slot[Address]) extends RingOp[Owner] with Repeatable {
  override protected def readinessFd: Int = fd
  override protected def readinessEvents: Int = POLLIN
  private val region = new BufferRegion(buf)
  private val header = new MessageHeader
  private var start = 0
  private var windowSize = 0

  private[uring] def prepare(sqe: Ptr[io_uring_sqe]): Unit = {
    region.validateFill()
    start = buf.position()
    windowSize = buf.remaining()
    header.set(region.toFill(), windowSize, SocketAddresses.storageSize)
    io_uring_prep_recvmsg(sqe, fd, header.msg, 0)
  }

  private[uring] def finish(res: Int): Boolean =
    if transient(res) then false
    else if res < 0 then throw failure("Failed to receive datagram", res)
    else {
      val received = math.min(res, windowSize)
      region.filled(received)
      buf.limit(start + received)
      buf.position(start)
      from.set(SocketAddresses.decode(SocketAddresses.pointer(header.name), header.msg.msg_namelen))
      true
    }
}

final class Send[Owner](val fd: Int, buf: ByteBuffer, to: Address | Null) extends RingOp[Owner] with Repeatable {
  override protected def readinessFd: Int = fd
  override protected def readinessEvents: Int = POLLOUT
  private val region = new BufferRegion(buf)
  private val header = new MessageHeader

  private[uring] def prepare(sqe: Ptr[io_uring_sqe]): Unit = {
    val nameLength = if to == null then 0 else SocketAddresses.encode(to.nn, header.name).toInt
    header.set(region.toDrain(), buf.remaining(), nameLength)
    io_uring_prep_sendmsg(sqe, fd, header.msg, socket.MSG_NOSIGNAL)
  }

  private[uring] def finish(res: Int): Boolean =
    if transient(res) then false
    else if res < 0 then throw failure("Failed to send datagram", res)
    else if res != buf.remaining() then throw new IOException(s"Incomplete datagram: sent $res of ${buf.remaining()}")
    else {
      region.drained(res)
      true
    }
}

final class UringTimer[Owner](val nanoseconds: Long) extends RingOp[Owner] with Repeatable {
  require(nanoseconds >= 0, "Timer duration must not be negative")
  private val timespec = new Array[Byte](sizeof[__kernel_timespec].toInt)

  private[uring] def prepare(sqe: Ptr[io_uring_sqe]): Unit = {
    val ts = timespec.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[__kernel_timespec]]
    ts.tv_sec = nanoseconds / 1_000_000_000L
    ts.tv_nsec = nanoseconds % 1_000_000_000L
    io_uring_prep_timeout(sqe, ts, 0.toULong, 0.toUInt)
  }

  private[uring] def finish(res: Int): Boolean =
    if res == -ETIME || res == 0 then true
    else throw failure("Timer failed", res)

  override private[uring] def prepareCancel(sqe: Ptr[io_uring_sqe], userData: Long): Unit =
    io_uring_prep_timeout_remove(sqe, userData.toULong, 0.toUInt)
}

final class OpenAt[Owner](val path: String, flags: Int, mode: Int, into: HandleSlot) extends RingOp[Owner] {
  private val cPath = {
    val utf8 = path.getBytes(StandardCharsets.UTF_8)
    java.util.Arrays.copyOf(utf8, utf8.length + 1)
  }

  private[uring] def prepare(sqe: Ptr[io_uring_sqe]): Unit =
    io_uring_prep_openat(
      sqe,
      scala.scalanative.posix.fcntl.AT_FDCWD,
      cPath.asInstanceOf[ByteArray].at(0),
      flags,
      mode.toUInt
    )

  private[uring] def finish(res: Int): Boolean =
    if res == -errno.EINTR then false
    else if res == -errno.ENOENT then throw new FileNotFoundException(path)
    else if res < 0 then throw failure(s"Failed to open $path", res)
    else {
      into.set(res)
      true
    }

  override private[uring] def discard(res: Int): Unit = if res >= 0 then unistd.close(res)
}
