package asyncio.uring

import java.io.FileNotFoundException
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.scalanative.memory.PointerBufferOps.given
import scala.scalanative.posix.arpa.inet
import scala.scalanative.posix.errno
import scala.scalanative.posix.netinet.in
import scala.scalanative.posix.netinet.inOps.{*, given}
import scala.scalanative.posix.string
import scala.scalanative.posix.sys.socket
import scala.scalanative.posix.sys.socketOps.{*, given}
import scala.scalanative.posix.sys.un
import scala.scalanative.posix.sys.unOps.{*, given}
import scala.scalanative.posix.unistd
import scala.scalanative.runtime.ByteArray
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import asyncio.Address
import asyncio.Interest
import asyncio.Repeatable
import asyncio.Slot
import asyncio.unsafe.PosixSockets
import uring.*
import uringOps.*

private[uring] object Ring {

  /** Linux values; Scala Native's posixlib does not expose them. */
  final val SOCK_NONBLOCK = 0x800
  final val SOCK_CLOEXEC = 0x80000
  final val POLLIN = 0x001
  final val POLLOUT = 0x004
  final val ETIME = 62

  /** Offset -1 makes a read or write use and advance the descriptor's own position, like `read(2)` and `write(2)`. */
  val currentPosition: __u64 = (-1L).toULong

  /** Interrupted or not ready: nothing happened, so submit again. */
  def transient(res: Int): Boolean = res == -errno.EINTR || res == -errno.EAGAIN

  def failure(what: String, res: Int): IOException =
    new IOException(s"$what: ${fromCString(string.strerror(-res))}")

  def pollMask(interest: Interest): CUnsignedInt =
    (if interest == Interest.Read then POLLIN else POLLOUT).toUInt
}

import Ring.*

/** Where the kernel reads or writes a buffer's bytes between its position and limit. A heap buffer is used in place,
  * since Scala Native's GC does not move objects and the op keeps the buffer reachable; a pointer buffer by its
  * pointer; anything else, such as a read-only buffer, through a scratch copy.
  */
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

  /** The address the kernel fills `buf.remaining` bytes at. */
  def toFill(): Ptr[Byte] = {
    val direct = inPlace()
    copied = direct == null && buf.remaining() > 0
    if copied then scratchOf(buf.remaining()) else direct.asInstanceOf[Ptr[Byte]]
  }

  /** The address the kernel drains `buf.remaining` bytes from. */
  def toDrain(): Ptr[Byte] = {
    val direct = inPlace()
    copied = direct == null && buf.remaining() > 0
    if copied then {
      val ptr = scratchOf(buf.remaining())
      buf.duplicate().get(scratch.nn, 0, buf.remaining())
      ptr
    } else direct.asInstanceOf[Ptr[Byte]]
  }

  /** The kernel filled `n` bytes at the address from `toFill`. */
  def filled(n: Int): Unit =
    if copied then buf.put(scratch.nn, 0, n) else buf.position(buf.position() + n)

  /** The kernel drained `n` bytes from the address from `toDrain`. */
  def drained(n: Int): Unit = buf.position(buf.position() + n)
}

/** Retains the iovec array and translates a completed byte count into ordered buffer-position advances. */
private[uring] final class VectorBufferRegion(bufs: Array[ByteBuffer]) {
  require(bufs.length <= 1024, "A vector I/O operation may contain at most 1024 buffers")
  private val vectors = new Array[Byte](math.max(1, sizeof[iovec].toInt * bufs.length))

  def prepare(): Ptr[iovec] =
    val base = vectors.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[iovec]]
    var i = 0
    while i < bufs.length do
      val buf = bufs(i)
      val ptr =
        if buf.remaining() == 0 then null
        else if buf.hasArray() then
          buf.array().asInstanceOf[ByteArray].at(buf.arrayOffset() + buf.position())
        else if buf.hasPointer() then buf.pointer() + buf.position()
        else throw new IllegalArgumentException("Buffer must be array-backed or pointer-backed")
      val vec = base + i
      vec.iov_base = ptr
      vec.iov_len = buf.remaining().toUSize
      i += 1
    base

  def advance(count: Int): Unit =
    var left = count
    var i = 0
    while i < bufs.length && left > 0 do
      val buf = bufs(i)
      val moved = math.min(left, buf.remaining())
      buf.position(buf.position() + moved)
      left -= moved
      i += 1
    if left != 0 then throw new IOException(s"Vector I/O returned $count bytes beyond the supplied buffers")
}

/** Socket addresses in the kernel's form, in storage that outlives the call that built them. */
object SocketAddresses {
  final val storageSize = sizeOf[socket.sockaddr_storage].toInt

  def storage(): Array[Byte] = new Array[Byte](storageSize)

  def pointer(storage: Array[Byte]): Ptr[socket.sockaddr] =
    storage.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[socket.sockaddr]]

  /** Writes `address` into `storage`, returning its length. */
  def encode(address: Address, storage: Array[Byte]): socket.socklen_t = {
    val copy: (Ptr[socket.sockaddr], socket.socklen_t) => socket.socklen_t = { (addr, length) =>
      string.memcpy(pointer(storage).asInstanceOf[Ptr[Byte]], addr.asInstanceOf[Ptr[Byte]], length.toLong.toCSize)
      length
    }
    address match {
      case Address.Unix(path)       => PosixSockets.withUnixSocketAddr(path)(copy)
      case Address.IPv4(host, port) => PosixSockets.withIPv4SocketAddr(host, port)(copy)
      case Address.IPv6(host, port) => PosixSockets.withIPv6SocketAddr(host, port)(copy)
    }
  }

  /** Decodes a socket address filled in by the kernel. An unnamed Unix socket has the empty path. */
  def decode(addr: Ptr[socket.sockaddr], length: socket.socklen_t): Address = {
    require(length.toLong >= sizeOf[socket.sa_family_t].toLong, s"Short socket address: ${length.toInt} bytes")
    require(length.toLong <= storageSize, s"Oversized socket address: ${length.toInt} bytes")
    val family = addr.sa_family.toInt
    if family == socket.AF_UNIX then {
      val unix = addr.asInstanceOf[Ptr[un.sockaddr_un]]
      val headerLength = sizeOf[un.sockaddr_un] - sizeOf[CArray[CChar, un._108]]
      if length.toInt <= headerLength.toInt then Address.Unix("")
      else Address.Unix(fromCString(unix.sun_path.at(0), StandardCharsets.UTF_8))
    } else {
      val text = stackalloc[Byte](64)
      if family == socket.AF_INET then {
        require(length.toLong >= sizeOf[in.sockaddr_in].toLong, s"Short IPv4 socket address: ${length.toInt} bytes")
        val v4 = addr.asInstanceOf[Ptr[in.sockaddr_in]]
        inet.inet_ntop(family, v4.at3.asInstanceOf[Ptr[Byte]], text, 64.toUInt)
        Address.IPv4(fromCString(text), inet.ntohs(v4.sin_port).toInt)
      } else if family == socket.AF_INET6 then {
        require(length.toLong >= sizeOf[in.sockaddr_in6].toLong, s"Short IPv6 socket address: ${length.toInt} bytes")
        val v6 = addr.asInstanceOf[Ptr[in.sockaddr_in6]]
        inet.inet_ntop(family, v6.at4.asInstanceOf[Ptr[Byte]], text, 64.toUInt)
        Address.IPv6(fromCString(text), inet.ntohs(v6.sin6_port).toInt)
      } else throw new IOException(s"Unsupported address family $family")
    }
  }
}

/** A `msghdr` with one `iovec` and room for a socket address, for `sendmsg` and `recvmsg`. */
private[uring] final class MessageHeader {
  private val header = new Array[Byte](sizeof[msghdr].toInt)
  private val vector = new Array[Byte](sizeof[iovec].toInt)
  val name: Array[Byte] = SocketAddresses.storage()

  def msg: Ptr[msghdr] = header.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[msghdr]]

  /** Points the header at `length` bytes of data at `data`, and at `nameLength` bytes of `name`, or no name. */
  def set(data: Ptr[Byte], length: Int, nameLength: Int): Unit = {
    val iov = vector.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[iovec]]
    iov.iov_base = data
    iov.iov_len = length.toCSize
    setIovecs(iov, 1, nameLength)
  }

  def setIovecs(iov: Ptr[iovec], count: Int, nameLength: Int): Unit = {
    val m = uringOps.msghdrOps(msg) // not posixlib's msghdr, whose fields are read-only
    m.msg_name = if nameLength > 0 then SocketAddresses.pointer(name).asInstanceOf[Ptr[Byte]] else null
    m.msg_namelen = nameLength.toUInt
    m.msg_iov = iov
    m.msg_iovlen = count.toCSize
    m.msg_control = null
    m.msg_controllen = 0.toCSize
    m.msg_flags = 0
  }
}

/** Fills `buf`. Done once at least one byte has been read, or at end of stream, leaving the buffer as it was. */
final class Read[Owner](val fd: Int, buf: ByteBuffer) extends RingOp[Owner] with Repeatable {
  private val region = new BufferRegion(buf)

  private[uring] def prepare(sqe: Ptr[io_uring_sqe]): Unit =
    io_uring_prep_read(sqe, fd, region.toFill(), buf.remaining().toUInt, currentPosition)

  private[uring] def finish(res: Int): Boolean =
    if transient(res) then false
    else if res < 0 then throw failure(s"Failed to read from descriptor $fd", res)
    else {
      region.filled(res)
      true
    }
}

/** Fills the buffers in order, completing once at least one byte is read or EOF is reached. */
final class ReadVector[Owner](val fd: Int, bufs: Array[ByteBuffer]) extends RingOp[Owner] with Repeatable {
  require(bufs.forall(buf => !buf.isReadOnly), "Read buffers must be writable")
  private val region = new VectorBufferRegion(bufs)

  private[uring] def prepare(sqe: Ptr[io_uring_sqe]): Unit =
    io_uring_prep_readv(sqe, fd, region.prepare(), bufs.length.toUInt, currentPosition)

  private[uring] def finish(res: Int): Boolean =
    if transient(res) then false
    else if res < 0 then throw failure(s"Failed to read from descriptor $fd", res)
    else {
      region.advance(res)
      true
    }
}

/** Drains `buf`. Done once at least one byte has been written. A socket is sent to with `MSG_NOSIGNAL`; anything else,
  * or a descriptor marked `plain`, is written to.
  */
final class Write[Owner](val fd: Int, buf: ByteBuffer, private var plain: Boolean)
    extends RingOp[Owner]
    with Repeatable {
  private val region = new BufferRegion(buf)

  private[uring] def prepare(sqe: Ptr[io_uring_sqe]): Unit = {
    val data = region.toDrain()
    if plain then io_uring_prep_write(sqe, fd, data, buf.remaining().toUInt, currentPosition)
    else io_uring_prep_send(sqe, fd, data, buf.remaining().toCSize, socket.MSG_NOSIGNAL)
  }

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

/** Drains the buffers in order; a partial completion advances only the bytes the kernel wrote. */
final class WriteVector[Owner](val fd: Int, bufs: Array[ByteBuffer]) extends RingOp[Owner] with Repeatable {
  private val region = new VectorBufferRegion(bufs)
  private val header = new MessageHeader
  private var plain = false

  private[uring] def prepare(sqe: Ptr[io_uring_sqe]): Unit = {
    val vectors = region.prepare()
    if plain then io_uring_prep_writev(sqe, fd, vectors, bufs.length.toUInt, currentPosition)
    else {
      header.setIovecs(vectors, bufs.length, 0)
      io_uring_prep_sendmsg(sqe, fd, header.msg, socket.MSG_NOSIGNAL)
    }
  }

  private[uring] def finish(res: Int): Boolean =
    if res == -errno.ENOTSOCK && !plain then {
      plain = true
      false
    } else if transient(res) then false
    else if res < 0 then throw failure(s"Failed to write to descriptor $fd", res)
    else if res == 0 && bufs.exists(_.hasRemaining()) then throw new IOException(s"Vector write to descriptor $fd made no progress")
    else {
      region.advance(res)
      true
    }
}

/** Accepts one connection on a listening socket, writing its non-blocking descriptor into `into`. */
final class Accept[Owner](val fd: Int, into: Slot[Integer]) extends RingOp[Owner] with Repeatable {
  private[uring] def prepare(sqe: Ptr[io_uring_sqe]): Unit =
    io_uring_prep_accept(sqe, fd, null, null, SOCK_NONBLOCK | SOCK_CLOEXEC)

  private[uring] def finish(res: Int): Boolean =
    if transient(res) || res == -errno.ECONNABORTED then false
    else if res < 0 then throw failure("Failed to accept connection", res)
    else {
      into.set(Integer.valueOf(res))
      true
    }

  override private[uring] def discard(res: Int): Unit = if res >= 0 then unistd.close(res)
}

/** Waits for a non-blocking connect to finish and checks its outcome. Done once connected; throws if it failed. */
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

/** Waits until `fd` is ready for `interest`, then runs `body`; done when it returns true. For `Ops.whenReady`. */
final class WhenReady[Owner](val fd: Int, val interest: Interest, body: () => Boolean)
    extends RingOp[Owner]
    with Repeatable {
  private[uring] def prepare(sqe: Ptr[io_uring_sqe]): Unit = io_uring_prep_poll_add(sqe, fd, pollMask(interest))

  private[uring] def finish(res: Int): Boolean =
    if transient(res) then false
    else if res < 0 then throw failure(s"Failed to wait for descriptor $fd", res)
    else body()
}

/** Receives one packet into `buf`, left flipped for reading, and writes its sender into `from`. A packet larger than the
  * buffer is truncated, as `recvmsg(2)` does.
  */
final class Receive[Owner](val fd: Int, buf: ByteBuffer, from: Slot[Address])
    extends RingOp[Owner]
    with Repeatable {
  private val region = new BufferRegion(buf)
  private val header = new MessageHeader

  private[uring] def prepare(sqe: Ptr[io_uring_sqe]): Unit = {
    buf.clear()
    header.set(region.toFill(), buf.remaining(), SocketAddresses.storageSize)
    io_uring_prep_recvmsg(sqe, fd, header.msg, 0)
  }

  private[uring] def finish(res: Int): Boolean =
    if transient(res) then false
    else if res < 0 then throw failure("Failed to receive datagram", res)
    else {
      region.filled(math.min(res, buf.remaining()))
      buf.flip()
      from.set(SocketAddresses.decode(SocketAddresses.pointer(header.name), header.msg.msg_namelen))
      true
    }
}

/** Sends `buf` as one packet, to `to` or to the connected peer when `to` is null, consuming `buf`. */
final class Send[Owner](val fd: Int, buf: ByteBuffer, to: Address | Null) extends RingOp[Owner] with Repeatable {
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

/** A one-shot io_uring timeout of `nanoseconds`. */
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

/** `openat(2)` relative to the working directory, writing the new descriptor into `into`. */
final class OpenAt[Owner](val path: String, flags: Int, mode: Int, into: Slot[Integer]) extends RingOp[Owner] {
  private val cPath = {
    val utf8 = path.getBytes(StandardCharsets.UTF_8)
    java.util.Arrays.copyOf(utf8, utf8.length + 1) // NUL-terminated
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
      into.set(Integer.valueOf(res))
      true
    }

  override private[uring] def discard(res: Int): Unit = if res >= 0 then unistd.close(res)
}
