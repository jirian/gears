package asyncio.uring

import java.io.IOException
import java.nio.ByteBuffer
import scala.scalanative.posix.errno
import scala.scalanative.posix.sys.socket
import scala.scalanative.posix.unistd
import scala.scalanative.runtime.ByteArray
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import asyncio.Handle
import asyncio.Received
import asyncio.Repeatable
import asyncio.Slot
import asyncio.unsafe.NativeBuffer
import asyncio.unsafe.NativePool
import asyncio.unsafe.NonBlocking
import asyncio.unsafe.PosixSockets
import uring.*
import uringStructOps.*

/** The io_uring commands behind `UringOps`. Buffers are used in place, so they must be the kind the kernel can address
  * (`asyncio.unsafe.NativeBuffer`), and anything else the kernel reads after a submission, such as a datagram's
  * address, is kept by the command until its completion arrives.
  */

/** Fills `buf` between position and limit. Done once at least one byte has been read, or at end of stream. Bytes that
  * arrive for a submission cancelled meanwhile are kept in `buf` all the same, so a read that raced its cancel loses
  * nothing of the stream.
  */
final class ReadIntoBuffer(val fd: Int, buf: ByteBuffer) extends Command with Repeatable {
  override protected def readinessEvents: Int = Linux.POLLIN

  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit =
    io_uring_prep_read(sqe, fd, NativeBuffer.atPosition(buf), buf.remaining().toUInt, Ring.currentPosition)

  protected def finish(res: Int): Boolean =
    if Ring.transient(res) then false
    else if res < 0 then throw Ring.failure(s"Failed to read from descriptor $fd", res)
    else {
      NativeBuffer.advance(buf, res)
      true
    }

  override def tryAtOnce(): Boolean = NonBlocking.read(fd, buf) != 0 // -1 at EOF, which leaves the buffer as it was

  override protected def discard(res: Int): Unit = if res > 0 then NativeBuffer.advance(buf, res)
}

/** Fills a buffer of `pool`'s, writing it into `into` (see `Ops.read` with a pool): it waits for the descriptor to be
  * readable and then reads, on the loop thread, into a buffer taken when it is, and given back if it was not readable
  * after all, or if the read fails. Reading straight into a buffer taken at submission would hold the buffer for as
  * long as the read waits, and lose the bytes of a read that raced its cancel.
  */
final class PooledRead(val fd: Int, pool: NativePool, into: Slot[Received]) extends Command with Repeatable {
  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit = io_uring_prep_poll_add(sqe, fd, Linux.POLLIN.toUInt)

  protected def finish(res: Int): Boolean =
    if Ring.transient(res) then false
    else if res < 0 then throw Ring.failure(s"Failed to wait for descriptor $fd", res)
    else tryAtOnce()

  override def tryAtOnce(): Boolean = {
    val slot = pool.take()
    if slot == null then throw new IOException("The pool has no buffer free")
    val buffer = slot.nn.buffer
    val read =
      try NonBlocking.read(fd, buffer) // -1 at EOF, which leaves the buffer as it was
      catch {
        case failure: Throwable =>
          slot.nn.release() // a failed read holds nothing
          throw failure
      }
    if read == 0 then {
      slot.nn.release()
      false
    } else {
      buffer.flip()
      into.set(slot.nn)
      true
    }
  }
}

/** Drains `buf` between position and limit. Done once at least one byte has been written. It sends with `MSG_NOSIGNAL`,
  * so a peer that has gone fails the write instead of raising `SIGPIPE`, and turns to `write(2)` for good on a
  * descriptor that turns out not to be a socket. Bytes written by a submission cancelled meanwhile are consumed from
  * `buf` all the same.
  */
final class WriteFromBuffer(val fd: Int, buf: ByteBuffer) extends Command with Repeatable {
  private var plain = false
  override protected def readinessEvents: Int = Linux.POLLOUT

  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit = {
    val data = NativeBuffer.atPosition(buf)
    if plain then io_uring_prep_write(sqe, fd, data, buf.remaining().toUInt, Ring.currentPosition)
    else io_uring_prep_send(sqe, fd, data, buf.remaining().toCSize, socket.MSG_NOSIGNAL)
  }

  protected def finish(res: Int): Boolean =
    if res == -errno.ENOTSOCK && !plain then {
      plain = true
      false
    } else if Ring.transient(res) then false
    else if res < 0 then throw Ring.failure(s"Failed to write to descriptor $fd", res)
    else {
      NativeBuffer.advance(buf, res)
      true
    }

  override def tryAtOnce(): Boolean = {
    if !plain then {
      val sent = LinuxCalls.send(fd, NativeBuffer.atPosition(buf), buf.remaining().toCSize, socket.MSG_NOSIGNAL)
      if sent >= 0 then {
        NativeBuffer.advance(buf, sent.toInt)
        return true
      }
      if errno.errno == errno.ENOTSOCK then plain = true
      else if Ring.wouldBlock then return false
      else throw new IOException(s"Failed to write to descriptor $fd: ${asyncio.unsafe.PosixErr.cError()}")
    }
    NonBlocking.write(fd, buf) >= 0 || !buf.hasRemaining()
  }

  override protected def discard(res: Int): Unit = if res > 0 then NativeBuffer.advance(buf, res)
}

/** Accepts one connection on a listening socket, writing its descriptor, non-blocking and close-on-exec, into `into`.
  * Done once a connection has been accepted; one aborted before it was is skipped. The same command may be submitted
  * again, after `into` is cleared, to accept the next connection. A connection accepted for a submission cancelled
  * meanwhile is closed.
  */
final class Accept(val fd: Int, into: Handle.Slot) extends Command with Repeatable {
  override protected def readinessEvents: Int = Linux.POLLIN

  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit =
    io_uring_prep_accept(sqe, fd, null, null, Linux.SOCK_NONBLOCK | Linux.SOCK_CLOEXEC)

  protected def finish(res: Int): Boolean =
    if Ring.transient(res) || res == -errno.ECONNABORTED then false
    else if res < 0 then throw Ring.failure("Failed to accept connection", res)
    else {
      accepted(res)
      true
    }

  override def tryAtOnce(): Boolean = {
    val client = LinuxCalls.accept4(fd, null, null, Linux.SOCK_NONBLOCK | Linux.SOCK_CLOEXEC)
    if client >= 0 then {
      accepted(client)
      true
    } else if Ring.wouldBlock || errno.errno == errno.ECONNABORTED then false
    else throw new IOException(s"Failed to accept connection: ${asyncio.unsafe.PosixErr.cError()}")
  }

  private def accepted(client: Int): Unit = {
    UringHandles.opened(client)
    into.set(Handle.wrap(client))
  }

  override protected def discard(res: Int): Unit = if res >= 0 then unistd.close(res): Unit
}

/** Waits for a non-blocking connect to finish and checks its outcome. Done once connected; throws if it failed. */
final class Connect(val fd: Int) extends Command {
  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit = io_uring_prep_poll_add(sqe, fd, Linux.POLLOUT.toUInt)

  protected def finish(res: Int): Boolean =
    if Ring.transient(res) then false
    else if res < 0 then throw Ring.failure("Failed to wait for connection", res)
    else {
      PosixSockets.checkConnect(fd)
      true
    }
}

/** A `msghdr`, its one `iovec`, and room for a socket address, for `sendmsg` and `recvmsg`: arrays the command owns,
  * since the kernel reads them after the submitting call returns.
  */
private[uring] final class MessageHeader {
  private val header = new Array[Byte](sizeof[msghdr].toInt)
  private val vector = new Array[Byte](sizeof[iovec].toInt)
  private val name = new Array[Byte](sizeof[socket.sockaddr_storage].toInt)

  def msg: Ptr[msghdr] = header.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[msghdr]]
  def address: Ptr[socket.sockaddr] = name.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[socket.sockaddr]]
  def capacity: Int = name.length

  /** Points the header at `length` bytes at `data`, and at the first `nameLength` bytes of `address`, or no address. */
  def set(data: Ptr[Byte], length: Int, nameLength: Int): Unit = {
    val iov = vector.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[iovec]]
    iov.iov_base = data
    iov.iov_len = length.toCSize
    val m = msg
    m.msg_name = if nameLength > 0 then address.asInstanceOf[Ptr[Byte]] else null
    m.msg_namelen = nameLength.toUInt
    m.msg_iov = iov
    m.msg_iovlen = 1.toCSize
    m.msg_control = null
    m.msg_controllen = 0.toCSize
    m.msg_flags = 0
  }
}

/** Receives one packet and writes its sender into `from`, which holds a Unix address if `unix`, and an IP one
  * otherwise. Done once a packet has arrived; one larger than `buf` is consumed and fails the op. A packet that arrives
  * for a submission cancelled meanwhile is dropped.
  */
final class ReceiveFrom(val fd: Int, buf: ByteBuffer, from: Slot[? <: SocketAddress], unix: Boolean)
    extends Command
    with Repeatable {
  override protected def readinessEvents: Int = Linux.POLLIN
  private val header = new MessageHeader

  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit = {
    buf.clear()
    require(buf.capacity() > 0, "Datagram buffer capacity must be positive")
    header.set(NativeBuffer.atPosition(buf), buf.remaining(), header.capacity)
    io_uring_prep_recvmsg(sqe, fd, header.msg, socket.MSG_TRUNC) // a packet's whole size, so a long one is noticed
  }

  protected def finish(res: Int): Boolean =
    if Ring.transient(res) then false
    else if res < 0 then throw Ring.failure("Failed to receive datagram", res)
    else if res > buf.remaining() then
      throw new IOException(s"Datagram exceeds buffer capacity of ${buf.remaining()} bytes")
    else {
      NativeBuffer.advance(buf, res)
      buf.flip()
      val sender = UringHandles.decode(header.address, header.msg.msg_namelen)
      if sender.isInstanceOf[asyncio.UnixAddress] != unix then
        throw new IOException(s"Received from $sender on a socket of the other family")
      from.asInstanceOf[Slot[SocketAddress]].set(sender)
      true
    }
}

/** Sends one packet, to `to` or to the connected peer when `to` is null, consuming `buf`. Done once it has gone. */
final class SendTo(val fd: Int, buf: ByteBuffer, to: SocketAddress | Null) extends Command with Repeatable {
  override protected def readinessEvents: Int = Linux.POLLOUT
  private val header = new MessageHeader

  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit = {
    val nameLength = if to == null then 0 else UringHandles.encode(to, header.address, header.capacity)
    header.set(NativeBuffer.atPosition(buf), buf.remaining(), nameLength)
    io_uring_prep_sendmsg(sqe, fd, header.msg, socket.MSG_NOSIGNAL)
  }

  protected def finish(res: Int): Boolean =
    if Ring.transient(res) then false
    else if res < 0 then throw Ring.failure("Failed to send datagram", res)
    else if res != buf.remaining() then
      throw new IOException(s"Incomplete datagram: sent $res of ${buf.remaining()} bytes")
    else {
      buf.position(buf.limit()) // a datagram goes whole or not at all
      true
    }
}
