package asyncio.kqueue

import java.nio.ByteBuffer
import scala.scalanative.posix.sys.socket
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import asyncio.Address
import asyncio.Interest
import asyncio.Slot
import asyncio.unsafe.NativeBuffer
import asyncio.unsafe.NonBlocking
import asyncio.unsafe.PosixSockets

/** The kqueue commands behind `KqueueOps`. `perform` returns true once the op is done, and false when the descriptor
  * was not ready after all, so the reactor waits again without completing it.
  */

/** Fills `buf` between position and limit. Done once at least one byte has been read, or at end of stream. */
final case class ReadIntoBuffer[Owner](fd: Int, buf: ByteBuffer) extends Command[Owner] {
  def interest: Interest = Interest.Read
  def perform(): Boolean = NonBlocking.read(fd, buf) != 0 // 0 is would-block; -1, EOF, leaves the buffer as it was
}

/** Fills buffers in order, completing once at least one byte was read or EOF was reached. */
final class ReadVector[Owner](val fd: Int, bufs: Array[ByteBuffer]) extends Command[Owner] {
  require(bufs.forall(buf => !buf.isReadOnly), "Read buffers must be writable")
  def interest: Interest = Interest.Read
  def perform(): Boolean = bufs.forall(buf => !buf.hasRemaining()) || NonBlocking.readv(fd, bufs) != 0
}

/** Drains `buf` between position and limit. Done once at least one byte has been written. */
final case class WriteFromBuffer[Owner](fd: Int, buf: ByteBuffer) extends Command[Owner] {
  def interest: Interest = Interest.Write
  def perform(): Boolean =
    if !buf.hasRemaining() then true
    else {
      val written = NonBlocking.write(fd, buf)
      if written > 0 then true
      else if written < 0 then false
      else throw new java.io.IOException(s"Write to descriptor $fd made no progress")
    }
}

/** Drains buffers in order, completing after a write makes progress or all buffers are empty. */
final class WriteVector[Owner](val fd: Int, bufs: Array[ByteBuffer]) extends Command[Owner] {
  def interest: Interest = Interest.Write
  def perform(): Boolean =
    if bufs.forall(buf => !buf.hasRemaining()) then true
    else {
      val written = NonBlocking.writev(fd, bufs)
      if written > 0 then true
      else if written < 0 then false
      else throw new java.io.IOException(s"Vector write to descriptor $fd made no progress")
    }
}

/** Accepts one connection on a listening socket, writing its descriptor into `into`. Done once a connection has been
  * accepted. The same command may be submitted again, after `into` is cleared, to accept the next connection.
  */
final case class Accept[Owner](fd: Int, into: Slot[Integer]) extends Command[Owner] {
  def interest: Interest = Interest.Read
  def perform(): Boolean = {
    val client = NonBlocking.accept(fd)
    if client >= 0 then into.set(client)
    client >= 0
  }
}

/** Waits for a non-blocking connect to finish and checks its outcome. Done once connected; throws if it failed. */
final case class Connect[Owner](fd: Int) extends Command[Owner] {
  def interest: Interest = Interest.Write
  def perform(): Boolean = {
    PosixSockets.checkConnect(fd)
    true
  }
}

/** Receives one packet and writes its sender into `from`. Done once a packet has arrived. */
final class ReceiveFrom[Owner](val fd: Int, buf: ByteBuffer, from: Slot[Address]) extends Command[Owner] {
  def interest: Interest = Interest.Read
  def perform(): Boolean = {
    val storage = stackalloc[socket.sockaddr_storage]()
    val length = stackalloc[socket.socklen_t]()
    !length = sizeOf[socket.sockaddr_storage].toUInt
    buf.clear()
    val address = storage.asInstanceOf[Ptr[socket.sockaddr]]
    val size = PosixSockets.receiveDatagram(fd, NativeBuffer.atPosition(buf), buf.remaining(), address, length)
    if size >= 0 then {
      NativeBuffer.advance(buf, size)
      buf.flip()
      from.set(KqueueHandles.decode(address, !length))
    }
    size >= 0
  }
}

/** Sends one packet, to `to` or to the connected peer when `to` is null, consuming `buf`. Done once it has gone. */
final class SendTo[Owner](val fd: Int, buf: ByteBuffer, to: Address | Null) extends Command[Owner] {
  def interest: Interest = Interest.Write
  def perform(): Boolean = {
    val size = buf.remaining()
    val sent =
      if to == null then PosixSockets.sendDatagram(fd, NativeBuffer.atPosition(buf), size)
      else
        KqueueHandles.withSocketAddr(to.nn) { (address, length) =>
          PosixSockets.sendDatagram(fd, NativeBuffer.atPosition(buf), size, address, length)
        }
    if sent then buf.position(buf.limit()) // a datagram goes whole or not at all
    sent
  }
}
