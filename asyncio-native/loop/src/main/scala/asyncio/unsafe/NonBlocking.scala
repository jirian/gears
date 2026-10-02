package asyncio.unsafe

import java.io.IOException
import java.nio.ByteBuffer
import scala.scalanative.posix.errno
import scala.scalanative.posix.sys.socket
import scala.scalanative.posix.unistd
import scala.scalanative.unsigned.*

import PosixErr.cError

/** Non-blocking reads, writes, and accepts that report would-block and EOF as values instead of exceptions. */
object NonBlocking {

  private def wouldBlock: Boolean =
    val err = errno.errno
    err == errno.EAGAIN || err == errno.EWOULDBLOCK || err == errno.EINTR

  inline val EOF = -1

  /** Accepts a new connection on `serverFd` in non-blocking mode. Returns the client file descriptor, or -1 if no
    * client was accepted.
    */
  def accept(serverFd: Int): Int = {
    val clientFd = socket.accept(serverFd, null, null)
    if clientFd < 0 then {
      val transient = wouldBlock
      if !transient then throw new IOException(s"Failed to accept connection: ${cError()}")
      -1 // no client was accepted
    } else {
      PosixSockets.setNonBlocking(clientFd)
      clientFd
    }
  }

  /** Fills `buf` from `fd` between position and limit. Returns the number of bytes read, 0 if the read would block, or
    * -1 at EOF.
    */
  def read(fd: Int, buf: ByteBuffer): Int = {
    val read = unistd.read(fd, NativeBuffer.atPosition(buf), buf.remaining().toCSize)
    if read < 0 then
      if !wouldBlock then throw new IOException(s"Failed to read from descriptor $fd: ${cError()}")
      0
    else if read == 0 then EOF
    else
      NativeBuffer.advance(buf, read.toInt)
      read.toInt
  }

  /** Drains `buf` into `fd` between position and limit. Returns the number of bytes written, or -1 if the write would
    * block.
    */
  def write(fd: Int, buf: ByteBuffer): Int = {
    val written = unistd.write(fd, NativeBuffer.atPosition(buf), buf.remaining().toCSize)
    if written < 0 then {
      if !wouldBlock then throw new IOException(s"Failed to write to descriptor $fd: ${cError()}")
      -1
    } else {
      NativeBuffer.advance(buf, written.toInt)
      written.toInt
    }
  }
}
