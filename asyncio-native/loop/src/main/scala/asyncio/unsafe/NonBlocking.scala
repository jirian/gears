package asyncio.unsafe

import java.io.IOException
import java.nio.ByteBuffer
import scala.scalanative.posix.errno
import scala.scalanative.posix.sys.socket
import scala.scalanative.posix.sys.uio
import scala.scalanative.posix.sys.uioOps.{*, given}
import scala.scalanative.unsafe.*
import scala.scalanative.posix.unistd
import scala.scalanative.unsigned.*

import PosixErr.cError

/** Non-blocking reads, writes, and accepts that report would-block and EOF as values instead of exceptions. */
object NonBlocking {

  private def withIovecs[A](buffers: Array[ByteBuffer])(body: Ptr[uio.iovec] => A): A = {
    require(buffers.length <= 1024, "A vector I/O operation may contain at most 1024 buffers")
    Zone.acquire { implicit z =>
      val vectors = alloc[uio.iovec](math.max(1, buffers.length))
      var i = 0
      while i < buffers.length do {
        val buf = buffers(i)
        vectors(i).iov_base = NativeBuffer.nativeAtPosition(buf)
        vectors(i).iov_len = buf.remaining().toCSize
        i += 1
      }
      body(vectors)
    }
  }

  private def advance(buffers: Array[ByteBuffer], count: Int): Unit = {
    var left = count
    var i = 0
    while i < buffers.length && left > 0 do {
      val buf = buffers(i)
      val moved = math.min(left, buf.remaining())
      NativeBuffer.advance(buf, moved)
      left -= moved
      i += 1
    }
  }

  /** Vector read with the same return values as `read`: 0 would block, -1 is EOF. */
  def readv(fd: Int, buffers: Array[ByteBuffer]): Int = withIovecs(buffers) { vectors =>
    if buffers.isEmpty then 0
    else {
      val count = uio.readv(fd, vectors, buffers.length)
      if count < 0 then
        if !wouldBlock then throw new IOException(s"Failed to read from descriptor $fd: ${cError()}")
        else 0
      else if count == 0 then EOF
      else {
        advance(buffers, count.toInt)
        count.toInt
      }
    }
  }

  /** Vector write with the same return values as `write`: -1 would block. */
  def writev(fd: Int, buffers: Array[ByteBuffer]): Int = withIovecs(buffers) { vectors =>
    if buffers.isEmpty then 0
    else {
      val count = uio.writev(fd, vectors, buffers.length)
      if count < 0 then {
        if !wouldBlock then throw new IOException(s"Failed to write to descriptor $fd: ${cError()}")
        -1
      } else {
        advance(buffers, count.toInt)
        count.toInt
      }
    }
  }

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
