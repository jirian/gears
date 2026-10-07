package asyncio.uring

import java.io.IOException
import java.nio.ByteBuffer
import scala.scalanative.posix.errno
import scala.scalanative.posix.unistd
import scala.scalanative.unsigned.*

import asyncio.Address
import asyncio.Handles
import asyncio.kqueue.KqueueHandles
import asyncio.unsafe.PosixErr.cError

/** The uring reactor's handles: non-blocking POSIX descriptors, opened and closed the same way as kqueue's. Unlike
  * kqueue's, `readNow` and `writeNow` take any buffer: a heap buffer, a pointer buffer, or a read-only one for writing.
  */
object UringHandles extends Handles {
  def openFile(path: String, write: Boolean): Int = KqueueHandles.openFile(path, write)
  def connect(address: Address): Int = KqueueHandles.connect(address)
  def listen(address: Address, reuseAddr: Boolean = true): Int = KqueueHandles.listen(address, reuseAddr)
  def datagram(local: Address | Null, remote: Address | Null): Int = KqueueHandles.datagram(local, remote)
  def pipe(): (Int, Int) = KqueueHandles.pipe()
  def close(handle: Int): Unit = KqueueHandles.close(handle)

  private def wouldBlock: Boolean = {
    val err = errno.errno
    err == errno.EAGAIN || err == errno.EWOULDBLOCK || err == errno.EINTR
  }

  def readNow(handle: Int, buf: ByteBuffer): Int = {
    val region = new BufferRegion(buf)
    val read = unistd.read(handle, region.toFill(), buf.remaining().toCSize)
    if read < 0 then
      if !wouldBlock then throw new IOException(s"Failed to read from descriptor $handle: ${cError()}")
      0
    else if read == 0 then -1
    else {
      region.filled(read.toInt)
      read.toInt
    }
  }

  def writeNow(handle: Int, buf: ByteBuffer): Int = {
    val region = new BufferRegion(buf)
    val written = unistd.write(handle, region.toDrain(), buf.remaining().toCSize)
    if written < 0 then
      if !wouldBlock then throw new IOException(s"Failed to write to descriptor $handle: ${cError()}")
      -1
    else {
      region.drained(written.toInt)
      written.toInt
    }
  }
}
