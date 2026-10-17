package gears.asyncio.uring

import gears.asyncio.Address
import gears.asyncio.Handles
import gears.asyncio.unsafe.PosixErr.cError
import gears.asyncio.unsafe.PosixFileOps
import gears.asyncio.unsafe.PosixSockets
import gears.asyncio.unsafe.Sockets.Flavor
import gears.asyncio.unsafe.Sockets.Transport

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap
import scala.scalanative.posix.errno
import scala.scalanative.posix.fcntl
import scala.scalanative.posix.unistd
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

object UringHandles extends Handles {
  private val boundPaths = new ConcurrentHashMap[Integer, String]()

  def openFile(path: String, write: Boolean): Int = {
    val fd = Zone.acquire { implicit z =>
      fcntl.open(toCString(path), (if write then fcntl.O_WRONLY else fcntl.O_RDONLY) | fcntl.O_NONBLOCK)
    }
    if fd < 0 then throw new IOException(s"Failed to open file: ${cError()}")
    fd
  }

  def connect(address: Address): Int =
    socketFor(address, Transport.Stream)(fd => connectTo(fd, address))

  def listen(address: Address, reuseAddr: Boolean = true): Int =
    socketFor(address, Transport.Stream) { fd =>
      if reuseAddr && !address.isInstanceOf[Address.Unix] then PosixSockets.setReuseAddress(fd)
      bindTo(fd, address)
      PosixSockets.listen(fd, PosixSockets.maxConnections)
    }

  def datagram(local: Address | Null, remote: Address | Null): Int = {
    val either = if local != null then local.nn else remote
    require(either != null, "A datagram socket needs a local or a remote address")
    require(
      local == null || remote == null || flavorOf(local.nn) == flavorOf(remote.nn),
      "Local and remote addresses must be of the same kind"
    )
    socketFor(either.nn, Transport.Datagram) { fd =>
      if local != null then bindTo(fd, local.nn)
      if remote != null then connectTo(fd, remote.nn)
    }
  }

  def pipe(): (Int, Int) = {
    val ends = stackalloc[CInt](2)
    if unistd.pipe(ends) < 0 then throw new IOException(s"Failed to create pipe: ${cError()}")
    PosixSockets.setNonBlocking(ends(0))
    PosixSockets.setNonBlocking(ends(1))
    (ends(0), ends(1))
  }

  def close(handle: Int): Unit = {
    val path = boundPaths.remove(handle)
    PosixSockets.close(handle)
    if path != null then Zone.acquire { implicit z => PosixFileOps.safeUnlink(toCString(path, StandardCharsets.UTF_8)) }
  }

  private def flavorOf(address: Address): Flavor = address match {
    case Address.Unix(_)    => Flavor.Unix
    case Address.IPv4(_, _) => Flavor.IPv4
    case Address.IPv6(_, _) => Flavor.IPv6
  }

  private def socketFor(address: Address, transport: Transport)(setup: Int => Unit): Int = {
    val fd = PosixSockets.open(flavorOf(address), transport)
    try {
      PosixSockets.setNonBlocking(fd)
      setup(fd)
      fd
    } catch {
      case t: Throwable =>
        close(fd)
        throw t
    }
  }

  private def bindTo(fd: Int, address: Address): Unit = address match {
    case Address.Unix(path) =>
      PosixSockets.bindUnixAddr(fd, path, removeExisting = true)
      boundPaths.put(fd, path)
    case Address.IPv4(host, port) => PosixSockets.bindIPv4Addr(fd, host, port)
    case Address.IPv6(host, port) => PosixSockets.bindIPv6Addr(fd, host, port)
  }

  private def connectTo(fd: Int, address: Address): Unit = address match {
    case Address.Unix(path)       => PosixSockets.connectUnixAddr(fd, path)
    case Address.IPv4(host, port) => PosixSockets.connectIPv4Addr(fd, host, port)
    case Address.IPv6(host, port) => PosixSockets.connectIPv6Addr(fd, host, port)
  }

  private def wouldBlock: Boolean = {
    val err = errno.errno
    err == errno.EAGAIN || err == errno.EWOULDBLOCK || err == errno.EINTR
  }

  def readNow(handle: Int, buf: ByteBuffer): Int = {
    if buf.isReadOnly() then throw new java.nio.ReadOnlyBufferException()
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
