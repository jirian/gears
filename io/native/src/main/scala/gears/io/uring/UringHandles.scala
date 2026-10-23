package gears.io.uring

import gears.io.reactor.Address
import gears.io.reactor.Handles
import gears.io.unsafe.PosixErr.cError
import gears.io.unsafe.PosixFileOps
import gears.io.unsafe.PosixSockets
import gears.io.unsafe.Sockets.Flavor
import gears.io.unsafe.Sockets.Transport

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap
import scala.scalanative.posix.errno
import scala.scalanative.posix.fcntl
import scala.scalanative.posix.sys.socket
import scala.scalanative.posix.unistd
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

/** The `Handles` of the uring reactor: handles are POSIX descriptors, opened non-blocking and close-on-exec. `readNow`
  * and `writeNow` take heap buffers as well as pointer buffers. A Unix socket path bound by `listen` or `datagram` is
  * remembered, so `close` can remove it.
  */
object UringHandles extends Handles {
  // Keyed by descriptor number. A number is only reused after its descriptor was closed, so whoever gets a new
  // descriptor drops the entry for its number, which can only be left over from a close that bypassed `close`.
  private val boundPaths = new ConcurrentHashMap[Integer, String]()

  /** Forgets a path left over under the number of `fd`, which was just opened.
    *
    * @param fd
    *   the new descriptor
    */
  private[uring] def opened(fd: Int): Unit = boundPaths.remove(fd)

  def openFile(path: String, write: Boolean): Int = {
    val flags = (if write then fcntl.O_WRONLY else fcntl.O_RDONLY) | fcntl.O_NONBLOCK | LinuxConstants.O_CLOEXEC
    val fd = Zone.acquire { implicit z => fcntl.open(toCString(path), flags) }
    if fd < 0 then throw new IOException(s"Failed to open file: ${cError()}")
    opened(fd)
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
    if linux.pipe2(ends, fcntl.O_NONBLOCK | LinuxConstants.O_CLOEXEC) < 0 then
      throw new IOException(s"Failed to create pipe: ${cError()}")
    opened(ends(0))
    opened(ends(1))
    (ends(0), ends(1))
  }

  /** Closes `handle`, first removing the Unix socket path it was bound to, so that the removal cannot hit a newer
    * socket bound to the same path after the close.
    *
    * @param handle
    *   the descriptor to close
    */
  def close(handle: Int): Unit = {
    val path = boundPaths.remove(handle)
    try
      if path != null then
        Zone.acquire { implicit z => PosixFileOps.safeUnlink(toCString(path, StandardCharsets.UTF_8)) }
    finally PosixSockets.close(handle)
  }

  private def flavorOf(address: Address): Flavor = address match {
    case Address.Unix(_)    => Flavor.Unix
    case Address.IPv4(_, _) => Flavor.IPv4
    case Address.IPv6(_, _) => Flavor.IPv6
  }

  private def socketFor(address: Address, transport: Transport)(setup: Int => Unit): Int = {
    val family = flavorOf(address) match {
      case Flavor.Unix => socket.AF_UNIX
      case Flavor.IPv4 => socket.AF_INET
      case Flavor.IPv6 => socket.AF_INET6
    }
    val kind = if transport == Transport.Stream then socket.SOCK_STREAM else socket.SOCK_DGRAM
    // Non-blocking and close-on-exec from the start, so a concurrent fork cannot inherit it.
    val fd = socket.socket(family, kind | LinuxConstants.SOCK_NONBLOCK | LinuxConstants.SOCK_CLOEXEC, 0)
    if fd < 0 then throw new IOException(s"Failed to open socket: ${cError()}")
    opened(fd)
    try {
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

@extern
private object linux {
  def pipe2(fds: Ptr[CInt], flags: CInt): CInt = extern
}
