package asyncio.kqueue

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap
import scala.scalanative.posix.arpa.inet
import scala.scalanative.posix.fcntl
import scala.scalanative.posix.netinet.in
import scala.scalanative.posix.netinet.inOps.{*, given}
import scala.scalanative.posix.sys.socket
import scala.scalanative.posix.sys.socketOps.{*, given}
import scala.scalanative.posix.sys.un
import scala.scalanative.posix.sys.unOps.{*, given}
import scala.scalanative.posix.unistd
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import asyncio.Address
import asyncio.Handles
import asyncio.unsafe.NonBlocking
import asyncio.unsafe.PosixErr.cError
import asyncio.unsafe.PosixFileOps
import asyncio.unsafe.PosixSockets
import asyncio.unsafe.Sockets.Flavor
import asyncio.unsafe.Sockets.Transport

/** Kqueue's handles are POSIX file descriptors, opened non-blocking. Descriptor numbers are process-wide, so the Unix
  * paths bound by `listen` and `datagram` are remembered per descriptor and removed when it is closed.
  */
object KqueueHandles extends Handles[Int] {
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

  def listen(address: Address): Int =
    socketFor(address, Transport.Stream) { fd =>
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
      if remote != null then connectTo(fd, remote.nn) // assigns an IP socket an ephemeral local port
    }
  }

  def pipe(): (Int, Int) = {
    val ends = stackalloc[CInt](2)
    if unistd.pipe(ends) < 0 then throw new IOException(s"Failed to create pipe: ${cError()}")
    PosixSockets.setNonBlocking(ends(0))
    PosixSockets.setNonBlocking(ends(1))
    (ends(0), ends(1))
  }

  def readNow(handle: Int, buf: ByteBuffer): Int = NonBlocking.read(handle, buf)
  def writeNow(handle: Int, buf: ByteBuffer): Int = NonBlocking.write(handle, buf)

  def close(handle: Int): Unit = {
    val path = boundPaths.remove(handle) // before closing, while the number is still ours
    PosixSockets.close(handle)
    if path != null then Zone.acquire { implicit z => PosixFileOps.safeUnlink(toCString(path, StandardCharsets.UTF_8)) }
  }

  private def flavorOf(address: Address): Flavor = address match {
    case Address.Unix(_)    => Flavor.Unix
    case Address.IPv4(_, _) => Flavor.IPv4
    case Address.IPv6(_, _) => Flavor.IPv6
  }

  /** Opens a non-blocking socket and runs `setup` on it, closing it again if `setup` fails. */
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

  /** Builds the socket address for `address` and passes it to `use`, valid only during the call. */
  private[kqueue] def withSocketAddr[A](address: Address)(use: (Ptr[socket.sockaddr], socket.socklen_t) => A): A =
    address match {
      case Address.Unix(path)       => PosixSockets.withUnixSocketAddr(path)(use)
      case Address.IPv4(host, port) => PosixSockets.withIPv4SocketAddr(host, port)(use)
      case Address.IPv6(host, port) => PosixSockets.withIPv6SocketAddr(host, port)(use)
    }

  /** Decodes a socket address filled in by the kernel. An unnamed Unix socket has the empty path. */
  private[kqueue] def decode(addr: Ptr[socket.sockaddr], length: socket.socklen_t): Address = {
    val family = addr.sa_family.toInt
    if family == socket.AF_UNIX then {
      val unix = addr.asInstanceOf[Ptr[un.sockaddr_un]]
      val headerLength = sizeOf[un.sockaddr_un] - sizeOf[CArray[CChar, un._108]]
      if length.toInt <= headerLength.toInt then Address.Unix("")
      else Address.Unix(fromCString(unix.sun_path.at(0), StandardCharsets.UTF_8))
    } else {
      val text = stackalloc[Byte](64)
      if family == socket.AF_INET then {
        val v4 = addr.asInstanceOf[Ptr[in.sockaddr_in]]
        inet.inet_ntop(family, v4.at3.asInstanceOf[Ptr[Byte]], text, 64.toUInt)
        Address.IPv4(fromCString(text), inet.ntohs(v4.sin_port).toInt)
      } else if family == socket.AF_INET6 then {
        val v6 = addr.asInstanceOf[Ptr[in.sockaddr_in6]]
        inet.inet_ntop(family, v6.at4.asInstanceOf[Ptr[Byte]], text, 64.toUInt)
        Address.IPv6(fromCString(text), inet.ntohs(v6.sin6_port).toInt)
      } else throw new IOException(s"Unsupported address family $family")
    }
  }
}
