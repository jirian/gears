package asyncio.unsafe

import java.io.IOException
import java.nio.charset.StandardCharsets
import scala.scalanative.annotation.alwaysinline
import scala.scalanative.posix.arpa.inet
import scala.scalanative.posix.errno
import scala.scalanative.posix.fcntl
import scala.scalanative.posix.netinet.in
import scala.scalanative.posix.netinet.inOps.{*, given}
import scala.scalanative.posix.string
import scala.scalanative.posix.sys.socket
import scala.scalanative.posix.sys.socketOps.{*, given}
import scala.scalanative.posix.sys.uio
import scala.scalanative.posix.sys.uioOps.{*, given}
import scala.scalanative.posix.sys.un
import scala.scalanative.posix.sys.unOps.{*, given}
import scala.scalanative.posix.unistd
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import Sockets.Flavor
import Sockets.Transport
import PosixErr.cError

object PosixSockets {
  def open(flavor: Flavor, transport: Transport): Int = {
    val posixFlavor = flavor match {
      case Flavor.Unix => socket.AF_UNIX
      case Flavor.IPv4 => socket.AF_INET
      case Flavor.IPv6 => socket.AF_INET6
    }
    val posixTransport = transport match {
      case Transport.Stream   => socket.SOCK_STREAM
      case Transport.Datagram => socket.SOCK_DGRAM
    }
    val fd = socket.socket(posixFlavor, posixTransport, 0)
    if (fd < 0) {
      throw new IOException(
        s"Failed to open socket: ${cError()}"
      )
    }
    fd
  }

  def setNonBlocking(fd: Int): Unit = {
    val flags = fcntl.fcntl(fd, fcntl.F_GETFL, 0)
    if (flags < 0 || fcntl.fcntl(fd, fcntl.F_SETFL, flags | fcntl.O_NONBLOCK) < 0) {
      throw new IOException(
        s"Failed to set socket descriptor to non-blocking: ${cError()}"
      )
    }
  }

  def maxConnections: Int = socket.SOMAXCONN

  @alwaysinline
  private def unixSocketAddrLen(pathLength: Int): socket.socklen_t =
    (sizeOf[socket.sa_family_t].toInt + pathLength + 1).toUInt

  def bindUnixAddr(fd: Int, sock: String, removeExisting: Boolean): Unit = {
    Sockets.checkUnixPathLength(sock)
    Zone.acquire { implicit z =>
      val path = toCString(sock, StandardCharsets.UTF_8)
      if (removeExisting) {
        PosixFileOps.safeUnlink(path) // Ensure the socket path is clean before binding
      }
      val pathLength = string.strlen(path).toInt
      val addr = createUnixSocketAddr(path, pathLength)
      val err = socket.bind(fd, addr, unixSocketAddrLen(pathLength))
      if (err < 0) {
        throw new IOException(
          s"Failed to bind socket: ${cError()}"
        )
      }
    }
  }

  /** Lets a listening socket bind a port that still has connections in `TIME_WAIT`, such as those a server closed just
    * before it was stopped. Without it, restarting the server fails with `EADDRINUSE` for up to a minute. Call it
    * before `bind`.
    */
  def setReuseAddress(fd: Int): Unit = {
    val on = stackalloc[CInt]()
    !on = 1
    if (
      socket.setsockopt(fd, socket.SOL_SOCKET, socket.SO_REUSEADDR, on.asInstanceOf[Ptr[Byte]], sizeOf[CInt].toUInt) < 0
    )
      throw new IOException(s"Failed to set SO_REUSEADDR: ${cError()}")
  }

  def listen(fd: Int, backlog: Int): Unit = {
    if (socket.listen(fd, backlog) < 0) {
      throw new IOException(
        s"Failed to listen on socket: ${cError()}"
      )
    }
  }

  def connectUnixAddr(fd: Int, sock: String): Unit = {
    Sockets.checkUnixPathLength(sock)
    Zone.acquire { implicit z =>
      val path = toCString(sock, StandardCharsets.UTF_8)
      val pathLength = string.strlen(path).toInt
      val addr = createUnixSocketAddr(path, pathLength)
      val err = socket.connect(fd, addr, unixSocketAddrLen(pathLength))
      if (err < 0 && errno.errno != errno.EINPROGRESS) {
        throw new IOException(s"Failed to connect socket: ${cError()}")
      }
    }
  }

  /** Bind a numeric IPv4 address. Port zero asks the OS to choose a free port. */
  def bindIPv4Addr(fd: Int, host: String, port: Int): Unit = {
    Zone.acquire { implicit z =>
      bindAddr(fd, createIPv4SocketAddr(host, port), sizeOf[in.sockaddr_in].toUInt)
    }
  }

  /** Bind a numeric IPv6 address (without brackets or a scope suffix). */
  def bindIPv6Addr(fd: Int, host: String, port: Int): Unit = {
    Zone.acquire { implicit z =>
      bindAddr(fd, createIPv6SocketAddr(host, port), sizeOf[in.sockaddr_in6].toUInt)
    }
  }

  def connectIPv4Addr(fd: Int, host: String, port: Int): Unit = {
    Zone.acquire { implicit z =>
      connectAddr(fd, createIPv4SocketAddr(host, port), sizeOf[in.sockaddr_in].toUInt)
    }
  }

  def connectIPv6Addr(fd: Int, host: String, port: Int): Unit = {
    Zone.acquire { implicit z =>
      connectAddr(fd, createIPv6SocketAddr(host, port), sizeOf[in.sockaddr_in6].toUInt)
    }
  }

  private def bindAddr(fd: Int, addr: Ptr[socket.sockaddr], length: socket.socklen_t): Unit = {
    if (socket.bind(fd, addr, length) < 0) {
      throw new IOException(s"Failed to bind socket: ${cError()}")
    }
  }

  private def connectAddr(fd: Int, addr: Ptr[socket.sockaddr], length: socket.socklen_t): Unit = {
    if (socket.connect(fd, addr, length) < 0 && errno.errno != errno.EINPROGRESS) {
      throw new IOException(s"Failed to connect socket: ${cError()}")
    }
  }

  /** Check SO_ERROR after a non-blocking connection becomes writable. */
  def checkConnect(fd: Int): Unit = {
    val error = stackalloc[CInt]()
    val length = stackalloc[socket.socklen_t]()
    !length = sizeOf[CInt].toUInt
    if (socket.getsockopt(fd, socket.SOL_SOCKET, socket.SO_ERROR, error, length) < 0) {
      throw new IOException(s"Failed to check socket connection: ${cError()}")
    }
    if (!error != 0) {
      throw new IOException(s"Failed to connect socket: ${fromCString(string.strerror(!error))}")
    }
  }

  private def checkPort(port: Int): Unit = {
    require(port >= 0 && port <= 65535, s"Port must be between 0 and 65535: $port")
  }

  private def parseAddress(family: Int, host: String, dest: Ptr[Byte]): Unit = {
    require(!host.contains('\u0000'), "IP address must not contain a NUL character")
    Zone.acquire { implicit z =>
      val result = inet.inet_pton(family, toCString(host), dest)
      if (result == 0) throw new IOException(s"Invalid numeric IP address: $host")
      if (result < 0) throw new IOException(s"Failed to parse IP address: ${cError()}")
    }
  }

  private def createIPv4SocketAddr(host: String, port: Int)(implicit z: Zone): Ptr[socket.sockaddr] = {
    checkPort(port)
    val addr = alloc[in.sockaddr_in]()
    addr.sin_len = sizeOf[in.sockaddr_in].toUByte
    addr.sin_family = socket.AF_INET.toUShort
    addr.sin_port = inet.htons(port.toUShort)
    parseAddress(socket.AF_INET, host, addr.at3.asInstanceOf[Ptr[Byte]])
    addr.asInstanceOf[Ptr[socket.sockaddr]]
  }

  private def createIPv6SocketAddr(host: String, port: Int)(implicit z: Zone): Ptr[socket.sockaddr] = {
    checkPort(port)
    val addr = alloc[in.sockaddr_in6]()
    addr.sin6_len = sizeOf[in.sockaddr_in6].toUByte
    addr.sin6_family = socket.AF_INET6.toUShort
    addr.sin6_port = inet.htons(port.toUShort)
    parseAddress(socket.AF_INET6, host, addr.at4.asInstanceOf[Ptr[Byte]])
    addr.asInstanceOf[Ptr[socket.sockaddr]]
  }

  /** Accept a stream socket connection of any address family.
    *
    * If the socket is non-blocking, it will throw if a connection is un-available. Use within an event loop to be
    * notified when a connection is available.
    */
  def accept(fd: Int): Int = {
    val clientFd = socket.accept(fd, null, null)
    if (clientFd < 0) {
      if (errno.errno == errno.EAGAIN || errno.errno == errno.EWOULDBLOCK)
        // TODO: perhaps between the event and calling accept the socket was closed?
        // So test this.
        throw new IOException("No pending connections to accept")
      else
        throw new IOException(s"Failed to accept connection: ${cError()}")
    }
    clientFd
  }

  def acceptUnix(fd: Int): Int = accept(fd)

  /** Receive one packet and its sender. Returns -1 if a non-blocking socket has no packet ready; zero is a valid empty
    * datagram, not EOF. Initialize senderLength to the capacity of sender before each call. A packet larger than
    * capacity is consumed and reported as an error.
    */
  def receiveDatagram(
      fd: Int,
      buffer: Ptr[Byte],
      capacity: Int,
      sender: Ptr[socket.sockaddr],
      senderLength: Ptr[socket.socklen_t]
  ): Int = {
    require(capacity > 0, "Datagram buffer capacity must be positive")
    val iov = stackalloc[uio.iovec]()
    iov.iov_base = buffer
    iov.iov_len = capacity.toCSize
    val msg = stackalloc[socket.msghdr]()
    msg.msg_name = sender.asInstanceOf[Ptr[Byte]]
    msg.msg_namelen = !senderLength
    msg.msg_iov = iov
    msg.msg_iovlen = 1
    msg.msg_control = null
    msg.msg_controllen = 0.toUInt
    msg.msg_flags = 0
    var read = socket.recvmsg(fd, msg, 0)
    while (read < 0 && errno.errno == errno.EINTR) {
      read = socket.recvmsg(fd, msg, 0)
    }
    if (read < 0) {
      if (errno.errno == errno.EAGAIN || errno.errno == errno.EWOULDBLOCK) -1
      else throw new IOException(s"Failed to receive datagram: ${cError()}")
    } else {
      if ((msg.msg_flags & socket.MSG_TRUNC) != 0) {
        throw new IOException(s"Datagram exceeds buffer capacity of $capacity bytes")
      }
      !senderLength = msg.msg_namelen
      read.toInt
    }
  }

  /** Send exactly one packet, returning false if the socket would block. A null destination sends to the peer of a
    * connected datagram socket.
    */
  def sendDatagram(
      fd: Int,
      buffer: Ptr[Byte],
      length: Int,
      destination: Ptr[socket.sockaddr] = null,
      destinationLength: socket.socklen_t = 0.toUInt
  ): Boolean = {
    require(length >= 0, "Datagram length must be non-negative")
    var written = socket.sendto(fd, buffer, length.toCSize, 0, destination, destinationLength)
    while (written < 0 && errno.errno == errno.EINTR) {
      written = socket.sendto(fd, buffer, length.toCSize, 0, destination, destinationLength)
    }
    if (written < 0) {
      if (errno.errno == errno.EAGAIN || errno.errno == errno.EWOULDBLOCK) false
      else throw new IOException(s"Failed to send datagram: ${cError()}")
    } else {
      if (written.toInt != length) throw new IOException(s"Incomplete datagram: sent $written of $length bytes")
      true
    }
  }

  @alwaysinline
  private def createUnixSocketAddr(path: CString, length: Int)(implicit z: Zone): Ptr[socket.sockaddr] = {
    val addr = alloc[un.sockaddr_un]()
    addr.sun_len = unixSocketAddrLen(length).toUByte
    addr.sun_family = socket.AF_UNIX.toUShort
    string.memcpy(addr.sun_path.at(0), path, length.toCSize)
    addr.sun_path(length) = 0.toByte // null-terminate the path
    addr.asInstanceOf[Ptr[socket.sockaddr]]
  }

  /** Builds a Unix socket address for `path` and passes it to `use`, valid only during the call. */
  def withUnixSocketAddr[A](path: String)(use: (Ptr[socket.sockaddr], socket.socklen_t) => A): A = {
    Sockets.checkUnixPathLength(path)
    Zone.acquire { implicit z =>
      val cPath = toCString(path, StandardCharsets.UTF_8)
      val length = string.strlen(cPath).toInt
      use(createUnixSocketAddr(cPath, length), unixSocketAddrLen(length))
    }
  }

  /** Builds an IPv4 socket address and passes it to `use`, valid only during the call. */
  def withIPv4SocketAddr[A](host: String, port: Int)(use: (Ptr[socket.sockaddr], socket.socklen_t) => A): A =
    Zone.acquire { implicit z => use(createIPv4SocketAddr(host, port), sizeOf[in.sockaddr_in].toUInt) }

  /** Builds an IPv6 socket address and passes it to `use`, valid only during the call. */
  def withIPv6SocketAddr[A](host: String, port: Int)(use: (Ptr[socket.sockaddr], socket.socklen_t) => A): A =
    Zone.acquire { implicit z => use(createIPv6SocketAddr(host, port), sizeOf[in.sockaddr_in6].toUInt) }

  def close(fd: Int): Unit = {
    if (unistd.close(fd) < 0) {
      throw new IOException(
        s"Failed to close socket: ${cError()}"
      )
    }
  }

  def maxPathLength: Int = {
    // Scala Native reserves 108 bytes, but Darwin's sun_path holds only 104.
    if (scala.scalanative.meta.LinktimeInfo.isMac) 104
    else sizeOf[CArray[CChar, un._108]]
  }
}
