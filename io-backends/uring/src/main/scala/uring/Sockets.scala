package gears.async.io
package uring

import java.io.IOException
import java.nio.charset.StandardCharsets
import scala.scalanative.libc.string
import scala.scalanative.posix.errno
import scala.scalanative.posix.sys.socket
import scala.scalanative.posix.sys.stat
import scala.scalanative.posix.sys.statOps.*
import scala.scalanative.posix.unistd
import scala.scalanative.runtime.ByteArray
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

/** Sockets on Linux, for the io_uring backend: opened non-blocking and close-on-exec, their addresses written into and
  * read out of `sockaddr`s byte by byte, as Linux lays them out (the family in host order, the port and address in
  * network order, an IPv6 scope in host order), and their options. An IPv6 socket is dual stack, and an IPv4 peer of it
  * is reported by its IPv4 address, as the interface has it.
  */
private[uring] object Sockets {

  /** Room for any socket address. */
  inline val StorageSize = 128

  private inline val AF_UNIX = 1
  private inline val AF_INET = 2
  private inline val AF_INET6 = 10
  private inline val UnixPathMax = 108

  inline val IPPROTO_IP = 0
  inline val IPPROTO_TCP = 6
  inline val IPPROTO_IPV6 = 41
  inline val TCP_NODELAY = 1
  inline val TCP_KEEPIDLE = 4
  inline val TCP_KEEPINTVL = 5
  inline val TCP_KEEPCNT = 6
  inline val IP_MTU_DISCOVER = 10
  inline val IP_PMTUDISC_DONT = 0
  inline val IP_PMTUDISC_DO = 2
  inline val IPV6_V6ONLY = 26
  inline val IPV6_DONTFRAG = 62
  inline val SO_REUSEPORT = 15
  inline val SO_REUSEADDR = 2
  inline val Backlog = 4096

  def cError(): String = fromCString(string.strerror(errno.errno))

  /** A socket address's room, for a call to fill or read: an array, which Scala Native never moves. */
  final class Storage {
    val bytes = new Array[Byte](StorageSize)
    def at: Ptr[socket.sockaddr] = bytes.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[socket.sockaddr]]
    private val lengthBytes = new Array[Byte](4)
    def lengthAt: Ptr[socket.socklen_t] = lengthBytes.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[socket.socklen_t]]
    def length: Int = (!lengthAt).toInt
    def length_=(n: Int): Unit = !lengthAt = n.toUInt
  }

  /** Opens a socket of `transport` (`SOCK_STREAM`, `SOCK_DGRAM`) for `address`'s family: an IPv6 one dual stack. */
  def open(address: IpSocketAddress | UnixAddress, transport: Int): Int = {
    val family = address match {
      case ip: IpSocketAddress => if ip.address.protocol == Protocol.IPv4 then AF_INET else AF_INET6
      case _: UnixAddress      => AF_UNIX
    }
    val fd = socket.socket(family, transport | Linux.SOCK_NONBLOCK | Linux.SOCK_CLOEXEC, 0)
    if fd < 0 then throw new IOException(s"Failed to open a socket: ${cError()}")
    if family == AF_INET6 then
      try setInt(fd, IPPROTO_IPV6, IPV6_V6ONLY, 0, "IPV6_V6ONLY")
      catch {
        case e: Throwable =>
          unistd.close(fd)
          throw e
      }
    SigPipe.ignored: Unit // a send once the peer has gone fails with EPIPE, not the signal
    fd
  }

  /** Writes `address` into `into`, setting its length. */
  def write(address: IpSocketAddress | UnixAddress, into: Storage): Unit = into.length = write(address, into.bytes)

  /** Writes `address` into `b`, at least `StorageSize` bytes, and returns its length. */
  def write(address: IpSocketAddress | UnixAddress, b: Array[Byte]): Int = {
    java.util.Arrays.fill(b, 0.toByte)
    address match {
      case IpSocketAddress(ip, port) =>
        val octets = ip.bytes
        b(2) = (port >>> 8).toByte
        b(3) = port.toByte
        if ip.protocol == Protocol.IPv4 then {
          family(b, AF_INET)
          var i = 0
          while i < 4 do { b(4 + i) = octets(i); i += 1 }
          16
        } else {
          family(b, AF_INET6)
          var i = 0
          while i < 16 do { b(8 + i) = octets(i); i += 1 }
          val scope = ip.scopeId
          b(24) = scope.toByte
          b(25) = (scope >>> 8).toByte
          b(26) = (scope >>> 16).toByte
          b(27) = (scope >>> 24).toByte
          28
        }
      case UnixAddress(path) =>
        val bytes = path.getBytes(StandardCharsets.UTF_8)
        checkPath(path, bytes)
        family(b, AF_UNIX)
        System.arraycopy(bytes, 0, b, 2, bytes.length)
        2 + bytes.length + 1
    }
  }

  /** Refuses a Unix path an op cannot use, as it is built: empty, with a NUL, or too long for a socket address. */
  def checkPath(path: String, bytes: Array[Byte] = null): Unit = {
    if path.isEmpty then throw new IllegalArgumentException("A Unix socket address is not the empty path")
    if path.indexOf('\u0000') >= 0 then throw new IllegalArgumentException(s"A path has no NUL: $path")
    val length = if bytes == null then path.getBytes(StandardCharsets.UTF_8).length else bytes.length
    if length >= UnixPathMax then
      throw new IllegalArgumentException(s"A Unix socket path is shorter than $UnixPathMax bytes: $path")
  }

  private def family(b: Array[Byte], f: Int): Unit = {
    b(0) = f.toByte
    b(1) = (f >>> 8).toByte
  }

  /** The address in `from`, `length` bytes of it. */
  def read(from: Storage): IpSocketAddress | UnixAddress = read(from.bytes, from.length)

  /** The address in `b`, `length` bytes of it: an IPv4-mapped IPv6 one as IPv4 if `unmap`, as a dual-stack socket's
    * peer is reported.
    */
  def read(b: Array[Byte], length: Int, unmap: Boolean = true): IpSocketAddress | UnixAddress = {
    val f = (b(0) & 0xff) | ((b(1) & 0xff) << 8)
    def port = ((b(2) & 0xff) << 8) | (b(3) & 0xff)
    f match {
      case AF_INET  => IpSocketAddress(IpAddress.fromBytes(Protocol.IPv4, java.util.Arrays.copyOfRange(b, 4, 8)), port)
      case AF_INET6 =>
        val octets = java.util.Arrays.copyOfRange(b, 8, 24)
        val mapped = (0 until 10).forall(octets(_) == 0) && octets(10) == -1 && octets(11) == -1
        if mapped && unmap then
          IpSocketAddress(IpAddress.fromBytes(Protocol.IPv4, java.util.Arrays.copyOfRange(octets, 12, 16)), port)
        else {
          val scope = (b(24) & 0xff) | ((b(25) & 0xff) << 8) | ((b(26) & 0xff) << 16) | ((b(27) & 0xff) << 24)
          IpSocketAddress(IpAddress.fromBytes(Protocol.IPv6, octets, scope), port)
        }
      case AF_UNIX =>
        var end = 2
        while end < length && b(end) != 0 do end += 1
        UnixAddress(new String(b, 2, end - 2, StandardCharsets.UTF_8))
      case other => throw new IOException(s"An address of an unknown family: $other")
    }
  }

  /** `to` as a socket of `fd`'s family sends to it: an IPv4 address as its IPv4-mapped one from a dual-stack IPv6
    * socket.
    */
  def target(ipv6Socket: Boolean, to: IpSocketAddress): IpSocketAddress =
    if !ipv6Socket || to.address.protocol == Protocol.IPv6 then to
    else {
      val v4 = to.address.bytes
      val mapped = new Array[Byte](16)
      mapped(10) = -1
      mapped(11) = -1
      var i = 0
      while i < 4 do { mapped(12 + i) = v4(i); i += 1 }
      IpSocketAddress(IpAddress.fromBytes(Protocol.IPv6, mapped), to.port)
    }

  /** The address `fd` is bound to (`getsockname`), or its peer's (`getpeername`). */
  def localAddress(fd: Int): IpSocketAddress | UnixAddress = name(fd, peer = false)
  def peerAddress(fd: Int): IpSocketAddress | UnixAddress = name(fd, peer = true)

  private def name(fd: Int, peer: Boolean): IpSocketAddress | UnixAddress = {
    val s = new Storage
    s.length = StorageSize
    val res = if peer then socket.getpeername(fd, s.at, s.lengthAt) else socket.getsockname(fd, s.at, s.lengthAt)
    if res != 0 then throw new IOException(s"Failed to read a socket's address: ${cError()}")
    read(s)
  }

  def bind(fd: Int, address: IpSocketAddress | UnixAddress): Unit = {
    val s = new Storage
    write(address, s)
    if socket.bind(fd, s.at, s.length.toUInt) != 0 then
      throw new IOException(s"Failed to bind to $address: ${cError()}")
  }

  def listen(fd: Int): Unit =
    if socket.listen(fd, Backlog) != 0 then throw new IOException(s"Failed to listen: ${cError()}")

  /** Starts connecting `fd` to `address`: true if it connected at once, false if it is connecting. */
  def startConnect(fd: Int, address: IpSocketAddress | UnixAddress): Boolean = {
    val s = new Storage
    write(address, s)
    if socket.connect(fd, s.at, s.length.toUInt) == 0 then true
    else if errno.errno == errno.EINPROGRESS || errno.errno == errno.EAGAIN || errno.errno == errno.EINTR then false
    else throw new IOException(s"Failed to connect to $address: ${cError()}")
  }

  /** Fails if a connection under way on `fd` failed (`SO_ERROR`). */
  def checkConnected(fd: Int, address: AnyRef): Unit = {
    val error = getInt(fd, socket.SOL_SOCKET, socket.SO_ERROR, "SO_ERROR")
    if error != 0 then throw new IOException(s"Failed to connect to $address: ${fromCString(string.strerror(error))}")
  }

  def shutdownOutput(fd: Int): Unit =
    if socket.shutdown(fd, socket.SHUT_WR) != 0 && errno.errno != errno.ENOTCONN then
      throw new IOException(s"Failed to shut down a connection's output: ${cError()}")

  /** Removes the socket file at `path`, as a restarted server's would be; fails, leaving it, on any other file. */
  def replaceSocketFile(path: String): Unit = Zone.acquire { implicit z =>
    val c = toCString(path)
    val info = stackalloc[stat.stat]()
    if stat.lstat(c, info) == 0 then {
      if stat.S_ISSOCK(info.st_mode) == 0 then throw new IOException(s"$path exists and is not a socket")
      unistd.unlink(c): Unit
    }
  }

  /** The device and inode of the file at `path`, to tell later whether it is still the one bound, or null. */
  def fileId(path: String): (Long, Long) | Null = Zone.acquire { implicit z =>
    val info = stackalloc[stat.stat]()
    if stat.lstat(toCString(path), info) != 0 then null
    else (info.st_dev.toLong, info.st_ino.toLong)
  }

  def unlink(path: String): Unit = Zone.acquire { implicit z => unistd.unlink(toCString(path)): Unit }

  def setInt(fd: Int, level: Int, name: Int, value: Int, what: String): Unit = {
    val v = stackalloc[CInt]()
    !v = value
    if socket.setsockopt(fd, level, name, v.asInstanceOf[Ptr[Byte]], sizeof[CInt].toUInt) != 0 then
      throw new IOException(s"Failed to set $what: ${cError()}")
  }

  def getInt(fd: Int, level: Int, name: Int, what: String): Int = {
    val v = stackalloc[CInt]()
    val length = stackalloc[socket.socklen_t]()
    !length = sizeof[CInt].toUInt
    if socket.getsockopt(fd, level, name, v.asInstanceOf[Ptr[Byte]], length) != 0 then
      throw new IOException(s"Failed to read $what: ${cError()}")
    !v
  }

  // ---- Options

  /** Whether this backend has `option` for a socket of `kind`: every option on Linux, where it applies. */
  def supports(option: SocketOption[?], kind: SocketOption.Kind): Boolean = option.appliesTo(kind)

  def canChange(option: SocketOption[?], kind: SocketOption.Kind): Boolean =
    option != SocketOption.ReusePort && SocketOption.onSocket(option, kind)

  /** Refuses options that do not apply to a socket of `kind`, as an op opening one is built. */
  def check(options: SocketOptions, kind: SocketOption.Kind): Unit =
    for option <- options.options do
      if !option.appliesTo(kind) then
        throw new IllegalArgumentException(s"$option does not apply to a ${kind.description}")

  private def isIpv6(fd: Int): Boolean = getInt(fd, socket.SOL_SOCKET, Linux.SO_DOMAIN, "SO_DOMAIN") == AF_INET6

  def set(fd: Int, option: SocketOption[?], value: Any): Unit = option match {
    case SocketOption.NoDelay =>
      setInt(fd, IPPROTO_TCP, TCP_NODELAY, if value.asInstanceOf[Boolean] then 1 else 0, "TCP_NODELAY")
    case SocketOption.KeepAlive =>
      value match {
        case SocketOption.KeepAlive.On(idle, interval, count) =>
          setInt(fd, socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1, "SO_KEEPALIVE")
          setInt(fd, IPPROTO_TCP, TCP_KEEPIDLE, idle, "TCP_KEEPIDLE")
          setInt(fd, IPPROTO_TCP, TCP_KEEPINTVL, interval, "TCP_KEEPINTVL")
          setInt(fd, IPPROTO_TCP, TCP_KEEPCNT, count, "TCP_KEEPCNT")
        case _ => setInt(fd, socket.SOL_SOCKET, socket.SO_KEEPALIVE, 0, "SO_KEEPALIVE")
      }
    case SocketOption.ReceiveBuffer =>
      setInt(fd, socket.SOL_SOCKET, socket.SO_RCVBUF, value.asInstanceOf[Int], "SO_RCVBUF")
    case SocketOption.SendBuffer =>
      setInt(fd, socket.SOL_SOCKET, socket.SO_SNDBUF, value.asInstanceOf[Int], "SO_SNDBUF")
    case SocketOption.DontFragment =>
      val on = value.asInstanceOf[Boolean]
      if isIpv6(fd) then setInt(fd, IPPROTO_IPV6, IPV6_DONTFRAG, if on then 1 else 0, "IPV6_DONTFRAG")
      setInt(fd, IPPROTO_IP, IP_MTU_DISCOVER, if on then IP_PMTUDISC_DO else IP_PMTUDISC_DONT, "IP_MTU_DISCOVER")
    case SocketOption.ReusePort =>
      setInt(fd, socket.SOL_SOCKET, SO_REUSEPORT, if value.asInstanceOf[Boolean] then 1 else 0, "SO_REUSEPORT")
  }

  def get(fd: Int, option: SocketOption[?]): Any = option match {
    case SocketOption.NoDelay   => getInt(fd, IPPROTO_TCP, TCP_NODELAY, "TCP_NODELAY") != 0
    case SocketOption.KeepAlive =>
      if getInt(fd, socket.SOL_SOCKET, socket.SO_KEEPALIVE, "SO_KEEPALIVE") == 0 then SocketOption.KeepAlive.Off
      else
        SocketOption.KeepAlive.On(
          getInt(fd, IPPROTO_TCP, TCP_KEEPIDLE, "TCP_KEEPIDLE"),
          getInt(fd, IPPROTO_TCP, TCP_KEEPINTVL, "TCP_KEEPINTVL"),
          getInt(fd, IPPROTO_TCP, TCP_KEEPCNT, "TCP_KEEPCNT")
        )
    case SocketOption.ReceiveBuffer => getInt(fd, socket.SOL_SOCKET, socket.SO_RCVBUF, "SO_RCVBUF")
    case SocketOption.SendBuffer    => getInt(fd, socket.SOL_SOCKET, socket.SO_SNDBUF, "SO_SNDBUF")
    case SocketOption.DontFragment  =>
      if isIpv6(fd) then getInt(fd, IPPROTO_IPV6, IPV6_DONTFRAG, "IPV6_DONTFRAG") != 0
      else getInt(fd, IPPROTO_IP, IP_MTU_DISCOVER, "IP_MTU_DISCOVER") == IP_PMTUDISC_DO
    case SocketOption.ReusePort => getInt(fd, socket.SOL_SOCKET, SO_REUSEPORT, "SO_REUSEPORT") != 0
  }

  /** Sets each of `values` on `fd`, closing it and rethrowing if one fails. */
  def apply(fd: Int, values: List[(SocketOption[?], Any)]): Unit =
    try values.foreach((o, v) => set(fd, o, v))
    catch {
      case e: Throwable =>
        unistd.close(fd)
        throw e
    }
}
