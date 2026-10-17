package gears.io.reactor

import gears.async.Async
import gears.io.Buffer
import gears.io.Error
import gears.io.Result
import gears.io.either
import gears.io.net
import gears.io.net.SocketOption
import gears.io.reactor.{Address, Slot}
import gears.io.unsafe.PosixFileOps
import gears.io.unsafe.PosixSockets
import gears.io.uring.SocketAddresses

import java.io.IOException
import java.net.Inet6Address
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.SocketAddress
import java.nio.charset.StandardCharsets
import scala.scalanative.posix.errno
import scala.scalanative.posix.netinet.in.IPPROTO_TCP
import scala.scalanative.posix.netinet.tcp.TCP_NODELAY
import scala.scalanative.posix.string.{memcpy, strerror}
import scala.scalanative.posix.sys.{socket => posixSocket}
import scala.scalanative.posix.unistd
import scala.scalanative.unsafe._
import scala.scalanative.unsigned._

final case class UnixSocketAddress(path: String) extends SocketAddress:
  override def toString: String = path

private[io] def posixError(what: String, err: Int = errno.errno): IOException =
  IOException(s"$what: ${fromCString(strerror(err))}")

private[io] def openSocket(family: CInt, kind: CInt): Int =
  val fd = posixSocket.socket(family, kind, 0)
  if fd < 0 then throw posixError("Failed to open socket")
  closeFdOnFailure(fd):
    PosixSockets.setNonBlocking(fd)
    if scala.scalanative.posix.fcntl
        .fcntl(fd, scala.scalanative.posix.fcntl.F_SETFD, scala.scalanative.posix.fcntl.FD_CLOEXEC) < 0
    then throw posixError("Failed to make socket close-on-exec")
  fd

private[io] def closeFdOnFailure[A](fd: Int)(body: => A): A =
  try body
  catch
    case e: Throwable =>
      unistd.close(fd)
      throw e

private[io] def socketFamily(address: SocketAddress): Int =
  address match
    case _: UnixSocketAddress => posixSocket.AF_UNIX
    case inet: InetSocketAddress if inet.getAddress != null && inet.getAddress.isInstanceOf[Inet6Address] =>
      posixSocket.AF_INET6
    case inet: InetSocketAddress if inet.getAddress != null => posixSocket.AF_INET
    case inet: InetSocketAddress => throw IllegalArgumentException(s"Unresolved address: $inet")
    case other                   => throw IllegalArgumentException(s"Unsupported socket address: $other")

private[io] def fillSockAddr(out: Ptr[Byte], addr: SocketAddress): posixSocket.socklen_t =
  val storage = SocketAddresses.storage()
  val length = SocketAddresses.encode(toAddress(addr), storage)
  memcpy(out, SocketAddresses.pointer(storage).asInstanceOf[Ptr[Byte]], length.toLong.toCSize)
  length

private[io] def parseSockAddr(addr: Ptr[Byte], length: posixSocket.socklen_t): SocketAddress =
  require(length.toLong <= SocketAddresses.storageSize, s"Oversized socket address: ${length.toInt} bytes")
  fromAddress(SocketAddresses.decode(addr.asInstanceOf[Ptr[posixSocket.sockaddr]], length))

private[io] def getLocalAddress(fd: Int): SocketAddress =
  Zone.acquire: zone =>
    val addrLen = alloc[posixSocket.socklen_t]()(using zone)
    val addr = alloc[posixSocket.sockaddr_storage]()(using zone).asInstanceOf[Ptr[Byte]]
    !addrLen = sizeof[posixSocket.sockaddr_storage].toUInt
    if posixSocket.getsockname(fd, addr.asInstanceOf[Ptr[posixSocket.sockaddr]], addrLen) < 0 then
      throw posixError("getsockname failed")
    if (!addrLen).toLong > sizeof[posixSocket.sockaddr_storage].toLong then
      throw IOException(s"getsockname returned oversized address: ${(!addrLen).toLong} bytes")
    parseSockAddr(addr, !addrLen)

private[io] def shutdownSocket(fd: Int, how: CInt): Unit =
  if posixSocket.shutdown(fd, how) < 0 then throw posixError("Failed to shut down socket")

private[io] def getPeerAddress(fd: Int): SocketAddress =
  Zone.acquire: zone =>
    val addrLen = alloc[posixSocket.socklen_t]()(using zone)
    val addr = alloc[posixSocket.sockaddr_storage]()(using zone).asInstanceOf[Ptr[Byte]]
    !addrLen = sizeof[posixSocket.sockaddr_storage].toUInt
    if posixSocket.getpeername(fd, addr.asInstanceOf[Ptr[posixSocket.sockaddr]], addrLen) < 0 then
      throw posixError("getpeername failed")
    if (!addrLen).toLong > sizeof[posixSocket.sockaddr_storage].toLong then
      throw IOException(s"getpeername returned oversized address: ${(!addrLen).toLong} bytes")
    parseSockAddr(addr, !addrLen)

private[io] def toAddress(address: InetSocketAddress): Address =
  address.getAddress match
    case null             => throw IllegalArgumentException(s"Unresolved address: $address")
    case a6: Inet6Address => Address.IPv6(a6.getHostAddress.takeWhile(_ != '%'), address.getPort)
    case a4               => Address.IPv4(a4.getHostAddress, address.getPort)

private[io] def fromAddress(address: Address): SocketAddress =
  address match
    case Address.IPv4(host, port) => InetSocketAddress(InetAddress.getByName(host), port)
    case Address.IPv6(host, port) => InetSocketAddress(InetAddress.getByName(host), port)
    case Address.Unix(path)       => UnixSocketAddress(path)

private[io] def toAddress(address: SocketAddress): Address = address match
  case unix: UnixSocketAddress => Address.Unix(unix.path)
  case inet: InetSocketAddress => toAddress(inet)
  case other                   => throw IOException(s"Unsupported socket address: $other")

private[io] def unlinkSocket(path: String): Unit =
  Zone.acquire { implicit z => PosixFileOps.safeUnlink(toCString(path, StandardCharsets.UTF_8)) }

private[io] def applySocketOption(fd: Int, opt: SocketOption): Unit =
  Zone.acquire: zone =>
    def setInt(level: CInt, name: CInt, v: Int): Unit =
      val p = alloc[CInt]()(using zone)
      !p = v
      if posixSocket.setsockopt(fd, level, name, p.asInstanceOf[Ptr[Byte]], sizeof[CInt].toUInt) < 0 then
        throw posixError("setsockopt failed")
    opt match
      case SocketOption.sendBufSize(n)   => setInt(posixSocket.SOL_SOCKET, posixSocket.SO_SNDBUF, n)
      case SocketOption.recvBufSize(n)   => setInt(posixSocket.SOL_SOCKET, posixSocket.SO_RCVBUF, n)
      case SocketOption.keepAlive(k)     => setInt(posixSocket.SOL_SOCKET, posixSocket.SO_KEEPALIVE, if k then 1 else 0)
      case SocketOption.reuseAddr(r)     => setInt(posixSocket.SOL_SOCKET, posixSocket.SO_REUSEADDR, if r then 1 else 0)
      case SocketOption.noDelay(nd)      => setInt(IPPROTO_TCP, TCP_NODELAY, if nd then 1 else 0)
      case SocketOption.linger(interval) =>
        val p = alloc[posixSocket.linger]()(using zone)
        p._1 = if interval < 0 then 0 else 1
        p._2 = if interval < 0 then 0 else interval
        if posixSocket.setsockopt(
            fd,
            posixSocket.SOL_SOCKET,
            posixSocket.SO_LINGER,
            p.asInstanceOf[Ptr[Byte]],
            sizeof[posixSocket.linger].toUInt
          ) < 0
        then throw posixError("setsockopt failed")

class ReactorTcpStream private[io] (
    private[io] val fd: Int,
    driver: ReactorDriver,
    override val localAddress: SocketAddress,
    override val remoteAddress: SocketAddress
) extends net.TcpStream:

  private val guard = IoGuard()
  private val access = gears.io.IoConcurrency()

  override def close(): Unit = guard.close { unistd.close(fd); () }

  override def shutdownOutput()(using Async): Unit = guard.whileOpen(shutdownSocket(fd, posixSocket.SHUT_WR))

  override def shutdownInput()(using Async): Unit = guard.whileOpen(shutdownSocket(fd, posixSocket.SHUT_RD))

  override def readBuf(buf: Buffer)(using Async): Result[Unit] = access.reading {
    either {
      guard.whileOpen(())
      require(!buf.isReadOnly, "Read buffers must be writable")
      if buf.hasRemaining() then
        val before = buf.position()
        driver.awaitFilling(guard, buf)((r, into) => r.ops.read(fd, into))
        if buf.position() == before then either.error(Error.EOF)
    }
  }

  override def writeBuf(buf: Buffer)(using Async): Result[Unit] = access.writing {
    either {
      guard.whileOpen(())
      while buf.hasRemaining() do driver.awaitDraining(guard, buf)((r, from) => r.ops.write(fd, from))
    }
  }

class ReactorTcpListener private[io] (
    private[io] val fd: Int,
    driver: ReactorDriver,
    override val localAddress: SocketAddress,
    private val unixPath: String | Null = null
) extends net.TcpListener:
  type Stream = ReactorTcpStream

  private val guard = IoGuard()
  private val access = gears.io.IoConcurrency()

  override def close(): Unit =
    guard.close {
      unistd.close(fd)
      if unixPath != null then unlinkSocket(unixPath.nn)
    }

  override def accept()(using Async): Result[Stream] = access.reading {
    either {
      var stream: ReactorTcpStream | Null = null
      while stream == null do
        val clientFd = driver.awaitHandle(guard)((r, accepted) => r.ops.accept(fd, accepted))
        val peer =
          try getPeerAddress(clientFd)
          catch
            case _: IOException =>
              unistd.close(clientFd)
              null
        if peer != null then stream = closeFdOnFailure(clientFd)(ReactorTcpStream(clientFd, driver, localAddress, peer))
      stream.nn
    }
  }

class ReactorUdpSocket private[io] (
    private[io] val fd: Int,
    driver: ReactorDriver,
    override val localAddress: SocketAddress,
    private val unixPath: String | Null = null
) extends net.UdpSocket:

  private val guard = IoGuard()
  private val access = gears.io.IoConcurrency()

  override def close(): Unit =
    guard.close {
      unistd.close(fd)
      if unixPath != null then unlinkSocket(unixPath.nn)
    }

  override def sendTo(buf: Buffer, target: SocketAddress)(using Async): Result[Unit] = access.writing {
    either {
      val to = toAddress(target)
      driver.awaitDraining(guard, buf)((r, from) => r.ops.send(fd, from, to))
    }
  }

  override def receiveFrom(buf: Buffer)(using Async): Result[SocketAddress] = access.reading {
    either {
      val free = buf.slice()
      val sender = Slot[Address]()
      if driver.acceptsHeapBuffers then driver.await(guard)(r => r.ops.receive(fd, free, sender))
      else
        Zone.acquire: zone =>
          val n = math.min(free.remaining(), 64 * 1024)
          val staged = scala.scalanative.memory.PointerBuffer.wrap(alloc[Byte](math.max(n, 1))(using zone), n)
          driver.await(guard)(r => r.ops.receive(fd, staged, sender))
          free.put(staged)
          free.flip()
      buf.position(buf.position() + free.remaining())
      fromAddress(sender.clear())
    }
  }

trait ReactorUdpSupport(driver: ReactorDriver) extends net.UdpSupport:
  type Socket = ReactorUdpSocket

  override def bind(address: SocketAddress, options: Seq[SocketOption])(using
      Async
  ): Result[Socket] = either:
    val family = socketFamily(address)
    val fd = openSocket(family, posixSocket.SOCK_DGRAM)
    closeFdOnFailure(fd):
      options.foreach(applySocketOption(fd, _))
      val unixPath = address match
        case unix: UnixSocketAddress => unix.path
        case _                       => null
      if unixPath != null then PosixSockets.bindUnixAddr(fd, unixPath.nn, removeExisting = true)
      else
        Zone.acquire: zone =>
          val sockAddr = alloc[posixSocket.sockaddr_storage]()(using zone).asInstanceOf[Ptr[Byte]]
          val actualSize = fillSockAddr(sockAddr, address)
          if posixSocket.bind(fd, sockAddr.asInstanceOf[Ptr[posixSocket.sockaddr]], actualSize) < 0 then
            throw posixError("Failed to bind socket")
      ReactorUdpSocket(fd, driver, getLocalAddress(fd), unixPath)

trait ReactorTcpSupport(driver: ReactorDriver) extends net.TcpSupport:
  type Stream = ReactorTcpStream
  type Listener = ReactorTcpListener

  override def connect(address: SocketAddress, options: Seq[SocketOption])(using
      Async
  ): Result[Stream] = either:
    val family = socketFamily(address)
    val fd = openSocket(family, posixSocket.SOCK_STREAM)
    closeFdOnFailure(fd):
      options.foreach(applySocketOption(fd, _))
      Zone.acquire: zone =>
        val sockAddr = alloc[posixSocket.sockaddr_storage]()(using zone).asInstanceOf[Ptr[Byte]]
        val addrSize = fillSockAddr(sockAddr, address)
        if posixSocket.connect(fd, sockAddr.asInstanceOf[Ptr[posixSocket.sockaddr]], addrSize) < 0
          && errno.errno != errno.EINPROGRESS
        then throw posixError(s"Failed to connect to $address")
      driver.await(null)(r => r.ops.connect(fd))
      ReactorTcpStream(fd, driver, localAddress = getLocalAddress(fd), remoteAddress = address)

  override def listen(address: SocketAddress, options: Seq[SocketOption])(using
      Async
  ): Result[Listener] = either:
    val family = socketFamily(address)
    val fd = openSocket(family, posixSocket.SOCK_STREAM)
    closeFdOnFailure(fd):
      options.foreach(applySocketOption(fd, _))
      val unixPath = address match
        case unix: UnixSocketAddress => unix.path
        case _                       => null
      if unixPath != null then PosixSockets.bindUnixAddr(fd, unixPath.nn, removeExisting = true)
      else
        Zone.acquire: zone =>
          val optVal = alloc[CInt]()(using zone)
          !optVal = 1
          posixSocket.setsockopt(
            fd,
            posixSocket.SOL_SOCKET,
            posixSocket.SO_REUSEADDR,
            optVal.asInstanceOf[Ptr[Byte]],
            sizeof[CInt].toUInt
          )
          val sockAddr = alloc[posixSocket.sockaddr_storage]()(using zone).asInstanceOf[Ptr[Byte]]
          val addrSize = fillSockAddr(sockAddr, address)
          if posixSocket.bind(fd, sockAddr.asInstanceOf[Ptr[posixSocket.sockaddr]], addrSize) < 0 then
            throw posixError("Failed to bind socket")
      if posixSocket.listen(fd, 128) < 0 then throw posixError("Failed to listen on socket")
      ReactorTcpListener(fd, driver, getLocalAddress(fd), unixPath)
