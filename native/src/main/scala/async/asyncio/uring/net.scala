package gears.async.asyncio.uring

import asyncio.{Address, Slot}
import gears.async.Async
import gears.async.asyncio.Buffer
import gears.async.asyncio.Error
import gears.async.asyncio.Result
import gears.async.net
import gears.async.net.SocketOption
import gears.util.either

import java.io.IOException
import java.net.Inet6Address
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.SocketAddress
import java.net.UnixDomainSocketAddress
import java.nio.charset.StandardCharsets
import java.nio.channels.ClosedChannelException
import scala.scalanative.posix.arpa.inet._
import scala.scalanative.posix.errno
import scala.scalanative.posix.netinet.in.IPPROTO_TCP
import scala.scalanative.posix.netinet.tcp.TCP_NODELAY
import scala.scalanative.posix.string.{memcpy, strerror}
import scala.scalanative.posix.sys.{socket => posixSocket}
import scala.scalanative.posix.unistd
import scala.scalanative.unsafe._
import scala.scalanative.unsigned._
import scala.scalanative.runtime.ByteArray

import asyncio.unsafe.PosixSockets
import asyncio.unsafe.PosixFileOps
import asyncio.uring.SocketAddresses

/** Linux values; Scala Native's posixlib does not expose them. */
private[uring] final val SOCK_NONBLOCK = 0x800
private[uring] final val SOCK_CLOEXEC = 0x80000

private[uring] def posixError(what: String, err: Int = errno.errno): IOException =
  IOException(s"$what: ${fromCString(strerror(err))}")

private[uring] def closeFdOnFailure[A](fd: Int)(body: => A): A =
  try body
  catch
    case e: Throwable =>
      unistd.close(fd)
      throw e

private[uring] def socketFamily(address: SocketAddress): Int =
  address match
    case _: UnixDomainSocketAddress => posixSocket.AF_UNIX
    case inet: InetSocketAddress if inet.getAddress != null && inet.getAddress.isInstanceOf[Inet6Address] =>
      posixSocket.AF_INET6
    case inet: InetSocketAddress if inet.getAddress != null => posixSocket.AF_INET
    case inet: InetSocketAddress => throw IllegalArgumentException(s"Unresolved address: $inet")
    case other => throw IllegalArgumentException(s"Unsupported socket address: $other")

private[uring] def fillSockAddr(out: Ptr[Byte], addr: SocketAddress): posixSocket.socklen_t =
  val storage = SocketAddresses.storage()
  val length = SocketAddresses.encode(toAddress(addr), storage)
  memcpy(out, SocketAddresses.pointer(storage).asInstanceOf[Ptr[Byte]], length.toLong.toCSize)
  length

private[uring] def parseSockAddr(addr: Ptr[Byte], length: posixSocket.socklen_t): SocketAddress =
  require(length.toLong <= SocketAddresses.storageSize, s"Oversized socket address: ${length.toInt} bytes")
  fromAddress(SocketAddresses.decode(addr.asInstanceOf[Ptr[posixSocket.sockaddr]], length))

private[uring] def getLocalAddress(fd: Int): SocketAddress =
  Zone.acquire: zone =>
    val addrLen = alloc[posixSocket.socklen_t]()(using zone)
    val addr = alloc[posixSocket.sockaddr_storage]()(using zone).asInstanceOf[Ptr[Byte]]
    !addrLen = sizeof[posixSocket.sockaddr_storage].toUInt
    if posixSocket.getsockname(fd, addr.asInstanceOf[Ptr[posixSocket.sockaddr]], addrLen) < 0 then
      throw posixError("getsockname failed")
    if addrLen.toLong > sizeof[posixSocket.sockaddr_storage].toLong then
      throw IOException(s"getsockname returned oversized address: ${addrLen.toLong} bytes")
    parseSockAddr(addr, !addrLen)

private[uring] def getPeerAddress(fd: Int): SocketAddress =
  Zone.acquire: zone =>
    val addrLen = alloc[posixSocket.socklen_t]()(using zone)
    val addr = alloc[posixSocket.sockaddr_storage]()(using zone).asInstanceOf[Ptr[Byte]]
    !addrLen = sizeof[posixSocket.sockaddr_storage].toUInt
    if posixSocket.getpeername(fd, addr.asInstanceOf[Ptr[posixSocket.sockaddr]], addrLen) < 0 then
      throw posixError("getpeername failed")
    if addrLen.toLong > sizeof[posixSocket.sockaddr_storage].toLong then
      throw IOException(s"getpeername returned oversized address: ${addrLen.toLong} bytes")
    parseSockAddr(addr, !addrLen)

/** The reactor's numeric form of an IP socket address. An IPv6 scope id is dropped. */
private[uring] def toAddress(address: InetSocketAddress): Address =
  address.getAddress match
    case null             => throw IllegalArgumentException(s"Unresolved address: $address")
    case a6: Inet6Address => Address.IPv6(a6.getHostAddress.takeWhile(_ != '%'), address.getPort)
    case a4               => Address.IPv4(a4.getHostAddress, address.getPort)

private[uring] def fromAddress(address: Address): SocketAddress =
  address match
    case Address.IPv4(host, port) => InetSocketAddress(InetAddress.getByName(host), port)
    case Address.IPv6(host, port) => InetSocketAddress(InetAddress.getByName(host), port)
    case Address.Unix(path)       => UnixDomainSocketAddress.of(path)

private[uring] def toAddress(address: SocketAddress): Address = address match
  case unix: UnixDomainSocketAddress => Address.Unix(unix.getPath.toString)
  case inet: InetSocketAddress       => toAddress(inet)
  case other                         => throw IOException(s"Unsupported socket address: $other")

private[uring] def unlinkSocket(path: String): Unit =
  Zone.acquire { implicit z => PosixFileOps.safeUnlink(toCString(path, StandardCharsets.UTF_8)) }

private[uring] def applySocketOption(fd: Int, opt: SocketOption): Unit =
  Zone.acquire: zone =>
    def setInt(level: CInt, name: CInt, v: Int): Unit =
      val p = alloc[CInt]()(using zone)
      !p = v
      if posixSocket.setsockopt(fd, level, name, p.asInstanceOf[Ptr[Byte]], sizeof[CInt].toUInt) < 0 then
        throw posixError("setsockopt failed")
    opt match
      case SocketOption.sendBufSize(n) => setInt(posixSocket.SOL_SOCKET, posixSocket.SO_SNDBUF, n)
      case SocketOption.recvBufSize(n) => setInt(posixSocket.SOL_SOCKET, posixSocket.SO_RCVBUF, n)
      case SocketOption.keepAlive(k)   => setInt(posixSocket.SOL_SOCKET, posixSocket.SO_KEEPALIVE, if k then 1 else 0)
      case SocketOption.reuseAddr(r)   => setInt(posixSocket.SOL_SOCKET, posixSocket.SO_REUSEADDR, if r then 1 else 0)
      case SocketOption.noDelay(nd)    => setInt(IPPROTO_TCP, TCP_NODELAY, if nd then 1 else 0)
      case SocketOption.linger(interval) =>
        // Java convention: a negative value disables lingering.
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

class UringTcpStream private[uring] (
    val fd: Int,
    scheduler: UringPerThreadScheduler,
    override val localAddress: SocketAddress,
    override val remoteAddress: SocketAddress
) extends net.TcpStream:

  private var closed = false

  private def checkOpen(): Unit = synchronized:
    if closed then throw new ClosedChannelException()

  override def close(): Unit = synchronized:
    if !closed then
      closed = true
      unistd.close(fd)

  override def readBuf(buf: Buffer)(using Async): Result[Unit] = either:
    checkOpen()
    val before = buf.position()
    scheduler.await(reactor => reactor.ops.read(fd, buf))
    if buf.position() == before then either.error(Error.EOF)

  override def writeBuf(buf: Buffer)(using Async): Result[Unit] = either:
    checkOpen()
    while buf.hasRemaining() do scheduler.await(reactor => reactor.ops.write(fd, buf))

class UringTcpListener private[uring] (
    val fd: Int,
    scheduler: UringPerThreadScheduler,
    override val localAddress: SocketAddress,
    private val unixPath: String | Null = null
) extends net.TcpListener:
  type Stream = UringTcpStream

  private var closed = false

  private def checkOpen(): Unit = synchronized:
    if closed then throw new ClosedChannelException()

  override def close(): Unit = synchronized:
    if !closed then
      closed = true
      unistd.close(fd)
      if unixPath != null then unlinkSocket(unixPath.nn)

  override def accept()(using Async): Result[Stream] = either:
    checkOpen()
    val accepted = Slot[Integer]()
    scheduler.await(reactor => reactor.ops.accept(fd, accepted))
    val clientFd = accepted.clear().intValue
    closeFdOnFailure(clientFd):
      UringTcpStream(clientFd, scheduler, localAddress, getPeerAddress(clientFd))

class UringUdpSocket private[uring] (
    val fd: Int,
    scheduler: UringPerThreadScheduler,
    override val localAddress: SocketAddress,
    private val unixPath: String | Null = null
) extends net.UdpSocket:

  private var closed = false

  private def checkOpen(): Unit = synchronized:
    if closed then throw new ClosedChannelException()

  override def close(): Unit = synchronized:
    if !closed then
      closed = true
      unistd.close(fd)
      if unixPath != null then unlinkSocket(unixPath.nn)

  override def sendTo(buf: Buffer, target: SocketAddress)(using Async): Result[Unit] = either:
    checkOpen()
    scheduler.await(reactor => reactor.ops.send(fd, buf, toAddress(target)))

  override def receiveFrom(buf: Buffer)(using Async): Result[SocketAddress] = either:
    checkOpen()
    // The op fills its buffer from the start and flips it, so give it the free part of `buf` only.
    val free = buf.slice()
    val sender = Slot[Address]()
    scheduler.await(reactor => reactor.ops.receive(fd, free, sender))
    buf.position(buf.position() + free.remaining())
    fromAddress(sender.clear())

trait UringUdpSupport(scheduler: UringPerThreadScheduler) extends net.UdpSupport:
  type Socket = UringUdpSocket

  override def bind(address: SocketAddress, options: Seq[SocketOption])(using
      Async
  ): Result[Socket] = either:
    val family = socketFamily(address)
    val fd = posixSocket.socket(family, posixSocket.SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0)
    if fd < 0 then throw posixError("Failed to open socket")
    closeFdOnFailure(fd):
      options.foreach(applySocketOption(fd, _))
      val unixPath = address match
        case unix: UnixDomainSocketAddress => unix.getPath.toString
        case _                             => null
      if unixPath != null then PosixSockets.bindUnixAddr(fd, unixPath.nn, removeExisting = true)
      else
        Zone.acquire: zone =>
          val sockAddr = alloc[posixSocket.sockaddr_storage]()(using zone).asInstanceOf[Ptr[Byte]]
          val actualSize = fillSockAddr(sockAddr, address)
          if posixSocket.bind(fd, sockAddr.asInstanceOf[Ptr[posixSocket.sockaddr]], actualSize) < 0 then
            throw posixError("Failed to bind socket")
      UringUdpSocket(fd, scheduler, getLocalAddress(fd), unixPath)

trait UringTcpSupport(scheduler: UringPerThreadScheduler) extends net.TcpSupport:
  type Stream = UringTcpStream
  type Listener = UringTcpListener

  override def connect(address: SocketAddress, options: Seq[SocketOption])(using
      Async
  ): Result[Stream] = either:
    val family = socketFamily(address)
    val fd = posixSocket.socket(family, posixSocket.SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0)
    if fd < 0 then throw posixError("Failed to open socket")
    closeFdOnFailure(fd):
      options.foreach(applySocketOption(fd, _))
      Zone.acquire: zone =>
        val sockAddr = alloc[posixSocket.sockaddr_storage]()(using zone).asInstanceOf[Ptr[Byte]]
        val addrSize = fillSockAddr(sockAddr, address)
        if posixSocket.connect(fd, sockAddr.asInstanceOf[Ptr[posixSocket.sockaddr]], addrSize) < 0
          && errno.errno != errno.EINPROGRESS
        then throw posixError(s"Failed to connect to $address")
      scheduler.await(reactor => reactor.ops.connect(fd)) // waits for the connection the call above started
      UringTcpStream(fd, scheduler, localAddress = getLocalAddress(fd), remoteAddress = address)

  override def listen(address: SocketAddress, options: Seq[SocketOption])(using
      Async
  ): Result[Listener] = either:
    val family = socketFamily(address)
    val fd = posixSocket.socket(family, posixSocket.SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0)
    if fd < 0 then throw posixError("Failed to open socket")
    closeFdOnFailure(fd):
      options.foreach(applySocketOption(fd, _))
      val unixPath = address match
        case unix: UnixDomainSocketAddress => unix.getPath.toString
        case _                             => null
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
      UringTcpListener(fd, scheduler, getLocalAddress(fd), unixPath)
