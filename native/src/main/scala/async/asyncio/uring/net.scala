package gears.async.asyncio.uring

import gears.async.Async
import gears.async.Future
import gears.async.asyncio.Buffer
import gears.async.asyncio.Error
import gears.async.asyncio.Result
import uring._
import uringOps._
import gears.async.net
import gears.async.net.SocketOption
import gears.util.either

import java.net.Inet6Address
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.SocketAddress
import java.nio.channels.ClosedChannelException
import scala.scalanative.posix.arpa.inet._
import scala.scalanative.posix.errno
import scala.scalanative.posix.netinet.in.{in_addr, sockaddr_in, sockaddr_in6, in6_addr, IPPROTO_TCP}
import scala.scalanative.posix.netinet.tcp.TCP_NODELAY
import scala.scalanative.posix.sys.{socket => posixSocket}
import scala.scalanative.posix.unistd
import scala.scalanative.runtime.ByteArray
import scala.scalanative.unsafe._
import scala.scalanative.unsigned._

/** `keepAlive` - the buffer (or tuple of buffers) whose raw address the
  * kernel holds onto for as long as this op is outstanding - rides in the
  * same shard-owned `handlers` entry as the completion closure below,
  * rather than needing a reachability fence of its own; see
  * `UringShard.Handler`'s doc for why that's a strictly stronger
  * guarantee than fencing a closure-captured value would be.
  */
private[uring] def submitAwait(scheduler: UringPerThreadScheduler)(keepAlive: AnyRef)(prep: Ptr[io_uring_sqe] => Unit)(using
    Async
): Int =
  Future
    .withResolver[Int]: resolver =>
      val (shard, userData) = scheduler.submit(prep, keepAlive): res =>
        if res == -errno.ECANCELED then resolver.rejectAsCancelled()
        else resolver.resolve(res)
      resolver.onCancel: () =>
        // Targets the exact shard this op was submitted to, not wherever
        // the cancelling thread happens to be - IORING_OP_ASYNC_CANCEL only
        // matches an op on the same ring it's submitted to, so routing
        // this through scheduler.submit's normal current-thread/round-robin
        // logic would often silently cancel nothing. See submitOn's doc.
        try scheduler.submitOn(shard)(sqe => io_uring_prep_cancel64(sqe, userData, 0), null)(_ => ())
        catch case _: IOException => ()
    .link()
    .await

/** Runs `body`, closing `fd` if it throws - for the setup sequence between
  * creating a socket and handing it off wrapped in a `UringTcpStream`/
  * `UringTcpListener`/`UringUdpSocket` (whose own `close()` is the only
  * thing that normally closes it). Without this, a failure partway through
  * setup (a bad socket option, a failed bind/listen/connect, even
  * `getLocalAddress` itself) leaks the fd for the life of the process -
  * nothing else ever closes it, since the wrapper that would own that
  * responsibility was never successfully constructed.
  */
private[uring] def closeFdOnFailure[A](fd: Int)(body: => A): A =
  try body
  catch
    case e: Throwable =>
      unistd.close(fd)
      throw e

private[uring] def sockaddrSize(addr: InetAddress): CUnsignedInt =
  addr match
    case _: Inet6Address => sizeof[sockaddr_in6].toUInt
    case _                => sizeof[sockaddr_in].toUInt

private[uring] def fillSockAddr(out: Ptr[Byte], addr: InetSocketAddress): Unit =
  addr.getAddress match
    case a6: Inet6Address =>
      val s = out.asInstanceOf[Ptr[sockaddr_in6]]
      s._1 = posixSocket.AF_INET6.toUShort
      s._2 = htons(addr.getPort.toUShort)
      s._3 = 0.toUInt
      val bytes = a6.getAddress
      val addrBytes = s.at4.asInstanceOf[Ptr[Byte]]
      var i = 0
      while i < 16 do
        !(addrBytes + i) = bytes(i)
        i += 1
      s._5 = a6.getScopeId.toUInt
    case a4 =>
      val s = out.asInstanceOf[Ptr[sockaddr_in]]
      s._1 = posixSocket.AF_INET.toUShort
      s._2 = htons(addr.getPort.toUShort)
      val bytes = a4.getAddress
      val addrBytes = s.at3.asInstanceOf[Ptr[Byte]]
      var i = 0
      while i < 4 do
        !(addrBytes + i) = bytes(i)
        i += 1

private[uring] def parseSockAddr(addr: Ptr[Byte]): InetSocketAddress =
  val family = (!addr.asInstanceOf[Ptr[CUnsignedShort]]).toInt
  if family == posixSocket.AF_INET6 then
    val s = addr.asInstanceOf[Ptr[sockaddr_in6]]
    val port = ntohs(s._2).toInt & 0xffff
    val addrBytes = s.at4.asInstanceOf[Ptr[Byte]]
    val bytes = new Array[Byte](16)
    var i = 0
    while i < 16 do
      bytes(i) = !(addrBytes + i)
      i += 1
    new InetSocketAddress(InetAddress.getByAddress(bytes), port)
  else
    val s = addr.asInstanceOf[Ptr[sockaddr_in]]
    val port = ntohs(s._2).toInt & 0xffff
    val addrBytes = s.at3.asInstanceOf[Ptr[Byte]]
    val bytes = new Array[Byte](4)
    var i = 0
    while i < 4 do
      bytes(i) = !(addrBytes + i)
      i += 1
    new InetSocketAddress(InetAddress.getByAddress(bytes), port)

/** TODO: Good idea?
  * The kernel-assigned local address of `fd` - the actual bound address and
  * (for an outbound connect) the ephemeral port the kernel picked.
  *
  * getsockname is a plain, synchronous POSIX call - not an io_uring op, no
  * suspension anywhere in here - so this only needs to live for one call on
  * this thread's own stack. Zone-scoped allocation is the right tool, not
  * malloc (nothing to explicitly free) and not a GC array (no need to
  * involve the collector for memory that's dead before this function
  * returns).
  */
private[uring] def getLocalAddress(fd: Int): SocketAddress =
  Zone.acquire: zone =>
    val addrLen = alloc[posixSocket.socklen_t]()(using zone)
    val addr = alloc[Byte](sizeof[sockaddr_in6])(using zone)
    !addrLen = sizeof[sockaddr_in6].toUInt
    if posixSocket.getsockname(fd, addr.asInstanceOf[Ptr[posixSocket.sockaddr]], addrLen) < 0 then
      throw IOException(errno.errno)
    parseSockAddr(addr)

private[uring] def applySocketOption(fd: Int, opt: SocketOption): Unit =
  Zone.acquire: zone =>
    def setInt(level: CInt, name: CInt, v: Int): Unit =
      val p = alloc[CInt]()(using zone)
      !p = v
      if posixSocket.setsockopt(fd, level, name, p.asInstanceOf[Ptr[Byte]], sizeof[CInt].toUInt) < 0 then
        throw IOException(errno.errno)
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
        then throw IOException(errno.errno)

private[uring] def copyToNative(buf: Buffer, dst: Ptr[Byte], len: Int): Unit =
  val arr = new Array[Byte](len)
  buf.get(arr)
  var i = 0
  while i < len do
    !(dst + i) = arr(i)
    i += 1

private[uring] def copyFromNative(src: Ptr[Byte], len: Int, buf: Buffer): Unit =
  var i = 0
  while i < len do
    buf.put(!(src + i))
    i += 1

private[uring] def nativePtrInto(buf: Buffer): Option[Ptr[Byte]] =
  if buf.hasArray then
    val arr = buf.array()
    val offset = buf.arrayOffset() + buf.position()
    Some(arr.asInstanceOf[ByteArray].at(offset))
  else None

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
    val len = buf.remaining()
    nativePtrInto(buf) match
      case Some(ptr) =>
        val res = submitAwait(scheduler)(buf.array())(sqe => io_uring_prep_recv(sqe, fd, ptr, len.toUSize, 0))
        if res < 0 then throw IOException(-res)
        else if res == 0 then either.error(Error.EOF)
        else buf.position(buf.position() + res)
      case None =>
        val scratchArr = new Array[Byte](len)
        val scratch = scratchArr.asInstanceOf[ByteArray].at(0)
        val res = submitAwait(scheduler)(scratchArr)(sqe => io_uring_prep_recv(sqe, fd, scratch, len.toUSize, 0))
        if res < 0 then throw IOException(-res)
        else if res == 0 then either.error(Error.EOF)
        else copyFromNative(scratch, res, buf)

  override def writeBuf(buf: Buffer)(using Async): Result[Unit] = either:
    checkOpen()
    val len = buf.remaining()
    nativePtrInto(buf) match
      case Some(ptr) =>
        var sent = 0
        while sent < len do
          val res = submitAwait(scheduler)(buf.array())(sqe =>
            io_uring_prep_send(sqe, fd, ptr + sent, (len - sent).toUSize, posixSocket.MSG_NOSIGNAL)
          )
          if res < 0 then throw IOException(-res)
          else sent += res
        buf.position(buf.position() + sent)
      case None =>
        val scratchArr = new Array[Byte](len)
        val scratch = scratchArr.asInstanceOf[ByteArray].at(0)
        copyToNative(buf, scratch, len)
        var sent = 0
        while sent < len do
          val res = submitAwait(scheduler)(scratchArr)(sqe =>
            io_uring_prep_send(sqe, fd, scratch + sent, (len - sent).toUSize, posixSocket.MSG_NOSIGNAL)
          )
          if res < 0 then throw IOException(-res)
          else sent += res

class UringTcpListener private[uring] (
    val fd: Int,
    scheduler: UringPerThreadScheduler,
    override val localAddress: SocketAddress
) extends net.TcpListener:
  type Stream = UringTcpStream

  private var closed = false

  private def checkOpen(): Unit = synchronized:
    if closed then throw new ClosedChannelException()

  override def close(): Unit = synchronized:
    if !closed then
      closed = true
      unistd.close(fd)

  override def accept()(using Async): Result[Stream] = either:
    checkOpen()
    val addrLenArr = new Array[Byte](sizeof[posixSocket.socklen_t].toInt)
    val addrLen = addrLenArr.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[posixSocket.socklen_t]]
    val addrArr = new Array[Byte](sizeof[sockaddr_in6].toInt)
    val addr = addrArr.asInstanceOf[ByteArray].at(0)
    !addrLen = sizeof[sockaddr_in6].toUInt
    val clientFd = submitAwait(scheduler)((addrLenArr, addrArr))(sqe =>
      io_uring_prep_accept(sqe, fd, addr.asInstanceOf[Ptr[posixSocket.sockaddr]], addrLen, 0)
    )
    if clientFd < 0 then throw IOException(-clientFd)
    closeFdOnFailure(clientFd):
      UringTcpStream(clientFd, scheduler, localAddress, parseSockAddr(addr))

class UringUdpSocket private[uring] (
    val fd: Int,
    scheduler: UringPerThreadScheduler,
    override val localAddress: SocketAddress
) extends net.UdpSocket:

  private var closed = false

  private def checkOpen(): Unit = synchronized:
    if closed then throw new ClosedChannelException()

  override def close(): Unit = synchronized:
    if !closed then
      closed = true
      unistd.close(fd)

  override def sendTo(buf: Buffer, target: SocketAddress)(using Async): Result[Unit] = either:
    checkOpen()
    val inetTarget = target.asInstanceOf[InetSocketAddress]
    val len = buf.remaining()
    val addrSize = sockaddrSize(inetTarget.getAddress)
    val sockAddrArr = new Array[Byte](addrSize.toInt)
    val sockAddr = sockAddrArr.asInstanceOf[ByteArray].at(0)
    fillSockAddr(sockAddr, inetTarget)

    val (dataPtr, dataOwner) = nativePtrInto(buf) match
      case Some(ptr) => (ptr, buf.array())
      case None =>
        val scratchArr = new Array[Byte](len)
        val scratch = scratchArr.asInstanceOf[ByteArray].at(0)
        copyToNative(buf, scratch, len)
        (scratch, scratchArr)

    val iovecArr = new Array[Byte](sizeof[iovec].toInt)
    val iov = iovecArr.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[iovec]]
    iov.iov_base = dataPtr
    iov.iov_len = len.toUSize

    val msgArr = new Array[Byte](sizeof[msghdr].toInt)
    val msg = msgArr.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[msghdr]]
    msg.msg_name = sockAddr
    msg.msg_namelen = addrSize
    msg.msg_iov = iov
    msg.msg_iovlen = 1.toUSize
    msg.msg_control = null
    msg.msg_controllen = 0.toUSize
    msg.msg_flags = 0

    val res = submitAwait(scheduler)((sockAddrArr, iovecArr, msgArr, dataOwner))(sqe =>
      io_uring_prep_sendmsg(sqe, fd, msg, posixSocket.MSG_NOSIGNAL)
    )
    if res < 0 then throw IOException(-res)
    buf.position(buf.position() + res)

  override def receiveFrom(buf: Buffer)(using Async): Result[SocketAddress] = either:
    checkOpen()
    val len = buf.remaining()
    val addrArr = new Array[Byte](sizeof[sockaddr_in6].toInt)
    val addr = addrArr.asInstanceOf[ByteArray].at(0)

    val (dataPtr, dataOwner, scratchArr) = nativePtrInto(buf) match
      case Some(ptr) => (ptr, buf.array(), null: Array[Byte])
      case None =>
        val arr = new Array[Byte](len)
        (arr.asInstanceOf[ByteArray].at(0), arr, arr)

    val iovecArr = new Array[Byte](sizeof[iovec].toInt)
    val iov = iovecArr.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[iovec]]
    iov.iov_base = dataPtr
    iov.iov_len = len.toUSize

    val msgArr = new Array[Byte](sizeof[msghdr].toInt)
    val msg = msgArr.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[msghdr]]
    msg.msg_name = addr
    msg.msg_namelen = sizeof[sockaddr_in6].toUInt
    msg.msg_iov = iov
    msg.msg_iovlen = 1.toUSize
    msg.msg_control = null
    msg.msg_controllen = 0.toUSize
    msg.msg_flags = 0

    val res = submitAwait(scheduler)((addrArr, iovecArr, msgArr, dataOwner))(sqe =>
      io_uring_prep_recvmsg(sqe, fd, msg, 0)
    )
    if res < 0 then throw IOException(-res)
    if scratchArr != null then copyFromNative(scratchArr.asInstanceOf[ByteArray].at(0), res, buf)
    else buf.position(buf.position() + res)
    parseSockAddr(addr)

trait UringUdpSupport(scheduler: UringPerThreadScheduler) extends net.UdpSupport:
  type Socket = UringUdpSocket

  override def bind(address: SocketAddress, options: Seq[SocketOption])(using
      Async
  ): Result[Socket] = either:
    val inetAddr = address.asInstanceOf[InetSocketAddress]
    val family =
      if inetAddr.getAddress.isInstanceOf[Inet6Address] then posixSocket.AF_INET6 else posixSocket.AF_INET
    val fd = posixSocket.socket(family, posixSocket.SOCK_DGRAM, 0)
    if fd < 0 then throw IOException(errno.errno)
    closeFdOnFailure(fd):
      options.foreach(applySocketOption(fd, _))
      Zone.acquire: zone =>
        val addrSize = sockaddrSize(inetAddr.getAddress)
        val sockAddr = alloc[Byte](addrSize)(using zone)
        fillSockAddr(sockAddr, inetAddr)
        if posixSocket.bind(fd, sockAddr.asInstanceOf[Ptr[posixSocket.sockaddr]], addrSize) < 0 then
          throw IOException(errno.errno)
      UringUdpSocket(fd, scheduler, getLocalAddress(fd))

trait UringTcpSupport(scheduler: UringPerThreadScheduler) extends net.TcpSupport:
  type Stream = UringTcpStream
  type Listener = UringTcpListener

  override def connect(address: SocketAddress, options: Seq[SocketOption])(using
      Async
  ): Result[Stream] = either:
    val inetAddr = address.asInstanceOf[InetSocketAddress]
    val family =
      if inetAddr.getAddress.isInstanceOf[Inet6Address] then posixSocket.AF_INET6 else posixSocket.AF_INET
    val fd =
      submitAwait(scheduler)(null)(sqe => io_uring_prep_socket(sqe, family, posixSocket.SOCK_STREAM, 0, 0.toUInt))
    if fd < 0 then throw IOException(-fd)
    closeFdOnFailure(fd):
      options.foreach(applySocketOption(fd, _))
      val addrSize = sockaddrSize(inetAddr.getAddress)
      val sockAddrArr = new Array[Byte](addrSize.toInt)
      val sockAddr = sockAddrArr.asInstanceOf[ByteArray].at(0)
      fillSockAddr(sockAddr, inetAddr)
      val res = submitAwait(scheduler)(sockAddrArr)(sqe =>
        io_uring_prep_connect(sqe, fd, sockAddr.asInstanceOf[Ptr[posixSocket.sockaddr]], addrSize)
      )
      if res < 0 then throw IOException(-res)
      UringTcpStream(fd, scheduler, localAddress = getLocalAddress(fd), remoteAddress = inetAddr)

  override def listen(address: SocketAddress, options: Seq[SocketOption])(using
      Async
  ): Result[Listener] = either:
    val inetAddr = address.asInstanceOf[InetSocketAddress]
    val family =
      if inetAddr.getAddress.isInstanceOf[Inet6Address] then posixSocket.AF_INET6 else posixSocket.AF_INET
    val fd = posixSocket.socket(family, posixSocket.SOCK_STREAM, 0)
    if fd < 0 then throw IOException(errno.errno)
    closeFdOnFailure(fd):
      options.foreach(applySocketOption(fd, _))
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
        val addrSize = sockaddrSize(inetAddr.getAddress)
        val sockAddr = alloc[Byte](addrSize)(using zone)
        fillSockAddr(sockAddr, inetAddr)
        if posixSocket.bind(fd, sockAddr.asInstanceOf[Ptr[posixSocket.sockaddr]], addrSize) < 0 then
          throw IOException(errno.errno)
      if posixSocket.listen(fd, 128) < 0 then throw IOException(errno.errno)
      UringTcpListener(fd, scheduler, getLocalAddress(fd))
