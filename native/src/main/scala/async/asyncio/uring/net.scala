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

/** Submits one op and suspends the current fiber until its single CQE
  * arrives, returning the raw `res` field. Unlike the epoll backend there is
  * no EAGAIN-retry loop: one submit, one completion.
  *
  * io_uring guarantees exactly one CQE per submitted SQE, no matter what
  * happens to it - including cancellation. So there is exactly one settler
  * for this op's `Resolver`, not two racing ones: the completion callback
  * below. `onCancel` never touches `resolver` itself; it only asks the
  * kernel to finish the op early (best-effort, fire-and-forget - we don't
  * wait for the cancel op's own, separate CQE). Per `Resolver.onCancel`'s
  * contract the handler only has to *eventually* settle the future, not
  * synchronously - so leaving that to the one guaranteed completion is
  * enough, and `Async.group`'s own cancel-then-`waitCompletion` split
  * already expects settling to take a real amount of time.
  */
private[uring] def submitAwait(ring: UringRing)(prep: Ptr[io_uring_sqe] => Unit)(using
    Async
): Int =
  Future
    .withResolver[Int]: resolver =>
      val userData = ring.submit(prep): res =>
        if res == -errno.ECANCELED then resolver.rejectAsCancelled()
        else resolver.resolve(res)
      resolver.onCancel: () =>
        // Best-effort: a full submission queue must not surface as an
        // exception from Cancellable.cancel() into whatever unrelated code
        // (e.g. Async.group's teardown) is cancelling us.
        try ring.submit(sqe => io_uring_prep_cancel64(sqe, userData, 0))(_ => ())
        catch case _: IOException => ()
    .link()
    .await

/** Size, in bytes, of the sockaddr struct needed to hold `addr` - either
  * `sockaddr_in` or `sockaddr_in6`.
  */
private[uring] def sockaddrSize(addr: InetAddress): CUnsignedInt =
  addr match
    case _: Inet6Address => sizeof[sockaddr_in6].toUInt
    case _                => sizeof[sockaddr_in].toUInt

/** Fills `out` (which must be at least `sockaddrSize(addr.getAddress)`
  * bytes) with either a `sockaddr_in` or `sockaddr_in6`, matching whichever
  * family `addr` actually is.
  */
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

/** Parses a `sockaddr_in`/`sockaddr_in6` (distinguished by its leading
  * `sa_family_t`, the same first field in both) back into an
  * `InetSocketAddress`. `addr` must have been filled by the kernel (accept,
  * getsockname, ...) or by `fillSockAddr`.
  */
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

/** The kernel-assigned local address of `fd` - the actual bound address and
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

/** Applies one `SocketOption` via `setsockopt`. */
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

/** A stable pointer straight into `buf`'s own backing array at its current
  * position, with no copy - safe to hand to io_uring directly because (a)
  * this GC (immix, as shipped by scala-native 0.5.12) never relocates live
  * objects - checked directly against its actual source, no forwarding
  * pointers/compaction anywhere in it - and (b) a reference kept alive only
  * by a suspended fiber's captured continuation state still stays visible
  * to the collector's root scan - verified empirically under real,
  * concurrent GC pressure (GcContinuationStress, DirectBufferExperiment).
  * `None` for a buffer with no accessible backing array (e.g. a direct
  * `ByteBuffer`), which falls back to the malloc'd-scratch path below -
  * untested for that case, and every buffer this codebase actually
  * constructs (`ByteBuffer.allocate`) is array-backed anyway.
  */
private[uring] def nativePtrInto(buf: Buffer): Option[Ptr[Byte]] =
  if buf.hasArray then
    val arr = buf.array()
    val offset = buf.arrayOffset() + buf.position()
    Some(arr.asInstanceOf[ByteArray].at(offset))
  else None

class UringTcpStream private[uring] (
    val fd: Int,
    ring: UringRing,
    override val localAddress: SocketAddress,
    override val remoteAddress: SocketAddress
) extends net.TcpStream:

  // Guards against this stream being used after close() - not against any
  // hazard to an op already in flight. io_uring resolves sqe->fd to a
  // refcounted struct file via fget() synchronously at submission time
  // (io_submit_sqes -> io_issue_sqe -> io_assign_file -> io_file_get_normal
  // in the kernel) and holds that reference for the whole request's
  // lifetime, so an already-submitted op is immune to whatever the fd
  // *number* gets reused for afterward - even though POSIX guarantees
  // close() frees the lowest-numbered fd for immediate reuse, which is the
  // routine case under load, not a rare one. The only real hazard is *this*
  // stream object submitting a *new* op against a number it no longer owns,
  // which this check alone prevents - no need to defer the close() syscall
  // itself until outstanding ops settle.
  private var closed = false

  private def checkOpen(): Unit = synchronized:
    if closed then throw new ClosedChannelException()

  // Closeable#close() has no Async to await IORING_OP_CLOSE's completion on,
  // so this stays a plain synchronous syscall.
  override def close(): Unit = synchronized:
    if !closed then
      closed = true
      unistd.close(fd)

  override def readBuf(buf: Buffer)(using Async): Result[Unit] = either:
    checkOpen()
    val len = buf.remaining()
    nativePtrInto(buf) match
      case Some(ptr) =>
        // Straight into buf's own backing array - no scratch buffer.
        val res = submitAwait(ring)(sqe => io_uring_prep_recv(sqe, fd, ptr, len.toUSize, 0))
        if res < 0 then throw IOException(-res)
        else if res == 0 then either.error(Error.EOF)
        else buf.position(buf.position() + res)
      case None =>
        // Fallback for a buffer with no accessible backing array (e.g. a
        // direct ByteBuffer) - nothing in this codebase constructs one
        // today, but Reader/Writer's contract doesn't rule it out. GC-
        // managed scratch space, not malloc'd: `scratchArr` is a local
        // referenced both before and after submitAwait's suspension, the
        // same continuation-rooted pattern already verified for the
        // array-backed case above.
        val scratchArr = new Array[Byte](len)
        val scratch = scratchArr.asInstanceOf[ByteArray].at(0)
        val res = submitAwait(ring)(sqe => io_uring_prep_recv(sqe, fd, scratch, len.toUSize, 0))
        if res < 0 then throw IOException(-res)
        else if res == 0 then either.error(Error.EOF)
        else copyFromNative(scratch, res, buf)

  override def writeBuf(buf: Buffer)(using Async): Result[Unit] = either:
    checkOpen()
    val len = buf.remaining()
    nativePtrInto(buf) match
      case Some(ptr) =>
        // Straight from buf's own backing array - no scratch buffer.
        var sent = 0
        while sent < len do
          val res = submitAwait(ring)(sqe =>
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
          val res = submitAwait(ring)(sqe =>
            io_uring_prep_send(sqe, fd, scratch + sent, (len - sent).toUSize, posixSocket.MSG_NOSIGNAL)
          )
          if res < 0 then throw IOException(-res)
          else sent += res

class UringTcpListener private[uring] (
    val fd: Int,
    ring: UringRing,
    override val localAddress: SocketAddress
) extends net.TcpListener:
  type Stream = UringTcpStream

  // Same reasoning as UringTcpStream.
  private var closed = false

  private def checkOpen(): Unit = synchronized:
    if closed then throw new ClosedChannelException()

  override def close(): Unit = synchronized:
    if !closed then
      closed = true
      unistd.close(fd)

  override def accept()(using Async): Result[Stream] = either:
    checkOpen()
    // GC-managed: both locals are referenced before AND after submitAwait's
    // suspension, the same continuation-rooted pattern verified for buffers.
    val addrLenArr = new Array[Byte](sizeof[posixSocket.socklen_t].toInt)
    val addrLen = addrLenArr.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[posixSocket.socklen_t]]
    val addrArr = new Array[Byte](sizeof[sockaddr_in6].toInt)
    val addr = addrArr.asInstanceOf[ByteArray].at(0)
    !addrLen = sizeof[sockaddr_in6].toUInt
    val clientFd = submitAwait(ring)(sqe =>
      io_uring_prep_accept(sqe, fd, addr.asInstanceOf[Ptr[posixSocket.sockaddr]], addrLen, 0)
    )
    if clientFd < 0 then throw IOException(-clientFd)
    UringTcpStream(clientFd, ring, localAddress, parseSockAddr(addr))

class UringUdpSocket private[uring] (
    val fd: Int,
    ring: UringRing,
    override val localAddress: SocketAddress
) extends net.UdpSocket:

  // Same reasoning as UringTcpStream.
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
    // GC-managed, all referenced both before and after submitAwait's
    // suspension (the kernel reads the whole msghdr/iovec/sockaddr chain
    // mid-flight) - same continuation-rooted pattern as the TCP path.
    val sockAddrArr = new Array[Byte](addrSize.toInt)
    val sockAddr = sockAddrArr.asInstanceOf[ByteArray].at(0)
    fillSockAddr(sockAddr, inetTarget)

    val dataPtr = nativePtrInto(buf) match
      case Some(ptr) => ptr
      case None =>
        // See readBuf/writeBuf's fallback: no buffer in this codebase
        // actually lacks a backing array, but the contract allows one.
        val scratchArr = new Array[Byte](len)
        val scratch = scratchArr.asInstanceOf[ByteArray].at(0)
        copyToNative(buf, scratch, len)
        scratch

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

    val res = submitAwait(ring)(sqe => io_uring_prep_sendmsg(sqe, fd, msg, posixSocket.MSG_NOSIGNAL))
    if res < 0 then throw IOException(-res)
    buf.position(buf.position() + res)

  override def receiveFrom(buf: Buffer)(using Async): Result[SocketAddress] = either:
    checkOpen()
    val len = buf.remaining()
    // Sized for either family, like accept()'s addr buffer - the actual
    // family is read back from the leading sa_family_t field regardless of
    // how much of this buffer the kernel filled in.
    val addrArr = new Array[Byte](sizeof[sockaddr_in6].toInt)
    val addr = addrArr.asInstanceOf[ByteArray].at(0)

    val (dataPtr, scratchArr) = nativePtrInto(buf) match
      case Some(ptr) => (ptr, null: Array[Byte])
      case None =>
        val arr = new Array[Byte](len)
        (arr.asInstanceOf[ByteArray].at(0), arr)

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

    val res = submitAwait(ring)(sqe => io_uring_prep_recvmsg(sqe, fd, msg, 0))
    if res < 0 then throw IOException(-res)
    if scratchArr != null then copyFromNative(scratchArr.asInstanceOf[ByteArray].at(0), res, buf)
    else buf.position(buf.position() + res)
    parseSockAddr(addr)

trait UringUdpSupport(ring: UringRing) extends net.UdpSupport:
  type Socket = UringUdpSocket

  override def bind(address: SocketAddress, options: Seq[SocketOption])(using
      Async
  ): Result[Socket] = either:
    val inetAddr = address.asInstanceOf[InetSocketAddress]
    val family =
      if inetAddr.getAddress.isInstanceOf[Inet6Address] then posixSocket.AF_INET6 else posixSocket.AF_INET
    // No io_uring op for socket/setsockopt/bind on a UDP socket either -
    // same as TCP's listen(), these are cheap synchronous POSIX calls.
    val fd = posixSocket.socket(family, posixSocket.SOCK_DGRAM, 0)
    if fd < 0 then throw IOException(errno.errno)
    options.foreach(applySocketOption(fd, _))
    Zone.acquire: zone =>
      val addrSize = sockaddrSize(inetAddr.getAddress)
      val sockAddr = alloc[Byte](addrSize)(using zone)
      fillSockAddr(sockAddr, inetAddr)
      if posixSocket.bind(fd, sockAddr.asInstanceOf[Ptr[posixSocket.sockaddr]], addrSize) < 0 then
        throw IOException(errno.errno)
    // Unlike TCP's listen(), read back the real bound address via
    // getsockname - the common UDP case of binding to port 0 for an
    // ephemeral port needs this to report anything useful.
    UringUdpSocket(fd, ring, getLocalAddress(fd))

trait UringTcpSupport(ring: UringRing) extends net.TcpSupport:
  type Stream = UringTcpStream
  type Listener = UringTcpListener

  override def connect(address: SocketAddress, options: Seq[SocketOption])(using
      Async
  ): Result[Stream] = either:
    val inetAddr = address.asInstanceOf[InetSocketAddress]
    val family =
      if inetAddr.getAddress.isInstanceOf[Inet6Address] then posixSocket.AF_INET6 else posixSocket.AF_INET
    val fd =
      submitAwait(ring)(sqe => io_uring_prep_socket(sqe, family, posixSocket.SOCK_STREAM, 0, 0.toUInt))
    if fd < 0 then throw IOException(-fd)
    // If anything below fails or is cancelled, close the fd from SOCKET -
    // otherwise it leaks, since nothing else owns it until UringTcpStream is
    // returned.
    try
      options.foreach(applySocketOption(fd, _))
      val addrSize = sockaddrSize(inetAddr.getAddress)
      // GC-managed: sockAddrArr is referenced both before and after
      // submitAwait's suspension (fillSockAddr, then the CONNECT SQE the
      // kernel reads from mid-flight) - same verified pattern.
      val sockAddrArr = new Array[Byte](addrSize.toInt)
      val sockAddr = sockAddrArr.asInstanceOf[ByteArray].at(0)
      fillSockAddr(sockAddr, inetAddr)
      val res = submitAwait(ring)(sqe =>
        io_uring_prep_connect(sqe, fd, sockAddr.asInstanceOf[Ptr[posixSocket.sockaddr]], addrSize)
      )
      if res < 0 then throw IOException(-res)
    catch
      case e: Throwable =>
        unistd.close(fd)
        throw e
    UringTcpStream(fd, ring, localAddress = getLocalAddress(fd), remoteAddress = inetAddr)

  override def listen(address: SocketAddress, options: Seq[SocketOption])(using
      Async
  ): Result[Listener] = either:
    val inetAddr = address.asInstanceOf[InetSocketAddress]
    val family =
      if inetAddr.getAddress.isInstanceOf[Inet6Address] then posixSocket.AF_INET6 else posixSocket.AF_INET
    // No io_uring op for socket/setsockopt/bind/listen - these are cheap,
    // fully synchronous POSIX calls done directly, same as the epoll backend.
    val fd = posixSocket.socket(family, posixSocket.SOCK_STREAM, 0)
    if fd < 0 then throw IOException(errno.errno)
    options.foreach(applySocketOption(fd, _))
    // socket/setsockopt/bind/listen are all plain, synchronous POSIX calls -
    // no io_uring op, no suspension - so this only needs to live for this
    // one call. Zone-scoped, like getLocalAddress; not malloc, and no need
    // to involve the GC for memory that's dead before this returns.
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
    // Read back the real bound address via getsockname, same as UDP's
    // bind() - binding to port 0 for an ephemeral port is a normal thing to
    // do (tests do it constantly to avoid port collisions), and callers
    // need `listener.localAddress` to reflect the port the kernel actually
    // picked, not the port-0 request they passed in.
    UringTcpListener(fd, ring, getLocalAddress(fd))
