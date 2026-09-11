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

import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.SocketAddress
import scala.scalanative.libc.stdlib
import scala.scalanative.posix.arpa.inet._
import scala.scalanative.posix.errno
import scala.scalanative.posix.netinet.in.{in_addr, sockaddr_in}
import scala.scalanative.posix.sys.{socket => posixSocket}
import scala.scalanative.posix.unistd
import scala.scalanative.unsafe._
import scala.scalanative.unsigned._

/** Submits one op and suspends the current fiber until its single CQE
  * arrives, returning the raw `res` field. Unlike the epoll backend there is
  * no EAGAIN-retry loop: one submit, one completion.
  */
private[uring] def submitAwait(ring: UringRing)(prep: Ptr[io_uring_sqe] => Unit)(using
    Async
): Int =
  Future
    .withResolver[Int]: resolver =>
      // ASYNC_CANCEL only asks the kernel to complete the original op early
      // (with -ECANCELED) - it doesn't stop the original callback below from
      // firing. Guard so a race between a natural completion and a
      // cancellation can't resolve the same Future twice.
      val settled = new java.util.concurrent.atomic.AtomicBoolean(false)
      val userData = ring.submit(prep): res =>
        if settled.compareAndSet(false, true) then resolver.resolve(res)
      resolver.onCancel: () =>
        if settled.compareAndSet(false, true) then
          // Best-effort: fire the cancellation and don't wait for it to land.
          ring.submit(sqe => io_uring_prep_cancel64(sqe, userData, 0))(_ => ())
          resolver.rejectAsCancelled()
    .link()
    .await

private[uring] def fillSockAddrIn(out: Ptr[sockaddr_in], addr: InetSocketAddress): Unit =
  val bytes = addr.getAddress.getAddress
  if bytes.length != 4 then
    throw new UnsupportedOperationException("uring TCP backend only supports IPv4 for now")
  out._1 = posixSocket.AF_INET.toUShort
  out._2 = htons(addr.getPort.toUShort)
  val addrBytes = out.at3.asInstanceOf[Ptr[Byte]]
  var i = 0
  while i < 4 do
    !(addrBytes + i) = bytes(i)
    i += 1

private[uring] def sockaddrInToInet(addr: Ptr[sockaddr_in]): InetSocketAddress =
  val port = ntohs(addr._2).toInt & 0xffff
  val addrBytes = addr.at3.asInstanceOf[Ptr[Byte]]
  val bytes = new Array[Byte](4)
  var i = 0
  while i < 4 do
    bytes(i) = !(addrBytes + i)
    i += 1
  new InetSocketAddress(InetAddress.getByAddress(bytes), port)

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

class UringTcpStream private[uring] (
    val fd: Int,
    ring: UringRing,
    override val localAddress: SocketAddress,
    override val remoteAddress: SocketAddress
) extends net.TcpStream:

  // Closeable#close() has no Async to await IORING_OP_CLOSE's completion on,
  // so this is a plain synchronous syscall rather than a ring op.
  override def close(): Unit =
    unistd.close(fd)

  override def readBuf(buf: Buffer)(using Async): Result[Unit] = either:
    val len = buf.remaining()
    val scratch = stdlib.malloc(len.toULong).asInstanceOf[Ptr[Byte]]
    try
      val res = submitAwait(ring)(sqe => io_uring_prep_recv(sqe, fd, scratch, len.toULong, 0))
      if res < 0 then throw IOException(-res)
      else if res == 0 then either.error(Error.EOF)
      else copyFromNative(scratch, res, buf)
    finally stdlib.free(scratch)

  override def writeBuf(buf: Buffer)(using Async): Result[Unit] = either:
    val len = buf.remaining()
    val scratch = stdlib.malloc(len.toULong).asInstanceOf[Ptr[Byte]]
    try
      copyToNative(buf, scratch, len)
      var sent = 0
      while sent < len do
        val res = submitAwait(ring)(sqe =>
          io_uring_prep_send(sqe, fd, scratch + sent, (len - sent).toULong, posixSocket.MSG_NOSIGNAL)
        )
        if res < 0 then throw IOException(-res)
        else sent += res
    finally stdlib.free(scratch)

class UringTcpListener private[uring] (
    val fd: Int,
    ring: UringRing,
    override val localAddress: SocketAddress
) extends net.TcpListener:
  type Stream = UringTcpStream

  override def close(): Unit =
    unistd.close(fd)

  override def accept()(using Async): Result[Stream] = either:
    val addrLen = stdlib.malloc(sizeof[posixSocket.socklen_t]).asInstanceOf[Ptr[posixSocket.socklen_t]]
    val addr = stdlib.malloc(sizeof[sockaddr_in]).asInstanceOf[Ptr[sockaddr_in]]
    !addrLen = sizeof[sockaddr_in].toUInt
    try
      val clientFd = submitAwait(ring)(sqe =>
        io_uring_prep_accept(sqe, fd, addr.asInstanceOf[Ptr[posixSocket.sockaddr]], addrLen, 0)
      )
      if clientFd < 0 then throw IOException(-clientFd)
      UringTcpStream(clientFd, ring, localAddress, sockaddrInToInet(addr))
    finally
      stdlib.free(addrLen.asInstanceOf[Ptr[Byte]])
      stdlib.free(addr.asInstanceOf[Ptr[Byte]])

trait UringTcpSupport(ring: UringRing) extends net.TcpSupport:
  type Stream = UringTcpStream
  type Listener = UringTcpListener

  // SocketOption plumbing (SO_SNDBUF, TCP_NODELAY, ...) isn't wired up yet -
  // options are currently ignored. Would need per-option setsockopt calls
  // once we bypass java.net.Socket.
  override def connect(address: SocketAddress, options: Seq[SocketOption])(using
      Async
  ): Result[Stream] = either:
    val inetAddr = address.asInstanceOf[InetSocketAddress]
    val fd =
      submitAwait(ring)(sqe => io_uring_prep_socket(sqe, posixSocket.AF_INET, posixSocket.SOCK_STREAM, 0, 0.toUInt))
    if fd < 0 then throw IOException(-fd)
    val sockAddr = stdlib.malloc(sizeof[sockaddr_in]).asInstanceOf[Ptr[sockaddr_in]]
    try
      fillSockAddrIn(sockAddr, inetAddr)
      val res = submitAwait(ring)(sqe =>
        io_uring_prep_connect(sqe, fd, sockAddr.asInstanceOf[Ptr[posixSocket.sockaddr]], sizeof[sockaddr_in].toUInt)
      )
      if res < 0 then throw IOException(-res)
    finally stdlib.free(sockAddr.asInstanceOf[Ptr[Byte]])
    // TODO: local ephemeral port requires getsockname; not implemented yet.
    UringTcpStream(fd, ring, localAddress = inetAddr, remoteAddress = inetAddr)

  override def listen(address: SocketAddress, options: Seq[SocketOption])(using
      Async
  ): Result[Listener] = either:
    val inetAddr = address.asInstanceOf[InetSocketAddress]
    // No io_uring op for socket/setsockopt/bind/listen - these are cheap,
    // fully synchronous POSIX calls done directly, same as the epoll backend.
    val fd = posixSocket.socket(posixSocket.AF_INET, posixSocket.SOCK_STREAM, 0)
    if fd < 0 then throw IOException(errno.errno)
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
      val sockAddr = alloc[sockaddr_in]()(using zone)
      fillSockAddrIn(sockAddr, inetAddr)
      if posixSocket.bind(fd, sockAddr.asInstanceOf[Ptr[posixSocket.sockaddr]], sizeof[sockaddr_in].toUInt) < 0
      then throw IOException(errno.errno)
    if posixSocket.listen(fd, 128) < 0 then throw IOException(errno.errno)
    UringTcpListener(fd, ring, inetAddr)
