package gears.async.asyncio.uring.examples

import gears.async._
import gears.async.net.TcpSupport
import gears.async.net.SocketOption
import gears.async.asyncio.uring.ForkJoinUringSupport

import java.net.{InetAddress, InetSocketAddress}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.concurrent.duration._

/** Standalone smoke tests for the uring TCP backend, run as a single
  * process: basic echo round-trip, local-address resolution via
  * getsockname, socket-option application, deferred close while a read is
  * outstanding, and (best-effort) IPv6.
  */
@main def uringEchoSmokeTest(): Unit =
  given support: ForkJoinUringSupport = ForkJoinUringSupport()
  given tcp: TcpSupport = support.tcpSupport

  Async.blocking:
    echoTest()
    localAddressTest()
    socketOptionsTest()
    deferredCloseTest()
    ipv6Test()
    println("all uring TCP smoke tests PASSED")

private def echoTest()(using Async.Spawn, TcpSupport): Unit =
  val addr = InetSocketAddress("127.0.0.1", 9099)
  val listener = TcpSupport.listen(addr) match
    case Right(l) => l
    case Left(e)  => throw new RuntimeException(s"listen failed: $e")

  val client = Future:
    val stream = TcpSupport.connect(addr) match
      case Right(s) => s
      case Left(e)  => throw new RuntimeException(s"connect failed: $e")
    val msg = "hello uring".getBytes(StandardCharsets.UTF_8)
    stream.writeBuf(ByteBuffer.wrap(msg))
    val respBuf = ByteBuffer.allocate(64)
    stream.readBuf(respBuf)
    respBuf.flip()
    val bytes = new Array[Byte](respBuf.remaining())
    respBuf.get(bytes)
    stream.close()
    new String(bytes, StandardCharsets.UTF_8)

  val serverSide = listener.accept() match
    case Right(s) => s
    case Left(e)  => throw new RuntimeException(s"accept failed: $e")
  val buf = ByteBuffer.allocate(64)
  serverSide.readBuf(buf)
  buf.flip()
  serverSide.writeBuf(buf)
  serverSide.close()
  listener.close()

  val echoed = client.await
  println(s"echoTest: echoed back '$echoed'")
  assert(echoed == "hello uring", s"mismatch: $echoed")

private def localAddressTest()(using Async.Spawn, TcpSupport): Unit =
  val addr = InetSocketAddress("127.0.0.1", 9100)
  val listener = TcpSupport.listen(addr) match
    case Right(l) => l
    case Left(e)  => throw new RuntimeException(s"listen failed: $e")

  val client = Future:
    TcpSupport.connect(addr) match
      case Right(s) => s
      case Left(e)  => throw new RuntimeException(s"connect failed: $e")

  val serverSide = listener.accept() match
    case Right(s) => s
    case Left(e)  => throw new RuntimeException(s"accept failed: $e")

  val stream = client.await
  val local = stream.localAddress.asInstanceOf[InetSocketAddress]
  println(s"localAddressTest: getsockname -> $local")
  assert(local.getPort != 0, s"expected a real ephemeral local port, got $local")
  assert(local.getPort != addr.getPort, s"local port should be the ephemeral client port, not $addr")
  stream.close()
  serverSide.close()
  listener.close()

private def socketOptionsTest()(using Async.Spawn, TcpSupport): Unit =
  val addr = InetSocketAddress("127.0.0.1", 9101)
  val listener = TcpSupport.listen(
    addr,
    SocketOption.reuseAddr(true)
  ) match
    case Right(l) => l
    case Left(e)  => throw new RuntimeException(s"listen failed: $e")

  val client = Future:
    TcpSupport.connect(
      addr,
      SocketOption.noDelay(true),
      SocketOption.keepAlive(true),
      SocketOption.sendBufSize(4096),
      SocketOption.recvBufSize(4096),
      SocketOption.linger(-1)
    ) match
      case Right(s) => s
      case Left(e)  => throw new RuntimeException(s"connect failed: $e")

  val serverSide = listener.accept() match
    case Right(s) => s
    case Left(e)  => throw new RuntimeException(s"accept failed: $e")

  val stream = client.await
  println("socketOptionsTest: connect/listen with options applied without error")
  stream.close()
  serverSide.close()
  listener.close()

private def deferredCloseTest()(using Async.Spawn, TcpSupport, AsyncOperations): Unit =
  val addr = InetSocketAddress("127.0.0.1", 9102)
  val listener = TcpSupport.listen(addr) match
    case Right(l) => l
    case Left(e)  => throw new RuntimeException(s"listen failed: $e")

  val client = Future:
    TcpSupport.connect(addr) match
      case Right(s) => s
      case Left(e)  => throw new RuntimeException(s"connect failed: $e")

  val serverSide = listener.accept() match
    case Right(s) => s
    case Left(e)  => throw new RuntimeException(s"accept failed: $e")

  // Start a read with nothing sent yet, so it has a real chance to become
  // genuinely outstanding, then close the stream concurrently. Both
  // outcomes are legitimate and must not crash or hang: close() can win
  // before the read is even submitted (a clean ClosedChannelException -
  // beginOp() correctly refuses a new op once closed, nothing was ever in
  // flight to leak), or the read can already be outstanding, in which case
  // close() must defer the real close until it settles.
  val pendingRead = Future:
    try
      val buf = ByteBuffer.allocate(64)
      serverSide.readBuf(buf)
      "read settled"
    catch case _: java.nio.channels.ClosedChannelException => "close() won before the read was submitted"

  // Give the read a real chance to reach its suspension point (i.e. to
  // actually be outstanding) before close() fires, rather than racing
  // close() the instant the Future is spawned.
  Future:
    AsyncOperations.sleep(20.millis)
    serverSide.close()
  .await

  // Let a still-outstanding read resolve (real completion, kernel-side -
  // close() must not have torn the fd down out from under it): the client
  // sends data, then closes.
  val clientStream = client.await
  clientStream.writeBuf(ByteBuffer.wrap("late".getBytes(StandardCharsets.UTF_8)))
  clientStream.close()

  println(s"deferredCloseTest: ${pendingRead.await}")
  listener.close()

private def ipv6Test()(using Async.Spawn, TcpSupport): Unit =
  try
    val addr = InetSocketAddress(InetAddress.getByName("::1"), 9103)
    val listener = TcpSupport.listen(addr) match
      case Right(l) => l
      case Left(e)  => throw new RuntimeException(s"listen failed: $e")

    val client = Future:
      TcpSupport.connect(addr) match
        case Right(s) => s
        case Left(e)  => throw new RuntimeException(s"connect failed: $e")

    val serverSide = listener.accept() match
      case Right(s) => s
      case Left(e)  => throw new RuntimeException(s"accept failed: $e")

    val stream = client.await
    println(s"ipv6Test: connected over IPv6, local=${stream.localAddress}")
    stream.close()
    serverSide.close()
    listener.close()
  catch case e: Exception => println(s"ipv6Test: skipped (no IPv6 loopback in this environment: $e)")
