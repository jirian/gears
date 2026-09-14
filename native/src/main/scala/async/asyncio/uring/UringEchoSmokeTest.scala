package gears.async.asyncio.uring.examples

import gears.async._
import gears.async.net.TcpSupport
import gears.async.asyncio.uring.ForkJoinUringSupport

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

/** Standalone round-trip smoke test for the uring TCP backend: listen,
  * connect, write, read, echo, close - exercises SOCKET/CONNECT/ACCEPT/
  * SEND/RECV/CLOSE all in one run.
  */
@main def uringEchoSmokeTest(): Unit =
  given support: ForkJoinUringSupport = ForkJoinUringSupport()
  given tcp: TcpSupport = support.tcpSupport

  Async.blocking:
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
    println(s"Echoed back: $echoed")
    assert(echoed == "hello uring", s"mismatch: $echoed")
    println("uring TCP smoke test PASSED")
