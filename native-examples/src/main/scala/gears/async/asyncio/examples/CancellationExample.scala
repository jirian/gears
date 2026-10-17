package gears.async.asyncio.examples

import gears.async.*
import gears.async.asyncio.uring.UringPerThreadSupport
import gears.async.net.TcpSupport

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousCloseException
import java.util.concurrent.{CancellationException, TimeoutException}
import scala.concurrent.duration.*

def cancellationExample(): Unit = withUring:
  given TcpSupport = summon[UringPerThreadSupport].tcpSupport
  Async.blocking:
    val listener = orFail("listen")(TcpSupport.listen(InetSocketAddress("127.0.0.1", 0)))
    try
      val accepted = Future(orFail("accept")(listener.accept()))
      val client = orFail("connect")(TcpSupport.connect(listener.localAddress))
      val server = accepted.await
      try
        val buf = ByteBuffer.allocate(64)
        try
          withTimeout(200.millis)(client.readBuf(buf))
          fail("read before the timeout")
        catch
          case _: TimeoutException => println("a read with nothing to read timed out after 200 ms, and was cancelled")

        orFail("write")(server.writeBuf(bytes("still usable")))
        orFail("read")(client.readBuf(buf))
        println(s"the same connection then reads '${new String(buf.array(), 0, buf.position())}'")

        val pending = Future(client.readBuf(ByteBuffer.allocate(64)))
        AsyncOperations.sleep(50)
        pending.cancel()
        pending.awaitResult match
          case scala.util.Failure(_: CancellationException) =>
            println("cancelling the future of a pending read cancels the read")
          case other => fail(s"cancelling a pending read gave $other")

        val blocked = Future(client.readBuf(ByteBuffer.allocate(64)))
        AsyncOperations.sleep(50)
        client.close()
        blocked.awaitResult match
          case scala.util.Failure(e: AsynchronousCloseException) =>
            println(s"closing the stream fails its pending read with ${e.getClass.getSimpleName}")
          case other => fail(s"closing under a pending read gave $other")
      finally
        client.close()
        server.close()
    finally listener.close()
