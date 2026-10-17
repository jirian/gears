package consumer.driver

import gears.async.*
import gears.async.asyncio.Error
import gears.async.asyncio.reactor.{IoGuard, ReactorDriver, SingleReactorDriver, tcpSupport}
import gears.async.default.given
import gears.async.net.TcpSupport
import gears.asyncio.Reactor

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicInteger

final class CountingDriver(inner: ReactorDriver) extends ReactorDriver:
  val submitted = AtomicInteger()

  def await(guard: IoGuard | Null)(build: (r: Reactor) => r.Op)(using Async): Unit =
    submitted.incrementAndGet()
    inner.await(guard)(build)

class ExternalDriverSuite extends munit.FunSuite:
  test("a driver implemented outside gears runs the network layer"):
    val inner = SingleReactorDriver(() => gears.asyncio.uring.UringReactor.open())
    try
      val driver = CountingDriver(inner)
      given TcpSupport = driver.tcpSupport
      Async.blocking:
        val listener = TcpSupport.listen(InetSocketAddress("127.0.0.1", 0)).toOption.get
        try
          val server = Future:
            val conn = listener.accept().toOption.get
            try conn.writeBuf(ByteBuffer.wrap("through the consumer's driver".getBytes(StandardCharsets.UTF_8)))
            finally conn.close()
          val client = TcpSupport.connect(listener.localAddress).toOption.get
          try
            val buf = ByteBuffer.allocate(64)
            while client.readBuf(buf) != Left(Error.EOF) do ()
            buf.flip()
            assertEquals(StandardCharsets.UTF_8.decode(buf).toString, "through the consumer's driver")
          finally client.close()
          server.await
        finally listener.close()
      assert(driver.submitted.get() >= 4, "accept, connect, write, and reads all went through the consumer's driver")
    finally inner.close()
