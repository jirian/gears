package gears.io.net

import gears.async.AsyncOperations
import gears.async.Future
import gears.async.io.Io
import gears.io.http.Http

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.concurrent.duration.DurationInt

import munit.FunSuite

class IdleReadSuite extends FunSuite {
  // - awaits go outside munit's assert: it holds the suite's lock, which a suspension would take to another thread

  test("an idle reader lets go of its buffers past the grace, then reads through the pool, and sees the end") {
    Http.run {
      val io = summon[Io[DefaultCapabilities]]
      val listener = Tcp.listen("127.0.0.1", 0)
      val client = Tcp.connect("127.0.0.1", Tcp.port(listener.localAddress))
      val server = listener.accept()
      val pool = io.backend.pool(4096, 4)
      try {
        val input = new BufferedInput(server, 4096)
        @volatile var released = false
        val waiting = Future(input.awaitMore(5.seconds, 100.millis, pool, () => released = true))
        AsyncOperations.sleep(300)
        assert(released, "not released past the grace")
        assertEquals(input.holding, 0)
        client.write(ByteBuffer.wrap("GET".getBytes(StandardCharsets.ISO_8859_1)))
        val waitingResult = waiting.await
        assert(waitingResult)
        assertEquals(new String(input.readExactly(3), StandardCharsets.ISO_8859_1), "GET")
        assertEquals(input.holding, 4096)
        assertEquals(pool.available, 4)
        val ending = Future(input.awaitMore(5.seconds, 50.millis, pool, () => ()))
        AsyncOperations.sleep(150)
        client.close()
        val endingResult = ending.await
        assert(!endingResult)
        assertEquals(pool.available, 4)
      } finally {
        server.close()
        listener.close()
        pool.close()
      }
    }
  }

  test("a reader that hears from its peer within the grace keeps its buffer and does not touch the pool") {
    Http.run {
      val io = summon[Io[DefaultCapabilities]]
      val listener = Tcp.listen("127.0.0.1", 0)
      val client = Tcp.connect("127.0.0.1", Tcp.port(listener.localAddress))
      val server = listener.accept()
      val pool = io.backend.pool(4096, 1)
      try {
        val input = new BufferedInput(server, 4096)
        val waiting = Future(input.awaitMore(5.seconds, 2.seconds, pool, () => fail("released within the grace")))
        AsyncOperations.sleep(50)
        client.write(ByteBuffer.wrap("x".getBytes(StandardCharsets.ISO_8859_1)))
        val waitingResult = waiting.await
        assert(waitingResult)
        assertEquals(input.holding, 4096)
      } finally {
        client.close()
        server.close()
        listener.close()
        pool.close()
      }
    }
  }
}
