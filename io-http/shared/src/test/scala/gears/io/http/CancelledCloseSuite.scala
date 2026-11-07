package gears.io.http

import gears.async.AsyncOperations
import gears.async.Future
import gears.async.io.IpAddress
import gears.async.io.Slot
import gears.io.net.Tcp

import java.util.concurrent.CancellationException
import java.util.concurrent.atomic.AtomicReference

import munit.FunSuite

/** A close in a `finally` runs as its future is cancelled: what every resource in a structured program relies on. */
class CancelledCloseSuite extends FunSuite {

  test("a listener's close in the finally of a cancelled future runs, and frees the port") {
    val (failure, port) = Http.run {
      val seen = new AtomicReference[Throwable | Null](null)
      val listener = Tcp.listen("127.0.0.1", 0)
      val port = Tcp.port(listener.localAddress)
      val f = Future {
        try AsyncOperations.sleep(60_000)
        finally
          try listener.close()
          catch case t: Throwable => seen.set(t)
      }
      AsyncOperations.sleep(50)
      f.cancel()
      f.awaitResult
      (seen.get(), port)
    }
    assertEquals(failure, null)
    Http.run(Tcp.listen("127.0.0.1", port).close()) // free again
  }

  test("the interface's close op, performed the same way, is refused, and the port stays taken") {
    val (failure, port) = Http.run {
      val io = summon[gears.async.io.Io[gears.io.net.DefaultCapabilities]]
      val into = Slot[io.Listener]()
      io.perform(io.backend.ops.listen(IpAddress.IPv4("127.0.0.1", 0), into))
      val listener = into.clear()
      val port = Tcp.port(io.backend.ops.localAddress(listener))
      val seen = new AtomicReference[Throwable | Null](null)
      val f = Future {
        try AsyncOperations.sleep(60_000)
        finally
          try io.perform(io.backend.ops.close(listener))
          catch case t: Throwable => seen.set(t)
      }
      AsyncOperations.sleep(50)
      f.cancel()
      f.awaitResult
      (seen.get(), port)
    }
    assert(failure.isInstanceOf[CancellationException], String.valueOf(failure))
    intercept[java.io.IOException](Http.run(Tcp.listen("127.0.0.1", port).close())) // still bound: it leaked
  }
}
