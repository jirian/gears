package consumer.driver

import gears.async.*
import gears.async.default.given
import gears.io.reactor.{IoGuard, ReactorDriver, SingleReactorDriver, UnixSocketAddress, tcpSupport}
import gears.io.uring.UringReactor

import java.util.concurrent.CountDownLatch

/** Runs every op as a blocking op that ignores interrupts until `release`, so a cancelled op holds its handle open. */
final class HoldingDriver(val inner: ReactorDriver) extends ReactorDriver:
  type R = inner.R
  private val held = CountDownLatch(1)

  def release(): Unit = held.countDown()

  def await(guard: IoGuard | Null)(build: (r: R) => r.Op)(using Async): Unit =
    inner.await(guard)(r =>
      r.ops.blocking(() =>
        while held.getCount() > 0 do
          try held.await()
          catch case _: InterruptedException => ()
      )
    )

class DeferredCloseSuite extends munit.FunSuite:
  test("closing a Unix listener with an op in flight leaves a newer listener's path alone"):
    val inner = SingleReactorDriver(() => UringReactor.open())
    try
      val driver = HoldingDriver(inner)
      val path = s"/tmp/gears-deferred-close-${scala.scalanative.posix.unistd.getpid()}.sock"
      Async.blocking:
        val old = driver.tcpSupport.listen(UnixSocketAddress(path), Nil).toOption.get
        val accepting = Future(old.accept())
        AsyncOperations.sleep(50)
        // The held op keeps the old descriptor open, so its close is deferred until the release below.
        old.close()
        val newer = inner.tcpSupport.listen(UnixSocketAddress(path), Nil).toOption.get
        try
          driver.release()
          accepting.awaitResult
          assert(java.io.File(path).exists(), "the old listener's deferred close removed the newer listener's path")
        finally newer.close()
    finally inner.close()
