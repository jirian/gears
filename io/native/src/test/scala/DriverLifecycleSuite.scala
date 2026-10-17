package gears.io.reactor

import gears.async.*
import gears.async.default.given
import gears.asyncio.Reactor
import gears.asyncio.uring.UringReactor
import gears.io.uring.UringPerThreadSupport

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CancellationException, CountDownLatch}
import scala.concurrent.duration.*

class DriverLifecycleSuite extends munit.FunSuite:
  test("physical close waits until the last registered operation acknowledges cancellation"):
    val guard = IoGuard()
    val closed = AtomicBoolean(false)
    val cancelled = AtomicBoolean(false)
    val registration = guard.submit(() => cancelled.set(true))(_ => ()).nn
    guard.close(closed.set(true))
    assert(cancelled.get())
    assert(!closed.get())
    assertEquals(guard.submit(() => ())(_ => ()), null)
    guard.release(registration)
    assert(closed.get())

  test("closing a scheduler cancels jobs queued behind a blocked shard"):
    val scheduler = UringPerThreadSupport(parallelism = 1, onStall = _ => ())
    val entered = CountDownLatch(1)
    val release = CountDownLatch(1)
    scheduler.execute(() => { entered.countDown(); release.await() })
    entered.await()
    try
      Async.blocking:
        val waiting = Future(scheduler.await(null)(_.ops.timer(100000)))
        AsyncOperations.sleep(30)
        val closing = Future(scheduler.close())
        try assert(waiting.awaitResult.isFailure)
        finally release.countDown()
        closing.await
    finally
      release.countDown()
      scheduler.close()
  test("a fatal error on a shard stops the scheduler instead of hanging its fibers"):
    val scheduler = UringPerThreadSupport(parallelism = 2, onStall = _ => ())
    val injected = new OutOfMemoryError("injected")
    val reported = java.util.concurrent.ConcurrentLinkedQueue[Throwable]()
    val previous = Thread.getDefaultUncaughtExceptionHandler()
    Thread.setDefaultUncaughtExceptionHandler((_, e) => reported.add(e))
    try
      Async.blocking:
        val waiting = Future(scheduler.await(null)(_.ops.timer(100000)))
        AsyncOperations.sleep(30)
        scheduler.execute(() => throw injected)
        assert(withTimeout(5.seconds)(waiting.awaitResult).isFailure, "a fiber waiting on the scheduler is released")
      val refused = intercept[IllegalStateException](Async.blocking(scheduler.await(null)(_.ops.timer(1))))
      assert(refused.getCause() eq injected, s"the refusal names the fatal error, got ${refused.getCause()}")
      assert(reported.contains(injected), "the fatal error is reported")
    finally
      Thread.setDefaultUncaughtExceptionHandler(previous)
      scheduler.close()

  test("cancellation waits for a blocking operation to release its borrowed memory"):
    val driver = SingleReactorDriver(() => UringReactor.open())
    try
      Async.blocking:
        val started = CountDownLatch(1)
        val release = CountDownLatch(1)
        val unwound = AtomicBoolean(false)
        val pending = Future:
          try
            driver.await(null)(r =>
              r.ops.blocking(() => {
                started.countDown()
                var ended = false
                while !ended do
                  try
                    release.await()
                    ended = true
                  catch case _: InterruptedException => ()
              })
            )
          finally unwound.set(true)
        started.await()
        try
          pending.cancel()
          AsyncOperations.sleep(30)
          assert(!unwound.get(), "The caller reclaimed its buffer before onCancel")
        finally release.countDown()
        assert(pending.awaitResult.isFailure)
        assert(unwound.get())
    finally driver.close()

  test("successful file opens are reclaimed when the suspending handoff fails"):
    val backend = SingleReactorDriver(() => UringReactor.open())
    val cancelled = new ReactorDriver:
      def await(guard: IoGuard | Null)(build: (r: Reactor) => r.Op)(using Async): Unit =
        backend.await(guard)(build)
        throw CancellationException()
    try
      Async.blocking:
        val before = new java.io.File("/proc/self/fd").list().length
        for _ <- 0 until 20 do
          intercept[CancellationException](cancelled.openFile("build.sbt", scala.scalanative.posix.fcntl.O_RDONLY, 0))
          intercept[CancellationException]:
            cancelled.awaitHandle(null)((r, into) =>
              r.asInstanceOf[UringReactor]
                .ops
                .openAt("build.sbt", scala.scalanative.posix.fcntl.O_RDONLY, 0, into)
                .asInstanceOf[r.Op]
            )
        assertEquals(new java.io.File("/proc/self/fd").list().length, before)
    finally backend.close()

  test("an op builder failure reaches its waiting fiber"):
    val driver = SingleReactorDriver(() => UringReactor.open())
    try
      Async.blocking:
        intercept[IllegalArgumentException](driver.await(null)(_ => throw IllegalArgumentException("builder")))
    finally driver.close()

  test("closing a driver completes an operation already submitted from another thread"):
    val driver = SingleReactorDriver(() => UringReactor.open())
    Async.blocking:
      val pending = Future(driver.await(null)(_.ops.timer(100000)))
      AsyncOperations.sleep(30)
      driver.close()
      assert(pending.awaitResult.isFailure)

  test("a foreign shard uses the requested scheduler's reactor"):
    val first = UringPerThreadSupport(parallelism = 1)
    val second = UringPerThreadSupport(parallelism = 1)
    try
      given UringPerThreadSupport = first
      Async.blocking:
        Future:
          val caller = Thread.currentThread()
          second.await(null)(r =>
            assert(Thread.currentThread() ne caller, "The second scheduler reused the first scheduler's shard")
            r.ops.timer(1)
          )
        .await
    finally
      first.close()
      second.close()

  test("closing the uring runtime resumes sleeping fibers so their cleanup can finish"):
    val support = UringPerThreadSupport(parallelism = 1)
    given UringPerThreadSupport = support
    Async.blocking:
      val entered = CountDownLatch(1)
      val finished = AtomicBoolean(false)
      val pending = Future:
        try
          entered.countDown()
          support.sleep(100000)
        finally finished.set(true)
      entered.await()
      Thread.sleep(30)
      support.close()
      assert(pending.awaitResult.isFailure)
      assert(finished.get())
