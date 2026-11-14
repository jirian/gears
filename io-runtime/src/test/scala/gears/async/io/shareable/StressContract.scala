package gears.async.io
package shareable

import gears.async.Async
import gears.async.Future

import java.util.concurrent.CancellationException
import scala.concurrent.duration.*

/** For a backend on the shared runtime (`WorkpoolScheduler`), with timers that complete as soon as they are scheduled,
  * so that completion races the performer's wait for it, and submissions race each worker going to sleep.
  */
abstract class StressContract extends munit.FunSuite {

  /** The backend, and a pool of `workers` workers performing its ops. */
  type I <: Interface
  val backend: Backend { type Interface = I }
  def newPool(workers: Int): WorkpoolScheduler[?, ?] & Io[I]

  override def munitTimeout: Duration = 60.seconds

  test("a run stops its workers before it returns") {
    given (Backend { type Interface = I }) = backend
    val worker = Io.run { io ?=>
      io.perform(io.ops.timer(1))
      Thread.currentThread()
    }
    assert(!worker.isAlive, "a worker outlived its run")
  }

  test("a run stops its workers when the program fails") {
    given (Backend { type Interface = I }) = backend
    var worker: Thread | Null = null
    intercept[IllegalStateException](Io.run { io ?=>
      worker = Thread.currentThread()
      throw new IllegalStateException("boom")
    })
    assert(!worker.nn.isAlive, "a worker outlived its run")
  }

  test("a run ending with many timers pending cancels them, and shuts down without waiting for them") {
    given (Backend { type Interface = I }) = backend
    val workers = java.util.concurrent.ConcurrentHashMap.newKeySet[Thread]()
    val scheduledRan = java.util.concurrent.atomic.AtomicBoolean(false)
    val started = System.nanoTime()
    val waiters = Io.run { io ?=>
      summon[Async].scheduler.schedule(60.seconds, () => scheduledRan.set(true)): Unit
      val waiters = for _ <- 1 to 1000 yield Future {
        workers.add(Thread.currentThread())
        io.perform(io.ops.timer(60_000))
      }
      io.perform(io.ops.timer(50)) // so that they are waiting
      waiters
    }
    val elapsed = System.nanoTime() - started
    assert(elapsed < 5_000_000_000L, s"it took ${elapsed / 1_000_000}ms, as if it waited for the timers")
    assert(waiters.forall(_.poll().exists(_.failed.toOption.exists(_.isInstanceOf[CancellationException]))))
    assert(workers.size > 0)
    workers.forEach(worker => assert(!worker.isAlive, "a worker outlived its run"))
    assert(!scheduledRan.get())
  }

  test("a scheduler that has shut down refuses work") {
    val pool = newPool(1)
    pool.shutdown()
    pool.shutdown() // nothing left to do
    intercept[IllegalStateException](pool.execute(() => ()))
    intercept[IllegalStateException](pool.schedule(1.milli, () => ()))
  }

  test("many short timers all complete") {
    given (Backend { type Interface = I }) = backend
    Io.run { io ?=>
      for round <- 1 to 3000 do {
        val futures = for _ <- 1 to 16 yield Future(io.perform(io.ops.timer(0)))
        futures.foreach(_.await)
      }
    }
  }

  test("a timer cancelled as it fires leaves its next performance to run its full time") {
    given (Backend { type Interface = I }) = backend
    Io.run { io ?=>
      val delay = 300.micros
      val timer = io.ops.timer(delay)
      for round <- 1 to 2000 do {
        val waiting = Future(io.perform(timer))
        val cancelAt = System.nanoTime() + delay.toNanos - 100_000L + (round % 20) * 10_000L // either side of it firing
        while System.nanoTime() < cancelAt do Thread.onSpinWait()
        waiting.cancel()
        waiting.awaitResult: Unit
        val started = System.nanoTime()
        io.perform(timer)
        assert(System.nanoTime() - started >= delay.toNanos, s"round $round fired early")
      }
    }
  }

  test("a timer cancelled by a waiter resumed from the same batch of timers still finishes") {
    given (Backend { type Interface = I }) = backend
    Io.run { io ?=>
      for round <- 1 to 200 do {
        val deadline = System.nanoTime() + 5_000_000L
        // More futures than workers, so that some share a heap: the first resumed cancels the rest of its batch.
        val waiters = new Array[Future[Unit]](4 * Runtime.getRuntime.availableProcessors)
        for i <- waiters.indices do
          waiters(i) = Future {
            io.perform(io.ops.deadlineTimer(deadline))
            for j <- waiters.indices if j != i && waiters(j) != null do waiters(j).cancel()
          }
        waiters.foreach(_.awaitResult)
      }
    }
  }

  test("timers shorter than a millisecond complete, the worker sleeping for less than one") {
    given (Backend { type Interface = I }) = backend
    Io.run { io ?=>
      for round <- 1 to 300 do {
        val futures = for _ <- 1 to 4 yield Future(io.perform(io.ops.timer(300.micros)))
        futures.foreach(_.await)
      }
    }
  }
}

abstract class DeadlineContract extends munit.FunSuite {
  type I <: Interface
  val backend: Backend { type Interface = I }

  test("a deadline already passed, even a negative one, is on the heap and fires") {
    given (Backend { type Interface = I }) = backend
    Io.run { io ?=>
      io.perform(io.ops.deadlineTimer(-5L))
    }
  }
}
