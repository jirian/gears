import gears.async.default.given
import gears.async.{Async, Future, SyncChannel}

import java.util.concurrent.{CountDownLatch, TimeUnit}

/** Regression test for a deadlock between channel locks and `Async.race` listener locks. It needs a watchdog thread
  * outside gears, so it lives with the JVM and Native tests rather than the shared ones: gears' structured concurrency
  * makes `Async.fromSync` wait for every child future, deadlocked ones included, so it cannot time itself out.
  */
class ChannelDeadlockBehavior extends munit.FunSuite:
  test("racing reads across channels while every channel matches senders does not deadlock") {
    // Completing an `Async.race` listener drops it from the race's other sources. Channels used to take their lock in
    // `dropListener`, so a thread completing the race (holding its listener's lock) could wait for channel A's lock
    // while another thread, matching a sender inside channel A's lock, waited for that same listener's lock.
    val finished = CountDownLatch(1)
    val worker = Thread(() =>
      Async.fromSync:
        val rounds = 2000
        val channels = (1 to 4).map(_ => SyncChannel[Int]())
        val senders = channels.map(c => Future { for i <- 1 to rounds do c.send(i) })
        val readers = channels.map(_ =>
          Future { for _ <- 1 to rounds do Async.race(channels.map(_.readSource)*).awaitResult.right.get }
        )
        readers.awaitAll
        senders.awaitAll
      finished.countDown()
    )
    worker.setDaemon(true) // a deadlocked worker must not keep the test process alive
    worker.start()
    assert(finished.await(20, TimeUnit.SECONDS), "racing channel reads deadlocked")
  }
