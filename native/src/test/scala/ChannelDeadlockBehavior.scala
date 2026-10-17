import gears.async.default.given
import gears.async.{Async, Future, SyncChannel}

import java.util.concurrent.{CountDownLatch, TimeUnit}

class ChannelDeadlockBehavior extends munit.FunSuite:
  test("racing reads across channels while every channel matches senders does not deadlock") {
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
    worker.setDaemon(true)
    worker.start()
    assert(finished.await(20, TimeUnit.SECONDS), "racing channel reads deadlocked")
  }
