package gears.async.io
package shareable

import gears.async.Async
import gears.async.AsyncOperations
import gears.async.Future

import java.io.IOException
import java.util.concurrent.CancellationException
import scala.concurrent.TimeoutException
import scala.concurrent.duration.*
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/** `Io.perform`'s promises, checked over the shared runtime with a scripted device for its poller (`FakeBackend`), for
  * both kinds of op a poller has and both ways of cancelling: an op that returns did its work once; one cancelled or
  * timed out did none; and either way, once `perform` has returned or thrown, the device no longer holds the op.
  */
class FakePollerSuite extends munit.FunSuite {
  override def munitTimeout: Duration = 60.seconds

  private def scheduler(using io: Io[FakeBackend.type]): FakeScheduler = io.asInstanceOf[FakeScheduler]

  /** Waits, on timers, until `condition` holds. */
  private def eventually(condition: => Boolean)(using Io[FakeBackend.type], Async): Unit = {
    val end = System.nanoTime() + 10.seconds.toNanos
    while !condition do {
      assert(System.nanoTime() < end, "the condition never held")
      AsyncOperations.sleep(1)
    }
  }

  private def isCancelled(outcome: Try[Unit]) = outcome match {
    case Failure(_: CancellationException) => true
    case _                                 => false
  }

  /** A future performing an op, and what `perform` itself did: a cancelled future fails with `CancellationException`
    * whatever its body did (`Future.checkCancellation`), and `perform` may return for a performer cancelled after its
    * op's work was done.
    */
  private final class Performer(
      val future: Future[Unit],
      performed: java.util.concurrent.atomic.AtomicReference[Try[Unit]]
  ) {
    def cancel(): Unit = future.cancel()
    def poll() = future.poll()

    /** Waits for the future, and gives what `perform` did. */
    def awaitResult(using Async): Try[Unit] = {
      future.awaitResult: Unit
      performed.get()
    }
  }

  private def performing(op: DeviceOp, timeout: FiniteDuration | Null = null)(using
      io: Io[FakeBackend.type],
      spawn: Async.Spawn
  ): Performer = {
    val performed = new java.util.concurrent.atomic.AtomicReference[Try[Unit]]()
    val future = Future {
      performed.set(Try(if timeout == null then io.perform(op) else io.perform(op, timeout.nn)))
      performed.get().get
    }
    Performer(future, performed)
  }

  /** Performs `op` in a future on the current worker's device, returning once the device holds it. */
  private def started(op: DeviceOp)(using io: Io[FakeBackend.type], spawn: Async.Spawn): (Performer, FakeDevice) = {
    val performer = performing(op)
    eventually(op.device != null && op.device.nn.holds(op))
    (performer, op.device.nn)
  }

  /** What every outcome must be: done once and returned, or not done and failed. */
  private def consistent(op: DeviceOp, outcome: Try[Unit], device: FakeDevice): Unit = {
    assert(!device.holds(op), s"perform ended ($outcome) while the device still held the op")
    outcome match {
      case Success(_) => assertEquals(op.worked.get, 1, "returned, so its work is done, once")
      case Failure(e) => assertEquals(op.worked.get, 0, s"failed with $e, so it did nothing")
    }
  }

  for kind <- List("completion", "readiness") do {
    def make(withdrawal: Withdrawal = Withdrawal.Sync): DeviceOp =
      if kind == "completion" then CompletionOp(withdrawal) else ReadinessOp()

    test(s"$kind: an op the device finishes returns, its work done once") {
      FakeBackend.onPool(2) { io ?=>
        val op = make()
        val (performer, device) = started(op)
        device.finish(op)
        val outcome = performer.awaitResult
        assertEquals(outcome, Success(()))
        consistent(op, outcome, device)
      }
    }

    test(s"$kind: an op the device fails throws its failure, having done nothing") {
      FakeBackend.onPool(2) { io ?=>
        val op = make()
        val (performer, device) = started(op)
        device.finish(op, Failure(new IOException("broken")))
        val outcome = performer.awaitResult
        assert(outcome.failed.toOption.exists(_.isInstanceOf[IOException]), outcome.toString)
        consistent(op, outcome, device)
      }
    }

    test(s"$kind: a cancel before the device finishes takes the op back at once (sync withdrawal)") {
      FakeBackend.onPool(2) { io ?=>
        val op = make()
        val (performer, device) = started(op)
        performer.cancel()
        val outcome = performer.awaitResult
        assert(isCancelled(outcome), outcome.toString)
        consistent(op, outcome, device)
        assert(!device.finish(op), "the device still had it")
      }
    }

    test(s"$kind: an op is performed again after a cancel, and finishes") {
      FakeBackend.onPool(2) { io ?=>
        val op = make()
        val (cancelled, _) = started(op)
        cancelled.cancel()
        cancelled.awaitResult: Unit
        val (performer, device) = started(op)
        device.finish(op)
        assertEquals(performer.awaitResult, Success(()))
        assertEquals(op.worked.get, 1)
      }
    }

    test(s"$kind: a time limit throws TimeoutException only once the device has let go, having done nothing") {
      FakeBackend.onPool(2) { io ?=>
        val op = make()
        val performer = performing(op, 20.millis)
        val outcome = performer.awaitResult
        assert(outcome.failed.toOption.exists(_.isInstanceOf[TimeoutException]), outcome.toString)
        val device = op.device.nn
        try consistent(op, outcome, device)
        finally device.cancelNow(op): Unit
      }
    }

    test(s"$kind: a finish racing a cancel is either done and returned, or not done and cancelled") {
      FakeBackend.onPool(4) { io ?=>
        for round <- 1 to 300 do {
          val op = make()
          val (performer, device) = started(op)
          val finisher = Future(device.finish(op): Unit)
          performer.cancel()
          val outcome = performer.awaitResult
          finisher.await
          consistent(op, outcome, device)
        }
      }
    }
  }

  test("completion: a cancel waits for the device to confirm it (deferred withdrawal), then throws") {
    FakeBackend.onPool(2) { io ?=>
      val op = CompletionOp(Withdrawal.Deferred)
      val (performer, device) = started(op)
      performer.cancel()
      eventually(device.isCancelling(op))
      AsyncOperations.sleep(50)
      assert(performer.poll().isEmpty, "perform ended while the device still held the op")
      device.confirmCancels()
      val outcome = performer.awaitResult
      assert(isCancelled(outcome), outcome.toString)
      consistent(op, outcome, device)
    }
  }

  test("completion: an op the device finished before a cancel reached it returns, as its work is done") {
    FakeBackend.onPool(1) { io ?=>
      val op = CompletionOp()
      val (performer, device) = started(op)
      // One worker, and this future holds it: the finish and the cancel both wait for it, the finish in the device.
      device.finish(op)
      performer.cancel()
      val outcome = performer.awaitResult
      consistent(op, outcome, device)
    }
  }

  test("completion: a deferred cancel that the device's finish overtakes returns, as its work is done") {
    FakeBackend.onPool(2) { io ?=>
      val op = CompletionOp(Withdrawal.Deferred)
      val (performer, device) = started(op)
      performer.cancel()
      eventually(device.isCancelling(op))
      device.finish(op) // the device did the work before it saw the cancel
      val outcome = performer.awaitResult
      consistent(op, outcome, device)
    }
  }
}
