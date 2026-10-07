package example

import java.nio.charset.StandardCharsets
import scala.scalanative.unsafe.*

import asyncio.Completion
import asyncio.Slot
import asyncio.kqueue.KqueueBlocking
import asyncio.kqueue.KqueueHandles
import asyncio.kqueue.KqueueOp
import asyncio.kqueue.KqueueOps
import asyncio.kqueue.KqueueReactor
import asyncio.kqueue.KqueueTimer
import asyncio.kqueue.KqueueValuePromise
import asyncio.kqueue.ReadIntoBuffer
import asyncio.unsafe.NativeBuffer
import KQueueExampleIO.*

/** Cancels one op of each kind while it is still pending: a read on an empty pipe, a blocking task, a plain promise,
  * and a timer. Then it shows the cancellations held: none of their completions ever run, even once the pipe has data,
  * the blocking task has finished, the promise is completed, and the timer's deadline has passed; and the freed read
  * slot accepts a fresh read. Every one of them, the timer included, is cancelled with the same `r.cancel`.
  */
object KQueueExampleCancel {

  /** A task for the blocker pool that sleeps for `millis`, recording whether cancellation interrupted it. */
  final class Sleeper(millis: Long) extends KqueueBlocking {
    @volatile var interrupted = false
    def block(): Unit =
      try Thread.sleep(millis)
      catch {
        case e: InterruptedException =>
          interrupted = true
          throw e
      }
  }

  /** A task that keeps running for `millis` whatever happens, like a native call that cannot be interrupted. */
  final class Stubborn(millis: Long) {
    @volatile var finished = false
    def body(): String = {
      val deadline = System.nanoTime() + millis * 1_000_000L
      while System.nanoTime() < deadline do ()
      finished = true
      "finished"
    }
  }

  /** Whether `slot` is empty, taking out any value it holds. */
  def isEmpty(slot: Slot[?]): Boolean =
    try {
      slot.clear()
      false
    } catch case _: IllegalStateException => true

  /** Waits up to two seconds for `sleeper` to record an interrupt. */
  def interruptedSoon(sleeper: Sleeper): Boolean = {
    val deadline = System.nanoTime() + 2_000_000_000L
    while !sleeper.interrupted && System.nanoTime() < deadline do Thread.sleep(10)
    sleeper.interrupted
  }

  def run(): Unit = {
    val leftover = new Sleeper(millis = 10000) // still running when the reactor closes
    var leftoverNotified = false // set by the leftover's onCancel when the reactor closes
    var leftoverStopped = false // whether the leftover had stopped by then
    KqueueReactor.scoped() { r =>
      withPipe { (readFd, writeFd) =>
        Zone.acquire { implicit z =>
          val buf = NativeBuffer.allocate(64)
          var lateCompletions = 0
          var cancelNotifications = 0
          val late: Completion[KqueueOp] = new Completion[KqueueOp] {
            def onComplete(op: KqueueOp): Unit = {
              lateCompletions += 1
              println(s"unexpected completion of cancelled $op")
            }
            override def onCancel(op: KqueueOp): Unit =
              if op eq leftover then {
                leftoverNotified = true
                leftoverStopped = leftover.interrupted
              } else cancelNotifications += 1
          }

          val read = ReadIntoBuffer(readFd, buf) // nothing has been written yet
          val sleeper = new Sleeper(millis = 1000)
          val promise = new KqueueValuePromise[Unit] // nobody will complete it before it is cancelled
          r.submit(read, late)
          r.submit(sleeper, late)
          r.submit(promise, late)
          val timer = new KqueueTimer(milliseconds = 300)
          r.submit(timer, late)
          r.submit(leftover, late) // Left running on purpose: closing the reactor must interrupt it.
          // Cancelled while it runs; it ignores the interrupt, so its onCancel must wait until it has finished, and the
          // value it produces after the cancel must not reach its slot.
          val stubborn = new Stubborn(millis = 300)
          val stubbornResult = Slot[String]()
          val stubbornTask = KqueueOps.blocking(() => stubborn.body(), stubbornResult)
          r.submit(
            stubbornTask,
            new Completion[KqueueOp] {
              def onComplete(op: KqueueOp): Unit = println("unexpected completion of the stubborn task")
              override def onCancel(op: KqueueOp): Unit = {
                println(s"a running blocking task had stopped before onCancel: ${stubborn.finished}")
                println(s"the cancelled task left its slot empty: ${isEmpty(stubbornResult)}")
              }
            }
          )
          println("submitted a read, a blocking task, a promise, and a timer")

          r.submit(
            new KqueueTimer(milliseconds = 100),
            _ => {
              println(s"read cancelled: ${r.cancel(read)}")
              println(s"blocking cancelled: ${r.cancel(sleeper)}")
              println(s"promise cancelled: ${r.cancel(promise)}")
              println(s"timer cancelled: ${r.cancel(timer)}")
              println(s"cancelling the read again: ${r.cancel(read)}")
              r.cancel(stubbornTask)
              // A promise is not Repeatable, so the reactor refuses to take it a second time.
              val refused =
                try {
                  r.submit(promise, late)
                  false
                } catch case _: IllegalStateException => true
              println(s"resubmitting the cancelled promise: ${if refused then "refused" else "accepted"}")

              // Closing a descriptor removes its kqueue filters, so a read still pending on it must stay cancellable.
              val (orphanRead, orphanWrite) = KqueueHandles.pipe()
              val orphan = ReadIntoBuffer(orphanRead, NativeBuffer.allocate(16))
              r.submit(orphan, _ => println("unexpected completion of the orphaned read"))
              KqueueHandles.close(orphanRead)
              KqueueHandles.close(orphanWrite)
              println(s"cancelling after close: ${r.cancel(orphan)}")

              // Completing the cancelled promise from another thread must not run its completion.
              val completer = new Thread(() => promise.complete(()))
              completer.start()
              completer.join()
              // Only the first completion counts.
              val again =
                try {
                  promise.complete(())
                  "accepted"
                } catch case _: IllegalStateException => "refused"
              println(s"completing the promise again: $again")

              // Data arrives only now; the cancelled read must not see it, and its slot takes a fresh read.
              nioWriteBytes(writeFd, NativeBuffer.of("hello".getBytes(StandardCharsets.UTF_8)))
              buf.clear()
              r.submit(
                ReadIntoBuffer(readFd, buf),
                _ => {
                  buf.flip()
                  println(s"read after cancel: `${text(buf)}`")
                }
              )
            }
          )

          // Past the cancelled timer's deadline, with the interrupted sleeper long stopped.
          r.submit(
            new KqueueTimer(milliseconds = 600),
            _ => {
              println(s"blocking interrupted: ${sleeper.interrupted}")
              println(s"cancelled completions run: $lateCompletions")
              println(s"cancel notifications: $cancelNotifications")
              r.stop()
            }
          )
          r.run()
        }
      }
    }
    println(s"close notified the leftover: $leftoverNotified")
    println(s"the leftover had stopped when notified: $leftoverStopped")
    println(s"blocking interrupted by close: ${interruptedSoon(leftover)}")
  }
}
