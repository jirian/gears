package example

import asyncio.Completion
import asyncio.kqueue.KqueueReactor
import asyncio.kqueue.KqueueTimer
import asyncio.unsafe.KqueueLoop

/** A five-second one-shot timer, with a one-second ticker running alongside. Both are `KqueueTimer` ops. */
object KQueueExampleTimer {
  def run(): Unit = {
    KqueueReactor.scoped(maxEvents = 1) { r =>
      println("Creating and registering timer event...")
      val tick = new KqueueTimer(milliseconds = 1000)
      r.submit(
        new KqueueTimer(milliseconds = 5000),
        timer => {
          println(s"Event triggered: ID = ${timer.id}, Filter = ${KqueueLoop.EVFILT_TIMER}")
          println("timer fired")
          r.cancel(tick)
          r.stop() // Exit after handling the event
        }
      )
      println("Event registered successfully. Waiting for it to trigger...")
      println("calling kevent to wait for events...")
      var seconds = 0
      lazy val ticker: Completion[KqueueTimer] = _ => {
        seconds += 1
        println(s"waited for $seconds seconds")
        r.submit(tick, ticker)
      }
      r.submit(tick, ticker)
      r.run()
    }
  }
}
