package gearsexample

import gears.async.Future

import asyncio.Reactor
import ReactorFutures.*

/** Mirror of `example.KQueueExampleTimer`: a five-second one-shot timer, with a one-second ticker running alongside. */
object GearsTimer {
  def run()(using Reactor.Factory[Reactor]): Unit = {
    ReactorFutures.run { r =>
      println("Creating and registering timer event...")
      val fired = Future(perform(r, r.ops.timer(5000)))
      println("Event registered successfully. Waiting for it to trigger...")
      println("calling kevent to wait for events...")
      val ticker = Future {
        var seconds = 0
        val timer = r.ops.timer(1000)
        while true do {
          perform(r, timer)
          seconds += 1
          println(s"waited for $seconds seconds")
        }
      }
      fired.await
      println("timer fired")
      ticker.cancel() // Also cancels its pending one-second timer in the reactor.
    }
  }
}
