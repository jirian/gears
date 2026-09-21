package gears.async.asyncio.uring.examples

import gears.async._
import gears.async.asyncio.uring.ForkJoinUringSupport

/** Empirical check for Finding 2: does an object referenced only through a
  * suspended continuation's copied-out stack fragment survive GC pressure
  * while suspended, or can it be prematurely collected/its memory reused?
  *
  * Allocates a marker array, suspends via a REAL `.await` (same delimited
  * continuation machinery as submitAwait) across a genuine cross-thread
  * gap, hammers the collector with allocation pressure and explicit
  * `System.gc()` calls *while suspended*, then checks the marker's content
  * is still intact after resuming.
  */
@main def gcContinuationStressTest(): Unit =
  given support: ForkJoinUringSupport = ForkJoinUringSupport()

  Async.blocking:
    val size = 1 << 20 // 1 MiB - large enough to go through the large-object path
    val pattern: Byte = 0x5a
    val marker = new Array[Byte](size)
    java.util.Arrays.fill(marker, pattern)
    println(s"allocated marker array (${marker.length} bytes), filled with 0x${pattern.toInt & 0xff}")

    val pressure = new Thread(() =>
      val until = System.currentTimeMillis() + 4000
      var rounds = 0
      while System.currentTimeMillis() < until do
        var i = 0
        while i < 5 do
          val other = new Array[Byte](1 << 20)
          java.util.Arrays.fill(other, 0xA5.toByte)
          i += 1
        System.gc()
        rounds += 1
      println(s"pressure thread: ran $rounds rounds of allocation + System.gc() while main fiber suspended")
    )
    pressure.setDaemon(true)

    // Suspend the main fiber via a real Future/await, resolved from a
    // background OS thread after a real delay - so `marker` is captured
    // only inside the continuation's copied-out stack state for a genuine
    // span of wall-clock time, with another thread free to run GC pressure
    // concurrently.
    val trigger = Future.withResolver[Unit]: resolver =>
      pressure.start()
      val resolver_thread = new Thread(() =>
        Thread.sleep(4200)
        resolver.resolve(())
      )
      resolver_thread.setDaemon(true)
      resolver_thread.start()

    trigger.link().await
    pressure.join()

    var corrupted = 0
    var i = 0
    while i < marker.length do
      if marker(i) != pattern then corrupted += 1
      i += 1

    if corrupted > 0 then
      println(s"CORRUPTED: $corrupted/${marker.length} bytes changed while suspended.")
      println("Continuation-captured references are NOT reliably visible to the GC's root scanner.")
    else
      println("INTACT: marker array unchanged after suspension + GC pressure.")
      println("Continuation-captured references survived this stress test.")
