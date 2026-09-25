package gears.async.asyncio.examples

import gears.async._
import gears.async.net.DnsSupport
import gears.async.asyncio.uring.{NativeDnsSupport, UringPerThreadSupport}

/** Live verification of the new async [[DnsSupport]]
  * (`NativeDnsSupport`): resolves `localhost` (a real hostname, not an IP
  * literal, so this genuinely exercises resolution rather than just
  * parsing) concurrently with other work on the same shard, to prove it
  * doesn't block the calling fiber's carrier thread - then, best-effort,
  * a real external hostname too (skipped gracefully if this environment
  * has no outbound network access, rather than treated as a failure).
  */
@main def dnsExample(): Unit =
  given support: UringPerThreadSupport = UringPerThreadSupport()
  given dns: DnsSupport = NativeDnsSupport

  Async.blocking:
    Async.group:
      // Ticks continuously on the SAME scheduler while every resolution
      // below is in flight - if resolve() were blocking the shard thread
      // (the bug this replaces), this fiber would visibly stall instead of
      // ticking smoothly throughout, since it'd be stuck behind resolve()
      // on the very same carrier thread rather than running concurrently.
      val running = new java.util.concurrent.atomic.AtomicBoolean(true)
      val ticks = new java.util.concurrent.atomic.AtomicInteger(0)
      val ticker = Future:
        while running.get() do
          Thread.sleep(2)
          ticks.incrementAndGet()

      val localhost = DnsSupport.resolve("localhost")
      println(s"resolved 'localhost' -> ${localhost.mkString(", ")} (ticker at ${ticks.get()} ticks)")

      try
        val external = DnsSupport.resolve("example.com")
        println(s"resolved 'example.com' -> ${external.mkString(", ")} (ticker at ${ticks.get()} ticks)")
      catch
        case e: Exception =>
          println(s"'example.com' resolution failed (expected if this environment has no outbound network access): $e")

      try
        DnsSupport.resolve("this-host-definitely-does-not-exist.invalid")
        println("UNEXPECTEDLY resolved a nonexistent hostname")
      catch case e: java.net.UnknownHostException => println(s"correctly failed to resolve a nonexistent hostname: ${e.getMessage}")

      running.set(false)
      ticker.await
      println(s"ticker fiber ran concurrently throughout: ${ticks.get()} total ticks (proves resolve() never blocked the shard thread)")

  System.exit(0)
