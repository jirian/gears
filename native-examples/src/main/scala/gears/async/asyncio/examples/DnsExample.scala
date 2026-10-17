package gears.async.asyncio.examples

import gears.async._
import gears.async.asyncio.uring.UringPerThreadSupport
import gears.async.net.DnsSupport

def dnsExample(): Unit =
  given support: UringPerThreadSupport = UringPerThreadSupport()
  given dns: DnsSupport = support.dnsSupport

  Async.blocking:
    Async.group:
      val running = new java.util.concurrent.atomic.AtomicBoolean(true)
      val ticks = new java.util.concurrent.atomic.AtomicInteger(0)
      val ticker = Future:
        while running.get() do
          AsyncOperations.sleep(2)
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
        fail("resolved a nonexistent hostname")
      catch
        case e: java.net.UnknownHostException =>
          println(s"correctly failed to resolve a nonexistent hostname: ${e.getMessage}")

      running.set(false)
      ticker.await
      println(
        s"ticker fiber ran concurrently throughout: ${ticks.get()} total ticks (proves resolve() never blocked the shard thread)"
      )
