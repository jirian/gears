import gears.async.*
import gears.async.asyncio.uring.UringPerThreadSupport
import gears.async.net.{DnsSupport, TcpSupport, UdpSupport}

import scala.concurrent.duration.*

class UringNetSuite extends NetBehavior("uring"):
  private lazy val support = UringPerThreadSupport(parallelism = 2)
  private given UringPerThreadSupport = support
  protected given tcp: TcpSupport = support.tcpSupport
  protected given udp: UdpSupport = support.udpSupport
  protected def dns: DnsSupport = support.dnsSupport
  protected given operations: AsyncOperations = support

  protected def run[A](body: Async.Spawn ?=> A): A = Async.blocking(body)

  override def afterAll(): Unit = support.close()

  test("a scheduler can be closed, stopping its shards"):
    val scheduler = UringPerThreadSupport(parallelism = 2)
    given UringPerThreadSupport = scheduler
    Async.blocking:
      AsyncOperations.sleep(10)
    scheduler.close()
    scheduler.close()
    intercept[IllegalStateException](Async.blocking(scheduler.fileSupport.openRead("build.sbt")))

  test("I/O submitted from outside the shards is not held up by a shard its fiber is blocking"):
    val scheduler = UringPerThreadSupport(parallelism = 2, onStall = _ => ())
    given UringPerThreadSupport = scheduler
    try
      Async.blocking:
        val blocker = Future(Thread.sleep(3000))
        AsyncOperations.sleep(100)
        val started = System.nanoTime()
        for _ <- 1 to 20 do scheduler.dnsSupport.resolve("localhost")
        val elapsedMillis = (System.nanoTime() - started) / 1_000_000
        assert(elapsedMillis < 2000, s"lookups waited on the blocked shard: $elapsedMillis ms")
        blocker.await
    finally scheduler.close()

  test("a shard stuck in one task is reported"):
    val reports = java.util.concurrent.ConcurrentLinkedQueue[String]()
    val scheduler = UringPerThreadSupport(parallelism = 2, stallWarningAfter = 200.millis, onStall = reports.add(_))
    given UringPerThreadSupport = scheduler
    try
      Async.blocking:
        Future(Thread.sleep(800)).await
      assert(reports.size == 1, s"expected one report, got: $reports")
      assert(reports.peek().contains("Thread.sleep"), reports.peek())
    finally scheduler.close()
