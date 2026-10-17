package gears.io.reactor

import gears.async.{Async, Future}
import gears.io.reactor.Reactor

import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch}
import scala.util.control.NonFatal

final class SingleReactorDriver(factory: Reactor.Factory[Reactor], threadName: String = "gears-reactor")
    extends ReactorDriver
    with AutoCloseable:

  private val tasks = ConcurrentLinkedQueue[Runnable]()

  @volatile private var closed = false
  @volatile private var running = true
  @volatile private var reactor: Reactor | Null = null
  @volatile private var startFailure: Throwable | Null = null
  private val started = CountDownLatch(1)

  private val thread = Thread(() => loop(), threadName)
  thread.setDaemon(true)
  thread.start()
  started.await()
  if startFailure != null then throw startFailure.nn

  private def loop(): Unit =
    val r =
      try factory.open()
      catch
        case t: Throwable =>
          startFailure = t
          started.countDown()
          return
    reactor = r
    started.countDown()
    try
      while running do
        drain()
        if running then
          try r.runOnce(block = tasks.isEmpty)
          catch case NonFatal(e) => report(e)
    finally
      synchronized { closed = true }
      drain()
      r.close()

  private def report(e: Throwable): Unit =
    val t = Thread.currentThread()
    t.getUncaughtExceptionHandler().uncaughtException(t, e)

  private def drain(): Unit =
    var task = tasks.poll()
    while task != null do
      try task.run()
      catch case NonFatal(e) => report(e)
      task = tasks.poll()

  private def onLoop(body: => Unit): Unit =
    if Thread.currentThread() eq thread then body
    else
      tasks.offer(() => body)
      reactor.nn.wakeup()

  def await(guard: IoGuard | Null)(build: (r: Reactor) => r.Op)(using Async): Unit =
    if closed then throw IllegalStateException("The reactor driver is closed")
    val r = reactor.nn
    val pending = Future
      .withResolver[Unit]: resolver =>
        val job = SharedJob[Unit](
          _ => ReactorDriver.submitOn(r, onLoop, guard, build, resolver),
          whenCancelledFirst = () => resolver.rejectAsCancelled()
        )
        resolver.onCancel(() => job.cancel())
        synchronized:
          if closed then resolver.reject(IllegalStateException("The reactor driver is closed"))
          else onLoop(job.run(()))
    ReactorDriver.waitFor(pending)

  override def close(): Unit =
    val first = synchronized:
      val wasOpen = !closed
      closed = true
      wasOpen
    if first then
      running = false
      reactor.nn.wakeup()
      if Thread.currentThread() ne thread then thread.join()
