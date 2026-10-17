package gears.io.uring

import gears.asyncio.uring.{BlockingPool, HostResolver, UringReactor}
import gears.io.reactor.SharedJob

import java.util.concurrent.ConcurrentLinkedQueue
import scala.scalanative.runtime.Continuations
import scala.util.control.NonFatal

private[uring] final class UringShard(
    val scheduler: UringPerThreadScheduler,
    entries: Int,
    globalQueue: ConcurrentLinkedQueue[Runnable],
    sharedJobs: ConcurrentLinkedQueue[SharedJob[UringShard]],
    blockerPool: BlockingPool,
    resolver: HostResolver
):
  val reactor: UringReactor = UringReactor.open(entries, blockerPool, resolver)

  private val taskQueue = new ConcurrentLinkedQueue[Runnable]()

  private val loopQueue = new ConcurrentLinkedQueue[Runnable]()

  private[uring] var siblings: Array[UringShard] = Array.empty

  private val rng = new scala.util.Random()

  private val PollTimeoutNanos = 2_000_000L

  @volatile private[uring] var busySince = 0L

  private inline def busy[A](inline body: A): A =
    busySince = System.nanoTime() | 1L
    try body
    finally busySince = 0L

  private val MaxSharedJobsPerTurn = 64

  private[uring] def execute(body: Runnable, calledFromOwnThread: Boolean): Unit =
    taskQueue.offer(body)
    if !calledFromOwnThread then wake()

  private[uring] def wake(): Unit = reactor.wakeup()

  private[uring] def isCurrent: Boolean = Thread.currentThread() match
    case t: UringShardThread => t.shard eq this
    case _                   => false

  private[uring] def onLoop(body: => Unit): Unit =
    if isCurrent then body
    else
      loopQueue.offer(() => body)
      wake()

  private def drainLoopQueue(): Boolean =
    var ran = false
    var r = loopQueue.poll()
    while r != null do
      ran = true
      Continuations.handlersReset()
      try busy(r.run())
      catch case NonFatal(e) => report(e)
      finally Continuations.handlersReset()
      r = loopQueue.poll()
    ran

  private def drainSharedJobs(): Boolean =
    var taken = 0
    var job = sharedJobs.poll()
    while job != null do
      taken += 1
      Continuations.handlersReset()
      try busy(job.run(this))
      catch case NonFatal(e) => report(e)
      finally Continuations.handlersReset()
      job = if taken < MaxSharedJobsPerTurn then sharedJobs.poll() else null
    taken > 0

  private def report(e: Throwable): Unit =
    val thread = Thread.currentThread()
    thread.getUncaughtExceptionHandler().uncaughtException(thread, e)

  private def runTask(task: Runnable): Unit =
    Continuations.handlersReset()
    try busy(task.run())
    catch case NonFatal(e) => report(e)
    finally Continuations.handlersReset()

  private def drainTasks(): Boolean =
    val r = taskQueue.poll()
    if r == null then false
    else
      runTask(r)
      true

  private def pollReactor(timeoutNanos: Long): Boolean =
    Continuations.handlersReset()
    try
      if timeoutNanos == 0 then busy(reactor.poll(0))
      else reactor.poll(timeoutNanos)
    catch
      case NonFatal(e) =>
        report(e)
        true
    finally Continuations.handlersReset()

  private[uring] def stealTask(): Runnable = taskQueue.poll()

  private val GlobalQueueInterval = 61

  private def runFromGlobal(): Unit =
    val task = globalQueue.poll()
    if task != null then runTask(task)

  private def tryStealOrGlobal(): Boolean =
    val fromGlobal = globalQueue.poll()
    val task = if fromGlobal != null then fromGlobal else stealFromSibling()
    if task == null then false
    else
      runTask(task)
      true

  private def stealFromSibling(): Runnable =
    val n = siblings.length
    if n <= 1 then null
    else
      val start = rng.nextInt(n)
      var i = 0
      var stolen: Runnable = null
      while stolen == null && i < n do
        val candidate = siblings((start + i) % n)
        if candidate ne this then stolen = candidate.stealTask()
        i += 1
      stolen

  @volatile private var running = true

  private[uring] def stop(): Unit =
    running = false
    wake()

  private[uring] def loop(): Unit =
    Continuations.handlersReset()
    try
      var turn = 0
      while running do
        turn += 1
        if turn % GlobalQueueInterval == 0 then runFromGlobal()
        val ranLoopWork = drainLoopQueue()
        val ranShared = drainSharedJobs()
        val ranTasks = drainTasks()
        val ranCompletions = pollReactor(0)
        if !ranLoopWork && !ranShared && !ranTasks && !ranCompletions && !tryStealOrGlobal() then
          pollReactor(PollTimeoutNanos)
    catch
      case fatal: Throwable =>
        running = false
        try report(fatal)
        catch case _: Throwable => ()
        scheduler.failed(fatal)
    finally
      drainLoopQueue()
      try reactor.close()
      catch case NonFatal(e) => report(e)
      var task = taskQueue.poll()
      while task != null do
        UringShard.afterClose(task)
        task = taskQueue.poll()

private[uring] final class UringShardThread(val shard: UringShard) extends Thread(() => shard.loop())

private[uring] object UringShard:
  def afterClose(task: Runnable): Unit = gears.async.default.DefaultSupport.execute(() =>
    Continuations.handlersReset()
    try task.run()
    finally Continuations.handlersReset()
  )
  def current(): Option[UringShard] = Thread.currentThread() match
    case t: UringShardThread => Some(t.shard)
    case _                   => None
