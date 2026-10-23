package gears.io.uring

import gears.io.reactor.SharedJob
import gears.io.uring.{BlockingPool, HostResolver, UringReactor}

import java.util.concurrent.ConcurrentLinkedQueue
import scala.scalanative.runtime.Continuations
import scala.util.control.NonFatal

/** One thread of a `UringPerThreadScheduler`, with its own `UringReactor`. Its loop runs, in turn: work posted to the
  * reactor thread, shared jobs from threads outside the scheduler, its own fiber tasks, and ready completions. When
  * idle, it takes a task from the global queue or steals one from a sibling, and otherwise waits in the reactor for up
  * to 2 ms.
  */
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

  /** When the current task started, for the stall watchdog; 0 while idle. The low bit is set so it is never 0. */
  @volatile private[uring] var busySince = 0L

  private inline def busy[A](inline body: A): A =
    busySince = System.nanoTime() | 1L
    try body
    finally busySince = 0L

  private val MaxSharedJobsPerTurn = 64

  /** Queues a fiber task. A task from another thread also wakes the loop.
    *
    * @param body
    *   the task
    * @param calledFromOwnThread
    *   whether the caller is this shard's thread, which needs no wake-up
    */
  private[uring] def execute(body: Runnable, calledFromOwnThread: Boolean): Unit =
    taskQueue.offer(body)
    if !calledFromOwnThread then wake()

  private[uring] def wake(): Unit = reactor.wakeup()

  private[uring] def isCurrent: Boolean = Thread.currentThread() match
    case t: UringShardThread => t.shard eq this
    case _                   => false

  /** Runs `body` on this shard's thread, the only one that may use its reactor: now if already there, else in the next
    * turn.
    *
    * @param body
    *   what to run
    */
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

  /** Starts up to 64 ops submitted from outside the scheduler, so a burst of them cannot starve this shard's fibers.
    *
    * @return
    *   whether any job ran
    */
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

  /** Runs one task. `Continuations.handlersReset` clears continuation state that a task may leave behind on this
    * thread.
    *
    * @param task
    *   the task to run
    */
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
      // Waiting for completions isn't a stall, so only a non-blocking poll counts as busy for the watchdog.
      if timeoutNanos == 0 then busy(reactor.poll(0))
      else reactor.poll(timeoutNanos)
    catch
      case NonFatal(e) =>
        report(e)
        true
    finally Continuations.handlersReset()

  private[uring] def stealTask(): Runnable = taskQueue.poll()

  /** Every this many turns, a busy shard still takes one task from the global queue, so those tasks are not starved. */
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

  /** Runs this shard's thread. A fatal error stops the whole scheduler, so its fibers fail instead of hanging. On the
    * way out it closes the reactor and hands its leftover tasks to `afterClose`.
    */
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

  /** Runs a task that arrived after its scheduler closed on gears' default runtime, so the fiber it resumes can see the
    * closed scheduler and finish its cleanup.
    *
    * @param task
    *   the task to run
    */
  def afterClose(task: Runnable): Unit = gears.async.default.DefaultSupport.execute(() =>
    Continuations.handlersReset()
    try task.run()
    finally Continuations.handlersReset()
  )

  /** Returns the shard whose thread this is, if any. */
  def current(): Option[UringShard] = Thread.currentThread() match
    case t: UringShardThread => Some(t.shard)
    case _                   => None
