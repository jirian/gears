package gears.async.asyncio.uring

import asyncio.uring.UringReactor

import java.util.concurrent.ConcurrentLinkedQueue
import scala.scalanative.runtime.Continuations
import scala.util.control.NonFatal

/** One OS thread of the scheduler: it runs gears tasks and drives its own [[UringReactor]] in between. A reactor is
  * single-threaded, so anything that touches it from another thread is hopped onto this one with [[onLoop]].
  */
private[uring] final class UringShard(entries: Int, globalQueue: ConcurrentLinkedQueue[Runnable]):
  val reactor: UringReactor = UringReactor.open(entries)

  private val taskQueue = new ConcurrentLinkedQueue[Runnable]()

  /** Work that must run on this shard's own thread, because it uses the reactor. Unlike [[taskQueue]], siblings never
    * steal from it.
    */
  private val loopQueue = new ConcurrentLinkedQueue[Runnable]()

  private[uring] var siblings: Array[UringShard] = Array.empty

  private val rng = new scala.util.Random()

  private val PollTimeoutNanos = 2_000_000L // 2ms

  private[uring] def execute(body: Runnable, calledFromOwnThread: Boolean): Unit =
    taskQueue.offer(body)
    if !calledFromOwnThread then wake()

  private[uring] def wake(): Unit = reactor.wake()

  private[uring] def isCurrent: Boolean = Thread.currentThread() match
    case t: UringShardThread => t.shard eq this
    case _                   => false

  /** Runs `body` on this shard's thread, where its reactor may be used: right away when already on it. */
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
      try r.run()
      catch case NonFatal(e) => report(e)
      finally Continuations.handlersReset()
      r = loopQueue.poll()
    ran

  private def report(e: Throwable): Unit =
    val thread = Thread.currentThread()
    thread.getUncaughtExceptionHandler().uncaughtException(thread, e)

  private def drainTasks(): Boolean =
    val r = taskQueue.poll()
    if r == null then false
    else
      Continuations.handlersReset()
      try r.run()
      finally Continuations.handlersReset()
      true

  /** Runs the reactor's ready completions, waiting up to `timeoutNanos` for one. A completion that throws is reported
    * and the loop carries on, rather than taking the shard's thread down with it.
    */
  private def pollReactor(timeoutNanos: Long): Boolean =
    Continuations.handlersReset()
    try reactor.poll(timeoutNanos)
    catch
      case NonFatal(e) =>
        report(e)
        true
    finally Continuations.handlersReset()

  private[uring] def stealTask(): Runnable = taskQueue.poll()

  private def tryStealOrGlobal(): Boolean =
    val fromGlobal = globalQueue.poll()
    val task = if fromGlobal != null then fromGlobal else stealFromSibling()
    if task == null then false
    else
      Continuations.handlersReset()
      try task.run()
      finally Continuations.handlersReset()
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

  private[uring] def loop(): Unit =
    Continuations.handlersReset()
    while true do
      val ranLoopWork = drainLoopQueue()
      val ranTasks = drainTasks()
      val ranCompletions = pollReactor(0)
      if !ranLoopWork && !ranTasks && !ranCompletions && !tryStealOrGlobal() then pollReactor(PollTimeoutNanos)

private[uring] final class UringShardThread(val shard: UringShard) extends Thread(() => shard.loop())

private[uring] object UringShard:
  def current(): Option[UringShard] = Thread.currentThread() match
    case t: UringShardThread => Some(t.shard)
    case _                   => None
