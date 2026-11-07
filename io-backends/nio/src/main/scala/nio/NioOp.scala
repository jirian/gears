package gears.async.io
package nio

import gears.async.Async
import gears.async.cancellationScope

import java.nio.channels.SelectableChannel
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.util.concurrent.CancellationException
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean

/** One performance of an op: its wait, when it began, and its deadline, `Long.MaxValue` for none. */
final class Ctx(val waiter: Wait, val start: Long, val deadline: Long) {

  /** Times the op found its channel not ready and yielded, before it waits on a selector. */
  private var tries = 0

  /** Whether the op has found its channel not ready at all, for `NioOp`'s budget of ops done at once. */
  private[nio] var waited = false

  /** A selector taken for a channel with no handle yet, a connecting socket's, given back by `releaseLoose`. */
  private var loose: Selector | Null = null

  def expired: Boolean = deadline != Long.MaxValue && System.nanoTime() - deadline >= 0

  /** Throws if the op has been cancelled, or its time limit has passed. */
  def checkStop(op: AnyRef)(using ac: Async): Unit = {
    if waiter.cancelled || ac.group.isCancelled then throw new CancellationException()
    if expired then throw new TimeoutException(s"$op timed out")
  }

  /** Parks until signalled, cancelled, or the deadline. */
  def park()(using Async): Unit = parkUntil(deadline)

  /** Parks until signalled, cancelled, or `at` on `System.nanoTime`'s clock. */
  def parkUntil(at: Long)(using Async): Unit = linked(waiter.park(at))

  /** Runs `wait` with the op linked to the caller's group, so that a cancel wakes it. An op is linked only while it
    * waits: one done at once, the usual case, does not lock its group twice, and is stopped by `checkStop` instead. A
    * group cancelled already cancels the wait as it is linked, so no cancel is missed.
    */
  private def linked(wait: => Unit)(using Async): Unit = cancellationScope(waiter)(wait)

  /** After `channel` was found not ready for `ops`: yields, up to `NioOnCaller.Retries` times, since in request and
    * response traffic the other end often answers within microseconds and a yield puts the caller behind every other
    * thread ready to run; after that waits on `handle`'s selector for that direction (a selector of its own for a
    * channel with no handle) until the channel is ready. Fails if `handle` is closed meanwhile, or the op is cancelled
    * or out of time; else returns, for the op to try again.
    */
  def ready(op: AnyRef, handle: NioHandle | Null, channel: SelectableChannel, ops: Int)(using Async): Unit = {
    waited = true
    if tries < NioOnCaller.Retries then {
      tries += 1
      Thread.`yield`()
    } else {
      waiter.reset()
      val write = (ops & (SelectionKey.OP_WRITE | SelectionKey.OP_CONNECT)) != 0
      val selector =
        if handle != null then handle.selector(write)
        else {
          if loose == null then loose = SelectorPool.take()
          loose.nn
        }
      val key = channel.keyFor(selector)
      if key == null then channel.register(selector, ops): Unit
      else if key.interestOps() != ops then key.interestOps(ops): Unit
      linked(waiter.select(selector, deadline))
    }
    if handle != null then handle.checkOpen()
    checkStop(op)
  }

  /** Gives back the selector `ready` took for a channel with no handle, with the channel taken off it. */
  def releaseLoose(): Unit = {
    val selector = loose
    if selector != null then {
      loose = null
      SelectorPool.release(selector)
    }
  }
}

/** An op of `NioOnCaller`, performed by the caller (`perform`), which makes the checks every op makes, in this order:
  * not cancelled already; a one-shot op not performed before (else `IllegalStateException`), nor any op being performed
  * now; its slots empty and what `check` checks (else `IllegalArgumentException`); its handle open (else the closed
  * handle's failure); then the op itself (`body`), with its handle taken up while it runs, so that a close fails it and
  * waits for it.
  */
abstract class NioOp {
  private val used = new AtomicBoolean()
  private val busy = new AtomicBoolean()

  /** Performances in a row that found their channel ready at once (`NioOnCaller.HereBudget`); one at a time. */
  private var hits = 0

  /** The slots the op writes, which must be empty as it is performed. */
  protected def slots: List[Slot[?]] = Nil

  /** The handle the op is on, if any. */
  protected def handle: NioHandle | Null = null

  /** Checks made as the op is performed, before anything is done: `IllegalArgumentException`s. */
  protected def check(): Unit = ()

  protected def body(ctx: Ctx)(using Async): Unit

  final def perform(timeoutMillis: Int)(using ac: Async): Unit = {
    if ac.group.isCancelled then throw new CancellationException()
    if !this.isInstanceOf[Repeatable] && !used.compareAndSet(false, true) then
      throw new IllegalStateException(s"$this is not Repeatable, and has been performed already")
    if !busy.compareAndSet(false, true) then throw new IllegalArgumentException(s"$this is already being performed")
    val wait = new Wait
    try {
      if slots.exists(!_.isEmpty) then
        throw new IllegalArgumentException(
          "The op's slot still holds a value, not yet cleared, which the op would overwrite"
        )
      check()
      val start = System.nanoTime()
      val deadline = if timeoutMillis < 0 then Long.MaxValue else start + timeoutMillis * 1_000_000L
      val ctx = Ctx(wait, start, deadline)
      // A run of ops done at once gives up the thread now and then, so the futures beside the caller run.
      if hits >= NioOnCaller.HereBudget then {
        hits = 0
        Thread.`yield`()
      }
      val h = handle
      try
        if h == null then body(ctx)
        else {
          h.enter(wait)
          try body(ctx)
          finally h.leave(wait)
        }
      finally ctx.releaseLoose()
      if ctx.waited then hits = 0 else hits += 1
    } finally {
      wait.retired = true
      busy.set(false)
    }
  }

  override def toString: String = getClass.getSimpleName.stripSuffix("Op")
}
