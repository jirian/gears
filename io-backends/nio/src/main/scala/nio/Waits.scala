package gears.async.io
package nio

import gears.async.Cancellable

import java.nio.channels.Selector
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.LockSupport

/** What an op performed by `NioOnCaller` waits with, on the thread performing it: parked until it is signalled, by a
  * close, a turn passed on or a moved deadline, or until its time limit, or until the caller's group cancels it, which
  * it is linked to while it waits; or, while it waits for its channel to be ready, in a selector's `select` (`select`),
  * which a signal or a cancel wakes with `Selector.wakeup`.
  */
final class Wait extends Cancellable {
  private val thread = Thread.currentThread()
  @volatile private var signalled = false
  @volatile private var stopped = false
  @volatile private var selecting: Selector | Null = null

  /** Set once the op's `perform` has returned: the wait is then used no more. */
  @volatile var retired = false

  /** Whether the caller's group has cancelled the op. */
  def cancelled: Boolean = stopped

  /** Forgets earlier signals: called before whatever is to signal it is looked at, so no signal is lost. */
  def reset(): Unit = signalled = false

  /** Wakes the thread waiting, from any thread. */
  def signal(): Unit = {
    signalled = true
    wake()
  }

  def cancel(): Unit = {
    stopped = true
    wake()
  }

  private def wake(): Unit = {
    LockSupport.unpark(thread)
    val selector = selecting
    if selector != null then selector.wakeup(): Unit
  }

  /** Parks until signalled, cancelled, or `deadline` on `System.nanoTime`'s clock, `Long.MaxValue` for none. */
  def park(deadline: Long): Unit =
    if deadline == Long.MaxValue then while !signalled && !stopped do LockSupport.park(this)
    else {
      var left = deadline - System.nanoTime()
      while !signalled && !stopped && left > 0 do {
        LockSupport.parkNanos(this, left)
        left = deadline - System.nanoTime()
      }
    }

  /** Parks until signalled, whether cancelled or not: for what must finish once begun. */
  def parkUntilSignalled(): Unit = while !signalled do LockSupport.park(this)

  /** Waits in `selector`, on which the channel is registered for the readiness the op needs, until it is ready, the
    * wait is signalled or cancelled, or `deadline`, and returns, for the op to try again. A virtual thread selecting is
    * parked by the JDK's poller, so this blocks no carrier. Set `selecting` before looking at the flags, and `wake`
    * reads it after setting them, so either the flags are seen here or the selector is woken.
    */
  def select(selector: Selector, deadline: Long): Unit = {
    selecting = selector
    try
      if !signalled && !stopped then {
        if deadline == Long.MaxValue then selector.select(Selects.Ignore): Unit
        else {
          val left = deadline - System.nanoTime()
          if left > 0 then selector.select(Selects.Ignore, (left + 999_999L) / 1_000_000L): Unit // due when it returns
        }
      }
    finally selecting = null
  }
}

private[nio] object Selects {

  /** For `select`'s action: the op tries again itself, so the selected keys need nothing done. */
  val Ignore: java.util.function.Consumer[java.nio.channels.SelectionKey] = _ => ()
}

/** Ops taking turns on a handle, first come first served: a datagram socket's receives, or its sends, or a listener's
  * accepts. The op at the head holds the turn; the others wait for it to be passed on.
  */
final class Turns {
  private val queue = new java.util.ArrayDeque[Wait]()

  /** Waits for `ctx`'s turn, failing as `ctx.stopped` says while it waits, out of the queue. */
  def take(ctx: Ctx, handle: NioHandle, op: AnyRef)(using gears.async.Async): Unit = {
    val wait = ctx.waiter
    synchronized(queue.addLast(wait))
    try
      while {
        wait.reset()
        !synchronized(queue.peekFirst() eq wait)
      } do {
        ctx.park()
        handle.checkOpen()
        ctx.checkStop(op)
      }
    catch {
      case e: Throwable =>
        release(wait)
        throw e
    }
  }

  /** Gives up `wait`'s turn, or its place in the queue, waking the op whose turn it is then. */
  def release(wait: Wait): Unit = {
    val next = synchronized {
      val head = queue.peekFirst() eq wait
      queue.remove(wait)
      if head then queue.peekFirst() else null
    }
    if next != null then next.signal()
  }
}

/** Selectors for handles to wait on (`NioHandle.selector`), reused: opening one makes an epoll or a kqueue and a
  * wake-up pipe, which a connection's ops would otherwise pay for on every connection.
  */
private[nio] object SelectorPool {

  /** Selectors kept at most, beyond which a returned one is closed. */
  private inline val Kept = 256

  private val idle = new ConcurrentLinkedQueue[Selector]()
  private val count = new AtomicInteger()

  def take(): Selector = {
    val kept = idle.poll()
    if kept == null then Selector.open()
    else {
      count.decrementAndGet()
      kept
    }
  }

  /** Takes back a selector no op waits on, with nothing registered on it. A `selectNow` clears a wake-up left from a
    * signal or a cancel.
    */
  def give(selector: Selector): Unit = {
    selector.selectNow(): Unit
    if count.incrementAndGet() <= Kept then idle.add(selector): Unit
    else {
      count.decrementAndGet()
      selector.close()
    }
  }

  /** Takes every channel off `selector` and returns it to the pool: the keys cancelled and selected away, so the JDK
    * lets go of their descriptors, and a close of one takes effect, the port free and the peer seeing the end, at once.
    */
  def release(selector: Selector): Unit = {
    selector.keys().forEach(_.cancel())
    give(selector)
  }
}
