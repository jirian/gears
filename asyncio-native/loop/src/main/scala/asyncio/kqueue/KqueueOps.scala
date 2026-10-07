package asyncio.kqueue

import java.nio.ByteBuffer

import asyncio.Address
import asyncio.HandleSlot
import asyncio.Interest
import asyncio.Ops
import asyncio.Repeatable
import asyncio.ResolvedAddress
import asyncio.Slot

/** Everything `KqueueReactor` can run: its `Op` member type. Kqueue-specific code may construct the classes below
  * directly; code that only knows the `Reactor` interface builds them through `reactor.ops`.
  */
sealed trait KqueueOp {

  /** Whether this op has been submitted before, so the reactor can refuse to run a one-shot op twice. */
  @volatile private[kqueue] var submitted = false
}

/** Builds the ops `KqueueReactor` understands. */
object KqueueOps extends Ops[KqueueOp] {
  def read(fd: Int, buf: ByteBuffer): ReadIntoBuffer = ReadIntoBuffer(fd, buf)
  def write(fd: Int, buf: ByteBuffer): WriteFromBuffer = WriteFromBuffer(fd, buf)
  def accept(fd: Int, into: HandleSlot): Accept = Accept(fd, into)
  def connect(fd: Int): KqueueOp = Connect(fd)
  def receive(fd: Int, buf: ByteBuffer, from: Slot[Address]): ReceiveFrom = new ReceiveFrom(fd, buf, from)
  def send(fd: Int, buf: ByteBuffer, to: Address | Null): SendTo = new SendTo(fd, buf, to)
  def timer(milliseconds: Int): KqueueTimer = new KqueueTimer(milliseconds)
  def whenReady(fd: Int, interest: Interest)(perform: () => Boolean): WhenReady = new WhenReady(fd, interest, perform)
  def blocking(body: () => Unit): KqueueBlocking = new BlockingTask(body)
  def blocking[A <: AnyRef](body: () => A, into: Slot[A]): KqueueBlocking = new BlockingResultTask(body, into)
  def promise[A](): KqueueValuePromise[A] = new KqueueValuePromise[A]
  def resolve(host: String, into: Slot[List[ResolvedAddress]]): Resolve = new Resolve(host, into)
}

/** What to do with a descriptor once kqueue reports it ready. `perform` runs a non-blocking operation on the reactor
  * thread; its return value is the result, and an exception it throws is the op's failure. Commands are `Repeatable`:
  * waiting for readiness again and performing again is always meaningful.
  */
trait Command extends KqueueOp with Repeatable {
  def fd: Int
  def interest: Interest
  def perform(): Boolean
}

/** A command built from a function, for `Ops.whenReady`. */
final class WhenReady(val fd: Int, val interest: Interest, body: () => Boolean) extends Command {
  def perform(): Boolean = body()
}

/** A one-shot kqueue timer. `id` is the `EVFILT_TIMER` identifier assigned on submission, or -1 before it. */
final class KqueueTimer(val milliseconds: Int) extends KqueueOp with Repeatable {
  require(milliseconds >= 0, "Timer duration must not be negative")
  @volatile private var _id = -1
  def id: Int = _id
  private[kqueue] def assign(id: Int): Unit = _id = id
}

/** The reactor's side of a promise: completed from any thread, after which the reactor runs its completion on the loop
  * thread. `KqueueValuePromise` is the kind callers complete; `KqueueBlocking` completes itself.
  */
abstract class KqueuePromise extends KqueueOp {
  // `Unsettled` until the first completion or failure wins the compare-and-set; later ones are refused.
  private val outcome = new java.util.concurrent.atomic.AtomicReference[Any](KqueuePromise.Unsettled)
  @volatile private[kqueue] var reactor: KqueueReactor | Null = null

  /** The reactor's record of the pending completion; null once delivered or cancelled. */
  @volatile private[kqueue] var pending: AnyRef | Null = null

  private[kqueue] def failure: Throwable | Null = outcome.get() match {
    case KqueuePromise.Failed(failure) => failure
    case _                             => null
  }

  /** Completes the promise with a failure instead. */
  def fail(failure: Throwable): Unit = settle(KqueuePromise.Failed(failure))

  /** Tells the reactor this promise has finished, without a value. */
  private[kqueue] def signal(): Unit = settle(())

  /** Records the outcome, if this is the first, and tells the reactor this promise has finished. */
  protected final def settle(value: Any): Unit = {
    val owner = reactor
    if owner == null then throw new IllegalStateException("A promise must be submitted before it is completed")
    if !outcome.compareAndSet(KqueuePromise.Unsettled, value) then
      throw new IllegalStateException("A promise can only be completed once")
    owner.resolved(this)
  }

  protected final def settled: Any = outcome.get()
}

object KqueuePromise {
  private[kqueue] object Unsettled
  private[kqueue] final case class Failed(failure: Throwable)
}

/** A promise whose completer supplies a value, which it then holds as its result. */
final class KqueueValuePromise[A] extends KqueuePromise with asyncio.Promise[A] {
  def complete(value: A): Unit = settle(value)

  def result: A = settled match {
    case KqueuePromise.Unsettled       => throw new IllegalStateException("The promise has not been completed")
    case KqueuePromise.Failed(failure) => throw new IllegalStateException("The promise failed", failure)
    case value                         => value.asInstanceOf[A]
  }
}

/** A promise the reactor completes by running `block` on the blocker pool. An exception thrown by `block` is the op's
  * failure.
  *
  * Cancelling it skips `block` if it has not started, and interrupts the worker thread if it is running. Interruption
  * is cooperative: blocking JDK calls such as `Thread.sleep` throw `InterruptedException`, and long computations should
  * check `Thread.interrupted()`. Native calls like `getaddrinfo` cannot be interrupted and run to the end, with their
  * result discarded. Either way the completion's `onCancel` waits until `block` has returned, since until then it may
  * still write to the op's buffers and slots.
  *
  * Writes made before the cancel are allowed, as with a partly filled buffer, but none may follow it: a subclass must
  * write its final result into its slot through `publish`, which skips the write once cancelled.
  */
abstract class KqueueBlocking extends KqueuePromise {
  def block(): Unit

  // Guarded by `this`: the worker running `block`, whether the task was cancelled, and whether `block` has returned.
  private var runner: Thread | Null = null
  private var cancelled = false
  private var ended = false

  /** The cancelled submission whose `onCancel` waits for `block` to return. Loop thread only. */
  private[kqueue] var cancelling: AnyRef | Null = null

  /** Called by the worker before `block`; false if the task was cancelled before it started. */
  private[kqueue] def begin(): Boolean = synchronized {
    if cancelled then false
    else {
      runner = Thread.currentThread()
      true
    }
  }

  /** Called by the worker after `block`. Once this returns no cancel can interrupt the worker, so any interrupt that
    * raced in is cleared and cannot leak into the worker's next task.
    */
  private[kqueue] def end(): Unit = {
    synchronized {
      runner = null
      ended = true
      notifyAll()
    }
    Thread.interrupted()
  }

  /** Marks the task cancelled and interrupts its worker if it is running. Returns true if it was running, so `block`
    * may still touch the op's buffers and slots until it returns: the worker reports that through the reactor, like an
    * outcome, and `awaitEnd` waits for it.
    */
  private[kqueue] def interrupt(): Boolean = synchronized {
    cancelled = true
    val running = runner
    if running != null then running.interrupt()
    running != null
  }

  /** Writes `value` into `into` unless the task has been cancelled. It holds the lock `interrupt` takes, so the write
    * either happens entirely before the cancel or not at all.
    */
  protected final def publish[A <: AnyRef](into: Slot[A], value: A): Unit = synchronized {
    if !cancelled then into.set(value)
  }

  /** Waits until a running `block` has returned. An interrupt of the waiting thread is kept for later. */
  private[kqueue] def awaitEnd(): Unit = {
    var interrupted = false
    synchronized {
      while !ended do
        try wait()
        catch case _: InterruptedException => interrupted = true
    }
    if interrupted then Thread.currentThread().interrupt()
  }
}

/** A blocking task built from a function, for `Ops.blocking`. */
final class BlockingTask(body: () => Unit) extends KqueueBlocking {
  def block(): Unit = body()
}

/** A blocking task whose function's value is written into `into`, for `Ops.blocking`. */
final class BlockingResultTask[A <: AnyRef](body: () => A, into: Slot[A]) extends KqueueBlocking {
  def block(): Unit = {
    publish(into, body())
  }
}

/** The blocker pool: a few daemon threads that run blocking tasks off the reactor thread. */
object Blockers {
  private val queue = new java.util.concurrent.LinkedBlockingQueue[Runnable]()
  private lazy val threads = (1 to 4).map { i =>
    val thread = new Thread(
      () =>
        while true do {
          Thread.interrupted() // Never start a task with a stale interrupt.
          try queue.take().run()
          catch case _: InterruptedException => () // A late interrupt must not end the worker.
        },
      s"reactor-blocker-$i"
    )
    thread.setDaemon(true)
    thread.start()
    thread
  }

  /** Runs `blocking` on the pool and completes it with the outcome, unless it is cancelled first. */
  def run(blocking: KqueueBlocking): Unit = {
    threads
    queue.put { () =>
      if blocking.begin() then {
        var failure: Throwable | Null = null
        try blocking.block()
        catch case t: Throwable => failure = t
        finally blocking.end()
        // A cancelled task is detached, so neither outcome reaches its completion, but the reactor learns from it that
        // `block` has returned, which is when its `onCancel` runs.
        if failure != null then blocking.fail(failure.nn) else blocking.signal()
      }
    }
  }
}
