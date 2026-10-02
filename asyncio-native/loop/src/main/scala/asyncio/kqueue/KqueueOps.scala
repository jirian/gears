package asyncio.kqueue

import java.nio.ByteBuffer

import asyncio.Address
import asyncio.BlockingPool
import asyncio.HostResolver
import asyncio.Interest
import asyncio.Ops
import asyncio.Repeatable
import asyncio.ResolvedAddress
import asyncio.Slot

/** Everything `KqueueReactor` can run: its `Op` member type. Kqueue-specific code may construct the classes below
  * directly; code that only knows the `Reactor` interface builds them through `reactor.ops`.
  */
sealed trait KqueueOp[Owner] {

  /** Whether this op has been submitted before, so the reactor can refuse to run a one-shot op twice. */
  @volatile private[kqueue] var submitted = false
}

/** Builds the ops `KqueueReactor` understands. */
final class KqueueOps[Owner](resolver: HostResolver) extends Ops[KqueueOp[Owner], Int, Integer] {
  def read(fd: Int, buf: ByteBuffer): ReadIntoBuffer[Owner] = ReadIntoBuffer(fd, buf)
  def readv(fd: Int, bufs: Array[ByteBuffer]): ReadVector[Owner] = new ReadVector(fd, bufs)
  def write(fd: Int, buf: ByteBuffer): WriteFromBuffer[Owner] = WriteFromBuffer(fd, buf)
  def writev(fd: Int, bufs: Array[ByteBuffer]): WriteVector[Owner] = new WriteVector(fd, bufs)
  def accept(fd: Int, into: Slot[Integer]): Accept[Owner] = Accept(fd, into)
  def connect(fd: Int): KqueueOp[Owner] = Connect(fd)
  def receive(fd: Int, buf: ByteBuffer, from: Slot[Address]): ReceiveFrom[Owner] = new ReceiveFrom(fd, buf, from)
  def send(fd: Int, buf: ByteBuffer, to: Address | Null): SendTo[Owner] = new SendTo(fd, buf, to)
  def timer(milliseconds: Int): KqueueTimer[Owner] = new KqueueTimer(milliseconds)
  def whenReady(fd: Int, interest: Interest)(perform: () => Boolean): WhenReady[Owner] =
    new WhenReady(fd, interest, perform)
  def blocking(body: () => Unit): KqueueBlocking[Owner] = new BlockingTask(body)
  def blocking[A <: AnyRef](body: () => A, into: Slot[A]): KqueueBlocking[Owner] =
    new BlockingTask(() => into.set(body()))
  def promise(): KqueueSignal[Owner] = new KqueueSignal
  def promise[A <: AnyRef](into: Slot[A]): KqueueValuePromise[Owner, A] = new KqueueValuePromise(into)
  def resolve(host: String, into: Slot[List[ResolvedAddress]]): Resolve[Owner] = new Resolve(host, into, resolver)
}

/** What to do with a descriptor once kqueue reports it ready. `perform` runs a non-blocking operation on the reactor
  * thread; its return value is the result, and an exception it throws is the op's failure. Commands are `Repeatable`:
  * waiting for readiness again and performing again is always meaningful.
  */
trait Command[Owner] extends KqueueOp[Owner] with Repeatable {
  def fd: Int
  def interest: Interest
  def perform(): Boolean
}

/** A command built from a function, for `Ops.whenReady`. */
final class WhenReady[Owner](val fd: Int, val interest: Interest, body: () => Boolean) extends Command[Owner] {
  def perform(): Boolean = body()
}

/** A one-shot kqueue timer. `id` is the `EVFILT_TIMER` identifier assigned on submission, or -1 before it. */
final class KqueueTimer[Owner](val milliseconds: Int) extends KqueueOp[Owner] with Repeatable {
  require(milliseconds >= 0, "Timer duration must not be negative")
  @volatile private var _id = -1
  def id: Int = _id
  private[kqueue] def assign(id: Int): Unit = _id = id
}

/** The reactor's side of a promise: completed from any thread, after which the reactor runs its completion on the loop
  * thread. `KqueueSignal` and `KqueueValuePromise` are the kinds callers complete; `KqueueBlocking` completes itself.
  */
abstract class KqueuePromise[Owner] extends KqueueOp[Owner] {
  @volatile private var _failure: Throwable | Null = null
  @volatile private[kqueue] var reactor: KqueueReactor | Null = null

  /** The reactor's record of the pending completion; null once delivered or cancelled. */
  @volatile private[kqueue] var pending: AnyRef | Null = null

  private[kqueue] def failure: Throwable | Null = _failure

  /** Completes the promise with a failure instead. */
  def fail(failure: Throwable): Unit = {
    _failure = failure
    signal()
  }

  /** Tells the reactor this promise has finished. */
  private[kqueue] def signal(): Unit = {
    val owner = reactor
    if owner == null then throw new IllegalStateException("A promise must be submitted before it is completed")
    owner.resolved(this)
  }
}

/** A promise that carries no value. */
final class KqueueSignal[Owner] extends KqueuePromise[Owner] with asyncio.Promise[Unit] {
  def complete(value: Unit): Unit = signal()
}

/** A promise whose completer supplies a value, written into `into` before the completion runs. */
final class KqueueValuePromise[Owner, A <: AnyRef](into: Slot[A]) extends KqueuePromise[Owner] with asyncio.Promise[A] {
  def complete(value: A): Unit = {
    into.set(value)
    signal()
  }
}

/** A promise the reactor completes by running `block` on the blocker pool. An exception thrown by `block` is the op's
  * failure.
  *
  * Cancelling it skips `block` if it has not started, and interrupts the worker thread if it is running. Interruption
  * is cooperative: blocking JDK calls such as `Thread.sleep` throw `InterruptedException`, and long computations should
  * check `Thread.interrupted()`. Native calls like `getaddrinfo` cannot be interrupted and run to the end, with their
  * result discarded.
  */
abstract class KqueueBlocking[Owner] extends KqueuePromise[Owner] {
  def block(): Unit

  // Guarded by `this`: the worker running `block`, and whether the task was cancelled.
  private var runner: Thread | Null = null
  private var cancelled = false
  private var finished = false
  private var settledCallback: (() => Unit) | Null = null

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
    synchronized { runner = null }
    Thread.interrupted()
  }

  /** Runs after the worker has stopped touching this op's result slot. */
  private[kqueue] def whenSettled(callback: () => Unit): Unit = {
    val runNow = synchronized {
      if finished then true
      else {
        settledCallback = callback
        false
      }
    }
    if runNow then callback()
  }

  private[kqueue] def finish(): Unit = {
    val callback = synchronized {
      finished = true
      val current = settledCallback
      settledCallback = null
      current
    }
    if callback != null then callback.nn()
  }

  /** Marks the task cancelled and interrupts its worker if it is running. */
  private[kqueue] def interrupt(): Unit = synchronized {
    cancelled = true
    if runner != null then runner.nn.interrupt()
  }
}

/** A blocking task built from a function, for `Ops.blocking`. */
final class BlockingTask[Owner](body: () => Unit) extends KqueueBlocking[Owner] {
  def block(): Unit = body()
}

/** The blocker pool: a few daemon threads that run blocking tasks off the reactor thread. */
object Blockers {
  /** Runs `blocking` on the pool and completes it with the outcome, unless it is cancelled first. */
  def run(blocking: KqueueBlocking[?], pool: BlockingPool): Unit =
    pool.execute(() => {
      val started = blocking.begin()
      try
        if started then {
          var failure: Throwable | Null = null
          try blocking.block()
          catch case t: Throwable => failure = t
          finally blocking.end()
          // A cancelled task is detached, so neither outcome reaches its completion.
          if failure != null then blocking.fail(failure.nn) else blocking.signal()
        }
      finally blocking.finish()
    })
}
