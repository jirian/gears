package asyncio.uring

import java.io.IOException
import java.nio.ByteBuffer
import scala.concurrent.duration.FiniteDuration
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import asyncio.Address
import asyncio.HandleSlot
import asyncio.Interest
import asyncio.Ops
import asyncio.Repeatable
import asyncio.ResolvedAddress
import asyncio.Slot
import uring.*
import uringOps.*

/** Everything `UringReactor` can run: its `Op` member type. Uring-specific code may construct the classes below
  * directly, or use the extra builders on `reactor.ops`; code that only knows the `Reactor` interface builds them through
  * `reactor.ops`.
  */
sealed abstract class UringOp[Owner] {

  /** Whether this op has been submitted before, so the reactor can refuse to run a one-shot op twice. */
  @volatile private[uring] var submitted = false
}

/** The ops the kernel runs: each submission is one SQE, and finishes with its CQE. Unlike kqueue's readiness commands,
  * the kernel works on the op's buffers while it is in flight, so the reactor keeps a cancelled op reachable until its
  * CQE arrives, and `discard` releases whatever such a late CQE produced.
  */
abstract class RingOp[Owner] extends UringOp[Owner] {

  /** The reactor's record of this op's submission in flight; null when it is not pending. Loop thread only. */
  private[uring] var inFlight: AnyRef | Null = null

  /** Fills in `sqe` for one submission. Runs again for each resubmission, so it reads the buffers afresh. */
  private[uring] def prepare(sqe: Ptr[io_uring_sqe]): Unit

  /** Interprets the CQE's `res`: true once the op is done, false to prepare and submit it again, such as after a
    * spurious wake-up. Throws if the op failed.
    */
  private[uring] def finish(res: Int): Boolean

  /** The CQE of a cancelled or abandoned submission arrived after all, with `res`. Releases what it produced. */
  private[uring] def discard(res: Int): Unit = ()

  /** Fills in `sqe` to cancel the submission whose user data is `userData`. */
  private[uring] def prepareCancel(sqe: Ptr[io_uring_sqe], userData: Long): Unit =
    io_uring_prep_cancel64(sqe, userData.toULong, 0)
}

/** Builds the ops `UringReactor` understands: the `Ops` interface, plus a few uring-only ones. */
final class UringOps[Owner](resolver: HostResolver) extends Ops[UringOp[Owner]] {
  def read(fd: Int, buf: ByteBuffer): Read[Owner] = new Read(fd, buf)
  def write(fd: Int, buf: ByteBuffer): Write[Owner] = new Write(fd, buf, plain = false)
  def accept(fd: Int, into: HandleSlot): Accept[Owner] = new Accept(fd, into)
  def connect(fd: Int): UringOp[Owner] = new Connect(fd)
  def receive(fd: Int, buf: ByteBuffer, from: Slot[Address]): Receive[Owner] = new Receive(fd, buf, from)
  def send(fd: Int, buf: ByteBuffer, to: Address | Null): Send[Owner] = new Send(fd, buf, to)
  def timer(milliseconds: Int): UringTimer[Owner] = new UringTimer(milliseconds * 1_000_000L)
  def whenReady(fd: Int, interest: Interest)(perform: () => Boolean): WhenReady[Owner] =
    new WhenReady(fd, interest, perform)
  def blocking(body: () => Unit): UringBlocking[Owner] = new BlockingTask(body)
  def blocking[A <: AnyRef](body: () => A, into: Slot[A]): UringBlocking[Owner] = new BlockingResultTask(body, into)
  def promise[A](): UringValuePromise[Owner, A] = new UringValuePromise
  def resolve(host: String, into: Slot[List[ResolvedAddress]]): Resolve[Owner] = new Resolve(host, into, resolver)

  // Uring-only ops, beyond the `Ops` interface.

  /** Like `read`, filling `bufs` in order. Completes once at least one byte has been read, or at end of stream. */
  def readv(fd: Int, bufs: Array[ByteBuffer]): ReadVector[Owner] = new ReadVector(fd, bufs)

  /** Like `write`, draining `bufs` in order. Completes after any progress, which may cover only part of the vector. */
  def writev(fd: Int, bufs: Array[ByteBuffer]): WriteVector[Owner] = new WriteVector(fd, bufs)

  /** Like `write`, for a descriptor known not to be a socket, such as a regular file. `write` sends with
    * `MSG_NOSIGNAL`, so a closed peer is an error rather than `SIGPIPE`, and only falls back to a plain write on the
    * first `ENOTSOCK`.
    */
  def writeFile(fd: Int, buf: ByteBuffer): Write[Owner] = new Write(fd, buf, plain = true)

  /** A timer with nanosecond resolution. */
  def timer(duration: FiniteDuration): UringTimer[Owner] = new UringTimer(duration.toNanos)

  /** `openat(2)` relative to the working directory, run by the kernel without blocking the reactor, unlike
    * `Handles.openFile`. Completes after writing the new descriptor into `into`, which must be empty.
    */
  def openAt(path: String, flags: Int, mode: Int, into: HandleSlot): OpenAt[Owner] =
    new OpenAt(path, flags, mode, into)
}

/** The reactor's side of a promise: completed from any thread, after which the reactor runs its completion on the loop
  * thread. `UringValuePromise` is the kind callers complete; `UringBlocking` completes itself.
  */
abstract class UringPromise[Owner] extends UringOp[Owner] {
  // `Unsettled` until the first completion or failure wins the compare-and-set; later ones are refused.
  private val outcome = new java.util.concurrent.atomic.AtomicReference[Any](UringPromise.Unsettled)
  @volatile private[uring] var reactor: UringReactor | Null = null

  /** The reactor's record of the pending completion; null once delivered or cancelled. */
  @volatile private[uring] var pending: AnyRef | Null = null

  private[uring] def failure: Throwable | Null = outcome.get() match {
    case UringPromise.Failed(failure) => failure
    case _                            => null
  }

  /** Completes the promise with a failure instead. */
  def fail(failure: Throwable): Unit = settle(UringPromise.Failed(failure))

  /** Tells the reactor this promise has finished, without a value. */
  private[uring] def signal(): Unit = settle(())

  /** Records the outcome, if this is the first, and tells the reactor this promise has finished. */
  protected final def settle(value: Any): Unit = {
    val owner = reactor
    if owner == null then throw new IllegalStateException("A promise must be submitted before it is completed")
    if !outcome.compareAndSet(UringPromise.Unsettled, value) then
      throw new IllegalStateException("A promise can only be completed once")
    owner.resolved(this)
  }

  protected final def settled: Any = outcome.get()
}

object UringPromise {
  private[uring] object Unsettled
  private[uring] final case class Failed(failure: Throwable)
}

/** A promise whose completer supplies a value, which it then holds as its result. */
final class UringValuePromise[Owner, A] extends UringPromise[Owner] with asyncio.Promise[A] {
  def complete(value: A): Unit = settle(value)

  def result: A = settled match {
    case UringPromise.Unsettled       => throw new IllegalStateException("The promise has not been completed")
    case UringPromise.Failed(failure) => throw new IllegalStateException("The promise failed", failure)
    case value                        => value.asInstanceOf[A]
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
abstract class UringBlocking[Owner] extends UringPromise[Owner] {
  def block(): Unit

  // Guarded by `this`: the worker running `block`, whether the task was cancelled, and whether `block` has returned.
  private var runner: Thread | Null = null
  private var cancelled = false
  private var ended = false

  /** The cancelled submission whose `onCancel` waits for `block` to return. Loop thread only. */
  private[uring] var cancelling: AnyRef | Null = null

  /** Called by the worker before `block`; false if the task was cancelled before it started. */
  private[uring] def begin(): Boolean = synchronized {
    if cancelled then false
    else {
      runner = Thread.currentThread()
      true
    }
  }

  /** Called by the worker after `block`. Once this returns no cancel can interrupt the worker, so any interrupt that
    * raced in is cleared and cannot leak into the worker's next task.
    */
  private[uring] def end(): Unit = {
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
  private[uring] def interrupt(): Boolean = synchronized {
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
  private[uring] def awaitEnd(): Unit = {
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
final class BlockingTask[Owner](body: () => Unit) extends UringBlocking[Owner] {
  def block(): Unit = body()
}

/** A blocking task whose function's value is written into `into`, for `Ops.blocking`. */
final class BlockingResultTask[Owner, A <: AnyRef](body: () => A, into: Slot[A]) extends UringBlocking[Owner] {
  def block(): Unit = publish(into, body())
}

/** Resolves a host name without blocking the reactor. There is no `getaddrinfo` op in io_uring, so the lookup runs on
  * the blocker pool. Completes after writing the addresses into `into`, or fails with an `IOException` carrying the
  * resolver's error.
  */
final class Resolve[Owner](val host: String, into: Slot[List[ResolvedAddress]], resolver: HostResolver)
    extends UringBlocking[Owner] {
  def block(): Unit = resolver.resolve(host) match {
    case Right(addresses) => publish(into, addresses)
    case Left(error)      => throw new IOException(s"Failed to resolve $host: $error")
  }
}

/** Runs blocking tasks on a reactor's `BlockingPool`. */
object Blockers {

  /** Runs `blocking` on `pool` and completes it with the outcome, unless it is cancelled first. A full or closed pool
    * throws `RejectedExecutionException` here, on the submitting thread.
    */
  def run(blocking: UringBlocking[?], pool: BlockingPool): Unit =
    pool.execute { () =>
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
