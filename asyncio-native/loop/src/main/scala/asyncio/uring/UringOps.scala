package asyncio.uring

import java.io.IOException
import java.nio.ByteBuffer
import scala.concurrent.duration.FiniteDuration
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import asyncio.Address
import asyncio.Interest
import asyncio.Ops
import asyncio.Repeatable
import asyncio.ResolvedAddress
import asyncio.Slot
import asyncio.unsafe.PosixResolver
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
final class UringOps[Owner] extends Ops[UringOp[Owner], Int, Integer] {
  def read(fd: Int, buf: ByteBuffer): Read[Owner] = new Read(fd, buf)
  def write(fd: Int, buf: ByteBuffer): Write[Owner] = new Write(fd, buf, plain = false)
  def accept(fd: Int, into: Slot[Integer]): Accept[Owner] = new Accept(fd, into)
  def connect(fd: Int): UringOp[Owner] = new Connect(fd)
  def receive(fd: Int, buf: ByteBuffer, from: Slot[Address]): Receive[Owner] = new Receive(fd, buf, from)
  def send(fd: Int, buf: ByteBuffer, to: Address | Null): Send[Owner] = new Send(fd, buf, to)
  def timer(milliseconds: Int): UringTimer[Owner] = new UringTimer(milliseconds * 1_000_000L)
  def whenReady(fd: Int, interest: Interest)(perform: () => Boolean): WhenReady[Owner] =
    new WhenReady(fd, interest, perform)
  def blocking(body: () => Unit): UringBlocking[Owner] = new BlockingTask(body)
  def blocking[A <: AnyRef](body: () => A, into: Slot[A]): UringBlocking[Owner] =
    new BlockingTask(() => into.set(body()))
  def promise(): UringSignal[Owner] = new UringSignal
  def promise[A <: AnyRef](into: Slot[A]): UringValuePromise[Owner, A] = new UringValuePromise(into)
  def resolve(host: String, into: Slot[List[ResolvedAddress]]): Resolve[Owner] = new Resolve(host, into)

  // Uring-only ops, beyond the `Ops` interface.

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
  def openAt(path: String, flags: Int, mode: Int, into: Slot[Integer]): OpenAt[Owner] =
    new OpenAt(path, flags, mode, into)
}

/** The reactor's side of a promise: completed from any thread, after which the reactor runs its completion on the loop
  * thread. `UringSignal` and `UringValuePromise` are the kinds callers complete; `UringBlocking` completes itself.
  */
abstract class UringPromise[Owner] extends UringOp[Owner] {
  @volatile private var _failure: Throwable | Null = null
  @volatile private[uring] var reactor: UringReactor | Null = null

  /** The reactor's record of the pending completion; null once delivered or cancelled. */
  @volatile private[uring] var pending: AnyRef | Null = null

  private[uring] def failure: Throwable | Null = _failure

  /** Completes the promise with a failure instead. */
  def fail(failure: Throwable): Unit = {
    _failure = failure
    signal()
  }

  /** Tells the reactor this promise has finished. */
  private[uring] def signal(): Unit = {
    val owner = reactor
    if owner == null then throw new IllegalStateException("A promise must be submitted before it is completed")
    owner.resolved(this)
  }
}

/** A promise that carries no value. */
final class UringSignal[Owner] extends UringPromise[Owner] with asyncio.Promise[Unit] {
  def complete(value: Unit): Unit = signal()
}

/** A promise whose completer supplies a value, written into `into` before the completion runs. */
final class UringValuePromise[Owner, A <: AnyRef](into: Slot[A]) extends UringPromise[Owner] with asyncio.Promise[A] {
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
abstract class UringBlocking[Owner] extends UringPromise[Owner] {
  def block(): Unit

  // Guarded by `this`: the worker running `block`, and whether the task was cancelled.
  private var runner: Thread | Null = null
  private var cancelled = false

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
    synchronized { runner = null }
    Thread.interrupted()
  }

  /** Marks the task cancelled and interrupts its worker if it is running. */
  private[uring] def interrupt(): Unit = synchronized {
    cancelled = true
    if runner != null then runner.nn.interrupt()
  }
}

/** A blocking task built from a function, for `Ops.blocking`. */
final class BlockingTask[Owner](body: () => Unit) extends UringBlocking[Owner] {
  def block(): Unit = body()
}

/** Resolves a host name without blocking the reactor. There is no `getaddrinfo` op in io_uring, so the lookup runs on
  * the blocker pool. Completes after writing the addresses into `into`, or fails with an `IOException` carrying the
  * resolver's error.
  */
final class Resolve[Owner](val host: String, into: Slot[List[ResolvedAddress]]) extends UringBlocking[Owner] {
  def block(): Unit = PosixResolver.lookup(host) match {
    case Right(addresses) => into.set(addresses)
    case Left(error)      => throw new IOException(s"Failed to resolve $host: $error")
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
      s"uring-blocker-$i"
    )
    thread.setDaemon(true)
    thread.start()
    thread
  }

  /** Runs `blocking` on the pool and completes it with the outcome, unless it is cancelled first. */
  def run(blocking: UringBlocking[?]): Unit = {
    threads
    queue.put { () =>
      if blocking.begin() then {
        var failure: Throwable | Null = null
        try blocking.block()
        catch case t: Throwable => failure = t
        finally blocking.end()
        // A cancelled task is detached, so neither outcome reaches its completion.
        if failure != null then blocking.fail(failure.nn) else blocking.signal()
      }
    }
  }
}
