package gears.io.uring

import gears.io.reactor.Address
import gears.io.reactor.HandleSlot
import gears.io.reactor.Interest
import gears.io.reactor.Ops
import gears.io.reactor.ResolvedAddress
import gears.io.reactor.Slot

import java.io.IOException
import java.nio.ByteBuffer
import scala.concurrent.duration.FiniteDuration
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import uring.*
import uringStructOps.*

/** Every op a `UringReactor` runs. An op is either a [[UringKernelOp]], which the kernel runs through the ring, or a
  * [[UringBlockingOp]], which a pool thread runs. A reactor accepts only ops of its own, see [[Owned]].
  */
sealed abstract class UringOp {
  private[uring] var submitted = false
}

/** An op built by the reactor whose singleton type is `R`. A `UringReactor`'s `Op` type is `Owned[this.type]`, and only
  * its `ops` brand ops that way, so an op built by one reactor cannot be submitted to or cancelled on another, even
  * when both are known to be `UringReactor`s.
  */
opaque type Owned[R] <: UringOp = UringOp

object Owned {

  /** Returns `op` branded as built by the reactor whose singleton type is `R`.
    *
    * @tparam R
    *   the singleton type of the reactor that owns `op`
    * @tparam T
    *   the op's own class, which the result keeps
    * @param op
    *   the op to brand
    */
  private[uring] def apply[R, T <: UringOp](op: T): Owned[R] & T = op
}

/** An op the kernel runs: the reactor turns it into one submission queue entry, and its completion queue entry settles
  * it. All of its methods run on the reactor thread.
  */
abstract class UringKernelOp extends UringOp {
  private[uring] var inFlight: AnyRef | Null = null

  /** Fills in `sqe` for one submission of this op. It is called again for each resubmission.
    *
    * @param sqe
    *   the submission queue entry to fill in
    */
  private[uring] def prepare(sqe: Ptr[io_uring_sqe]): Unit

  /** Handles the result of one submission, and throws to fail this op.
    *
    * @param res
    *   the completion's result: a count or descriptor, or a negated `errno`
    * @return
    *   true when this op is done, false to submit it again
    */
  private[uring] def finish(res: Int): Boolean

  /** Releases what a submission produced when nobody will see it, such as a descriptor an accept returned after its
    * cancel.
    *
    * @param res
    *   the result of the submission nobody will see
    */
  private[uring] def discard(res: Int): Unit = ()

  /** Records the bytes a cancelled submission still moved, so a read or write that raced its cancel loses nothing.
    *
    * @param res
    *   the number of bytes the cancelled submission moved
    */
  private[uring] def keepProgress(res: Int): Unit = ()

  /** Fills in `sqe` to cancel the submission whose user data is `userData`. Timers override it, because the kernel
    * cancels those with `TIMEOUT_REMOVE` instead of `ASYNC_CANCEL`.
    *
    * @param sqe
    *   the submission queue entry to fill in
    * @param userData
    *   the user data of the submission to cancel
    */
  private[uring] def prepareCancel(sqe: Ptr[io_uring_sqe], userData: Long): Unit =
    io_uring_prep_cancel64(sqe, userData.toULong, 0)

  // On EAGAIN, wait for readiness instead of resubmitting, which would spin. A readiness wait's result is a poll mask,
  // never a descriptor to release.
  protected def readinessFd: Int = -1
  protected def readinessEvents: Int = 0

  private var awaitingReadiness = false

  private[uring] final def prepareSubmission(sqe: Ptr[io_uring_sqe]): Unit =
    if awaitingReadiness then io_uring_prep_poll_add(sqe, readinessFd, readinessEvents.toUInt)
    else prepare(sqe)

  private[uring] final def finishSubmission(res: Int): Boolean =
    if awaitingReadiness then {
      // The descriptor is ready: returning false resubmits the op itself.
      awaitingReadiness = false
      if res < 0 && !Ring.transient(res) then throw Ring.failure(s"Failed to wait for descriptor $readinessFd", res)
      false
    } else if res == -scala.scalanative.posix.errno.EAGAIN && readinessEvents != 0 then {
      awaitingReadiness = true
      false
    } else finish(res)

  /** Forgets a readiness wait that the last result started, for an op that is cancelled instead of resubmitted. */
  private[uring] final def abandonRetry(): Unit = awaitingReadiness = false

  private[uring] final def discardSubmission(res: Int): Unit =
    if awaitingReadiness then awaitingReadiness = false
    else {
      if res > 0 then keepProgress(res)
      discard(res)
    }
}

/** Builds a `UringReactor`'s ops. Besides the `Ops` of the `Reactor` contract, it has the file and vector ops the uring
  * backend uses directly: positioned and sequential file I/O, `readv`/`writev`, `openAt`, and nanosecond timers.
  * Building an op does not touch the reactor, so this may be used from any thread.
  */
final class UringOps[R] private[uring] (resolver: HostResolver) extends Ops[Owned[R]] {
  def read(fd: Int, buf: ByteBuffer): Owned[R] & Read = Owned(new Read(fd, buf))
  def write(fd: Int, buf: ByteBuffer): Owned[R] & Write = Owned(new Write(fd, buf, plain = false))
  def accept(fd: Int, into: HandleSlot): Owned[R] & Accept = Owned(new Accept(fd, into))
  def connect(fd: Int): Owned[R] & Connect = Owned(new Connect(fd))
  def receive(fd: Int, buf: ByteBuffer, from: Slot[Address]): Owned[R] & Receive = Owned(new Receive(fd, buf, from))
  def send(fd: Int, buf: ByteBuffer, to: Address | Null): Owned[R] & Send = Owned(new Send(fd, buf, to))
  def timer(milliseconds: Int): Owned[R] & UringTimer = Owned(new UringTimer(milliseconds * 1_000_000L))
  def whenReady(fd: Int, interest: Interest)(perform: () => Boolean): Owned[R] & WhenReady =
    Owned(new WhenReady(fd, interest, perform))
  def blocking(body: () => Unit): Owned[R] & UringBlockingOp = Owned(new BlockingTask(body))
  def blocking[A <: AnyRef](body: () => A, into: Slot[A]): Owned[R] & UringBlockingOp =
    Owned(new BlockingResultTask(body, into))
  def resolve(host: String, into: Slot[List[ResolvedAddress]]): Owned[R] & Resolve =
    Owned(new Resolve(host, into, resolver))

  /** Returns an op that fills `bufs` in order with one `readv(2)`. It completes once at least one byte was read, or at
    * end of stream.
    *
    * @param fd
    *   the descriptor to read from
    * @param bufs
    *   the buffers to fill, between position and limit
    */
  def readv(fd: Int, bufs: Array[ByteBuffer]): Owned[R] & ReadVector = Owned(new ReadVector(fd, bufs))

  /** Returns an op that drains `bufs` in order with one `sendmsg(2)`, or `writev(2)` if `fd` is not a socket. It
    * completes once at least one byte was written.
    *
    * @param fd
    *   the descriptor to write to
    * @param bufs
    *   the buffers to drain, between position and limit
    */
  def writev(fd: Int, bufs: Array[ByteBuffer]): Owned[R] & WriteVector = Owned(new WriteVector(fd, bufs))

  /** Returns an op like `write` that uses `write(2)` from the start instead of trying `send(2)` first, for files and
    * pipes.
    *
    * @param fd
    *   the descriptor to write to
    * @param buf
    *   the buffer to drain
    */
  def writeFile(fd: Int, buf: ByteBuffer): Owned[R] & Write = Owned(new Write(fd, buf, plain = true))

  /** Returns an op that reads at `offset` without moving the file position, like `pread(2)`.
    *
    * @param fd
    *   the file to read from
    * @param buf
    *   the buffer to fill
    * @param offset
    *   the position in the file to read at, which must not be negative
    */
  def readAt(fd: Int, buf: ByteBuffer, offset: Long): Owned[R] & Read =
    require(offset >= 0, "offset must be nonnegative")
    Owned(new Read(fd, buf, offset))

  /** Returns an op that writes at `offset` without moving the file position, like `pwrite(2)`.
    *
    * @param fd
    *   the file to write to
    * @param buf
    *   the buffer to drain
    * @param offset
    *   the position in the file to write at, which must not be negative
    */
  def writeAt(fd: Int, buf: ByteBuffer, offset: Long): Owned[R] & Write =
    require(offset >= 0, "offset must be nonnegative")
    Owned(new Write(fd, buf, plain = true, offset = offset))

  /** Returns an op that completes once `duration` has passed, with nanosecond precision.
    *
    * @param duration
    *   how long to wait, which must not be negative
    */
  def timer(duration: FiniteDuration): Owned[R] & UringTimer = Owned(new UringTimer(duration.toNanos))

  /** Returns an op that opens `path` like `openat(2)` relative to the current directory, and completes after writing
    * the new descriptor into `into`. A missing file fails it with `FileNotFoundException`.
    *
    * @param path
    *   the file to open
    * @param flags
    *   the `open(2)` flags, such as `O_RDONLY`
    * @param mode
    *   the permissions of a file that `O_CREAT` creates
    * @param into
    *   the empty slot that receives the new descriptor
    */
  def openAt(path: String, flags: Int, mode: Int, into: HandleSlot): Owned[R] & OpenAt =
    Owned(new OpenAt(path, flags, mode, into))
}

/** An op a `BlockingPool` thread runs, so the reactor thread never blocks. The pool thread settles it, and the reactor
  * delivers the outcome on its own thread.
  *
  * The pool thread and the reactor share `runner`, `cancelled`, `published` and `ended`, under this op's lock. Only the
  * settling thread writes `outcome`, and the reactor reads it after taking the op from its resolved queue, which makes
  * the write visible. The other fields are used by the reactor thread only.
  */
sealed abstract class UringBlockingOp extends UringOp {

  /** Runs the blocking work on a pool thread. It may be interrupted when this op is cancelled. */
  def block(): Unit

  /** Returns whether a cancel may report `onCancel` at once instead of waiting for `block` to return. That is safe only
    * for an op whose `block` touches nothing of the caller's after a cancel.
    */
  private[uring] def abandonOnCancel: Boolean = false

  private var outcome: Any = UringBlockingOp.Unsettled

  private[uring] var reactor: UringReactor | Null = null

  private[uring] var pending: AnyRef | Null = null

  private[uring] var cancelling: AnyRef | Null = null

  private var runner: Thread | Null = null
  private var cancelled = false
  private var published = false
  private var ended = false

  private[uring] def failure: Throwable | Null = outcome match {
    case UringBlockingOp.Failed(failure) => failure
    case _                               => null
  }

  private[uring] def fail(failure: Throwable): Unit = settle(UringBlockingOp.Failed(failure))

  private[uring] def signal(): Unit = settle(())

  private def settle(value: Any): Unit = {
    if outcome != UringBlockingOp.Unsettled then throw new IllegalStateException("An op can only be settled once")
    outcome = value
    val owner = reactor
    if owner != null then owner.resolved(this)
  }

  /** Marks this op as running on the calling pool thread, before `block`.
    *
    * @return
    *   false if this op was cancelled first, so `block` must not run
    */
  private[uring] def begin(): Boolean = synchronized {
    if cancelled then false
    else {
      runner = Thread.currentThread()
      true
    }
  }

  /** Marks this op as no longer running, after `block`. It clears an interrupt aimed at this op, so the interrupt
    * cannot hit the pool's next task.
    */
  private[uring] def end(): Unit = {
    synchronized {
      runner = null
      ended = true
      notifyAll()
    }
    Thread.interrupted()
  }

  /** Marks this op cancelled and interrupts `block` if it is running.
    *
    * @return
    *   whether `block` was running
    */
  private[uring] def interrupt(): Boolean = synchronized {
    cancelled = true
    val running = runner
    if running != null then running.interrupt()
    running != null
  }

  /** Cancels this op like `interrupt`, unless `block` has already published its result or returned. Then this op is
    * left to complete, so what it produced reaches the caller.
    *
    * @return
    *   `TooLate` if this op was left to complete, otherwise whether `block` was running
    */
  private[uring] def cancelUnlessDone(): UringBlockingOp.CancelOutcome = synchronized {
    if published || ended then UringBlockingOp.CancelOutcome.TooLate
    else if interrupt() then UringBlockingOp.CancelOutcome.Interrupted
    else UringBlockingOp.CancelOutcome.NotRunning
  }

  /** Returns whether `block` has returned. */
  private[uring] def hasEnded: Boolean = synchronized(ended)

  /** Writes `value` into `into` unless this op was cancelled. It is atomic with respect to `interrupt`, so nothing is
    * written into the caller's slot after a cancel.
    *
    * @tparam A
    *   the type of the result
    * @param into
    *   the caller's slot
    * @param value
    *   the result to write
    */
  protected final def publish[A <: AnyRef](into: Slot[A], value: A): Unit = synchronized {
    if !cancelled then {
      into.set(value)
      published = true
    }
  }

  /** Waits until `block` has returned. The reactor's `close` uses it, because it may not return while a pool thread
    * still uses the caller's memory.
    */
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

object UringBlockingOp {
  private object Unsettled
  private final case class Failed(failure: Throwable)

  /** What `cancelUnlessDone` did. */
  private[uring] enum CancelOutcome {
    case TooLate, Interrupted, NotRunning
  }

  /** Runs `op` on `pool`, settling it with its outcome unless it was cancelled before it started.
    *
    * @param op
    *   the op to run
    * @param pool
    *   the pool whose thread runs it
    */
  private[uring] def run(op: UringBlockingOp, pool: BlockingPool): Unit =
    pool.execute { () =>
      if op.begin() then {
        var failure: Throwable | Null = null
        try op.block()
        catch case t: Throwable => failure = t
        finally op.end()
        if failure != null then op.fail(failure.nn) else op.signal()
      }
    }
}

/** `Ops.blocking` without a result. */
final class BlockingTask(body: () => Unit) extends UringBlockingOp {
  def block(): Unit = body()
}

/** `Ops.blocking` with a result, which is written into `into` unless the op was cancelled first. */
final class BlockingResultTask[A <: AnyRef](body: () => A, into: Slot[A]) extends UringBlockingOp {
  def block(): Unit = publish(into, body())
}

/** `Ops.resolve`: looks `host` up with the reactor's `HostResolver` on a pool thread. */
final class Resolve(val host: String, into: Slot[List[ResolvedAddress]], resolver: HostResolver)
    extends UringBlockingOp {
  // A lookup writes only through publish, which drops results after a cancel, so it can be left to finish alone.
  override private[uring] def abandonOnCancel: Boolean = true

  def block(): Unit = resolver.resolve(host) match {
    case Right(addresses) => publish(into, addresses)
    case Left(error)      => throw new IOException(s"Failed to resolve $host: $error")
  }
}
