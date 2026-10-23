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

/** Every op a `UringReactor` runs: its `Op` type is `UringOp[this.type]`. `Owner` is the reactor's singleton type, so
  * an op built by one reactor cannot be submitted to or cancelled on another, even when both are known to be
  * `UringReactor`s. An op is either a [[UringKernelOp]], which the kernel runs through the ring, or a
  * [[UringBlockingOp]], which a pool thread runs.
  */
sealed abstract class UringOp[Owner] {
  private[uring] var submitted = false
}

/** An op the kernel runs: the reactor turns it into one submission queue entry, and its completion queue entry settles
  * it. All of its methods run on the reactor thread.
  */
abstract class UringKernelOp[Owner] extends UringOp[Owner] {
  private[uring] var inFlight: AnyRef | Null = null

  /** Fills in `sqe` for one submission. Called again for each resubmission. */
  private[uring] def prepare(sqe: Ptr[io_uring_sqe]): Unit

  /** Handles the result of a submission: true when the op is done, false to submit it again. Throws to fail it. */
  private[uring] def finish(res: Int): Boolean

  /** Releases what a submission produced when nobody will see it, such as a descriptor an accept returned after its
    * cancel.
    */
  private[uring] def discard(res: Int): Unit = ()

  /** Records the bytes a cancelled submission still moved, so a read or write that raced its cancel loses nothing. */
  private[uring] def keepProgress(res: Int): Unit = ()

  /** Fills in `sqe` to cancel the submission whose user data is `userData`. Timers override it, because the kernel
    * cancels those with `TIMEOUT_REMOVE` instead of `ASYNC_CANCEL`.
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
  def blocking(body: () => Unit): UringBlockingOp[Owner] = new BlockingTask(body)
  def blocking[A <: AnyRef](body: () => A, into: Slot[A]): UringBlockingOp[Owner] = new BlockingResultTask(body, into)
  def resolve(host: String, into: Slot[List[ResolvedAddress]]): Resolve[Owner] = new Resolve(host, into, resolver)

  /** Fills `bufs` in order with one `readv(2)`. Completes once at least one byte was read, or at end of stream. */
  def readv(fd: Int, bufs: Array[ByteBuffer]): ReadVector[Owner] = new ReadVector(fd, bufs)

  /** Drains `bufs` in order with one `sendmsg(2)`, or `writev(2)` if `fd` is not a socket. Completes once at least one
    * byte was written.
    */
  def writev(fd: Int, bufs: Array[ByteBuffer]): WriteVector[Owner] = new WriteVector(fd, bufs)

  /** Like `write`, but with `write(2)` from the start instead of trying `send(2)` first: for files and pipes. */
  def writeFile(fd: Int, buf: ByteBuffer): Write[Owner] = new Write(fd, buf, plain = true)

  /** Reads at `offset` without moving the file position, like `pread(2)`. */
  def readAt(fd: Int, buf: ByteBuffer, offset: Long): Read[Owner] =
    require(offset >= 0, "offset must be nonnegative")
    new Read(fd, buf, offset)

  /** Writes at `offset` without moving the file position, like `pwrite(2)`. */
  def writeAt(fd: Int, buf: ByteBuffer, offset: Long): Write[Owner] =
    require(offset >= 0, "offset must be nonnegative")
    new Write(fd, buf, plain = true, offset = offset)

  /** Completes once `duration` has passed, with nanosecond precision. */
  def timer(duration: FiniteDuration): UringTimer[Owner] = new UringTimer(duration.toNanos)

  /** Opens `path` like `openat(2)` relative to the current directory, and completes after writing the new descriptor
    * into `into`. A missing file fails with `FileNotFoundException`.
    */
  def openAt(path: String, flags: Int, mode: Int, into: HandleSlot): OpenAt[Owner] =
    new OpenAt(path, flags, mode, into)
}

/** An op a `BlockingPool` thread runs, so the reactor thread never blocks. The pool thread settles it, and the reactor
  * delivers the outcome on its own thread.
  *
  * The pool thread and the reactor share `runner`, `cancelled` and `ended`, under this op's lock. Only the settling
  * thread writes `outcome`, and the reactor reads it after taking the op from its resolved queue, which makes the write
  * visible. The other fields are used by the reactor thread only.
  */
sealed abstract class UringBlockingOp[Owner] extends UringOp[Owner] {

  /** The blocking work, run on a pool thread. It may be interrupted when the op is cancelled. */
  def block(): Unit

  /** Whether a cancel may report `onCancel` at once instead of waiting for `block` to return. That is safe only for an
    * op whose `block` touches nothing of the caller's after a cancel.
    */
  private[uring] def abandonOnCancel: Boolean = false

  private var outcome: Any = UringBlockingOp.Unsettled

  private[uring] var reactor: UringReactor | Null = null

  private[uring] var pending: AnyRef | Null = null

  private[uring] var cancelling: AnyRef | Null = null

  private var runner: Thread | Null = null
  private var cancelled = false
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

  /** Called by the pool thread before `block`: false if the op was cancelled first, so `block` must not run. */
  private[uring] def begin(): Boolean = synchronized {
    if cancelled then false
    else {
      runner = Thread.currentThread()
      true
    }
  }

  /** Called by the pool thread after `block`. Clears an interrupt aimed at this op, so it cannot hit the pool's next
    * task.
    */
  private[uring] def end(): Unit = {
    synchronized {
      runner = null
      ended = true
      notifyAll()
    }
    Thread.interrupted()
  }

  /** Marks the op cancelled and interrupts `block` if it is running. Returns whether it was running. */
  private[uring] def interrupt(): Boolean = synchronized {
    cancelled = true
    val running = runner
    if running != null then running.interrupt()
    running != null
  }

  /** Writes `value` into `into` unless the op was cancelled, atomically with respect to `interrupt`, so nothing is
    * written into the caller's slot after a cancel.
    */
  protected final def publish[A <: AnyRef](into: Slot[A], value: A): Unit = synchronized {
    if !cancelled then into.set(value)
  }

  /** Waits until `block` has returned. Used by the reactor's `close`, which may not return while a pool thread still
    * uses the caller's memory.
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

  /** Runs `op` on `pool`, settling it with its outcome unless it was cancelled before it started. */
  private[uring] def run(op: UringBlockingOp[?], pool: BlockingPool): Unit =
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
final class BlockingTask[Owner](body: () => Unit) extends UringBlockingOp[Owner] {
  def block(): Unit = body()
}

/** `Ops.blocking` with a result, which is written into `into` unless the op was cancelled first. */
final class BlockingResultTask[Owner, A <: AnyRef](body: () => A, into: Slot[A]) extends UringBlockingOp[Owner] {
  def block(): Unit = publish(into, body())
}

/** `Ops.resolve`: looks `host` up with the reactor's `HostResolver` on a pool thread. */
final class Resolve[Owner](val host: String, into: Slot[List[ResolvedAddress]], resolver: HostResolver)
    extends UringBlockingOp[Owner] {
  // A lookup writes only through publish, which drops results after a cancel, so it can be left to finish alone.
  override private[uring] def abandonOnCancel: Boolean = true

  def block(): Unit = resolver.resolve(host) match {
    case Right(addresses) => publish(into, addresses)
    case Left(error)      => throw new IOException(s"Failed to resolve $host: $error")
  }
}
