package asyncio.uring

import java.nio.ByteBuffer
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import asyncio.BlockingOps
import asyncio.BufferPool
import asyncio.Completion
import asyncio.FileOps
import asyncio.Handle
import asyncio.Interest
import asyncio.IpAddress
import asyncio.ReadinessOps
import asyncio.Received
import asyncio.Repeatable
import asyncio.ResolveOps
import asyncio.ResolvedAddress
import asyncio.Slot
import asyncio.UnixAddress
import asyncio.UnixDatagramOps
import asyncio.unsafe.BlockingRun
import asyncio.unsafe.NativePool
import uring.*
import uringStructOps.*

/** Everything `UringReactor` can run: its `Op` member type. Uring-specific code may construct the classes below
  * directly; code that only knows the `Reactor` interface builds them through `reactor.ops`.
  */
sealed trait UringOp extends asyncio.unsafe.TimerHeap.Entry {
  // An op is in its reactor's timer heap while it is a pending timer, or a pending op with a time limit (`Entry`).

  /** Whether this op has been submitted before, so the reactor can refuse to run a one-shot op twice. */
  @volatile private[uring] var submitted = false

  /** While the op is pending in the reactor: its completion, so the op is its own record and submitting it allocates
    * nothing. Taken off just before the completion runs, which may then submit the op again. Loop thread only.
    */
  private[uring] var completion: Completion[UringOp] | Null = null

  /** For an op that finished, or failed, as soon as it was submitted: whether its completion is queued for the next
    * turn, and its failure, if any, until then.
    */
  private[uring] var finishedAtOnce = false
  private[uring] var atOnceFailure: Throwable | Null = null

  /** The caller's own data, kept across submissions and never read by the reactor (`Reactor.attachment`). */
  private[uring] var attachment: AnyRef | Null = null
}

/** Builds the ops `UringReactor` understands. */
object UringOps
    extends UnixDatagramOps[UringOp]
    with ReadinessOps[UringOp]
    with BlockingOps[UringOp]
    with FileOps[UringOp]
    with ResolveOps[UringOp] {
  def read(handle: Handle, buf: ByteBuffer): ReadIntoBuffer = new ReadIntoBuffer(fd(handle), buf)
  def write(handle: Handle, buf: ByteBuffer): WriteFromBuffer = new WriteFromBuffer(fd(handle), buf)
  def accept(handle: Handle, into: Handle.Slot): Accept = new Accept(fd(handle), into)
  def connect(handle: Handle): UringOp = new Connect(fd(handle))
  def receive(handle: Handle, buf: ByteBuffer, from: Slot[IpAddress]): ReceiveFrom =
    new ReceiveFrom(fd(handle), buf, from, unix = false)
  def send(handle: Handle, buf: ByteBuffer, to: IpAddress): SendTo = new SendTo(fd(handle), buf, to)
  def send(handle: Handle, buf: ByteBuffer): SendTo = new SendTo(fd(handle), buf, null)
  @annotation.targetName("receiveUnix")
  def receive(handle: Handle, buf: ByteBuffer, from: Slot[UnixAddress]): ReceiveFrom =
    new ReceiveFrom(fd(handle), buf, from, unix = true)
  def send(handle: Handle, buf: ByteBuffer, to: UnixAddress): SendTo = new SendTo(fd(handle), buf, to)
  def timer(milliseconds: Int): UringTimer = new UringTimer(milliseconds)
  def ready(handle: Handle, interest: Interest): Ready = new Ready(fd(handle), interest)
  def blocking(body: () => Unit): UringBlocking = new BlockingTask(body)
  def blocking[A <: AnyRef](body: () => A, into: Slot[A]): UringBlocking = new BlockingResultTask(body, into)
  def resolve(host: String, into: Slot[List[ResolvedAddress]]): Resolve = new Resolve(host, into)
  def read(handle: Handle, pool: BufferPool, into: Slot[Received]): PooledRead =
    new PooledRead(fd(handle), UringOps.native(pool), into)
  def listening(handle: Handle): UringOp = new Listening(fd(handle))
  def close(handle: Handle): UringOp = new Close(fd(handle))
  def open(path: String, write: Boolean, into: Handle.Slot): UringOp = new OpenFile(path, write, into)
  def read(handle: Handle, buf: ByteBuffer, offset: Long): PositionalRead = new PositionalRead(fd(handle), buf, offset)
  def write(handle: Handle, buf: ByteBuffer, offset: Long): PositionalWrite =
    new PositionalWrite(fd(handle), buf, offset)
  def sync(handle: Handle): FileSync = new FileSync(fd(handle))

  private def fd(handle: Handle): Int = UringHandles.fd(handle)

  private def native(pool: BufferPool): NativePool = pool match {
    case native: NativePool => native
    case other              => throw new IllegalArgumentException(s"$other is not a pool of this backend's")
  }
}

/** An op the reactor performs as soon as it is submitted, with nothing for the kernel to do: its completion runs on the
  * next turn, with the failure `perform` threw, if any. One-shot.
  */
sealed abstract class UringAtOnce extends UringOp {
  def perform(): Unit
}

/** Binds and listens a socket from `UringHandles.listener`, or finds a datagram socket bound, as it was when made (see
  * `SocketOps.listening`).
  */
final class Listening(val fd: Int) extends UringAtOnce {
  def perform(): Unit = UringHandles.listening(fd)
}

/** Closes a descriptor (see `Ops.close`): at once, as `UringHandles.close` does, which tells every reactor, so that the
  * commands pending on it there are cancelled and fail.
  */
final class Close(val fd: Int) extends UringAtOnce {
  def perform(): Unit = UringHandles.closeDescriptor(fd)
}

/** An op io_uring runs: each submission of it is one submission queue entry, which `prepare` fills in, and the
  * completion queue entry that answers it is handed to `finish`. Every method runs on the loop thread.
  *
  * A command on a non-blocking descriptor that has nothing to do yet may complete with `EAGAIN`, where the kernel does
  * not wait for it itself: the command then waits for readiness with a poll for `readinessEvents`, and is submitted
  * again once the descriptor is ready, rather than spinning.
  */
trait Command extends UringOp {

  /** The descriptor the command works on, or -1 for none; its pending submissions are cancelled when it is closed. */
  def fd: Int

  /** Fills in `sqe` for one submission. Throwing fails the op, as it would at once: nothing has reached the kernel. */
  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit

  /** Handles the result of one submission, a count, a descriptor or a negated `errno`: true once the op is done, false
    * to submit it again. Throwing fails the op.
    */
  protected def finish(res: Int): Boolean

  /** For `trySubmit`: performs the operation at once with a non-blocking system call, and returns true if that finished
    * the op, or false, having done nothing, if it would block. Throwing fails the op. By default the command is never
    * tried, so it always goes to the ring.
    */
  def tryAtOnce(): Boolean = false

  /** Releases what a submission produced that nobody will see, because the op was cancelled meanwhile, such as the
    * descriptor an accept returned.
    */
  protected def discard(res: Int): Unit = ()

  /** The poll events to wait for when a submission completes with `EAGAIN`, or 0 for a command that never does. */
  protected def readinessEvents: Int = 0

  /** Fills in `sqe` to cancel the submission whose user data is `userData`. */
  private[uring] def prepareCancel(sqe: Ptr[io_uring_sqe], userData: Long): Unit =
    io_uring_prep_cancel64(sqe, userData.toULong, 0)

  // What the reactor keeps about the command's submission in flight, on the loop thread: its user data, 0 when none is;
  // the generation its descriptor had when submitted (see `UringReactor.closed`); and, once it has been withdrawn
  // while the kernel still had it, the completion that gets `onCancel`, or `onFailure` with `cancelledBy`, when its
  // completion queue entry arrives, and whether the cancel's own entry has been taken.
  private[uring] var userData = 0L
  private[uring] var generation = 0
  private[uring] var cancelling: Completion[UringOp] | Null = null
  private[uring] var cancelledBy: Throwable | Null = null
  private[uring] var cancelQueued = false

  private var awaitingReadiness = false

  private[uring] final def prepareSubmission(sqe: Ptr[io_uring_sqe]): Unit =
    if awaitingReadiness then io_uring_prep_poll_add(sqe, fd, readinessEvents.toUInt)
    else prepare(sqe)

  private[uring] final def finishSubmission(res: Int): Boolean =
    if awaitingReadiness then {
      awaitingReadiness = false // the descriptor is ready: submitting again runs the command itself
      if res < 0 && !Ring.transient(res) then throw Ring.failure(s"Failed to wait for descriptor $fd", res)
      false
    } else if res == -scala.scalanative.posix.errno.EAGAIN && readinessEvents != 0 then {
      awaitingReadiness = true
      false
    } else finish(res)

  /** Takes in the result of a submission whose op was cancelled meanwhile: a readiness wait's is a poll mask, which
    * holds nothing, and anything else is the command's to discard.
    */
  private[uring] final def discardSubmission(res: Int): Unit =
    if awaitingReadiness then awaitingReadiness = false
    else discard(res)

  /** Forgets a readiness wait the last result started, for a command withdrawn instead of submitted again. */
  private[uring] final def abandonRetry(): Unit = awaitingReadiness = false
}

/** Readiness itself, for `ReadinessOps.ready`: a one-shot poll, done as soon as the kernel reports the descriptor
  * ready, which it does at once while the descriptor stays so.
  */
final class Ready(val fd: Int, val interest: Interest) extends Command with Repeatable {
  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit = io_uring_prep_poll_add(sqe, fd, Ring.pollMask(interest))
  protected def finish(res: Int): Boolean =
    if Ring.transient(res) then false
    else if res < 0 then throw Ring.failure(s"Failed to wait for descriptor $fd", res)
    else true
}

/** A one-shot timer, kept in the reactor's heap while pending (`TimerHeap.Entry`). */
final class UringTimer(val milliseconds: Int) extends UringOp with Repeatable {
  require(milliseconds >= 0, "Timer duration must not be negative")

  /** Whether the pending submission's time limit is shorter than the timer, so it fails when due. Loop thread only. */
  private[uring] var expires = false
}

/** The reactor's side of a promise: completed from any thread, after which the reactor runs its completion on the loop
  * thread. `UringBlocking`, the only kind, completes itself from the blocker pool.
  */
sealed abstract class UringPromise extends UringOp {
  // `Unsettled` until the first completion or failure wins the compare-and-set; later ones are refused.
  private val outcome = new java.util.concurrent.atomic.AtomicReference[Any](UringPromise.Unsettled)
  @volatile private[uring] var reactor: UringReactor | Null = null

  private[uring] def failure: Throwable | Null = outcome.get() match {
    case UringPromise.Failed(failure) => failure
    case _                            => null
  }

  /** Completes the promise with a failure instead. */
  def fail(failure: Throwable): Unit = settle(UringPromise.Failed(failure))

  /** Forgets the previous submission's outcome, for a `Repeatable` promise submitted again. Loop thread only, before
    * the submission can complete.
    */
  private[uring] def reset(): Unit = outcome.set(UringPromise.Unsettled)

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
}

object UringPromise {
  private[uring] object Unsettled
  private[uring] final case class Failed(failure: Throwable)
}

/** A promise the reactor completes by running `block` on the blocker pool, with `BlockingRun`'s cancellation: skipped
  * if not started, interrupted if running, and `onCancel` waits for `block` to return.
  */
abstract class UringBlocking extends UringPromise with BlockingRun {

  /** The cancelled submission whose `onCancel` waits for `block` to return, or whose `onFailure` does, with
    * `cancelledBy`, for one whose time limit ran out. Loop thread only.
    */
  private[uring] var cancelling: Completion[UringOp] | Null = null
  private[uring] var cancelledBy: Throwable | Null = null

  protected def settled(failure: Throwable | Null): Unit =
    if failure != null then fail(failure) else signal()
}

/** A blocking task built from a function, for `BlockingOps.blocking`. */
final class BlockingTask(body: () => Unit) extends UringBlocking {
  def block(): Unit = body()
}

/** A blocking task whose function's value is written into `into`, for `BlockingOps.blocking`. */
final class BlockingResultTask[A <: AnyRef](body: () => A, into: Slot[A]) extends UringBlocking {
  def block(): Unit = {
    publish(into, body())
  }
}
