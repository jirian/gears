package gears.io.uring

import gears.io.reactor.Completion
import gears.io.reactor.Handles
import gears.io.reactor.Reactor
import gears.io.reactor.Repeatable

import java.io.IOException
import scala.scalanative.linux.eventfd
import scala.scalanative.posix.errno
import scala.scalanative.posix.unistd
import scala.scalanative.runtime.ByteArray
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import uring.*
import uringStructOps.*

/** A `Reactor` on one Linux io_uring (kernel 5.11 or later). Kernel ops become submission queue entries, and their
  * completions are dispatched by `poll`; blocking ops and DNS lookups run on a `BlockingPool`. Other threads wake a
  * waiting `poll` through an eventfd that the ring always has a read pending on.
  *
  * Like every `Reactor`, it is used from one thread, apart from `wakeup` and building ops.
  */
// The kernel owns a kernel op's buffers until its CQE arrives, so a cancelled op's onCancel waits for that CQE.
final class UringReactor private (entries: Int, blockerPool: BlockingPool, resolver: HostResolver) extends Reactor {

  type Op = Owned[this.type]

  private val opsBuilder: UringOps[this.type] = new UringOps[this.type](resolver)

  def ops: UringOps[this.type] = opsBuilder

  def handles: Handles = UringHandles

  /** A submitted op with the completion to run for it. */
  private final class Pending[C <: Op](val op: C, val completion: Completion[C]) {
    def complete(): Unit = completion.onComplete(op)
    def fail(failure: Throwable): Unit = completion.onFailure(op, failure)
    def cancelled(): Unit = completion.onCancel(op)
  }

  /** One submission of a kernel op, keyed by the user data `id` its SQE and CQE carry. After a cancel, `pending` moves
    * to `cancelled` until the CQE arrives; `cancelQueued` records that the cancel SQE itself was submitted.
    */
  private final class InFlight(val id: Long, val op: UringKernelOp, var pending: Pending[?] | Null) {
    var cancelled: Pending[?] | Null = null
    var submissionFailure: Throwable | Null = null
    var cancelRequested = false
    var cancelQueued = false
  }

  private val ringStorage = new Array[Byte](sizeof[io_uring].toInt)
  private def ring: Ptr[io_uring] = ringStorage.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[io_uring]]
  locally {
    val res = io_uring_queue_init(entries.toUInt, ring, 0.toUInt)
    if res < 0 then throw Ring.failure("Failed to set up io_uring", res)
  }

  // User data of the eventfd read, and of SQEs whose CQEs are ignored: cancels, and no-ops that fill a taken SQE.
  private inline val WakeId = 0L
  private inline val InternalId = 1L
  private var nextId = 2L

  private val inFlight = scala.collection.mutable.LongMap.empty[InFlight]
  private val cancelBacklog = scala.collection.mutable.ArrayBuffer.empty[InFlight]
  private var unsubmitted = false
  private var wakeInFlight = false
  private var closing = false

  private val MaxBatch = 64
  private val batchStorage = new Array[Byte]((sizeof[Ptr[io_uring_cqe]] * MaxBatch.toUInt).toInt)
  private def batch: Ptr[Ptr[io_uring_cqe]] =
    batchStorage.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[Ptr[io_uring_cqe]]]
  private val cqeSlotStorage = new Array[Byte](sizeof[Ptr[io_uring_cqe]].toInt)
  private def cqeSlot: Ptr[Ptr[io_uring_cqe]] =
    cqeSlotStorage.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[Ptr[io_uring_cqe]]]
  // CQEs are copied out and dispatched one by one, so a completion that cancels a later op of the same batch can
  // still see that op's CQE here.
  private val reapedIds = new Array[Long](MaxBatch)
  private val reapedResults = new Array[Int](MaxBatch)
  private var reapedCount = 0
  private var reapedCursor = 0

  private val waitStorage = new Array[Byte](sizeof[__kernel_timespec].toInt)
  private def waitTimeout(nanoseconds: Long): Ptr[__kernel_timespec] = {
    val ts = waitStorage.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[__kernel_timespec]]
    ts.tv_sec = nanoseconds / 1_000_000_000L
    ts.tv_nsec = nanoseconds % 1_000_000_000L
    ts
  }

  private val resolvedQueue = new java.util.concurrent.ConcurrentLinkedQueue[UringBlockingOp]()
  private val wakeFd: Int = eventfd.eventfd(0.toUInt, eventfd.EFD_CLOEXEC)
  if wakeFd < 0 then {
    io_uring_queue_exit(ring)
    throw new IOException(s"Failed to create eventfd: errno ${errno.errno}")
  }
  private val wakeBuffer = new Array[Byte](8)

  private val wakePending = new java.util.concurrent.atomic.AtomicBoolean()

  private val pendingBlocking = scala.collection.mutable.HashSet.empty[UringBlockingOp]

  private val cancellingBlocking = scala.collection.mutable.HashSet.empty[UringBlockingOp]

  private object wakeLock {}
  @volatile private var closed = false
  private var stopped = false

  armWake()

  /** Submits `op`. A kernel op gets an SQE and is flushed to the kernel at once; a blocking op is queued on the pool.
    */
  def submit[C <: Op](op: C, completion: Completion[C]): Unit = {
    if closed then throw new IllegalStateException("The reactor is closed")
    op match {
      case _: Repeatable => ()
      case one: UringOp  =>
        if one.submitted then throw new IllegalStateException(s"$op is not Repeatable and was already submitted")
    }
    val pending = new Pending(op, completion)
    op match {
      case kernel: UringKernelOp =>
        if kernel.inFlight != null then throw new IllegalStateException(s"$op is already pending")
        enqueue(kernel, pending)
      case blocking: UringBlockingOp =>
        if blocking.pending != null then throw new IllegalStateException(s"$op is already pending")
        if blocking.cancelling != null then
          throw new IllegalStateException(s"$op is still running after being cancelled")
        blocking.pending = pending
        blocking.reactor = this
        pendingBlocking += blocking
        try UringBlockingOp.run(blocking, blockerPool)
        catch {
          case t: Throwable =>
            blocking.pending = null
            pendingBlocking -= blocking
            throw t
        }
    }
    op.submitted = true
  }

  /** Prepares and submits one SQE for `op`. If the submit fails, the op is cancelled and fails with that error once its
    * CQE arrives, since the kernel may already have its buffers.
    */
  private def enqueue(op: UringKernelOp, pending: Pending[?]): Unit = {
    val sqe = nextSqe()
    try op.prepareSubmission(sqe)
    catch {
      case t: Throwable =>
        // The SQE is already taken from the ring and will be submitted, so it must not hold a half-prepared op.
        io_uring_prep_nop(sqe)
        sqe.user_data = InternalId.toULong
        throw t
    }
    val flight = new InFlight(nextId, op, pending)
    nextId += 1
    sqe.user_data = flight.id.toULong
    inFlight(flight.id) = flight
    op.inFlight = flight
    try flush()
    catch
      case t: IOException =>
        flight.submissionFailure = t
        requestCancel(flight)
  }

  /** A free SQE, flushing the queue once if it is full. */
  private def nextSqe(): Ptr[io_uring_sqe] = {
    var sqe = io_uring_get_sqe(ring)
    if sqe == null then {
      flush()
      sqe = io_uring_get_sqe(ring)
    }
    if sqe == null then throw new IOException("The io_uring submission queue is full")
    sqe
  }

  /** Hands prepared SQEs to the kernel. A busy or interrupted submit is left for the next `poll` to retry. */
  private def flush(): Unit = {
    val res = io_uring_submit(ring)
    unsubmitted = res < 0 || io_uring_sq_ready(ring) != 0.toUInt
    if res < 0 && res != -errno.EBUSY && res != -errno.EAGAIN && res != -errno.EINTR then
      throw Ring.failure("Failed to submit to io_uring", res)
  }

  /** Keeps one read of the wake eventfd in the ring, so a write to it ends a blocking wait. */
  private def armWake(): Unit =
    if !closing && !wakeInFlight then
      try {
        val sqe = nextSqe()
        io_uring_prep_read(sqe, wakeFd, wakeBuffer.asInstanceOf[ByteArray].at(0), 8.toUInt, 0.toULong)
        sqe.user_data = WakeId.toULong
        wakeInFlight = true
        flush()
      } catch case _: IOException => ()

  /** Ends a blocking `poll`, from any thread. Wake-ups requested before the loop sees one are merged. */
  def wakeup(): Unit =
    if wakePending.compareAndSet(false, true) then
      // Under the lock, so close can't free the eventfd's number between the check and the write.
      wakeLock.synchronized {
        if !closed then {
          val signal = stackalloc[CLongLong]()
          !signal = 1L
          unistd.write(wakeFd, signal.asInstanceOf[Ptr[Byte]], 8.toCSize)
        }
      }

  /** Called by a pool thread when `blocking` has settled: queues it for the reactor thread and wakes the loop. */
  private[uring] def resolved(blocking: UringBlockingOp): Unit =
    if !closed then {
      resolvedQueue.add(blocking)
      wakeup()
    }

  /** Runs the completions of settled blocking ops on the reactor thread. Returns whether any ran. */
  private def deliverResolved(): Boolean = {
    var ran = false
    var blocking = resolvedQueue.poll()
    while blocking != null do {
      val pending = blocking.pending
      blocking.pending = null
      pendingBlocking -= blocking
      if blocking.cancelling != null then {
        ran = true
        val cancelled = blocking.cancelling.asInstanceOf[Pending[?]]
        blocking.cancelling = null
        cancellingBlocking -= blocking
        cancelled.cancelled()
      } else if pending != null then {
        ran = true
        val p = pending.asInstanceOf[Pending[?]]
        val failure = blocking.failure
        if failure != null then p.fail(failure.nn) else p.complete()
      }
      blocking = resolvedQueue.poll()
    }
    ran
  }

  /** Whether the CQE for `id` is already reaped or waiting in the completion queue. Then a cancel is too late, and the
    * op completes normally instead.
    */
  private def completionArrived(id: Long): Boolean = {
    var i = reapedCursor
    while i < reapedCount do {
      if reapedIds(i) == id then return true
      i += 1
    }
    val ready = io_uring_peek_batch_cqe(ring, batch, MaxBatch.toUInt).toInt
    i = 0
    while i < ready do {
      if (!(batch + i)).user_data.toLong == id then return true
      i += 1
    }
    false
  }

  /** Copies up to a batch of CQEs out of the ring and marks them consumed, so dispatching may submit again safely. */
  private def reap(): Unit = {
    val count = io_uring_peek_batch_cqe(ring, batch, MaxBatch.toUInt).toInt
    var i = 0
    while i < count do {
      val cqe = !(batch + i)
      reapedIds(i) = cqe.user_data.toLong
      reapedResults(i) = cqe.res
      i += 1
    }
    if count > 0 then io_uring_cq_advance(ring, count.toUInt)
    reapedCount = count
    reapedCursor = 0
  }

  /** Settles the submission `id` with `res`: completes, fails or resubmits its op, or, after a cancel, keeps its
    * progress and runs `onCancel`.
    */
  private def dispatch(id: Long, res: Int): Unit =
    if id == WakeId then {
      wakeInFlight = false
      wakePending.set(false)
      armWake()
    } else if id != InternalId then
      inFlight.remove(id) match {
        case None         => ()
        case Some(flight) =>
          val op = flight.op
          // A repeatable op may already be in flight again under a newer submission; only clear our own.
          if op.inFlight.asInstanceOf[AnyRef] eq flight then op.inFlight = null
          val pending = flight.pending
          if pending == null then {
            op.discardSubmission(res)
            val cancelled = flight.cancelled
            flight.cancelled = null
            if cancelled != null then cancelled.cancelled()
          } else if flight.submissionFailure != null then {
            op.discardSubmission(res)
            pending.fail(flight.submissionFailure.nn)
          } else {
            var failure: Throwable | Null = null
            val done =
              try op.finishSubmission(res)
              catch {
                case t: Throwable =>
                  failure = t
                  true
              }
            if failure != null then pending.fail(failure.nn)
            else if done then pending.complete()
            else
              try enqueue(op, pending)
              catch case t: Throwable => pending.fail(t)
          }
      }

  /** One turn: runs everything ready, waiting up to `timeoutNanos` first if nothing is (forever if negative, not at all
    * if 0). Returns whether any op's completion ran. While submissions or the wake read are not in the kernel yet, the
    * wait is capped at 10 ms so they are retried.
    */
  def poll(timeoutNanos: Long): Boolean = {
    if closed then throw new IllegalStateException("The reactor is closed")
    retryCancels()
    armWake()
    if unsubmitted then flush()
    var ran = deliverResolved()
    if reapedCursor >= reapedCount then reap()
    if reapedCursor >= reapedCount && !ran && timeoutNanos != 0 then {
      val waitNanos =
        if (unsubmitted || !wakeInFlight) && (timeoutNanos < 0 || timeoutNanos > 10_000_000L) then 10_000_000L
        else timeoutNanos
      val ts = if waitNanos < 0 then null else waitTimeout(waitNanos)
      io_uring_wait_cqe_timeout(ring, cqeSlot, ts)
      reap()
    }
    while reapedCursor < reapedCount do {
      val i = reapedCursor
      reapedCursor += 1
      if reapedIds(i) != WakeId && reapedIds(i) != InternalId then ran = true
      dispatch(reapedIds(i), reapedResults(i))
    }
    deliverResolved() || ran
  }

  def runOnce(block: Boolean): Unit = { poll(if block then -1L else 0L); () }

  def run(): Unit = {
    stopped = false
    while !stopped do runOnce(block = true)
  }

  def stop(): Unit = stopped = true

  /** Cancels `op`. For a kernel op, `onCancel` runs only when its CQE arrives, because until then the kernel may still
    * write to its buffers. A running blocking op is interrupted, and `onCancel` waits for it to return, unless the op
    * can be abandoned.
    */
  def cancel(op: Op): Boolean = op match {
    case kernel: UringKernelOp =>
      kernel.inFlight match {
        case flight: InFlight if flight.pending != null && !completionArrived(flight.id) =>
          flight.cancelled = flight.pending
          flight.pending = null
          requestCancel(flight)
          true
        case _ => false
      }
    case blocking: UringBlockingOp =>
      val pending = detach(blocking)
      if pending == null then false
      else {
        if blocking.interrupt() && !blocking.abandonOnCancel then {
          blocking.cancelling = pending
          cancellingBlocking += blocking
        } else pending.nn.cancelled()
        true
      }
  }

  /** Submits a cancel SQE for `flight`. If the queue is full, it goes to the backlog that the next `poll` retries. */
  private def requestCancel(flight: InFlight): Unit =
    flight.cancelRequested = true
    try {
      val sqe = nextSqe()
      try flight.op.prepareCancel(sqe, flight.id)
      catch
        case t: Throwable =>
          io_uring_prep_nop(sqe)
          sqe.user_data = InternalId.toULong
          unsubmitted = true
          throw t
      sqe.user_data = InternalId.toULong
      flight.cancelQueued = true
      flush()
    } catch {
      case _: IOException => if !flight.cancelQueued then cancelBacklog += flight
    }

  /** Resubmits cancels that did not fit into the queue earlier. */
  private def retryCancels(): Unit =
    if cancelBacklog.nonEmpty then {
      val retry = cancelBacklog.toList
      cancelBacklog.clear()
      retry.foreach(flight => if !flight.cancelQueued && inFlight.contains(flight.id) then requestCancel(flight))
    }

  /** Takes the pending record of `blocking`, so its outcome is no longer delivered. */
  private def detach(blocking: UringBlockingOp): Pending[?] | Null = {
    val pending = blocking.pending
    blocking.pending = null
    pendingBlocking -= blocking
    pending.asInstanceOf[Pending[?] | Null]
  }

  /** Cancels everything, waits for every CQE and every running blocking op, then unmaps the ring. Only then does it run
    * the `onCancel`s, so no callback sees memory the kernel or a pool thread still uses.
    */
  def close(): Unit = {
    if !closed then {
      closing = true
      val dropped = scala.collection.mutable.ArrayBuffer.empty[Pending[?]]
      for flight <- inFlight.values do {
        if flight.pending != null then dropped += flight.pending.nn
        else if flight.cancelled != null then dropped += flight.cancelled.nn
        flight.pending = null
        flight.cancelled = null
        if !flight.cancelQueued then requestCancel(flight)
      }
      val running = scala.collection.mutable.ArrayBuffer.empty[UringBlockingOp]
      for blocking <- pendingBlocking.toList do {
        val pending = detach(blocking)
        if pending != null then {
          dropped += pending.nn
          if blocking.interrupt() && !blocking.abandonOnCancel then running += blocking
        }
      }
      for blocking <- cancellingBlocking do {
        dropped += blocking.cancelling.asInstanceOf[Pending[?]]
        blocking.cancelling = null
        running += blocking
      }
      cancellingBlocking.clear()
      resolvedQueue.clear()
      var wakeCancelQueued = false
      def cancelWake(): Unit = if wakeInFlight && !wakeCancelQueued then
        try {
          val sqe = nextSqe()
          io_uring_prep_cancel64(sqe, WakeId.toULong, 0)
          sqe.user_data = InternalId.toULong
          wakeCancelQueued = true
          flush()
        } catch { case _: IOException => () }
      cancelWake()
      while inFlight.nonEmpty || wakeInFlight do {
        retryCancels()
        cancelWake()
        if unsubmitted then
          try flush()
          catch case _: IOException => ()
        while reapedCursor < reapedCount do {
          val i = reapedCursor
          reapedCursor += 1
          val id = reapedIds(i)
          if id == WakeId then wakeInFlight = false
          else if id != InternalId then
            inFlight.remove(id).foreach { flight =>
              flight.op.discardSubmission(reapedResults(i))
              flight.op.inFlight = null
            }
        }
        if inFlight.nonEmpty || wakeInFlight then {
          io_uring_wait_cqe_timeout(ring, cqeSlot, waitTimeout(10_000_000L))
          reap()
        }
      }
      wakeLock.synchronized {
        closed = true
        unistd.close(wakeFd)
      }
      io_uring_queue_exit(ring)
      running.foreach(_.awaitEnd())
      var callbackFailure: Throwable | Null = null
      dropped.foreach { pending =>
        try pending.cancelled()
        catch {
          case t: Throwable =>
            if callbackFailure == null then callbackFailure = t
            else if callbackFailure.nn ne t then callbackFailure.nn.addSuppressed(t)
        }
      }
      if callbackFailure != null then throw callbackFailure.nn
    }
  }
}

object UringReactor {

  /** Opens a reactor with a ring of `entries` submission slots. */
  def open(
      entries: Int = 256,
      blockerPool: BlockingPool = BlockingPool.global,
      resolver: HostResolver = PosixHostResolver
  ): UringReactor = new UringReactor(entries, blockerPool, resolver)

  /** Opens a reactor for `body` and closes it afterwards. */
  def scoped(
      entries: Int = 256,
      blockerPool: BlockingPool = BlockingPool.global,
      resolver: HostResolver = PosixHostResolver
  )(body: UringReactor => Unit): Unit = {
    val reactor = open(entries, blockerPool, resolver)
    try body(reactor)
    finally reactor.close()
  }
}
