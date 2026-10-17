package gears.asyncio.uring

import gears.asyncio.Completion
import gears.asyncio.Handles
import gears.asyncio.Reactor
import gears.asyncio.Repeatable

import java.io.IOException
import scala.scalanative.linux.eventfd
import scala.scalanative.posix.errno
import scala.scalanative.posix.unistd
import scala.scalanative.runtime.ByteArray
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import uring.*
import uringStructOps.*

// The kernel owns a ring op's buffers until its CQE arrives, so a cancelled op's onCancel waits for that CQE.
final class UringReactor private (entries: Int, blockerPool: BlockingPool, resolver: HostResolver) extends Reactor {

  type Op = UringOp[this.type]

  private val opsBuilder: UringOps[this.type] = new UringOps[this.type](resolver)

  def ops: UringOps[this.type] = opsBuilder

  def handles: Handles = UringHandles

  private final class Pending[C <: Op](val op: C, val completion: Completion[C]) {
    def complete(): Unit = completion.onComplete(op)
    def fail(failure: Throwable): Unit = completion.onFailure(op, failure)
    def cancelled(): Unit = completion.onCancel(op)
  }

  private final class InFlight(val id: Long, val op: RingOp[?], var pending: Pending[?] | Null) {
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

  private val resolvedQueue = new java.util.concurrent.ConcurrentLinkedQueue[UringPromise[?]]()
  private val wakeFd: Int = eventfd.eventfd(0.toUInt, eventfd.EFD_CLOEXEC)
  if wakeFd < 0 then {
    io_uring_queue_exit(ring)
    throw new IOException(s"Failed to create eventfd: errno ${errno.errno}")
  }
  private val wakeBuffer = new Array[Byte](8)

  private val wakePending = new java.util.concurrent.atomic.AtomicBoolean()

  private val pendingPromises = scala.collection.mutable.HashSet.empty[UringPromise[?]]

  private val cancellingBlockers = scala.collection.mutable.HashSet.empty[UringBlocking[?]]

  private object wakeLock {}
  @volatile private var closed = false
  private var stopped = false

  armWake()

  def submit[C <: Op](op: C, completion: Completion[C]): Unit = {
    if closed then throw new IllegalStateException("The reactor is closed")
    op match {
      case _: Repeatable   => ()
      case one: UringOp[?] =>
        if one.submitted then throw new IllegalStateException(s"$op is not Repeatable and was already submitted")
    }
    val pending = new Pending(op, completion)
    op match {
      case ring: RingOp[?] =>
        if ring.inFlight != null then throw new IllegalStateException(s"$op is already pending")
        enqueue(ring, pending)
      case promise: UringPromise[?] =>
        if promise.pending != null then throw new IllegalStateException(s"$op is already pending")
        promise match {
          case blocking: UringBlocking[?] if blocking.cancelling != null =>
            throw new IllegalStateException(s"$op is still running after being cancelled")
          case _ => ()
        }
        promise.pending = pending
        promise.reactor = this
        pendingPromises += promise
        promise match {
          case blocking: UringBlocking[?] =>
            try Blockers.run(blocking, blockerPool)
            catch {
              case t: Throwable =>
                promise.pending = null
                pendingPromises -= promise
                throw t
            }
        }
    }
    op.submitted = true
  }

  private def enqueue(op: RingOp[?], pending: Pending[?]): Unit = {
    val sqe = nextSqe()
    try op.prepareSubmission(sqe)
    catch {
      case t: Throwable =>
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

  private def nextSqe(): Ptr[io_uring_sqe] = {
    var sqe = io_uring_get_sqe(ring)
    if sqe == null then {
      flush()
      sqe = io_uring_get_sqe(ring)
    }
    if sqe == null then throw new IOException("The io_uring submission queue is full")
    sqe
  }

  private def flush(): Unit = {
    val res = io_uring_submit(ring)
    unsubmitted = res < 0 || io_uring_sq_ready(ring) != 0.toUInt
    if res < 0 && res != -errno.EBUSY && res != -errno.EAGAIN && res != -errno.EINTR then
      throw Ring.failure("Failed to submit to io_uring", res)
  }

  private def armWake(): Unit =
    if !closing && !wakeInFlight then
      try {
        val sqe = nextSqe()
        io_uring_prep_read(sqe, wakeFd, wakeBuffer.asInstanceOf[ByteArray].at(0), 8.toUInt, 0.toULong)
        sqe.user_data = WakeId.toULong
        wakeInFlight = true
        flush()
      } catch case _: IOException => ()

  def wakeup(): Unit =
    if wakePending.compareAndSet(false, true) then
      wakeLock.synchronized {
        if !closed then {
          val signal = stackalloc[CLongLong]()
          !signal = 1L
          unistd.write(wakeFd, signal.asInstanceOf[Ptr[Byte]], 8.toCSize)
        }
      }

  private[uring] def resolved(promise: UringPromise[?]): Unit =
    if !closed then {
      resolvedQueue.add(promise)
      wakeup()
    }

  private def deliverResolved(): Boolean = {
    var ran = false
    var promise = resolvedQueue.poll()
    while promise != null do {
      val pending = promise.pending
      promise.pending = null
      pendingPromises -= promise
      promise match {
        case blocking: UringBlocking[?] if blocking.cancelling != null =>
          ran = true
          val cancelled = blocking.cancelling.asInstanceOf[Pending[?]]
          blocking.cancelling = null
          cancellingBlockers -= blocking
          cancelled.cancelled()
        case _ =>
          if pending != null then {
            ran = true
            val p = pending.asInstanceOf[Pending[?]]
            val failure = promise.failure
            if failure != null then p.fail(failure.nn) else p.complete()
          }
      }
      promise = resolvedQueue.poll()
    }
    ran
  }

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

  def cancel(op: Op): Boolean = op match {
    case ring: RingOp[?] =>
      ring.inFlight match {
        case flight: InFlight if flight.pending != null && !completionArrived(flight.id) =>
          flight.cancelled = flight.pending
          flight.pending = null
          requestCancel(flight)
          true
        case _ => false
      }
    case promise: UringPromise[?] =>
      val pending = detach(promise)
      if pending == null then false
      else {
        promise match {
          case blocking: UringBlocking[?] if blocking.interrupt() =>
            blocking.cancelling = pending
            cancellingBlockers += blocking
          case _ => pending.nn.cancelled()
        }
        true
      }
  }

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

  private def retryCancels(): Unit =
    if cancelBacklog.nonEmpty then {
      val retry = cancelBacklog.toList
      cancelBacklog.clear()
      retry.foreach(flight => if !flight.cancelQueued && inFlight.contains(flight.id) then requestCancel(flight))
    }

  private def detach(promise: UringPromise[?]): Pending[?] | Null = {
    val pending = promise.pending
    promise.pending = null
    pendingPromises -= promise
    pending.asInstanceOf[Pending[?] | Null]
  }

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
      val running = scala.collection.mutable.ArrayBuffer.empty[UringBlocking[?]]
      for promise <- pendingPromises.toList do {
        val pending = detach(promise)
        if pending != null then {
          dropped += pending.nn
          promise match {
            case blocking: UringBlocking[?] if blocking.interrupt() => running += blocking
            case _                                                  => ()
          }
        }
      }
      for blocking <- cancellingBlockers do {
        dropped += blocking.cancelling.asInstanceOf[Pending[?]]
        blocking.cancelling = null
        running += blocking
      }
      cancellingBlockers.clear()
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

  def open(
      entries: Int = 256,
      blockerPool: BlockingPool = BlockingPool.global,
      resolver: HostResolver = PosixHostResolver
  ): UringReactor = new UringReactor(entries, blockerPool, resolver)

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
