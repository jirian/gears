package asyncio.uring

import java.io.IOException
import scala.scalanative.linux.eventfd
import scala.scalanative.posix.errno
import scala.scalanative.posix.unistd
import scala.scalanative.runtime.ByteArray
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import asyncio.Completion
import asyncio.Handles
import asyncio.Reactor
import asyncio.Repeatable
import uring.*
import uringOps.*

/** The io_uring implementation of `Reactor`, for ops built by `UringOps`. A `RingOp` is one SQE, submitted right away,
  * and finishes with its CQE; resolved promises wake the loop through an eventfd the ring keeps a read pending on.
  *
  * Besides `run`, a driver that interleaves the reactor with its own work, such as a scheduler running tasks on the
  * same thread, calls `poll` instead, and `wake` from other threads to interrupt a waiting `poll`.
  *
  * Cancelling a ring op asks the kernel to cancel it, but the kernel only lets go of the op, and its buffers, when the
  * op's own CQE arrives, so its `onCancel` runs then, from `poll` or `run`, as `Reactor.cancel` allows. A result that
  * raced the cancellation is discarded: an accepted or opened descriptor is closed, and bytes read are lost.
  */
final class UringReactor private (entries: Int, blockerPool: BlockingPool, resolver: HostResolver) extends Reactor {

  /** Every uring reactor runs the same kinds of op, so uring-specific code may build them directly. Code that only sees
    * the `Reactor` interface still cannot mix ops between reactors.
    */
  type Op = UringOp[this.type]

  private val opsBuilder: UringOps[this.type] = new UringOps[this.type](resolver)

  def ops: UringOps[this.type] = opsBuilder

  /** The handles are plain non-blocking POSIX descriptors. */
  def handles: Handles = UringHandles

  private final class Pending[C <: Op](val op: C, val completion: Completion[C]) {
    def complete(): Unit = completion.onComplete(op)
    def fail(failure: Throwable): Unit = completion.onFailure(op, failure)
    def cancelled(): Unit = completion.onCancel(op)
  }

  /** One submission of a ring op, under its user data `id`. Once cancelled, `pending` is null and `cancelled` holds the
    * submission whose `onCancel` waits for the op's CQE, while the kernel may still hold the op.
    */
  private final class InFlight(val id: Long, val op: RingOp[?], var pending: Pending[?] | Null) {
    var cancelled: Pending[?] | Null = null
  }

  private val ringStorage = new Array[Byte](sizeof[io_uring].toInt)
  private def ring: Ptr[io_uring] = ringStorage.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[io_uring]]
  locally {
    val res = io_uring_queue_init(entries.toUInt, ring, 0.toUInt)
    if res < 0 then throw Ring.failure("Failed to set up io_uring", res)
  }

  // User data 0 is the eventfd read, 1 a cancellation request; ring ops count up from 2.
  private inline val WakeId = 0L
  private inline val InternalId = 1L
  private var nextId = 2L

  private val inFlight = scala.collection.mutable.LongMap.empty[InFlight]
  private var unsubmitted = false
  private var wakeInFlight = false
  private var closing = false

  // CQEs reaped from the ring but not dispatched yet, so a completion that throws loses none of them.
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

  // Promises resolve on other threads; they queue here and a write to the eventfd interrupts a waiting poll.
  private val resolvedQueue = new java.util.concurrent.ConcurrentLinkedQueue[UringPromise[?]]()
  private val wakeFd: Int = eventfd.eventfd(0.toUInt, eventfd.EFD_CLOEXEC)
  if wakeFd < 0 then {
    io_uring_queue_exit(ring)
    throw new IOException(s"Failed to create eventfd: errno ${errno.errno}")
  }
  private val wakeBuffer = new Array[Byte](8)
  armWake()

  // Promises submitted here whose completion has not run, so `close` can cancel them. Loop thread only.
  private val pendingPromises = scala.collection.mutable.HashSet.empty[UringPromise[?]]

  // Cancelled blockers whose `block` was still running, so their `onCancel` waits for it to return. Loop thread only.
  private val cancellingBlockers = scala.collection.mutable.HashSet.empty[UringBlocking[?]]

  // Guards the eventfd against a blocker finishing after `close`, when its descriptor number may already be reused.
  private object wakeLock {}
  @volatile private var closed = false
  @volatile private var stopped = false

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
        promise.reactor = this
        promise.pending = pending
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
          case _ => () // Completed by whoever holds it.
        }
    }
    op.submitted = true
  }

  /** Prepares one submission of `op` and hands it to the kernel. */
  private def enqueue(op: RingOp[?], pending: Pending[?]): Unit = {
    val sqe = nextSqe()
    try op.prepare(sqe)
    catch {
      case t: Throwable =>
        io_uring_prep_nop(sqe) // The entry is taken; make it harmless.
        sqe.user_data = InternalId.toULong
        throw t
    }
    val flight = new InFlight(nextId, op, pending)
    nextId += 1
    sqe.user_data = flight.id.toULong
    inFlight(flight.id) = flight
    op.inFlight = flight
    flush()
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

  /** Submits the prepared entries. A busy ring keeps them queued, and the next `poll` submits them. */
  private def flush(): Unit = {
    val res = io_uring_submit(ring)
    unsubmitted = res == -errno.EBUSY || res == -errno.EAGAIN || res == -errno.EINTR
    if res < 0 && !unsubmitted then throw Ring.failure("Failed to submit to io_uring", res)
  }

  /** Keeps a read pending on the eventfd, so that a write to it completes a CQE and ends a waiting poll. */
  private def armWake(): Unit = {
    if closing then return
    val sqe = nextSqe()
    io_uring_prep_read(sqe, wakeFd, wakeBuffer.asInstanceOf[ByteArray].at(0), 8.toUInt, 0.toULong)
    sqe.user_data = WakeId.toULong
    flush()
    wakeInFlight = true
  }

  /** Interrupts a waiting `poll` or `run`. May be called from any thread. */
  def wake(): Unit = wakeLock.synchronized {
    if !closed then {
      val signal = stackalloc[CLongLong]()
      !signal = 1L
      unistd.write(wakeFd, signal.asInstanceOf[Ptr[Byte]], 8.toCSize) // A saturated counter already wakes.
    }
  }

  /** Called from any thread when a promise has its result: queue it and wake the loop. */
  private[uring] def resolved(promise: UringPromise[?]): Unit = {
    if closed then return // Its completion was dropped by `close`.
    resolvedQueue.add(promise)
    wake()
  }

  /** Runs the completions of every promise resolved so far, on the loop thread. */
  private def deliverResolved(stoppable: Boolean): Boolean = {
    var ran = false
    var promise = if stoppable && stopped then null else resolvedQueue.poll()
    while promise != null do {
      val pending = promise.pending
      promise.pending = null
      pendingPromises -= promise
      promise match {
        case blocking: UringBlocking[?] if blocking.cancelling != null =>
          // A cancelled blocker's `block` has returned, so its op's buffers and slots are the caller's again. Its
          // outcome is dropped: `cancel` detached it before deferring, and `submit` refuses it until now.
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
      promise = if stoppable && stopped then null else resolvedQueue.poll()
    }
    ran
  }

  /** Moves up to a batch of CQEs out of the ring. */
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

  /** Hands one CQE to the op it is for. The op stops being pending first, so the completion may submit it again. */
  private def dispatch(id: Long, res: Int): Unit =
    if id == WakeId then {
      wakeInFlight = false
      armWake() // promises are delivered by the loop itself
    } else if id != InternalId then
      inFlight.remove(id) match {
        case None         => ()
        case Some(flight) =>
          val op = flight.op
          if op.inFlight.asInstanceOf[AnyRef] eq flight then op.inFlight = null
          val pending = flight.pending
          if pending == null then {
            // A cancelled submission: the kernel has let go of the op, so its `onCancel` runs now.
            op.discard(res)
            val cancelled = flight.cancelled
            flight.cancelled = null
            if cancelled != null then cancelled.cancelled()
          } else {
            // A failing op fails its own completion; an exception thrown by the completion itself still escapes.
            var failure: Throwable | Null = null
            val done =
              try op.finish(res)
              catch {
                case t: Throwable =>
                  failure = t
                  true
              }
            if failure != null then pending.fail(failure.nn)
            else if done then pending.complete()
            else
              // Not finished: submit again, without the completion ever seeing it.
              try enqueue(op, pending)
              catch case t: Throwable => pending.fail(t)
          }
      }

  /** Runs the completions that are ready, first waiting up to `timeoutNanos` for one if none is: 0 does not wait, and a
    * negative timeout waits as long as it takes. Returns whether any completion ran. An exception thrown by a
    * completion escapes, and the next call carries on with the rest. Loop thread only.
    */
  def poll(timeoutNanos: Long): Boolean = pollOnce(timeoutNanos, stoppable = false)

  private def pollOnce(timeoutNanos: Long, stoppable: Boolean): Boolean = {
    if closed then throw new IllegalStateException("The reactor is closed")
    if unsubmitted then flush()
    var ran = deliverResolved(stoppable)
    if reapedCursor >= reapedCount then reap()
    if reapedCursor >= reapedCount && !ran && timeoutNanos != 0 then {
      val ts = if timeoutNanos < 0 then null else waitTimeout(timeoutNanos)
      io_uring_wait_cqe_timeout(ring, cqeSlot, ts) // -ETIME and -EINTR just mean nothing arrived
      reap()
    }
    while reapedCursor < reapedCount && !(stoppable && stopped) do {
      val i = reapedCursor
      reapedCursor += 1
      if reapedIds(i) != WakeId && reapedIds(i) != InternalId then ran = true
      dispatch(reapedIds(i), reapedResults(i))
    }
    ran
  }

  /** Runs completions until `stop` is called. */
  def run(): Unit = {
    stopped = false
    while !stopped do pollOnce(-1, stoppable = true)
  }

  def stop(): Unit = {
    stopped = true
    wake()
  }

  /** A pending ring op is detached and the kernel asked to cancel it; its `onCancel` runs once its CQE arrives, when
    * the kernel has let go of its buffers. A promise is detached, so completing it later does nothing, and its
    * `onCancel` runs before this returns, except for a blocking task that is running: it is interrupted, and its
    * `onCancel` waits until `block` returns.
    */
  def cancel(op: Op): Boolean = op match {
    case ring: RingOp[?] =>
      ring.inFlight match {
        case flight: InFlight if flight.pending != null =>
          flight.cancelled = flight.pending
          flight.pending = null
          // The op stays marked in flight until its CQE arrives, so it cannot be resubmitted before `onCancel`.
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

  /** Asks the kernel to cancel a submission. Its CQE still arrives, and then `discard` releases what it produced. */
  private def requestCancel(flight: InFlight): Unit =
    try {
      val sqe = nextSqe()
      flight.op.prepareCancel(sqe, flight.id)
      sqe.user_data = InternalId.toULong
      flush()
    } catch case _: IOException => () // The op then finishes on its own, and is discarded all the same.

  /** Detaches a pending promise so its outcome is never delivered. */
  private def detach(promise: UringPromise[?]): Pending[?] | Null = {
    val pending = promise.pending
    promise.pending = null
    pendingPromises -= promise
    pending.asInstanceOf[Pending[?] | Null]
  }

  /** Cancels every pending op, then waits until the kernel has let go of every ring op and every running blocking task
    * has returned, since until then they may still write to their ops' buffers and slots. Ring ops the kernel has not
    * released after a few seconds are abandoned, kept reachable forever instead. Then it closes the eventfd and the
    * ring, and only then runs the cancelled completions' `onCancel`.
    */
  def close(): Unit = {
    if !closed then {
      closing = true
      // Detach every pending op first, so nothing can be delivered while the reactor shuts down.
      val dropped = scala.collection.mutable.ArrayBuffer.empty[Pending[?]]
      for flight <- inFlight.values do {
        if flight.pending != null then dropped += flight.pending.nn
        else if flight.cancelled != null then dropped += flight.cancelled.nn
        flight.pending = null
        flight.cancelled = null
        requestCancel(flight)
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
      // Keep operation and wake buffers alive until the kernel has released them. queue_exit alone does not
      // establish that guarantee.
      if wakeInFlight then {
        val sqe = nextSqe()
        io_uring_prep_cancel64(sqe, WakeId.toULong, 0)
        sqe.user_data = InternalId.toULong
        try flush()
        catch { case _: IOException => () }
      }
      val deadline = System.nanoTime() + UringReactor.CloseTimeoutNanos
      while (inFlight.nonEmpty || wakeInFlight) && System.nanoTime() < deadline do {
        if unsubmitted then flush()
        while reapedCursor < reapedCount do {
          val i = reapedCursor
          reapedCursor += 1
          val id = reapedIds(i)
          if id == WakeId then wakeInFlight = false
          else if id != InternalId then
            inFlight.remove(id).foreach { flight =>
              flight.op.discard(reapedResults(i))
              flight.op.inFlight = null
            }
        }
        if inFlight.nonEmpty || wakeInFlight then {
          io_uring_wait_cqe_timeout(ring, cqeSlot, waitTimeout(10_000_000L))
          reap()
        }
      }
      // An op the kernel could not cancel in time may still write to its buffers after the ring is gone. Rather than
      // wait forever, keep this reactor, and with it those ops and their buffers, reachable for the rest of the process.
      if inFlight.nonEmpty || wakeInFlight then UringReactor.abandoned.add(this)
      wakeLock.synchronized {
        closed = true
        unistd.close(wakeFd)
      }
      io_uring_queue_exit(ring)
      running.foreach(_.awaitEnd()) // Until then they may still write to their ops' buffers and slots.
      // Only now tell the completions, so one that throws cannot leak the reactor's resources.
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

  /** How long `close` waits for the kernel to release cancelled ops before abandoning them. */
  private val CloseTimeoutNanos = 5_000_000_000L

  /** Closed reactors whose ops the kernel had not released in time, kept reachable so their buffers are never freed. */
  private val abandoned = java.util.concurrent.ConcurrentLinkedQueue[UringReactor]()

  /** Opens a reactor on a new ring with room for `entries` submissions at a time. Close it when done. */
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
