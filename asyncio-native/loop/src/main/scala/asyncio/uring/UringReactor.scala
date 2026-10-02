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
import asyncio.kqueue.KqueueHandles
import uring.*
import uringOps.*

/** The io_uring implementation of `Reactor`, for ops built by `UringOps`. A `RingOp` is one SQE, submitted right away,
  * and finishes with its CQE; resolved promises wake the loop through an eventfd the ring keeps a read pending on.
  *
  * Besides `run`, a driver that interleaves the reactor with its own work, such as a scheduler running tasks on the
  * same thread, calls `poll` instead, and `wake` from other threads to interrupt a waiting `poll`.
  *
  * Cancelling a ring op runs its `onCancel` right away, as `Reactor.cancel` requires, but the kernel only lets go of
  * the op when the cancellation's CQE arrives. Until then the reactor keeps the op, and with it its buffers, reachable,
  * and a result that raced the cancellation is discarded: an accepted or opened descriptor is closed, and bytes read
  * are lost.
  */
final class UringReactor private (entries: Int) extends Reactor {

  /** Every uring reactor runs the same kinds of op, so uring-specific code may build them directly. Code that only sees
    * the `Reactor` interface still cannot mix ops between reactors.
    */
  type Op = UringOp[this.type]

  /** Io_uring works on POSIX file descriptors. */
  type Handle = Int

  type BoxedHandle = Integer

  private val opsBuilder: UringOps[this.type] = new UringOps[this.type]

  def ops: UringOps[this.type] = opsBuilder

  /** The handles are plain non-blocking POSIX descriptors, the same as kqueue's. */
  def handles: Handles[Handle] = KqueueHandles

  def unbox(boxed: BoxedHandle): Handle = boxed.intValue()

  private final class Pending[C <: Op](val op: C, val completion: Completion[C]) {
    def complete(): Unit = completion.onComplete(op)
    def fail(failure: Throwable): Unit = completion.onFailure(op, failure)
    def cancelled(): Unit = completion.onCancel(op)
  }

  /** One submission of a ring op, under its user data `id`. `pending` is null once cancelled, while the kernel may
    * still hold the op.
    */
  private final class InFlight(val id: Long, val op: RingOp[?], var pending: Pending[?] | Null)

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

  // Guards the eventfd against a blocker finishing after `close`, when its descriptor number may already be reused.
  private object wakeLock {}
  @volatile private var closed = false
  @volatile private var stopped = false

  def submit[C <: Op](op: C, completion: Completion[C]): Unit = {
    if closed then throw new IllegalStateException("The reactor is closed")
    op match {
      case _: Repeatable => ()
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
        promise.reactor = this
        promise.pending = pending
        pendingPromises += promise
        promise match {
          case blocking: UringBlocking[?] => Blockers.run(blocking)
          case _                       => () // Completed by whoever holds it.
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
    val sqe = nextSqe()
    io_uring_prep_read(sqe, wakeFd, wakeBuffer.asInstanceOf[ByteArray].at(0), 8.toUInt, 0.toULong)
    sqe.user_data = WakeId.toULong
    flush()
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
      if pending != null then {
        ran = true
        val p = pending.asInstanceOf[Pending[?]]
        val failure = promise.failure
        if failure != null then p.fail(failure.nn) else p.complete()
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
    if id == WakeId then armWake() // promises are delivered by the loop itself
    else if id != InternalId then
      inFlight.remove(id) match {
        case None         => ()
        case Some(flight) =>
          val op = flight.op
          if op.inFlight.asInstanceOf[AnyRef] eq flight then op.inFlight = null
          val pending = flight.pending
          if pending == null then op.discard(res)
          else {
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

  /** A pending ring op is detached and the kernel asked to cancel it; a promise is detached, so completing it later
    * does nothing, and a blocking task is skipped or interrupted. The completion's `onCancel` then runs before this
    * returns.
    */
  def cancel(op: Op): Boolean = {
    val pending: Pending[?] | Null = op match {
      case ring: RingOp[?] =>
        ring.inFlight match {
          case flight: InFlight if flight.pending != null =>
            val p = flight.pending
            flight.pending = null
            ring.inFlight = null
            requestCancel(flight)
            p
          case _ => null
        }
      case promise: UringPromise[?] => detach(promise)
    }
    if pending == null then false
    else {
      pending.nn.cancelled()
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

  /** Detaches a pending promise so its outcome is never delivered, skipping or interrupting a blocking task. */
  private def detach(promise: UringPromise[?]): Pending[?] | Null = {
    val pending = promise.pending
    promise.pending = null
    pendingPromises -= promise
    if pending != null then
      promise match {
        case blocking: UringBlocking[?] => blocking.interrupt() // skip it, or interrupt its worker
        case _                       => ()
      }
    pending.asInstanceOf[Pending[?] | Null]
  }

  /** Cancels every pending op, waits a little for the kernel to let go of the ring ops, then closes the eventfd and the
    * ring.
    */
  def close(): Unit = {
    if !closed then {
      // Detach every pending op first, so nothing can be delivered while the reactor shuts down.
      val dropped = scala.collection.mutable.ArrayBuffer.empty[Pending[?]]
      for flight <- inFlight.values do {
        if flight.pending != null then dropped += flight.pending.nn
        flight.pending = null
        flight.op.inFlight = null
        requestCancel(flight)
      }
      for promise <- pendingPromises.toList do {
        val pending = detach(promise)
        if pending != null then dropped += pending.nn
      }
      resolvedQueue.clear()
      // The kernel may still be writing into the ops' buffers: wait for their CQEs before the memory can go.
      val deadline = System.nanoTime() + 1_000_000_000L
      while inFlight.nonEmpty && System.nanoTime() < deadline do {
        while reapedCursor < reapedCount do {
          val i = reapedCursor
          reapedCursor += 1
          inFlight.remove(reapedIds(i)).foreach(_.op.discard(reapedResults(i)))
        }
        if inFlight.nonEmpty then {
          io_uring_wait_cqe_timeout(ring, cqeSlot, waitTimeout(10_000_000L))
          reap()
        }
      }
      wakeLock.synchronized {
        closed = true
        unistd.close(wakeFd)
      }
      io_uring_queue_exit(ring)
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

  /** Opens a reactor on a new ring with room for `entries` submissions at a time. Close it when done. */
  def open(entries: Int = 256): UringReactor = new UringReactor(entries)

  /** Opens a reactor for `body` and closes it afterwards. */
  def scoped(entries: Int = 256)(body: UringReactor => Unit): Unit = {
    val reactor = open(entries)
    try body(reactor)
    finally reactor.close()
  }
}
