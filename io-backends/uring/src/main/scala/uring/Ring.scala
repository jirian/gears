package gears.async.io
package uring

import java.io.IOException
import java.util.concurrent.atomic.AtomicBoolean
import scala.scalanative.linux.eventfd
import scala.scalanative.posix.errno
import scala.scalanative.posix.unistd
import scala.scalanative.runtime.ByteArray
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import uring.*
import uringStructOps.*

/** One io_uring (Linux 6.0 or later), prepared and reaped by its worker's thread alone, and woken from any thread. The
  * ring always has a read pending on an eventfd, so a write to it ends the worker's wait in the kernel. `wake` writes
  * only while the worker is asleep: the worker sets `sleeping` before it looks for work, and a waker sets `signalled`
  * before it reads `sleeping`, so either the worker sees the signal and does not sleep, or the waker sees it asleep and
  * writes.
  */
private[uring] final class Ring(entries: Int, completions: Int) {
  private val storage = new Array[Byte](io_uring_size().toInt)
  private def ring: Ptr[io_uring] = storage.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[io_uring]]

  locally {
    val res = io_uring_init(entries.toUInt, ring, completions.toUInt)
    if res < 0 then throw Ring.failure("Failed to set up io_uring", res)
    val refusal = Ring.unsupported(ring)
    if refusal != null then {
      io_uring_queue_exit(ring)
      throw refusal
    }
  }

  private val wakeFd: Int = eventfd.eventfd(0.toUInt, eventfd.EFD_CLOEXEC)
  if wakeFd < 0 then {
    io_uring_queue_exit(ring)
    throw Results.failed(s"Failed to create an eventfd")
  }
  private val wakeBuffer = new Array[Byte](8)
  private val wakeWrite = {
    val one = new Array[Byte](8)
    one(0) = 1 // a counter of 1, little-endian
    one
  }
  private var wakeArmed = false

  /** Set by the worker while it may wait in the kernel, cleared by the waker that writes to the eventfd. */
  private val sleeping = new AtomicBoolean(false)

  /** Set by every `wake`, cleared by the worker once it has looked for work again. */
  private val signalled = new AtomicBoolean(false)

  @volatile private var closed = false

  armWake()

  /** Wakes the worker, from any thread: at once if it waits in the kernel, else before it next would. One that comes
    * once the ring is closed does nothing: the worker has stopped. The write and `close` take a lock, so the eventfd's
    * number is never written once closed, when it may name another file; only a wake of a sleeping worker writes.
    */
  def wake(): Unit = {
    signalled.set(true)
    if sleeping.getAndSet(false) then
      synchronized {
        if !closed then {
          val n = unistd.write(wakeFd, wakeWrite.asInstanceOf[ByteArray].at(0), 8.toUSize)
          if n < 0 && errno.errno != errno.EAGAIN then throw Results.failed(s"Failed to wake io_uring")
        }
      }
  }

  /** Submits the entries prepared and waits for completions: not at all if `hasWork` or a `wake` came, else until
    * `deadline` on `System.nanoTime`'s clock, or with no limit if it is negative. Then hands each completion, by its
    * user data and result, to `dispatch`. Worker thread only.
    */
  def await(deadline: Long, hasWork: => Boolean)(dispatch: (Long, Int) => Unit): Unit = {
    armWake()
    // First, as it may settle ops, whose continuations are work: the worker's own thread does not wake it.
    onSubmit()
    sleeping.set(true)
    val wait =
      if signalled.get() || hasWork then 0L
      else if deadline < 0 then -1L
      else math.max(deadline - System.nanoTime(), 0L)
    if wait == 0 then sleeping.set(false)
    val res =
      if wait == 0 then (if io_uring_sq_ready(ring).toInt > 0 then io_uring_submit(ring) else 0)
      else io_uring_submit_and_wait(ring, wait)
    sleeping.set(false)
    signalled.set(false)
    afterSubmit()
    // Out of time, interrupted, completions overflowing, which the reap takes in, or the kernel short of memory for
    // now, which the next wait tries again.
    if res < 0 && res != -Linux.ETIME && res != -errno.EINTR && res != -errno.EBUSY && res != -errno.EAGAIN &&
      res != -errno.ENOMEM
    then throw Ring.failure("Failed to submit to io_uring", res)
    reap(dispatch)
  }

  /** Called on the worker's thread just before the entries prepared go to the kernel, which may be in `await` or in
    * `nextSqe`: from then on they are the kernel's.
    */
  var onSubmit: () => Unit = () => ()

  /** Called on the worker's thread once entries have gone to the kernel, before the completions are reaped. */
  var afterSubmit: () => Unit = () => ()

  /** The next free submission queue entry, submitting those prepared to make room if there is none. Worker thread. */
  def nextSqe(): Ptr[io_uring_sqe] = {
    var sqe = io_uring_get_sqe(ring)
    if sqe == null then {
      onSubmit()
      val res = io_uring_submit(ring)
      afterSubmit()
      if res < 0 && res != -errno.EBUSY then throw Ring.failure("Failed to submit to io_uring", res)
      sqe = io_uring_get_sqe(ring)
      if sqe == null then throw new IOException("The io_uring submission queue is full")
    }
    sqe
  }

  /** Turns an entry prepared and not yet submitted into a no-op whose completion is ignored. Worker thread. */
  def nop(sqe: Ptr[io_uring_sqe]): Unit = {
    io_uring_prep_nop(sqe)
    sqe.user_data = Ring.InternalId.toULong
  }

  /** Cancels the entry with user data `id` in the kernel, from the worker's thread, and waits until the kernel has let
    * go of it: 0 if it was cancelled, its completion then `ECANCELED`; `-ENOENT` if it had finished, its completion in
    * the ring; else what the kernel said.
    */
  def cancelNow(id: Long): Int = io_uring_cancel_now(ring, id.toULong, 0, 1)

  /** Cancels every entry on descriptor `fd` in the kernel, from any thread, without waiting for one a kernel worker is
    * running: each completes with `ECANCELED`, unless it finished first.
    */
  def cancelFd(fd: Int): Unit = synchronized(if !closed then io_uring_cancel_fd(ring, fd): Unit)

  private inline val MaxBatch = 64
  private val batchStorage = new Array[Byte]((sizeof[Ptr[io_uring_cqe]] * MaxBatch.toUInt).toInt)
  private def batch: Ptr[Ptr[io_uring_cqe]] =
    batchStorage.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[Ptr[io_uring_cqe]]]

  /** Hands every completion in the ring, and in the kernel's overflow list, to `dispatch`, and re-arms the wake-up. */
  private def reap(dispatch: (Long, Int) => Unit): Unit = {
    var more = true
    while more do {
      val count = io_uring_peek_batch_cqe(ring, batch, MaxBatch.toUInt).toInt
      var i = 0
      while i < count do {
        val cqe = !(batch + i)
        val id = cqe.user_data.toLong
        val res = cqe.res
        // Each is copied out before the next is looked at, and marked seen before it is dispatched, which may submit.
        io_uring_cq_advance(ring, 1.toUInt)
        if id == Ring.WakeId then wakeArmed = false
        else if id != Ring.InternalId then dispatch(id, res)
        i += 1
      }
      if count < MaxBatch then
        if io_uring_cq_overflowing(ring) == 0 then more = false
        else {
          val res = io_uring_get_events(ring)
          if res < 0 && res != -errno.EINTR && res != -errno.EAGAIN && res != -errno.EBUSY then
            throw Ring.failure("Failed to take completions from io_uring", res)
        }
    }
    armWake()
  }

  /** Keeps a read of the eventfd pending, which a `wake` completes. Taken with `nextSqe`, which makes room in a full
    * queue: without the read a sleep would not end on a wake-up.
    */
  private def armWake(): Unit =
    if !wakeArmed && !closed then {
      val sqe = nextSqe()
      io_uring_prep_read(sqe, wakeFd, wakeBuffer.asInstanceOf[ByteArray].at(0), 8.toUInt, 0.toULong)
      sqe.user_data = Ring.WakeId.toULong
      wakeArmed = true
    }

  /** Tears the ring down, once its worker has stopped: the kernel cancels what is left in it. Worker thread. */
  def close(): Unit = synchronized {
    if !closed then {
      closed = true
      io_uring_queue_exit(ring)
      unistd.close(wakeFd): Unit
    }
  }
}

private[uring] object Ring {

  /** The user data of the eventfd's read, and of entries whose completions are ignored. Ops get the numbers after. */
  inline val WakeId = 0L
  inline val InternalId = 1L

  /** The failure of a call whose result is `res`, a negated `errno`. */
  def failure(what: String, res: Int): IOException = Results.failure(what, res)

  /** Why this kernel's io_uring will not do, or null: the backend needs the synchronous cancel of Linux 6.0, so that
    * taking an op back returns once the kernel has let go of it, and completions that wait rather than drop when the
    * queue is full, and timed waits. No op has `InternalId`, so a kernel that has the cancel finds none.
    */
  private def unsupported(ring: Ptr[io_uring]): IOException | Null = {
    val features = io_uring_features(ring).toInt
    val probe = io_uring_cancel_now(ring, InternalId.toULong, 0, 1)
    if probe != -errno.ENOENT || (features & IORING_FEAT_NODROP) == 0 || (features & IORING_FEAT_EXT_ARG) == 0 then
      new IOException(s"The io_uring backend needs Linux 6.0 or later (synchronous cancel: $probe)")
    else null
  }
}
