package gears.async.asyncio.uring

import uring._
import uringOps._

import java.io.Closeable
import scala.annotation.tailrec
import scala.scalanative.libc.stdlib
import scala.scalanative.unsafe._
import scala.scalanative.unsigned._

class IOException(errno: Int) extends Exception:
  override def toString(): String = s"IO Error: errno=$errno"

/** Owns a single io_uring instance.
  *
  * Submission (`submit`) is guarded by a lock and may be called from any
  * thread. A single dedicated thread drains completions and dispatches them
  * to the callback that was registered at submission time - no persistent
  * readiness source is needed since every io_uring op completes exactly
  * once.
  */
class UringRing(entries: Int = 256) extends Closeable:
  private val ring: Ptr[io_uring] =
    stdlib.malloc(sizeof[io_uring]).asInstanceOf[Ptr[io_uring]]
  if io_uring_queue_init(entries.toUInt, ring, 0.toUInt) < 0 then
    throw IOException(-1)

  private val submitLock = new Object

  /** Submits one op. `prep` fills in the SQE (opcode, fd, buffer, ...);
    * `onComplete` is invoked from the ring thread with the raw `res` field
    * of the CQE once it arrives. Returns the op's `user_data` tag, which can
    * be fed into `IORING_OP_ASYNC_CANCEL`/`IORING_OP_TIMEOUT_REMOVE` to
    * cancel it.
    */
  def submit(prep: Ptr[io_uring_sqe] => Unit)(onComplete: Int => Unit): __u64 =
    submitLock.synchronized:
      val sqe = io_uring_get_sqe(ring)
      if sqe == null then throw IOException(-1) // submission queue full
      prep(sqe)
      io_uring_sqe_set_data(sqe, onComplete.asInstanceOf[AnyRef])
      if io_uring_submit(ring) < 0 then throw IOException(-1)
      sqe.user_data

  override def close(): Unit =
    io_uring_queue_exit(ring)
    stdlib.free(ring.asInstanceOf[Ptr[Byte]])

  @tailrec private def drainBatch(batch: Ptr[Ptr[io_uring_cqe]], max: Int): Unit =
    val count = io_uring_peek_batch_cqe(ring, batch, max.toUInt)
    var i = 0
    while i < count.toInt do
      val cqe = !(batch + i)
      val handler = io_uring_cqe_get_data[Int => Unit](cqe)
      val res = cqe.res
      io_uring_cq_advance(ring, 1.toUInt)
      handler(res)
      i += 1
    if count.toInt == max then drainBatch(batch, max)

  private def completionLoop(): Unit =
    val MAX_BATCH = 64
    val cqePtr =
      stdlib.malloc(sizeof[Ptr[io_uring_cqe]]).asInstanceOf[Ptr[Ptr[io_uring_cqe]]]
    val batch = stdlib
      .malloc(sizeof[Ptr[io_uring_cqe]] * MAX_BATCH.toUInt)
      .asInstanceOf[Ptr[Ptr[io_uring_cqe]]]
    try
      while true do
        // Blocks until at least one CQE is available (null timeout = wait
        // indefinitely). Timers ride on this same queue via
        // IORING_OP_TIMEOUT, so no separate wakeup/timer thread is needed.
        val rc = io_uring_wait_cqe_timeout(ring, cqePtr, null)
        if rc == 0 then drainBatch(batch, MAX_BATCH)
    finally
      stdlib.free(cqePtr.asInstanceOf[Ptr[Byte]])
      stdlib.free(batch.asInstanceOf[Ptr[Byte]])

  private val ringThread = new Thread(() => completionLoop())
  ringThread.setDaemon(true)
  ringThread.start()
