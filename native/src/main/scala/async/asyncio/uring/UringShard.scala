package gears.async.asyncio.uring

import uring._
import uringOps._

import java.util.concurrent.ConcurrentLinkedQueue
import scala.annotation.tailrec
import scala.scalanative.linux.{eventfd => eventfdOps}
import scala.scalanative.posix.unistd
import scala.scalanative.runtime.ByteArray
import scala.scalanative.runtime.Continuations
import scala.scalanative.unsafe._
import scala.scalanative.unsigned._

class IOException(errno: Int) extends Exception:
  override def toString(): String = s"IO Error: errno=$errno"

private[uring] final class UringShard(entries: Int):
  private val ringStorage = new Array[Byte](sizeof[io_uring].toInt)
  private val ring: Ptr[io_uring] =
    ringStorage.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[io_uring]]
  if io_uring_queue_init(entries.toUInt, ring, 0.toUInt) < 0 then
    throw IOException(-1)

  private case class SubmitRequest(id: Long, prep: Ptr[io_uring_sqe] => Unit, onComplete: Int => Unit)
  private val taskQueue = new ConcurrentLinkedQueue[Runnable]()
  private val submitQueue = new ConcurrentLinkedQueue[SubmitRequest]()

  private val handlers = scala.collection.mutable.LongMap[Int => Unit]()

  private val wakeFd: Int = eventfdOps.eventfd(0.toUInt, 0)
  if wakeFd < 0 then throw IOException(-1)
  private val wakeBufArr = new Array[Byte](8)
  private val wakeBufPtr = wakeBufArr.asInstanceOf[ByteArray].at(0)

  private val cqePtrStorage = new Array[Byte](sizeof[Ptr[io_uring_cqe]].toInt)
  private val cqePtrSlot: Ptr[Ptr[io_uring_cqe]] =
    cqePtrStorage.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[Ptr[io_uring_cqe]]]
  private val MAX_BATCH = 64
  private val batchStorage = new Array[Byte]((sizeof[Ptr[io_uring_cqe]] * MAX_BATCH.toUInt).toInt)
  private val batch: Ptr[Ptr[io_uring_cqe]] =
    batchStorage.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[Ptr[io_uring_cqe]]]
  private val idsBuf = new Array[Long](MAX_BATCH)
  private val resBuf = new Array[Int](MAX_BATCH)
  private var pendingCount = 0
  private var pendingCursor = 0

  private def submitLocal(id: Long, prep: Ptr[io_uring_sqe] => Unit, onComplete: Int => Unit): Unit =
    val sqe = io_uring_get_sqe(ring)
    if sqe == null then submitQueue.offer(SubmitRequest(id, prep, onComplete))
    else
      prep(sqe)
      handlers(id) = onComplete
      sqe.user_data = id.toULong
      if io_uring_submit(ring) < 0 then
        handlers.remove(id)
        throw IOException(-1)

  private[uring] def submitFast(id: Long, prep: Ptr[io_uring_sqe] => Unit, onComplete: Int => Unit): Unit =
    submitLocal(id, prep, onComplete)

  private[uring] def submitRemote(id: Long, prep: Ptr[io_uring_sqe] => Unit, onComplete: Int => Unit): Unit =
    submitQueue.offer(SubmitRequest(id, prep, onComplete))
    wake()

  private[uring] def execute(body: Runnable, calledFromOwnThread: Boolean): Unit =
    taskQueue.offer(body)
    if !calledFromOwnThread then wake()

  private def wake(): Unit =
    val buf = stackalloc[CLongLong]()
    !buf = 1L
    val _ = unistd.write(wakeFd, buf.asInstanceOf[Ptr[Byte]], 8.toUSize)

  private def armWake(): Unit =
    submitLocal(
      UringShard.nextId(),
      sqe => io_uring_prep_read(sqe, wakeFd, wakeBufPtr, 8.toUInt, 0.toULong),
      _ => armWake()
    )

  private def drainTasks(): Boolean =
    var any = false
    var r = taskQueue.poll()
    while r != null do
      any = true
      Continuations.handlersReset()
      try r.run()
      finally Continuations.handlersReset()
      r = taskQueue.poll()
    any

  private def drainSubmits(): Boolean =
    var any = false
    var req = submitQueue.poll()
    while req != null do
      any = true
      submitLocal(req.id, req.prep, req.onComplete)
      req = submitQueue.poll()
    any

  private def refillPending(): Unit =
    val count = io_uring_peek_batch_cqe(ring, batch, MAX_BATCH.toUInt).toInt
    var i = 0
    while i < count do
      val cqe = !(batch + i)
      idsBuf(i) = cqe.user_data.toLong
      resBuf(i) = cqe.res
      i += 1
    if count > 0 then io_uring_cq_advance(ring, count.toUInt)
    pendingCount = count
    pendingCursor = 0

  private def drainCompletions(): Boolean =
    if pendingCursor >= pendingCount then refillPending()
    if pendingCursor >= pendingCount then false
    else
      val id = idsBuf(pendingCursor)
      val res = resBuf(pendingCursor)
      pendingCursor += 1
      handlers.remove(id).foreach { handler =>
        Continuations.handlersReset()
        try handler(res)
        finally Continuations.handlersReset()
      }
      true

  private def waitOnce(): Unit =
    val _ = io_uring_wait_cqe_timeout(ring, cqePtrSlot, null)

  private[uring] def loop(): Unit =
    Continuations.handlersReset()
    armWake()
    while true do
      val ranTasks = drainTasks()
      val ranSubmits = drainSubmits()
      val ranCompletions = drainCompletions()
      if !ranTasks && !ranSubmits && !ranCompletions then waitOnce()

private[uring] final class UringShardThread(val shard: UringShard) extends Thread(() => shard.loop())

private[uring] object UringShard:
  def current(): Option[UringShard] = Thread.currentThread() match
    case t: UringShardThread => Some(t.shard)
    case _                   => None

  private val idGen = new java.util.concurrent.atomic.AtomicLong(0)
  private[uring] def nextId(): Long = idGen.getAndIncrement()
