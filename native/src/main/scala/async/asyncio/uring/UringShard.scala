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

private[uring] final class UringShard(entries: Int, globalQueue: ConcurrentLinkedQueue[Runnable]):
  private val ringStorage = new Array[Byte](sizeof[io_uring].toInt)
  private def ring: Ptr[io_uring] =
    ringStorage.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[io_uring]]
  if io_uring_queue_init(entries.toUInt, ring, 0.toUInt) < 0 then
    throw IOException(-1)

  private case class SubmitRequest(id: Long, prep: Ptr[io_uring_sqe] => Unit, onComplete: Int => Unit, keepAlive: AnyRef)
  private val taskQueue = new ConcurrentLinkedQueue[Runnable]()
  private val submitQueue = new ConcurrentLinkedQueue[SubmitRequest]()

  private case class Handler(onComplete: Int => Unit, keepAlive: AnyRef)
  private val handlers = scala.collection.mutable.LongMap[Handler]()

  private val wakeFd: Int = eventfdOps.eventfd(0.toUInt, 0)
  if wakeFd < 0 then throw IOException(-1)
  private val wakeBufArr = new Array[Byte](8)
  private def wakeBufPtr = wakeBufArr.asInstanceOf[ByteArray].at(0)

  private val cqePtrStorage = new Array[Byte](sizeof[Ptr[io_uring_cqe]].toInt)
  private def cqePtrSlot: Ptr[Ptr[io_uring_cqe]] =
    cqePtrStorage.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[Ptr[io_uring_cqe]]]
  private val MAX_BATCH = 64
  private val batchStorage = new Array[Byte]((sizeof[Ptr[io_uring_cqe]] * MAX_BATCH.toUInt).toInt)
  private def batch: Ptr[Ptr[io_uring_cqe]] =
    batchStorage.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[Ptr[io_uring_cqe]]]
  private val idsBuf = new Array[Long](MAX_BATCH)
  private val resBuf = new Array[Int](MAX_BATCH)
  private var pendingCount = 0
  private var pendingCursor = 0

  private[uring] var siblings: Array[UringShard] = Array.empty

  private val rng = new scala.util.Random()

  private def submitLocal(id: Long, prep: Ptr[io_uring_sqe] => Unit, onComplete: Int => Unit, keepAlive: AnyRef): Unit =
    val sqe = io_uring_get_sqe(ring)
    if sqe == null then submitQueue.offer(SubmitRequest(id, prep, onComplete, keepAlive))
    else
      prep(sqe)
      handlers(id) = Handler(onComplete, keepAlive)
      sqe.user_data = id.toULong
      if io_uring_submit(ring) < 0 then
        handlers.remove(id)
        throw IOException(-1)

  private[uring] def submitFast(id: Long, prep: Ptr[io_uring_sqe] => Unit, onComplete: Int => Unit, keepAlive: AnyRef): Unit =
    submitLocal(id, prep, onComplete, keepAlive)

  private[uring] def submitRemote(id: Long, prep: Ptr[io_uring_sqe] => Unit, onComplete: Int => Unit, keepAlive: AnyRef): Unit =
    submitQueue.offer(SubmitRequest(id, prep, onComplete, keepAlive))
    wake()

  private[uring] def execute(body: Runnable, calledFromOwnThread: Boolean): Unit =
    taskQueue.offer(body)
    if !calledFromOwnThread then wake()

  private[uring] def wake(): Unit =
    val buf = stackalloc[CLongLong]()
    !buf = 1L
    val _ = unistd.write(wakeFd, buf.asInstanceOf[Ptr[Byte]], 8.toUSize)

  private def armWake(): Unit =
    submitLocal(
      UringShard.nextId(),
      sqe => io_uring_prep_read(sqe, wakeFd, wakeBufPtr, 8.toUInt, 0.toULong),
      _ => armWake(),
      null
    )

  private def drainTasks(): Boolean =
    val r = taskQueue.poll()
    if r == null then false
    else
      Continuations.handlersReset()
      try r.run()
      finally Continuations.handlersReset()
      true

  private def drainSubmits(): Boolean =
    var any = false
    var req = submitQueue.poll()
    while req != null do
      any = true
      submitLocal(req.id, req.prep, req.onComplete, req.keepAlive)
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
      handlers.remove(id).foreach { h =>
        Continuations.handlersReset()
        try h.onComplete(res)
        finally Continuations.handlersReset()
      }
      true

  private[uring] def stealTask(): Runnable = taskQueue.poll()

  private def tryStealOrGlobal(): Boolean =
    val fromGlobal = globalQueue.poll()
    val task = if fromGlobal != null then fromGlobal else stealFromSibling()
    if task == null then false
    else
      Continuations.handlersReset()
      try task.run()
      finally Continuations.handlersReset()
      true

  private def stealFromSibling(): Runnable =
    val n = siblings.length
    if n <= 1 then null
    else
      val start = rng.nextInt(n)
      var i = 0
      var stolen: Runnable = null
      while stolen == null && i < n do
        val candidate = siblings((start + i) % n)
        if candidate ne this then stolen = candidate.stealTask()
        i += 1
      stolen

  private val pollTimeoutStorage = new Array[Byte](sizeof[__kernel_timespec].toInt)
  private def pollTimeout: Ptr[__kernel_timespec] =
    val ts = pollTimeoutStorage.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[__kernel_timespec]]
    ts.tv_sec = 0
    ts.tv_nsec = 2_000_000L // 2ms
    ts

  private def waitOnce(): Unit =
    val _ = io_uring_wait_cqe_timeout(ring, cqePtrSlot, pollTimeout)

  private[uring] def loop(): Unit =
    Continuations.handlersReset()
    armWake()
    while true do
      val ranTasks = drainTasks()
      val ranSubmits = drainSubmits()
      val ranCompletions = drainCompletions()
      if !ranTasks && !ranSubmits && !ranCompletions && !tryStealOrGlobal() then waitOnce()

private[uring] final class UringShardThread(val shard: UringShard) extends Thread(() => shard.loop())

private[uring] object UringShard:
  def current(): Option[UringShard] = Thread.currentThread() match
    case t: UringShardThread => Some(t.shard)
    case _                   => None

  private val idGen = new java.util.concurrent.atomic.AtomicLong(0)
  private[uring] def nextId(): Long = idGen.getAndIncrement()
