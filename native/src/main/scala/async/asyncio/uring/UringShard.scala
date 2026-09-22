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
  /** `ring`/`wakeBufPtr`/`cqePtrSlot`/`batch` below are all `def`s that
    * recompute their pointer from a backing array on every single access,
    * deliberately never cached in a `val`. A cached-once pointer leaves
    * the array itself read by name exactly one time, at construction -
    * exactly the shape of a real, gdb-confirmed liveness bug found this
    * session (`io_uring`'s own `sq.khead` field overwritten with what
    * looked like an unrelated Scala object's pointer), and the kind of
    * thing no amount of "it's obviously still a live field" reasoning
    * reliably prevents. Recomputing instead means every one of these
    * arrays gets a genuine, unavoidable read on every op - not a
    * discarded one either, since the recomputed pointer is immediately
    * passed to a real `extern` call - so nothing can prove the read
    * droppable without changing what that call receives. That's a
    * strictly stronger guarantee than a reachability fence (which is why
    * none of these fields need one, unlike the per-operation buffers that
    * ride `handlers` - see `Handler`'s doc below), at the cost of a few
    * extra address-arithmetic instructions per call, on values `.at`
    * computes cheaply and without allocating.
    */
  private val ringStorage = new Array[Byte](sizeof[io_uring].toInt)
  private def ring: Ptr[io_uring] =
    ringStorage.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[io_uring]]
  if io_uring_queue_init(entries.toUInt, ring, 0.toUInt) < 0 then
    throw IOException(-1)

  private case class SubmitRequest(id: Long, prep: Ptr[io_uring_sqe] => Unit, onComplete: Int => Unit, keepAlive: AnyRef)
  private val taskQueue = new ConcurrentLinkedQueue[Runnable]()
  private val submitQueue = new ConcurrentLinkedQueue[SubmitRequest]()

  /** Holds `keepAlive` (a buffer, or any other value whose raw address the
    * kernel holds onto for as long as this op is outstanding) directly
    * alongside its completion closure, rather than relying on the closure
    * to capture it - the same map entry standing for both. `handlers`
    * itself is what makes this reliable: unlike a plain field touched once
    * and never read again (the actual shape of a real, gdb-confirmed
    * liveness bug found and fixed this session), every entry here is
    * genuinely inserted and looked up per-op, so nothing about it can be
    * mistaken for dead. That's a strictly stronger guarantee than a
    * standalone reachability fence, and makes one unnecessary for any
    * value that already flows through here.
    */
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

  /** The other shards this one can steal a task from when its own queues
    * and ring both come up empty (Go-style work stealing - see `loop`'s
    * fallback sequence). Set exactly once by `UringPerThreadScheduler`,
    * right after every shard has been constructed and before any shard's
    * thread starts - `Thread.start()` itself establishes the
    * happens-before edge that makes this plain field read safe from this
    * shard's own thread afterward, with no further synchronization needed
    * since it's never written again.
    */
  private[uring] var siblings: Array[UringShard] = Array.empty

  // Only ever touched from this shard's own thread (loop()/stealFromSibling),
  // so a plain, non-thread-safe Random is fine - no shared state to race on.
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

  /** `private[uring]`, not `private`, so `UringPerThreadScheduler` can nudge
    * this shard directly after pushing work onto the *global* queue (see
    * `execute`'s external-thread path in scheduler.scala) - distinct from
    * `execute(_, calledFromOwnThread = false)`, which both enqueues onto
    * *this shard's own* local queue and wakes it; a global-queue push must
    * not also bind the work to one specific shard.
    */
  private[uring] def wake(): Unit =
    val buf = stackalloc[CLongLong]()
    !buf = 1L
    val _ = unistd.write(wakeFd, buf.asInstanceOf[Ptr[Byte]], 8.toUSize)

  private def armWake(): Unit =
    submitLocal(
      UringShard.nextId(),
      sqe => io_uring_prep_read(sqe, wakeFd, wakeBufPtr, 8.toUInt, 0.toULong),
      _ => armWake(),
      null // wakeBufArr needs nothing here - see wakeBufPtr's own doc above
    )

  /** Runs at most *one* queued task per call, for the same reason
    * `drainCompletions` only runs one handler per call: a task may enter a
    * fresh `Continuations.boundary` (a fiber's first dispatch) or resume
    * one already suspended, and running a second such dispatch immediately
    * after, within the same native call frame, without first looping back
    * through `loop`'s outer `while true`, was the actual cause of a crash
    * inside `io_uring_get_sqe` (verified via a core dump: the crash hit on
    * a fiber's very first op, right after another fiber's boundary was
    * entered earlier in the same `drainTasks` call - i.e. this loop's own
    * inner `while` previously let multiple dispatches run back-to-back
    * exactly like the batch of completions once did).
    */
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
      // h.keepAlive needs no separate mention here - it's alive for exactly
      // as long as h is, which is exactly as long as this op needed it.
      handlers.remove(id).foreach { h =>
        Continuations.handlersReset()
        try h.onComplete(res)
        finally Continuations.handlersReset()
      }
      true

  /** Lets a sibling take one task off *this* shard's local queue - the
    * Go-scheduler-style "steal" half of work stealing. Safe with no extra
    * synchronization: `taskQueue` is a `ConcurrentLinkedQueue`, already
    * safe for concurrent `poll()` from multiple threads, this shard's own
    * included.
    */
  private[uring] def stealTask(): Runnable = taskQueue.poll()

  /** The global-queue-then-steal fallback, tried once `drainTasks`/
    * `drainSubmits`/`drainCompletions` have all come up empty - mirrors
    * Go's own local -> global -> steal-from-a-random-P order. Runs at most
    * one task, for the same reason every other dispatch point in this file
    * does: see `drainTasks`'s doc.
    *
    * Deliberately not implemented: Go's other half of "work sharing" -
    * proactively routing *self-produced* work to the global queue once a
    * local queue passes some size threshold, so one P can't hoard work a
    * stalled sibling never gets a chance to steal. Go needs that because
    * its local queues are fixed at 256 slots and goroutines routinely
    * spawn more goroutines in bursts. Ours are unbounded, and this
    * backend's actual workload doesn't really produce that pattern - one
    * fiber per connection, doing its own reads and writes, not spawning
    * a burst of siblings for itself - so stealing alone (plus the bounded
    * poll below, so an idle shard keeps re-checking rather than blocking
    * past the point a sibling's backlog appears) covers it. Revisit if a
    * workload with self-spawning fibers ever shows the gap in practice.
    */
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

  // How long an idle shard sleeps before waking on its own to recheck for
  // stealable work, rather than only ever waking via an explicit wake()
  // (a real completion on its own ring, or a sibling/scheduler nudge).
  // Without this, a shard that goes idle and blocks has no way to notice
  // a sibling's backlog growing later - nothing currently calls wake() on
  // it just because *another* shard's local queue got deeper. Short
  // enough to keep rebalancing latency low, long enough that a genuinely
  // idle shard isn't busy-polling; tune if this proves wrong in practice.
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
