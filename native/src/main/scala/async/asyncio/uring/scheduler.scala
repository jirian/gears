package gears.async.asyncio.uring

import gears.async._
import uring._
import uringOps._
import gears.async.native

import scala.concurrent.duration._
import scala.scalanative.runtime.ByteArray
import scala.scalanative.unsafe._
import scala.scalanative.unsigned._

class UringPerThreadScheduler(
    parallelism: Int = Runtime.getRuntime().availableProcessors(),
    entriesPerShard: Int = 256
) extends Scheduler:

  /** Fiber dispatches arriving from outside every shard (a brand new fiber's
    * first dispatch, mainly - see `execute`) land here rather than being
    * bound to one round-robin-picked shard, so *whichever* shard goes idle
    * first picks them up - the Go-scheduler-style global run queue. Unlike
    * `UringShard.submitQueue`, this is genuinely stealable: it holds plain
    * `Runnable`s (fiber boundary-entries/resumes), not io_uring submissions
    * bound to one specific ring.
    */
  private val globalQueue = new java.util.concurrent.ConcurrentLinkedQueue[Runnable]()

  private val shards: Array[UringShard] = Array.fill(parallelism)(new UringShard(entriesPerShard, globalQueue))
  // Every shard needs to see every other shard to steal from, but the full
  // array doesn't exist until Array.fill above returns - wired up here,
  // strictly before any shard's thread starts, rather than threaded through
  // the constructor. See UringShard.siblings's own doc for why this plain,
  // set-once-before-start field needs no synchronization.
  shards.foreach(_.siblings = shards)
  private val threads: Array[UringShardThread] = shards.map { shard =>
    val t = new UringShardThread(shard)
    t.setDaemon(true)
    t
  }
  threads.foreach(_.start())

  private var nextShard = 0
  private def pickShard(): UringShard = synchronized:
    val s = shards(nextShard)
    nextShard = (nextShard + 1) % shards.length
    s

  override def execute(body: Runnable): Unit =
    UringShard.current() match
      case Some(shard) => shard.execute(body, calledFromOwnThread = true)
      case None =>
        // Not bound to any one shard - pushed to the global queue so any
        // shard that goes idle can steal it, not just whichever one
        // round-robin happens to land on next. pickShard() here is purely
        // "who to nudge awake first", not an assignment - any other idle
        // shard notices this too, either via its own next poll-timeout
        // wakeup or by stealing it once this one's been taken.
        globalQueue.offer(body)
        pickShard().wake()

  /** Returns the shard the op actually landed on alongside its id, not
    * just the id - a cancellation (`IORING_OP_ASYNC_CANCEL`) or timer
    * removal (`IORING_OP_TIMEOUT_REMOVE`) only matches an op submitted to
    * the *same ring*; routing a follow-up purely by `submit`'s own normal
    * current-thread/round-robin logic would frequently target the wrong
    * shard (whichever thread happens to be cancelling, not whichever
    * shard owns the original op), silently failing to cancel anything.
    * Callers needing to target this specific op later should use
    * `submitOn` with the returned shard, not call `submit` again.
    *
    * `keepAlive` (any buffer the kernel holds a raw pointer into for as
    * long as this op is outstanding, e.g. a socket read target or - here -
    * a timer's own timespec) rides directly in the same shard-owned
    * `handlers` entry as the completion closure; see `UringShard.Handler`.
    * Pass `null` for ops with nothing to keep alive (a bare cancel, a
    * timer removal, `IORING_OP_SOCKET`'s no-buffer creation call, ...).
    */
  private[uring] def submit(prep: Ptr[io_uring_sqe] => Unit, keepAlive: AnyRef)(onComplete: Int => Unit): (UringShard, __u64) =
    val id = UringShard.nextId()
    val shard = UringShard.current() match
      case Some(s) =>
        s.submitFast(id, prep, onComplete, keepAlive)
        s
      case None =>
        val s = pickShard()
        s.submitRemote(id, prep, onComplete, keepAlive)
        s
    (shard, id.toULong)

  /** Submits to a specific, already-known shard - the fast path if the
    * calling thread happens to already own it, the cross-thread queued
    * path otherwise. Used to target a follow-up op (cancel, timer
    * removal) at the exact shard an earlier `submit` landed on.
    */
  private[uring] def submitOn(shard: UringShard)(prep: Ptr[io_uring_sqe] => Unit, keepAlive: AnyRef)(onComplete: Int => Unit): Unit =
    val id = UringShard.nextId()
    if UringShard.current().contains(shard) then shard.submitFast(id, prep, onComplete, keepAlive)
    else shard.submitRemote(id, prep, onComplete, keepAlive)

  override def schedule(delay: FiniteDuration, body: Runnable): Cancellable =
    val tsArr = new Array[Byte](sizeof[__kernel_timespec].toInt)
    val ts = tsArr.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[__kernel_timespec]]
    ts.tv_sec = delay.toSeconds
    ts.tv_nsec = (delay - delay.toSeconds.seconds).toNanos

    val (shard, userData) = submit(sqe => io_uring_prep_timeout(sqe, ts, 0.toULong, 0.toUInt), tsArr) { res =>
      if res == -62 /* -ETIME */ then execute(body)
    }

    () =>
      try submitOn(shard)(sqe => io_uring_prep_timeout_remove(sqe, userData, 0.toUInt), null)(_ => ())
      catch case _: IOException => ()

  def tcpSupport = new UringTcpSupport(this) {}
  def udpSupport = new UringUdpSupport(this) {}

class UringPerThreadSupport(parallelism: Int = Runtime.getRuntime().availableProcessors())
    extends UringPerThreadScheduler(parallelism)
    with AsyncSupport
    with AsyncOperations
    with native.NativeSuspend:
  type Scheduler = this.type
