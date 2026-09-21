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

  private val shards: Array[UringShard] = Array.fill(parallelism)(new UringShard(entriesPerShard))
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
      case None        => pickShard().execute(body, calledFromOwnThread = false)

  /** Returns the shard the op actually landed on alongside its id, not
    * just the id - a cancellation (`IORING_OP_ASYNC_CANCEL`) or timer
    * removal (`IORING_OP_TIMEOUT_REMOVE`) only matches an op submitted to
    * the *same ring*; routing a follow-up purely by `submit`'s own normal
    * current-thread/round-robin logic would frequently target the wrong
    * shard (whichever thread happens to be cancelling, not whichever
    * shard owns the original op), silently failing to cancel anything.
    * Callers needing to target this specific op later should use
    * `submitOn` with the returned shard, not call `submit` again.
    */
  private[uring] def submit(prep: Ptr[io_uring_sqe] => Unit)(onComplete: Int => Unit): (UringShard, __u64) =
    val id = UringShard.nextId()
    val shard = UringShard.current() match
      case Some(s) =>
        s.submitFast(id, prep, onComplete)
        s
      case None =>
        val s = pickShard()
        s.submitRemote(id, prep, onComplete)
        s
    (shard, id.toULong)

  /** Submits to a specific, already-known shard - the fast path if the
    * calling thread happens to already own it, the cross-thread queued
    * path otherwise. Used to target a follow-up op (cancel, timer
    * removal) at the exact shard an earlier `submit` landed on.
    */
  private[uring] def submitOn(shard: UringShard)(prep: Ptr[io_uring_sqe] => Unit)(onComplete: Int => Unit): Unit =
    val id = UringShard.nextId()
    if UringShard.current().contains(shard) then shard.submitFast(id, prep, onComplete)
    else shard.submitRemote(id, prep, onComplete)

  override def schedule(delay: FiniteDuration, body: Runnable): Cancellable =
    val tsArr = new Array[Byte](sizeof[__kernel_timespec].toInt)
    val ts = tsArr.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[__kernel_timespec]]
    ts.tv_sec = delay.toSeconds
    ts.tv_nsec = (delay - delay.toSeconds.seconds).toNanos

    val (shard, userData) = submit(sqe => io_uring_prep_timeout(sqe, ts, 0.toULong, 0.toUInt)) { res =>
      // keep tsArr (and the memory `ts` points into) alive until here - see
      // uringOps.reachabilityFence for why a plain reference isn't enough
      reachabilityFence(tsArr)
      if res == -62 /* -ETIME */ then execute(body)
    }

    () =>
      try submitOn(shard)(sqe => io_uring_prep_timeout_remove(sqe, userData, 0.toUInt))(_ => ())
      catch case _: IOException => ()

  def tcpSupport = new UringTcpSupport(this) {}
  def udpSupport = new UringUdpSupport(this) {}

class UringPerThreadSupport(parallelism: Int = Runtime.getRuntime().availableProcessors())
    extends UringPerThreadScheduler(parallelism)
    with AsyncSupport
    with AsyncOperations
    with native.NativeSuspend:
  type Scheduler = this.type
