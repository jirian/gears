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

  private val globalQueue = new java.util.concurrent.ConcurrentLinkedQueue[Runnable]()

  private val shards: Array[UringShard] = Array.fill(parallelism)(new UringShard(entriesPerShard, globalQueue))
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
        globalQueue.offer(body)
        pickShard().wake()

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
  def fileSupport = new UringFileSupport(this) {}

class UringPerThreadSupport(parallelism: Int = Runtime.getRuntime().availableProcessors())
    extends UringPerThreadScheduler(parallelism)
    with AsyncSupport
    with AsyncOperations
    with native.NativeSuspend:
  type Scheduler = this.type
