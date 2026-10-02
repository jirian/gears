package gears.async.asyncio.uring

import asyncio.Completion
import asyncio.BlockingPool
import asyncio.HostResolver
import asyncio.uring.{UringOp, UringReactor}
import asyncio.unsafe.PosixResolver
import gears.async._
import gears.async.native

import scala.concurrent.duration._

class UringPerThreadScheduler(
    parallelism: Int = Runtime.getRuntime().availableProcessors(),
    entriesPerShard: Int = 256,
    blockerPool: BlockingPool = BlockingPool.global,
    resolver: HostResolver = PosixResolver
) extends Scheduler:

  private val globalQueue = new java.util.concurrent.ConcurrentLinkedQueue[Runnable]()

  private val shards: Array[UringShard] =
    Array.fill(parallelism)(new UringShard(entriesPerShard, globalQueue, blockerPool, resolver))
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

  /** The shard to submit to from here: the current one on a shard thread, otherwise the next in turn. */
  private[uring] def shardForSubmit(): UringShard = UringShard.current().getOrElse(pickShard())

  /** Submits `op` to a shard's reactor and suspends until it finishes, rethrowing its failure. Cancelling the caller
    * cancels the op; the caller then sees it as cancelled, even if the kernel was already done with it.
    */
  private[uring] def await(build: UringReactor => UringOp[?])(using Async): Unit =
    val shard = shardForSubmit()
    val reactor = shard.reactor
    val op = build(reactor).asInstanceOf[reactor.Op]
    Future
      .withResolver[Unit]: resolver =>
        resolver.onCancel(() => shard.onLoop(reactor.cancel(op)))
        shard.onLoop:
          try
            reactor.submit(
              op,
              new Completion[reactor.Op]:
                def onComplete(op: reactor.Op): Unit = resolver.resolve(())
                override def onFailure(op: reactor.Op, failure: Throwable): Unit = resolver.reject(failure)
                override def onCancel(op: reactor.Op): Unit = resolver.rejectAsCancelled()
            )
          catch case t: Throwable => resolver.reject(t)
      .link()
      .await

  override def schedule(delay: FiniteDuration, body: Runnable): Cancellable =
    val shard = shardForSubmit()
    val timer = shard.reactor.ops.timer(delay)
    shard.onLoop(shard.reactor.submit(timer, _ => execute(body)))
    () => shard.onLoop(shard.reactor.cancel(timer))

  def tcpSupport = new UringTcpSupport(this) {}
  def udpSupport = new UringUdpSupport(this) {}
  def fileSupport = new UringFileSupport(this) {}

class UringPerThreadSupport(
    parallelism: Int = Runtime.getRuntime().availableProcessors(),
    entriesPerShard: Int = 256,
    blockerPool: BlockingPool = BlockingPool.global,
    resolver: HostResolver = PosixResolver
) extends UringPerThreadScheduler(parallelism, entriesPerShard, blockerPool, resolver)
    with AsyncSupport
    with AsyncOperations
    with native.NativeSuspend:
  type Scheduler = this.type
