package gears.async.asyncio.uring

import gears.async._
import gears.async.native
import gears.async.net.DnsSupport

import java.io.IOException
import java.net.{InetAddress, UnknownHostException}
import java.nio.channels.{AsynchronousCloseException, ClosedChannelException}
import scala.concurrent.duration._

import _root_.asyncio.uring.{BlockingPool, HostResolver, PosixHostResolver, UringOp, UringReactor}
import _root_.asyncio.{Completion, ResolvedAddress, Slot}

/** A gears scheduler with one OS thread per shard, each running gears tasks and driving its own [[UringReactor]]. Close
  * it to stop the threads and release the rings; tasks scheduled after that are discarded.
  */
class UringPerThreadScheduler(
    parallelism: Int = Runtime.getRuntime().availableProcessors(),
    entriesPerShard: Int = 256,
    blockerPool: BlockingPool = BlockingPool.global,
    resolver: HostResolver = PosixHostResolver
) extends Scheduler
    with AutoCloseable:

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

  @volatile private var closed = false

  private var nextShard = 0
  private def pickShard(): UringShard = synchronized:
    val s = shards(nextShard)
    nextShard = (nextShard + 1) % shards.length
    s

  override def execute(body: Runnable): Unit =
    if !closed then
      UringShard.current() match
        case Some(shard) => shard.execute(body, calledFromOwnThread = true)
        case None        =>
          globalQueue.offer(body)
          pickShard().wake()

  /** The shard to submit to from here: the current one on a shard thread, otherwise the next in turn. */
  private[uring] def shardForSubmit(): UringShard = UringShard.current().getOrElse(pickShard())

  /** Submits an op to a shard's reactor and suspends until it finishes, rethrowing its failure. Cancelling the caller
    * cancels the op; the caller then sees it as cancelled, even if the kernel was already done with it.
    *
    * With a `guard`, the op belongs to that resource: it is refused with `ClosedChannelException` once the resource is
    * closed, and closing the resource while it is pending cancels it and fails the caller with
    * `AsynchronousCloseException`.
    */
  private[uring] def await(guard: IoGuard | Null)(build: UringReactor => UringOp[?])(using Async): Unit =
    if closed then throw IllegalStateException("The scheduler is closed")
    val shard = shardForSubmit()
    val reactor = shard.reactor
    val op = build(reactor).asInstanceOf[reactor.Op]
    val cancel = () => shard.onLoop(reactor.cancel(op))
    Future
      .withResolver[Unit]: resolver =>
        resolver.onCancel(cancel)
        shard.onLoop:
          var registration: IoGuard.Registration | Null = null
          def release(): Unit = if registration != null then guard.nn.release(registration.nn)
          val completion = new Completion[reactor.Op]:
            def onComplete(op: reactor.Op): Unit =
              release()
              resolver.resolve(())
            override def onFailure(op: reactor.Op, failure: Throwable): Unit =
              release()
              resolver.reject(failure)
            override def onCancel(op: reactor.Op): Unit =
              release()
              if guard != null && guard.nn.isClosed then resolver.reject(AsynchronousCloseException())
              else resolver.rejectAsCancelled()
          try
            if guard == null then reactor.submit(op, completion)
            else
              registration = guard.nn.submit(cancel)(reactor.submit(op, completion))
              if registration == null then resolver.reject(ClosedChannelException())
          catch case t: Throwable => resolver.reject(t)
      .link()
      .await

  override def schedule(delay: FiniteDuration, body: Runnable): Cancellable =
    val shard = shardForSubmit()
    val timer = shard.reactor.ops.timer(delay)
    shard.onLoop(shard.reactor.submit(timer, _ => execute(body)))
    () => shard.onLoop(shard.reactor.cancel(timer))

  /** Stops every shard, which cancels the ops pending on its reactor and closes it, and waits for the shard threads to
    * finish. Fibers still waiting on this scheduler never resume. Closing twice does nothing.
    */
  override def close(): Unit =
    val first = synchronized:
      val wasOpen = !closed
      closed = true
      wasOpen
    if first then
      shards.foreach(_.stop())
      val self = Thread.currentThread()
      threads.foreach(t => if t ne self then t.join())

  def tcpSupport = new UringTcpSupport(this) {}
  def udpSupport = new UringUdpSupport(this) {}
  def fileSupport = new UringFileSupport(this) {}

  /** Host name resolution through the reactors' `resolve` op: it runs on the blocker pool, so it never blocks a shard
    * thread, and cancelling the caller cancels the lookup.
    */
  def dnsSupport: DnsSupport = new DnsSupport:
    override def resolve(host: String)(using Async): Array[InetAddress] =
      val addresses = Slot[List[ResolvedAddress]]()
      try await(null)(reactor => reactor.ops.resolve(host, addresses))
      catch
        case e: IOException =>
          // The message already names the host, as in "Failed to resolve example.invalid: Name or service not known".
          throw UnknownHostException(Option(e.getMessage).getOrElse(s"Failed to resolve $host")).initCause(e)
      // The addresses are numeric, so parsing them does no further lookup; like `InetAddress.getAllByName`, each keeps
      // the name it was resolved from.
      addresses.clear().map(a => InetAddress.getByAddress(host, InetAddress.getByName(a.host).getAddress)).toArray

class UringPerThreadSupport(
    parallelism: Int = Runtime.getRuntime().availableProcessors(),
    entriesPerShard: Int = 256,
    blockerPool: BlockingPool = BlockingPool.global,
    resolver: HostResolver = PosixHostResolver
) extends UringPerThreadScheduler(parallelism, entriesPerShard, blockerPool, resolver)
    with AsyncSupport
    with AsyncOperations
    with native.NativeSuspend:
  type Scheduler = this.type
