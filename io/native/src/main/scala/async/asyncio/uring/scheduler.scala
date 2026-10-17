package gears.async.asyncio.uring

import gears.async._
import gears.async.asyncio.reactor.{
  IoGuard,
  ReactorDnsSupport,
  ReactorDriver,
  ReactorFileSupport,
  ReactorTcpSupport,
  ReactorUdpSupport,
  SharedJob
}
import gears.async.native
import gears.asyncio.Reactor
import gears.asyncio.uring.{BlockingPool, HostResolver, PosixHostResolver, UringReactor}

import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentLinkedQueue
import scala.concurrent.duration._

// A fiber must not block its shard's thread: nothing else on that shard runs, and its reactor's I/O cannot complete.
class UringPerThreadScheduler(
    parallelism: Int = Runtime.getRuntime().availableProcessors(),
    entriesPerShard: Int = 256,
    blockerPool: BlockingPool = BlockingPool.global,
    resolver: HostResolver = PosixHostResolver,
    stallWarningAfter: FiniteDuration = 1.second,
    onStall: String => Unit = message => System.err.println(message)
) extends Scheduler
    with ReactorDriver
    with AutoCloseable:

  require(parallelism > 0, "parallelism must be positive")
  require(entriesPerShard > 0, "entriesPerShard must be positive")

  private val globalQueue = new ConcurrentLinkedQueue[Runnable]()
  private val sharedJobs = new ConcurrentLinkedQueue[SharedJob[UringShard]]()

  private val shards: Array[UringShard] =
    val opened = scala.collection.mutable.ArrayBuffer.empty[UringShard]
    try
      for _ <- 0 until parallelism do
        opened += new UringShard(this, entriesPerShard, globalQueue, sharedJobs, blockerPool, resolver)
      opened.toArray
    catch
      case t: Throwable =>
        opened.foreach(_.reactor.close())
        throw t
  shards.foreach(_.siblings = shards)
  private val threads: Array[UringShardThread] = shards.zipWithIndex.map { (shard, i) =>
    val t = new UringShardThread(shard)
    t.setName(s"uring-shard-$i")
    t.setDaemon(true)
    t
  }
  threads.foreach(_.start())

  @volatile private var closed = false

  private val watchdog = new Thread(() => watch(), "uring-shard-watchdog")
  watchdog.setDaemon(true)
  watchdog.start()

  private val nextShard = java.util.concurrent.atomic.AtomicInteger()
  private def currentShard = UringShard.current().filter(_.scheduler eq this)
  private def pickShard(): UringShard = shards(Math.floorMod(nextShard.getAndIncrement(), shards.length))

  override def execute(body: Runnable): Unit =
    currentShard match
      case Some(shard) => shard.execute(body, calledFromOwnThread = true)
      case None        =>
        synchronized:
          if closed then UringShard.afterClose(body)
          else
            globalQueue.offer(body)
            pickShard().wake()

  private def submitShared(job: SharedJob[UringShard]): Unit = synchronized:
    if closed then job.cancel()
    else
      sharedJobs.offer(job)
      pickShard().wake()

  override def await(guard: IoGuard | Null)(build: (r: Reactor) => r.Op)(using Async): Unit =
    if closed then throw closedError()
    val pending = Future
      .withResolver[Unit]: resolver =>
        currentShard match
          case Some(shard) =>
            val cancel = ReactorDriver.submitOn(shard.reactor, shard.onLoop(_), guard, build, resolver)
            resolver.onCancel(cancel)
          case None =>
            val job = SharedJob[UringShard](
              shard => ReactorDriver.submitOn(shard.reactor, shard.onLoop(_), guard, build, resolver),
              whenCancelledFirst = () => resolver.rejectAsCancelled()
            )
            resolver.onCancel(() => job.cancel())
            submitShared(job)
    ReactorDriver.waitFor(pending)

  override def openFile(path: String, flags: Int, mode: Int)(using Async): Int =
    val closeOnExec = gears.asyncio.uring.LinuxConstants.O_CLOEXEC
    awaitHandle(null)((r, opened) => uring(r).ops.openAt(path, flags | closeOnExec, mode, opened).asInstanceOf[r.Op])

  override def acceptsHeapBuffers: Boolean = true

  override def readFile(guard: IoGuard | Null, fd: Int, buf: ByteBuffer)(using Async): Unit =
    await(guard)(r => r.ops.read(fd, buf))

  override def writeFile(guard: IoGuard | Null, fd: Int, buf: ByteBuffer)(using Async): Unit =
    await(guard)(r => uring(r).ops.writeFile(fd, buf).asInstanceOf[r.Op])
  override def readAt(guard: IoGuard, fd: Int, buf: ByteBuffer, offset: Long)(using Async): Unit =
    await(guard)(r => uring(r).ops.readAt(fd, buf, offset).asInstanceOf[r.Op])
  override def writeAt(guard: IoGuard, fd: Int, buf: ByteBuffer, offset: Long)(using Async): Unit =
    await(guard)(r => uring(r).ops.writeAt(fd, buf, offset).asInstanceOf[r.Op])

  private def uring(r: Reactor): UringReactor = r.asInstanceOf[UringReactor]

  override def schedule(delay: FiniteDuration, body: Runnable): Cancellable =
    if closed then throw closedError()
    def start(shard: UringShard): () => Unit =
      val timer = shard.reactor.ops.timer(delay)
      shard.reactor.submit(timer, _ => execute(body))
      () => shard.onLoop(shard.reactor.cancel(timer))
    currentShard match
      case Some(shard) =>
        val cancel = start(shard)
        () => cancel()
      case None =>
        val job = SharedJob[UringShard](start, whenCancelledFirst = () => ())
        submitShared(job)
        () => job.cancel()

  private def watch(): Unit =
    val threshold = stallWarningAfter.toNanos
    val reported = new Array[Long](shards.length)
    val period = math.max(10L, math.min(250L, stallWarningAfter.toMillis / 4))
    while !closed do
      try Thread.sleep(period)
      catch case _: InterruptedException => ()
      val now = System.nanoTime()
      var i = 0
      while i < shards.length do
        val since = shards(i).busySince
        if since != 0L && now - since > threshold && reported(i) != since then
          reported(i) = since
          onStall(
            s"gears uring: shard $i (thread ${threads(i).getName}) has been running one task for " +
              s"${(now - since) / 1_000_000} ms. Nothing else runs on that shard meanwhile, and the I/O in flight on " +
              "its reactor cannot complete; a fiber blocking its thread, such as with Thread.sleep, is the usual cause."
          )
        i += 1

  @volatile private var failure: Throwable | Null = null

  private def closedError(): IllegalStateException =
    val cause = failure
    if cause == null then IllegalStateException("The scheduler is closed")
    else IllegalStateException("The scheduler stopped after a fatal error on one of its shards", cause)

  private[uring] def failed(cause: Throwable): Unit =
    synchronized:
      if failure == null && !closed then failure = cause
    close()

  override def close(): Unit =
    val first = synchronized:
      val wasOpen = !closed
      closed = true
      wasOpen
    if first then
      watchdog.interrupt()
      var job = sharedJobs.poll()
      while job != null do
        job.cancel()
        job = sharedJobs.poll()
      shards.foreach(_.stop())
      val self = Thread.currentThread()
      threads.foreach(t => if t ne self then t.join())
      var task = globalQueue.poll()
      while task != null do
        UringShard.afterClose(task)
        task = globalQueue.poll()

  def tcpSupport = new ReactorTcpSupport(this) {}
  def udpSupport = new ReactorUdpSupport(this) {}
  def fileSupport = new ReactorFileSupport(this) {}

  def dnsSupport = new ReactorDnsSupport(this) {}

class UringPerThreadSupport(
    parallelism: Int = Runtime.getRuntime().availableProcessors(),
    entriesPerShard: Int = 256,
    blockerPool: BlockingPool = BlockingPool.global,
    resolver: HostResolver = PosixHostResolver,
    stallWarningAfter: FiniteDuration = 1.second,
    onStall: String => Unit = message => System.err.println(message)
) extends UringPerThreadScheduler(parallelism, entriesPerShard, blockerPool, resolver, stallWarningAfter, onStall)
    with AsyncSupport
    with AsyncOperations
    with native.NativeSuspend:
  type Scheduler = this.type

  override def sleep(millis: Long)(using Async): Unit =
    require(millis >= 0, "sleep duration must be nonnegative")
    await(null)(r => r.asInstanceOf[UringReactor].ops.timer(millis.millis).asInstanceOf[r.Op])
