package gears.io.uring

import gears.async._
import gears.async.native
import gears.io.reactor.{
  IoGuard,
  ReactorDnsSupport,
  ReactorDriver,
  ReactorFileSupport,
  ReactorTcpSupport,
  ReactorUdpSupport,
  SharedJob
}
import gears.io.uring.{BlockingPool, HostResolver, PosixHostResolver, UringReactor}

import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentLinkedQueue
import scala.concurrent.duration._

/** A gears `Scheduler` and `ReactorDriver` with `parallelism` shard threads, each running fibers and its own io_uring.
  * A fiber's I/O goes into the ring of the shard it runs on; I/O from any other thread is handed to a shard. Idle
  * shards steal tasks from busy ones. A watchdog reports a shard that runs one task for longer than
  * `stallWarningAfter`, which usually means a fiber is blocking its thread.
  *
  * Use [[UringPerThreadSupport]], which adds the gears `AsyncSupport`, to run fibers on it.
  */
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
  type R = UringReactor

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

  /** Runs `body` on the current shard, or, from any other thread, on the next free shard through the global queue.
    *
    * @param body
    *   the task to run
    */
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

  /** Runs the op that `build` makes and waits for it. On a shard of this scheduler, the op goes into that shard's ring;
    * otherwise it becomes a shared job that the next free shard starts.
    *
    * @param guard
    *   the guard of the handle the op uses, which cancels the op when it closes, or null
    * @param build
    *   builds the op on the reactor that will run it
    */
  override def await(guard: IoGuard | Null)(build: (r: UringReactor) => r.Op)(using Async): Unit =
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

  /** Opens `path` with `IORING_OP_OPENAT` instead of a pool thread, always adding `O_CLOEXEC`.
    *
    * @param path
    *   the file to open
    * @param flags
    *   the `open(2)` flags
    * @param mode
    *   the permissions of a file that `O_CREAT` creates
    * @return
    *   the new descriptor
    */
  override def openFile(path: String, flags: Int, mode: Int)(using Async): Int =
    val closeOnExec = gears.io.uring.LinuxConstants.O_CLOEXEC
    awaitHandle(null)((r, opened) => r.ops.openAt(path, flags | closeOnExec, mode, opened))

  /** Returns true: io_uring takes heap buffers in place. */
  override def acceptsHeapBuffers: Boolean = true

  /** Reads from a file at its position through the ring, not through the blocking pool. So do the other file methods
    * here.
    *
    * @param guard
    *   the file's guard, or null
    * @param fd
    *   the file to read from
    * @param buf
    *   the buffer to fill
    */
  override def readFile(guard: IoGuard | Null, fd: Int, buf: ByteBuffer)(using Async): Unit =
    await(guard)(r => r.ops.read(fd, buf))

  override def writeFile(guard: IoGuard | Null, fd: Int, buf: ByteBuffer)(using Async): Unit =
    await(guard)(r => r.ops.writeFile(fd, buf))
  override def readAt(guard: IoGuard, fd: Int, buf: ByteBuffer, offset: Long)(using Async): Unit =
    await(guard)(r => r.ops.readAt(fd, buf, offset))
  override def writeAt(guard: IoGuard, fd: Int, buf: ByteBuffer, offset: Long)(using Async): Unit =
    await(guard)(r => r.ops.writeAt(fd, buf, offset))

  /** Runs `body` after `delay`, using a ring timer on the current shard or on the next free one.
    *
    * @param delay
    *   how long to wait
    * @param body
    *   the task to run
    * @return
    *   what cancels the timer
    */
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

  /** Closes this scheduler after a shard hit a fatal error. Later calls fail with `cause`.
    *
    * @param cause
    *   the shard's error
    */
  private[uring] def failed(cause: Throwable): Unit =
    synchronized:
      if failure == null && !closed then failure = cause
    close()

  /** Stops the shards and waits for their threads, cancelling queued jobs. Each shard closes its reactor, which cancels
    * its pending I/O. Tasks still queued run on gears' default runtime, so suspended fibers can finish.
    */
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

  /** Returns TCP and Unix stream sockets on this scheduler. Fibers on any runtime may use them; see `await`. */
  def tcpSupport = new ReactorTcpSupport(this) {}

  /** Returns UDP and Unix datagram sockets on this scheduler. */
  def udpSupport = new ReactorUdpSupport(this) {}

  /** Returns file I/O on this scheduler. */
  def fileSupport = new ReactorFileSupport(this) {}

  /** Returns host name resolution on this scheduler. */
  def dnsSupport = new ReactorDnsSupport(this) {}

/** The io_uring runtime for gears: a `UringPerThreadScheduler` that is also the `AsyncSupport` and `AsyncOperations`
  * for fibers on its shards. Close it when done; its shard threads are daemons, but its rings stay mapped until then.
  *
  * {{{
  * Using.resource(UringPerThreadSupport()): support =>
  *   given UringPerThreadSupport = support
  *   given TcpSupport = support.tcpSupport
  *   Async.blocking { ... }
  * }}}
  */
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

  /** Sleeps on a ring timer, so a sleeping fiber doesn't hold a thread.
    *
    * @param millis
    *   how long to sleep, in milliseconds
    */
  override def sleep(millis: Long)(using Async): Unit =
    require(millis >= 0, "sleep duration must be nonnegative")
    await(null)(r => r.ops.timer(millis.millis))
