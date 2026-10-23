package gears.io.uring

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.control.NonFatal

/** A fixed set of daemon threads for work that must block, such as DNS lookups and the default file I/O. Reactors hand
  * their blocking ops to it, so the reactor thread never blocks. The defaults come from the system properties
  * `gears.io.blocking.parallelism` (4 threads) and `gears.io.blocking.queueCapacity` (unbounded).
  */
final class BlockingPool(
    parallelism: Int = BlockingPool.defaultParallelism,
    queueCapacity: Int = BlockingPool.defaultQueueCapacity,
    threadNamePrefix: String = "gears-io-blocking"
) extends AutoCloseable {
  require(parallelism > 0, "Blocking pool parallelism must be positive")
  require(queueCapacity > 0, "Blocking pool queue capacity must be positive")

  private val accepting = new AtomicBoolean(true)
  private val lifecycleLock = new Object
  private val queue = new LinkedBlockingQueue[Runnable](queueCapacity)
  for i <- 1 to parallelism do {
    val worker = new Thread(() => runWorker(), s"$threadNamePrefix-$i")
    worker.setDaemon(true)
    worker.start()
  }

  /** Queues `task`. A task that throws is ignored, since blocking ops catch their own failures.
    *
    * @param task
    *   the task to run on a pool thread
    * @throws java.util.concurrent.RejectedExecutionException
    *   once this pool is closed, or if a bounded queue is full
    */
  def execute(task: Runnable): Unit = lifecycleLock.synchronized {
    if !accepting.get() || !queue.offer(task) then
      throw new RejectedExecutionException("The blocking I/O pool is closed or its queue is full")
  }

  private def runWorker(): Unit = {
    var continue = true
    while continue do {
      try {
        val task = queue.poll(100, TimeUnit.MILLISECONDS)
        if task != null then
          try task.run()
          catch case NonFatal(_) => ()
        else if lifecycleLock.synchronized(!accepting.get() && queue.isEmpty) then continue = false
      } catch {
        case _: InterruptedException => if !accepting.get() && queue.isEmpty then continue = false
      }
    }
  }

  /** Stops accepting tasks. Queued tasks still run, then the threads exit. It does not wait for them. */
  def close(): Unit = lifecycleLock.synchronized { accepting.set(false) }
}

object BlockingPool {
  val defaultParallelism: Int =
    sys.props.get("gears.io.blocking.parallelism").fold(4)(_.toInt)
  val defaultQueueCapacity: Int =
    sys.props.get("gears.io.blocking.queueCapacity").fold(Int.MaxValue)(_.toInt)

  /** The pool every reactor uses unless given another. It is never closed. */
  lazy val global: BlockingPool = new BlockingPool()
}
