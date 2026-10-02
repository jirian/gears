package asyncio

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.control.NonFatal

/** A bounded pool for operations which must run away from a reactor thread. */
final class BlockingPool(
    parallelism: Int = BlockingPool.defaultParallelism,
    queueCapacity: Int = BlockingPool.defaultQueueCapacity,
    threadNamePrefix: String = "asyncio-blocking"
) extends AutoCloseable {
  require(parallelism > 0, "Blocking pool parallelism must be positive")
  require(queueCapacity > 0, "Blocking pool queue capacity must be positive")

  private val accepting = new AtomicBoolean(true)
  private val lifecycleLock = new Object
  private val queue = new ArrayBlockingQueue[Runnable](queueCapacity)
  private val workers = Array.tabulate(parallelism) { i =>
    val worker = new Thread(() => runWorker(), s"$threadNamePrefix-${i + 1}")
    worker.setDaemon(true)
    worker.start()
    worker
  }

  /** Adds a task without blocking the caller. Saturation is reported to the submitting reactor. */
  def execute(task: Runnable): Unit = lifecycleLock.synchronized {
    if !accepting.get() || !queue.offer(task) then
      throw new RejectedExecutionException("The blocking I/O pool is closed or its queue is full")
  }

  def queuedTasks: Int = queue.size()

  private def runWorker(): Unit = {
    var continue = true
    while continue do {
      try {
        val task = queue.poll(100, TimeUnit.MILLISECONDS)
        if task != null then
          try task.run()
          catch case NonFatal(_) => () // A failed task must not retire a worker.
        else if lifecycleLock.synchronized(!accepting.get() && queue.isEmpty) then continue = false
      } catch {
        case _: InterruptedException => if !accepting.get() && queue.isEmpty then continue = false
      }
    }
  }

  /** Stops accepting tasks and lets queued work drain. Active tasks are not interrupted. */
  def close(): Unit = lifecycleLock.synchronized { accepting.set(false) }
}

object BlockingPool {
  val defaultParallelism: Int =
    sys.props.get("asyncio.blocking.parallelism").fold(4)(_.toInt)
  val defaultQueueCapacity: Int =
    sys.props.get("asyncio.blocking.queueCapacity").fold(256)(_.toInt)

  /** Shared default for reactors which are not given their own pool. */
  lazy val global: BlockingPool = new BlockingPool()
}
