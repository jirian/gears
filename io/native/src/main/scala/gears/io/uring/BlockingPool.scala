package gears.io.uring

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.control.NonFatal

final class BlockingPool(
    parallelism: Int = BlockingPool.defaultParallelism,
    queueCapacity: Int = BlockingPool.defaultQueueCapacity,
    threadNamePrefix: String = "asyncio-blocking"
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

  def close(): Unit = lifecycleLock.synchronized { accepting.set(false) }
}

object BlockingPool {
  val defaultParallelism: Int =
    sys.props.get("asyncio.blocking.parallelism").fold(4)(_.toInt)
  val defaultQueueCapacity: Int =
    sys.props.get("asyncio.blocking.queueCapacity").fold(Int.MaxValue)(_.toInt)

  lazy val global: BlockingPool = new BlockingPool()
}
