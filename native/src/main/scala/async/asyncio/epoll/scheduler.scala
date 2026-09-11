package gears.async.asyncio.epoll

import gears.async._
import gears.async.asyncio.epoll.unsafe.epoll.epoll_create1
import gears.async.native

import java.util.concurrent.ForkJoinPool
import scala.concurrent.ExecutionContext
import scala.concurrent.JavaConversions._
import scala.concurrent.duration._
import scala.scalanative.posix.errno

class ForkJoinEpollSupport extends EpollExecutorWithSleep(new ForkJoinPool())

class EpollExecutorWithSleep(exec: ExecutionContext)
    extends EpollScheduler(exec)
    with AsyncSupport
    with AsyncOperations
    with native.NativeSuspend {
  type Scheduler = this.type

  override def sleep(millis: Long)(using Async): Unit =
    Future
      .withResolver[Unit]: resolver =>
        val cancellable = schedule(millis.millis, () => resolver.resolve(()))
        resolver.onCancel: () =>
          cancellable.cancel()
          resolver.rejectAsCancelled()
      .link()
      .await
}

class EpollScheduler(val exec: ExecutionContext) extends Scheduler:
  override def execute(body: Runnable): Unit = executor.execute(body)
  override def schedule(delay: FiniteDuration, body: Runnable): Cancellable =
    executor.schedule(delay, body)

  private val epfd =
    val fd = epoll_create1(0)
    if fd < 0 then throw IOException(errno.errno)
    else fd
  private val poller = EpollPoller(epfd)
  private val executor = ExecutorWithPollThread(exec, poller)

  def tcpSupport = new EpollTcpSupport(poller) {}

trait Poller:
  def poll(timeout: Duration): Unit
  def wake(): Unit

/** Spawns a single thread that does all the polling. */
class ExecutorWithPollThread(val exec: ExecutionContext, val poller: Poller)
    extends ExecutionContext
    with Scheduler {
  import scala.collection.mutable
  private case class Sleeper(wakeTime: Deadline, toRun: Runnable):
    var isCancelled = false
  private given Ordering[Sleeper] =
    Ordering.by((sleeper: Sleeper) => sleeper.wakeTime).reverse
  private val sleepers = mutable.PriorityQueue.empty[Sleeper]
  private var sleepingUntil: Option[Deadline] = None

  override def execute(body: Runnable): Unit = exec.execute(body)
  override def reportFailure(cause: Throwable): Unit = exec.reportFailure(cause)
  override def schedule(delay: FiniteDuration, body: Runnable): Cancellable = {
    val sleeper = Sleeper(delay.fromNow, body)
    // push to the sleeping priority queue
    this.synchronized {
      sleepers += sleeper
      val shouldWake = sleepingUntil.map(sleeper.wakeTime < _).getOrElse(true)
      if shouldWake then poller.wake()
    }
    () => { sleeper.isCancelled = true }
  }

  // Sleep until the first timer is due, or .wait() otherwise
  private def sleepLoop(): Unit = this.synchronized {
    while (true) {
      sleepingUntil = sleepers.headOption.map(_.wakeTime)
      val current = sleepingUntil match
        case None =>
          poller.poll(-1.seconds)
          Deadline.now
        case Some(value) =>
          val current0 = Deadline.now
          val timeLeft = value - current0

          if timeLeft.toNanos > 0 then
            poller.poll(timeLeft)
            Deadline.now
          else current0

      // Pop sleepers until no more available
      while (sleepers.headOption.exists(_.wakeTime <= current)) {
        val task = sleepers.dequeue()
        if !task.isCancelled then execute(task.toRun)
      }
    }
  }

  val sleeperThread = Thread(() => sleepLoop())
  sleeperThread.setDaemon(true)
  sleeperThread.start()
}
