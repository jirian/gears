package gears.async.asyncio.kqueue

import gears.async._
import gears.async.asyncio.epoll.{ExecutorWithPollThread, IOException, Poller}
import gears.async.asyncio.kqueue.unsafe.kqueue.kqueue
import gears.async.native

import java.util.concurrent.ForkJoinPool
import scala.concurrent.ExecutionContext
import scala.concurrent.JavaConversions._
import scala.concurrent.duration._
import scala.scalanative.posix.errno

class ForkJoinKqueueSupport extends KqueueExecutorWithSleep(new ForkJoinPool())

class KqueueExecutorWithSleep(exec: ExecutionContext)
    extends KqueueScheduler(exec)
    with AsyncSupport
    with AsyncOperations
    with native.NativeSuspend {
  type Scheduler = this.type
}

/** UNVERIFIED, see [[KqueuePoller]]. Unlike the uring backend, this needs
  * the same hand-rolled sleeper priority-queue as epoll -
  * [[ExecutorWithPollThread]] is reused as-is from the epoll package rather
  * than re-derived, since it's already generic over any [[Poller]] (kqueue's
  * `kevent()` timeout parameter plays exactly the same role as
  * `epoll_wait`'s).
  */
class KqueueScheduler(val exec: ExecutionContext) extends Scheduler:
  override def execute(body: Runnable): Unit = executor.execute(body)
  override def schedule(delay: FiniteDuration, body: Runnable): Cancellable =
    executor.schedule(delay, body)

  private val kq =
    val fd = kqueue()
    if fd < 0 then throw IOException(errno.errno)
    else fd
  private val poller = KqueuePoller(kq)
  private val executor = ExecutorWithPollThread(exec, poller)

  def tcpSupport = new KqueueTcpSupport(poller) {}
