package gears.async.asyncio.uring

import gears.async._
import uring._
import uringOps._
import gears.async.native

import java.util.concurrent.ForkJoinPool
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.scalanative.libc.stdlib
import scala.scalanative.unsafe._
import scala.scalanative.unsigned._

class ForkJoinUringSupport extends UringExecutorWithSleep(new ForkJoinPool())

class UringExecutorWithSleep(exec: ExecutionContext)
    extends UringScheduler(exec)
    with AsyncSupport
    with AsyncOperations
    with native.NativeSuspend:
  type Scheduler = this.type

/** Unlike [[gears.async.asyncio.epoll.EpollScheduler]], this needs no
  * separate sleeper priority-queue or "wake the poll thread early" trick:
  * `IORING_OP_TIMEOUT` makes a timer just another op flowing through the
  * same completion queue as every read/write/accept/connect, so
  * [[UringRing]]'s single completion thread already multiplexes timers and
  * I/O for free.
  */
class UringScheduler(val exec: ExecutionContext) extends Scheduler:
  val ring = UringRing()

  override def execute(body: Runnable): Unit = exec.execute(body)

  override def schedule(delay: FiniteDuration, body: Runnable): Cancellable =
    val ts = stdlib.malloc(sizeof[__kernel_timespec]).asInstanceOf[Ptr[__kernel_timespec]]
    ts.tv_sec = delay.toSeconds
    ts.tv_nsec = (delay - delay.toSeconds.seconds).toNanos

    val userData = ring.submit { sqe =>
      io_uring_prep_timeout(sqe, ts, 0.toULong, 0.toUInt)
    } { res =>
      stdlib.free(ts.asInstanceOf[Ptr[Byte]])
      // -ETIME is the expected "deadline reached" completion; a cancelled
      // timeout completes with -ECANCELED and must not run the body.
      if res == -62 /* -ETIME */ then exec.execute(body)
    }

    () =>
      ring.submit { sqe =>
        io_uring_prep_timeout_remove(sqe, userData, 0.toUInt)
      } { _ => () }

  def tcpSupport = new UringTcpSupport(ring) {}
