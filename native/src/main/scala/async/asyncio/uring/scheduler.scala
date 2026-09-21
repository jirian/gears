package gears.async.asyncio.uring

import gears.async._
import uring._
import uringOps._
import gears.async.native

import java.util.concurrent.ForkJoinPool
import scala.concurrent.ExecutionContext
import scala.concurrent.JavaConversions._
import scala.concurrent.duration._
import scala.scalanative.runtime.ByteArray
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
    // GC-managed, not malloc'd: same reasoning as net.scala's direct buffer
    // access - this GC never relocates live objects (checked directly
    // against its actual source: no forwarding pointers/compaction
    // anywhere in it), and a reference kept alive only by an in-flight
    // completion closure's own capture survives real, concurrent GC
    // pressure - verified empirically, specifically for "a closure with no
    // OTHER reference, reachable only via the kernel-stored raw pointer in
    // user_data," under both debug and release-fast builds
    // (ClosureLivenessStress). `tsArr` is captured by the completion
    // closure below so its lifetime is tied to the op's, explicitly rather
    // than relying on that alone.
    val tsArr = new Array[Byte](sizeof[__kernel_timespec].toInt)
    val ts = tsArr.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[__kernel_timespec]]
    ts.tv_sec = delay.toSeconds
    ts.tv_nsec = (delay - delay.toSeconds.seconds).toNanos

    val userData = ring.submit { sqe =>
      io_uring_prep_timeout(sqe, ts, 0.toULong, 0.toUInt)
    } { res =>
      val _ = tsArr // keep tsArr (and the memory `ts` points into) alive until here
      // -ETIME is the expected "deadline reached" completion; a cancelled
      // timeout completes with -ECANCELED and must not run the body.
      //
      // Deliberately NOT errno.ETIME: confirmed empirically (by adding a
      // diagnostic print and comparing against a small C program using the
      // same glibc) that this specific scala-native posixlib binding
      // resolves to 0 instead of 62 in this environment, silently making
      // every scheduled timeout/sleep a no-op forever. errno.ECANCELED,
      // errno.EAGAIN etc. all checked out fine - this one specifically is
      // broken. Worth reporting upstream to scala-native; until then, use
      // the real glibc value directly.
      if res == -62 /* -ETIME */ then exec.execute(body)
    }

    () =>
      // Same best-effort reasoning as net.scala's submitAwait: don't let a
      // full submission queue throw out of Cancellable.cancel().
      try
        ring.submit { sqe =>
          io_uring_prep_timeout_remove(sqe, userData, 0.toUInt)
        } { _ => () }
      catch case _: IOException => ()

  def tcpSupport = new UringTcpSupport(ring) {}
  def udpSupport = new UringUdpSupport(ring) {}
