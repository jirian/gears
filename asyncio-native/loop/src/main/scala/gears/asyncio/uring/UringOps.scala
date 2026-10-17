package gears.asyncio.uring

import gears.asyncio.Address
import gears.asyncio.HandleSlot
import gears.asyncio.Interest
import gears.asyncio.Ops
import gears.asyncio.ResolvedAddress
import gears.asyncio.Slot

import java.io.IOException
import java.nio.ByteBuffer
import scala.concurrent.duration.FiniteDuration
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import uring.*
import uringStructOps.*

sealed abstract class UringOp[Owner] {

  @volatile private[uring] var submitted = false
}

abstract class RingOp[Owner] extends UringOp[Owner] {

  private[uring] var inFlight: AnyRef | Null = null

  private[uring] def prepare(sqe: Ptr[io_uring_sqe]): Unit

  private[uring] def finish(res: Int): Boolean

  private[uring] def discard(res: Int): Unit = ()

  private[uring] def keepProgress(res: Int): Unit = ()

  private[uring] def prepareCancel(sqe: Ptr[io_uring_sqe], userData: Long): Unit =
    io_uring_prep_cancel64(sqe, userData.toULong, 0)

  // On EAGAIN, wait for readiness instead of resubmitting, which would spin. A readiness wait's result is a poll mask,
  // never a descriptor to release.
  protected def readinessFd: Int = -1
  protected def readinessEvents: Int = 0

  private var awaitingReadiness = false

  private[uring] final def prepareSubmission(sqe: Ptr[io_uring_sqe]): Unit =
    if awaitingReadiness then io_uring_prep_poll_add(sqe, readinessFd, readinessEvents.toUInt)
    else prepare(sqe)

  private[uring] final def finishSubmission(res: Int): Boolean =
    if awaitingReadiness then {
      awaitingReadiness = false
      if res < 0 && !Ring.transient(res) then throw Ring.failure(s"Failed to wait for descriptor $readinessFd", res)
      false
    } else if res == -scala.scalanative.posix.errno.EAGAIN && readinessEvents != 0 then {
      awaitingReadiness = true
      false
    } else finish(res)

  private[uring] final def discardSubmission(res: Int): Unit =
    if awaitingReadiness then awaitingReadiness = false
    else {
      if res > 0 then keepProgress(res)
      discard(res)
    }
}

final class UringOps[Owner](resolver: HostResolver) extends Ops[UringOp[Owner]] {
  def read(fd: Int, buf: ByteBuffer): Read[Owner] = new Read(fd, buf)
  def write(fd: Int, buf: ByteBuffer): Write[Owner] = new Write(fd, buf, plain = false)
  def accept(fd: Int, into: HandleSlot): Accept[Owner] = new Accept(fd, into)
  def connect(fd: Int): UringOp[Owner] = new Connect(fd)
  def receive(fd: Int, buf: ByteBuffer, from: Slot[Address]): Receive[Owner] = new Receive(fd, buf, from)
  def send(fd: Int, buf: ByteBuffer, to: Address | Null): Send[Owner] = new Send(fd, buf, to)
  def timer(milliseconds: Int): UringTimer[Owner] = new UringTimer(milliseconds * 1_000_000L)
  def whenReady(fd: Int, interest: Interest)(perform: () => Boolean): WhenReady[Owner] =
    new WhenReady(fd, interest, perform)
  def blocking(body: () => Unit): UringBlocking[Owner] = new BlockingTask(body)
  def blocking[A <: AnyRef](body: () => A, into: Slot[A]): UringBlocking[Owner] = new BlockingResultTask(body, into)
  def resolve(host: String, into: Slot[List[ResolvedAddress]]): Resolve[Owner] = new Resolve(host, into, resolver)

  def readv(fd: Int, bufs: Array[ByteBuffer]): ReadVector[Owner] = new ReadVector(fd, bufs)

  def writev(fd: Int, bufs: Array[ByteBuffer]): WriteVector[Owner] = new WriteVector(fd, bufs)

  def writeFile(fd: Int, buf: ByteBuffer): Write[Owner] = new Write(fd, buf, plain = true)
  def readAt(fd: Int, buf: ByteBuffer, offset: Long): Read[Owner] =
    require(offset >= 0, "offset must be nonnegative")
    new Read(fd, buf, offset)
  def writeAt(fd: Int, buf: ByteBuffer, offset: Long): Write[Owner] =
    require(offset >= 0, "offset must be nonnegative")
    new Write(fd, buf, plain = true, offset = offset)

  def timer(duration: FiniteDuration): UringTimer[Owner] = new UringTimer(duration.toNanos)

  def openAt(path: String, flags: Int, mode: Int, into: HandleSlot): OpenAt[Owner] =
    new OpenAt(path, flags, mode, into)
}

sealed abstract class UringPromise[Owner] extends UringOp[Owner] {
  private val outcome = new java.util.concurrent.atomic.AtomicReference[Any](UringPromise.Unsettled)

  @volatile private[uring] var reactor: UringReactor | Null = null

  @volatile private[uring] var pending: AnyRef | Null = null

  private[uring] def failure: Throwable | Null = outcome.get() match {
    case UringPromise.Failed(failure) => failure
    case _                            => null
  }

  private[uring] def fail(failure: Throwable): Unit = settle(UringPromise.Failed(failure))

  private[uring] def signal(): Unit = settle(())

  private def settle(value: Any): Unit = {
    if !outcome.compareAndSet(UringPromise.Unsettled, value) then
      throw new IllegalStateException("An op can only be settled once")
    val owner = reactor
    if owner != null then owner.resolved(this)
  }
}

object UringPromise {
  private[uring] object Unsettled
  private[uring] final case class Failed(failure: Throwable)
}

abstract class UringBlocking[Owner] extends UringPromise[Owner] {
  def block(): Unit

  private var runner: Thread | Null = null
  private var cancelled = false
  private var ended = false

  private[uring] var cancelling: AnyRef | Null = null

  private[uring] def begin(): Boolean = synchronized {
    if cancelled then false
    else {
      runner = Thread.currentThread()
      true
    }
  }

  private[uring] def end(): Unit = {
    synchronized {
      runner = null
      ended = true
      notifyAll()
    }
    Thread.interrupted()
  }

  private[uring] def interrupt(): Boolean = synchronized {
    cancelled = true
    val running = runner
    if running != null then running.interrupt()
    running != null
  }

  protected final def publish[A <: AnyRef](into: Slot[A], value: A): Unit = synchronized {
    if !cancelled then into.set(value)
  }

  private[uring] def awaitEnd(): Unit = {
    var interrupted = false
    synchronized {
      while !ended do
        try wait()
        catch case _: InterruptedException => interrupted = true
    }
    if interrupted then Thread.currentThread().interrupt()
  }
}

final class BlockingTask[Owner](body: () => Unit) extends UringBlocking[Owner] {
  def block(): Unit = body()
}

final class BlockingResultTask[Owner, A <: AnyRef](body: () => A, into: Slot[A]) extends UringBlocking[Owner] {
  def block(): Unit = publish(into, body())
}

final class Resolve[Owner](val host: String, into: Slot[List[ResolvedAddress]], resolver: HostResolver)
    extends UringBlocking[Owner] {
  def block(): Unit = resolver.resolve(host) match {
    case Right(addresses) => publish(into, addresses)
    case Left(error)      => throw new IOException(s"Failed to resolve $host: $error")
  }
}

object Blockers {

  def run(blocking: UringBlocking[?], pool: BlockingPool): Unit =
    pool.execute { () =>
      if blocking.begin() then {
        var failure: Throwable | Null = null
        try blocking.block()
        catch case t: Throwable => failure = t
        finally blocking.end()
        if failure != null then blocking.fail(failure.nn) else blocking.signal()
      }
    }
}
