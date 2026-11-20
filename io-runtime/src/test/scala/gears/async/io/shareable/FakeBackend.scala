package gears.async.io
package shareable

import gears.async.Async
import gears.async.AsyncSupport
import gears.async.Future
import gears.async.native.NativeSuspend

import java.nio.ByteBuffer
import java.util.concurrent.CancellationException
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.FiniteDuration
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object FakeBackend extends Backend {
  type Interface = ops.type

  object ops extends ByteStreams {
    type Op = FakeOp
    type Handle = AnyRef
    type Readable = AnyRef
    type Writable = AnyRef
    def read(handle: AnyRef, buf: ByteBuffer): FakeOp & Repeatable = ???
    def write(handle: AnyRef, buf: ByteBuffer): FakeOp & Repeatable = ???
    def read(handle: AnyRef, bufs: Array[ByteBuffer]): FakeOp & Repeatable = ???
    def write(handle: AnyRef, bufs: Array[ByteBuffer]): FakeOp & Repeatable = ???
    def timer(duration: FiniteDuration): FakeOp & Repeatable = {
      if duration.length < 0 then throw new IllegalArgumentException(s"A timer's time is not negative: $duration")
      new FakeTimer(duration)
    }
    def deadlineTimer(nanoTime: Long): FakeOp & Repeatable & Deadline = new FakeDeadline(nanoTime)
    def read(handle: AnyRef, pool: BufferPool, into: Slot[Received]): FakeOp & Repeatable = ???
    def close(handle: AnyRef): FakeOp = ???
  }

  def pool(bufferSize: Int, buffers: Int): BufferPool = ???

  private[io] def run[T](program: Io[Interface] ?=> Async.Spawn ?=> T): T = onPool(4)(program)

  def onPool[T](workers: Int)(program: Io[Interface] ?=> Async.Spawn ?=> T): T = {
    val pool = FakeScheduler(workers)
    given poolSupport: (AsyncSupport { type Scheduler = pool.Scheduler }) = pool
    given poolScheduler: pool.Scheduler = pool
    try Async.blocking(Future(program(using pool)).await)
    finally pool.shutdown()
  }
}

sealed trait FakeOp extends TimerHeapOp

final class FakeTimer(duration: FiniteDuration) extends AbstractMakeTimerOp(duration) with FakeOp

final class FakeDeadline(initial: Long) extends AbstractMakeDeadlineOp(initial) with FakeOp

enum Withdrawal {
  case Sync, Deferred
}

sealed abstract class DeviceOp(val withdrawal: Withdrawal) extends RepeatableAbstractTimerHeapOp with FakeOp {

  def deviceWorks: Boolean

  val worked = new AtomicInteger()

  @volatile private[shareable] var device: FakeDevice | Null = null

  @volatile private[shareable] var outcome: Try[Unit] = WorkpoolScheduler.SuccessUnit

  @volatile private[shareable] var done = false

  @volatile private[shareable] var queued = false

  override def onEntry(): Unit = {
    super.onEntry()
    offHeap = false
    done = false
  }

  override protected def committed: Boolean = done

  override protected def limitReached(): Boolean = {
    val d = device.nn
    if withdrawal == Withdrawal.Sync && d.cancelNow(this) then true
    else if !queued && !d.holds(this) then true
    else {
      if withdrawal == Withdrawal.Deferred then d.requestCancel(this)
      offHeap = true
      false
    }
  }

  def wakeup(resumedAt: Long): Try[Unit] = {
    if !deviceWorks && outcome.isSuccess then worked.incrementAndGet(): Unit
    outcome
  }
}

final class CompletionOp(withdrawal: Withdrawal = Withdrawal.Sync) extends DeviceOp(withdrawal) {
  def deviceWorks = true
}

final class ReadinessOp extends DeviceOp(Withdrawal.Sync) {
  def deviceWorks = false
}

final class FakeDevice(worker: FakeScheduler#FakeWorker) {
  private val lock = new Object
  private val held = scala.collection.mutable.Set.empty[DeviceOp]
  private val cancelling = scala.collection.mutable.Set.empty[DeviceOp]
  private val finished = new ConcurrentLinkedQueue[DeviceOp]()

  def submit(op: DeviceOp): Unit = lock.synchronized(held += op): Unit

  def holds(op: DeviceOp): Boolean = lock.synchronized(held(op))

  def isCancelling(op: DeviceOp): Boolean = lock.synchronized(cancelling(op))

  def finish(op: DeviceOp, outcome: Try[Unit] = WorkpoolScheduler.SuccessUnit): Boolean = {
    val had = lock.synchronized {
      cancelling -= op
      val had = held.remove(op)
      if had then op.queued = true
      had
    }
    if had then {
      op.outcome = outcome
      if op.deviceWorks && outcome.isSuccess then {
        op.worked.incrementAndGet(): Unit
        op.done = true
      }
      finished.add(op)
      worker.poke()
    }
    had
  }

  def cancelNow(op: DeviceOp): Boolean = lock.synchronized {
    cancelling -= op
    held.remove(op)
  }

  def requestCancel(op: DeviceOp): Unit = lock.synchronized(if held(op) then cancelling += op): Unit

  def confirmCancels(): Unit = {
    val ops = lock.synchronized(cancelling.toList)
    ops.foreach(finish(_, Failure(new CancellationException())))
  }

  private[shareable] def reap(): DeviceOp | Null = finished.poll()
}

final class FakeScheduler(workers: Int)
    extends WorkpoolScheduler[FakeOp, FakeBackend.ops.type](FakeBackend, FakeBackend.ops, workers)
    with NativeSuspend {
  override protected def selectWorker() = workerOfCurrentThread()

  override def makeWorker(id: Int): FakeWorker = new FakeWorker(id)

  def device: FakeDevice = workerOfCurrentThread().asInstanceOf[FakeWorker].device

  class FakeWorker(id: Int) extends WorkpoolScheduler.Worker[FakeOp, FakeBackend.ops.type](id, FakeBackend.ops) {
    lazy val device = new FakeDevice(this)

    private[shareable] def poke(): Unit = wake()

    override def tryPerform(op: FakeOp): Boolean = op match {
      case timer: (FakeTimer | FakeDeadline) =>
        addTimer(timer)
        false
      case io: DeviceOp =>
        io.device = device
        if io.hasDeadline then addTimer(io)
        device.submit(io)
        false
    }

    override def withdraw(op: FakeOp): Boolean = op match {
      case io: DeviceOp =>
        if io.hasDeadline && !takeOff(io) then return false
        val device = io.device.nn
        io.withdrawal match {
          case Withdrawal.Sync     => device.cancelNow(io)
          case Withdrawal.Deferred =>
            device.requestCancel(io)
            false
        }
      case _ => super.withdraw(op)
    }

    override def waitForDeadline(deadlineTask: Long): Unit = {
      deliver()
      super.waitForDeadline(deadlineTask)
      deliver()
    }

    private def deliver(): Unit = {
      var op = device.reap()
      while op != null do {
        val io = op.nn
        io.queued = false
        if !io.hasDeadline || claimFiring(io) then io.timerWakeup(System.nanoTime())
        op = device.reap()
      }
    }
  }
}
