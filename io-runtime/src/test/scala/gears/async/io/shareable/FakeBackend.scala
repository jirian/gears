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

/** A backend on the shared runtime whose poller is a scripted device, one for each worker, standing in for a kernel's
  * I/O facility: a test decides when the device finishes an op, and how it lets go of one that is cancelled. It has the
  * two kinds of op a real poller has:
  *
  *   - a `CompletionOp`, as on io_uring or IOCP: the device does the op's work, and its completion carries the outcome;
  *   - a `ReadinessOp`, as on kqueue or epoll: the device says the op may go on, and the op does its work as it fires.
  *
  * and the two ways of being cancelled: at once (`Withdrawal.Sync`, io_uring's sync cancel, kqueue's `EV_DELETE`), or
  * by a request the device confirms later (`Withdrawal.Deferred`, io_uring's async cancel, IOCP's `CancelIoEx`).
  */
object FakeBackend extends Backend {
  type Op = FakeOp
  type Handle = AnyRef
  type Readable = AnyRef
  type Writable = AnyRef

  object ops extends Ops[FakeOp, AnyRef] {
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

  private[io] def run[T](program: Io[this.type] ?=> Async.Spawn ?=> T): T = onPool(4)(program)

  /** Runs `program` on a pool of `workers` workers, shutting it down afterwards. */
  def onPool[T](workers: Int)(program: Io[this.type] ?=> Async.Spawn ?=> T): T = {
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

/** An op the device holds once performed, until it finishes it or lets go of it. */
sealed abstract class DeviceOp(val withdrawal: Withdrawal) extends RepeatableAbstractTimerHeapOp with FakeOp {

  /** Whether the device does the op's work, as io_uring does, rather than the op as it fires. */
  def deviceWorks: Boolean

  /** How many times the op's work has been done: by the device, or as it fired. */
  val worked = new AtomicInteger()

  /** The device that holds it in this performance. */
  @volatile private[shareable] var device: FakeDevice | Null = null

  /** What the device finished it with. */
  @volatile private[shareable] var outcome: Try[Unit] = WorkpoolScheduler.SuccessUnit

  /** Set when a withdrawal took it off its worker's heap: its completion still fires it. */
  @volatile private[shareable] var offHeap = false

  /** Set once the device has done the op's work, which can't be undone (a `CompletionOp`'s). */
  @volatile private[shareable] var done = false

  /** Set while the device's completion of it waits for the worker to deliver it. Written under the device's lock. */
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
    else if !queued && !d.holds(this) then true // let go, and this is the delivery of its completion
    else {
      if withdrawal == Withdrawal.Deferred then d.requestCancel(this)
      offHeap = true // its completion, on its way or to come, fires it again
      false
    }
  }

  def wakeup(resumedAt: Long): Try[Unit] = {
    if !deviceWorks && outcome.isSuccess then worked.incrementAndGet(): Unit
    outcome
  }
}

/** As on io_uring: the device does the work, and the completion reports it. */
final class CompletionOp(withdrawal: Withdrawal = Withdrawal.Sync) extends DeviceOp(withdrawal) {
  def deviceWorks = true
}

/** As on kqueue: the device says the op may go on, and the op does its work as it fires. */
final class ReadinessOp extends DeviceOp(Withdrawal.Sync) {
  def deviceWorks = false
}

/** A worker's scripted I/O facility. Thread-safe: a test drives it from any thread. */
final class FakeDevice(worker: FakeScheduler#FakeWorker) {
  private val lock = new Object
  private val held = scala.collection.mutable.Set.empty[DeviceOp]
  private val cancelling = scala.collection.mutable.Set.empty[DeviceOp]
  private val finished = new ConcurrentLinkedQueue[DeviceOp]()

  def submit(op: DeviceOp): Unit = lock.synchronized(held += op): Unit

  /** Whether the device still has `op`: it may yet do its work, or write into its buffers. */
  def holds(op: DeviceOp): Boolean = lock.synchronized(held(op))

  def isCancelling(op: DeviceOp): Boolean = lock.synchronized(cancelling(op))

  /** Finishes `op` with `outcome`, its work done if it is the device's and `outcome` a success, and queues its
    * completion for the worker. False if the device no longer had it.
    */
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

  /** A sync cancel: true if the device had `op` and has let go of it, so that nothing will complete it. */
  def cancelNow(op: DeviceOp): Boolean = lock.synchronized {
    cancelling -= op
    held.remove(op)
  }

  /** An async cancel: the device lets go of `op` when `confirmCancels` is called, with a cancelled completion. */
  def requestCancel(op: DeviceOp): Unit = lock.synchronized(if held(op) then cancelling += op): Unit

  def confirmCancels(): Unit = {
    val ops = lock.synchronized(cancelling.toList)
    ops.foreach(finish(_, Failure(new CancellationException())))
  }

  private[shareable] def reap(): DeviceOp | Null = finished.poll()
}

/** The shared runtime over the fake device: each worker's poller waits on its monitor, as Jamie's does, and delivers
  * the completions its device has queued before and after each wait.
  */
final class FakeScheduler(workers: Int)
    extends WorkpoolScheduler[FakeOp, FakeBackend.type](FakeBackend, workers)
    with NativeSuspend {
  override protected def selectWorker() = workerOfCurrentThread()

  override def makeWorker(id: Int): FakeWorker = new FakeWorker(id)

  /** The device of the calling thread's worker. */
  def device: FakeDevice = workerOfCurrentThread().asInstanceOf[FakeWorker].device

  class FakeWorker(id: Int) extends WorkpoolScheduler.Worker[FakeOp, FakeBackend.type](id, FakeBackend) {
    // Lazy: `Worker` starts its thread in its constructor, which runs `waitForDeadline` before this class's fields
    // are set.
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

    /** As `Worker.withdraw`, and takes the op back from the device, which may confirm it only later. */
    override def withdraw(op: FakeOp): Boolean = op match {
      case io: DeviceOp =>
        if io.hasDeadline then {
          if !removeTimer(io) then return false // it left the heap to fire
          io.offHeap = true
        }
        val device = io.device.nn
        io.withdrawal match {
          case Withdrawal.Sync     => device.cancelNow(io) // if not, its completion is on its way
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

    /** Fires each op the device has finished, unless its time limit already fired it. */
    private def deliver(): Unit = {
      var op = device.reap()
      while op != null do {
        val io = op.nn
        io.queued = false
        if !io.hasDeadline || io.offHeap || removeTimer(io) then io.timerWakeup(System.nanoTime())
        op = device.reap()
      }
    }
  }
}
