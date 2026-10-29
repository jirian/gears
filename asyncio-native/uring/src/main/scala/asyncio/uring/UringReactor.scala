package asyncio.uring

import java.io.IOException
import java.util.concurrent.TimeoutException
import scala.scalanative.linux.eventfd
import scala.scalanative.posix.errno
import scala.scalanative.posix.unistd
import scala.scalanative.runtime.ByteArray
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import asyncio.Completion
import asyncio.Reactor
import asyncio.Repeatable
import asyncio.unsafe.DescriptorTable
import uring.*
import uringStructOps.*

/** The io_uring implementation of `Reactor`, for ops built by `UringOps`, on one ring (Linux 5.11 or later). A command
  * becomes a submission queue entry when it is submitted, and the entries a turn prepares go to the kernel together, at
  * the start of the next turn or the end of this one, or at once for `trySubmit`, which first tries the command with a
  * non-blocking system call. The kernel owns a command's buffers until its completion queue entry arrives: a cancelled
  * command asks the kernel to cancel it, and its `onCancel` runs once that entry is in, unless the entry was already in
  * when the cancel came, in which case the command is settled at once, finished or cancelled. Descriptors must be
  * closed through `UringHandles.close`, from any thread, which tells every reactor (`closed`): the commands pending on
  * the descriptor are cancelled, and fail. Timers, and the ops with a time limit, are kept in a heap on the loop
  * thread, by deadline, and bound how long a turn waits, so setting or cancelling one costs no system call, and an op
  * that times out is withdrawn as a cancelled one is, with `onFailure` instead of `onCancel`. `wakeup`, and blocking
  * tasks finishing on other threads, wake the loop through an eventfd the ring always has a read pending on.
  */
final class UringReactor private (entries: Int) extends Reactor {

  /** Every uring reactor runs the same kinds of op, so uring-specific code may build them directly, and an op may go to
    * any uring reactor in turn (see `UringBackend`).
    */
  type Op = UringOp

  def ops: UringOps.type = UringOps

  /** io_uring runs operations on POSIX file descriptors, so a handle is the descriptor itself. */
  def handles: UringHandles.type = UringHandles

  /** Takes a pending op's completion off it, ending the submission, and its time limit with it: the completion may then
    * submit the op again.
    */
  private def take(op: UringOp): Completion[UringOp] = {
    val completion = op.completion.nn
    op.completion = null
    if op.heapIndex >= 0 then timers.remove(op)
    completion
  }

  private def complete(op: UringOp): Unit = take(op).onComplete(op)
  private def fail(op: UringOp, failure: Throwable): Unit = take(op).onFailure(op, failure)

  def attachment(op: Op): AnyRef | Null = op.attachment
  def attach(op: Op, value: AnyRef | Null): Unit = op.attachment = value

  private val ringStorage = new Array[Byte](sizeof[io_uring].toInt)
  private def ring: Ptr[io_uring] = ringStorage.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[io_uring]]
  locally {
    val res = io_uring_queue_init(entries.toUInt, ring, 0.toUInt)
    if res < 0 then throw Ring.failure("Failed to set up io_uring", res)
  }

  // The user data of the eventfd's read, and of the entries whose completions are ignored: cancels, and the no-ops
  // that fill an entry taken for a command that then failed to prepare. Commands get the numbers after them.
  private inline val WakeId = 0L
  private inline val InternalId = 1L
  private var nextId = 2L

  // The commands in the kernel, by the user data of their submission. Loop thread only.
  private val inFlight = scala.collection.mutable.LongMap.empty[Command]

  // Withdrawn commands whose cancel did not fit into the submission queue, retried each turn. Loop thread only.
  private val cancelBacklog = scala.collection.mutable.ArrayBuffer.empty[Command]

  // Whether entries have been prepared that the kernel has not taken yet. Loop thread only.
  private var unsubmitted = false

  private var wakeInFlight = false

  // Completion queue entries are copied out a batch at a time and dispatched one by one, so a completion that cancels
  // a command of the same batch still finds that command's entry here. Loop thread only.
  private inline val MaxBatch = 64
  private val batchStorage = new Array[Byte]((sizeof[Ptr[io_uring_cqe]] * MaxBatch.toUInt).toInt)
  private def batch: Ptr[Ptr[io_uring_cqe]] =
    batchStorage.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[Ptr[io_uring_cqe]]]
  private val cqeSlotStorage = new Array[Byte](sizeof[Ptr[io_uring_cqe]].toInt)
  private def cqeSlot: Ptr[Ptr[io_uring_cqe]] =
    cqeSlotStorage.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[Ptr[io_uring_cqe]]]
  // Room to look at every entry of the completion queue, twice as long as the submission queue, for `arrivedResult`.
  private val scanSize = 2 * entries
  private val scanStorage = new Array[Byte]((sizeof[Ptr[io_uring_cqe]] * scanSize.toUInt).toInt)
  private def scan: Ptr[Ptr[io_uring_cqe]] =
    scanStorage.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[Ptr[io_uring_cqe]]]
  private val reapedIds = new Array[Long](MaxBatch)
  private val reapedResults = new Array[Int](MaxBatch)
  private var reapedCount = 0
  private var reapedCursor = 0

  private val waitStorage = new Array[Byte](sizeof[__kernel_timespec].toInt)
  private def waitTimeout(nanoseconds: Long): Ptr[__kernel_timespec] = {
    val ts = waitStorage.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[__kernel_timespec]]
    ts.tv_sec = nanoseconds / 1_000_000_000L
    ts.tv_nsec = nanoseconds % 1_000_000_000L
    ts
  }

  // Ops that finished, or failed, as soon as they were submitted, or whose completion queue entry a cancel settled
  // out of turn: their completions run on the next turn. Loop thread only.
  private val ready = new java.util.ArrayDeque[UringOp]()

  // Promises resolve on other threads; they queue here and the eventfd interrupts a blocked wait.
  private val resolvedQueue = new java.util.concurrent.ConcurrentLinkedQueue[UringPromise]()

  // Descriptors close on any thread (`UringHandles.close`): each close queues here, as the number with the generation
  // that ended, and wakes a blocked wait, so that the commands pending on it are cancelled (see `closed`).
  private val closedQueue = new java.util.concurrent.ConcurrentLinkedQueue[java.lang.Long]()

  private val wakeFd: Int = eventfd.eventfd(0.toUInt, eventfd.EFD_CLOEXEC)
  if wakeFd < 0 then {
    io_uring_queue_exit(ring)
    throw new IOException(s"Failed to create eventfd: ${asyncio.unsafe.PosixErr.cError()}")
  }
  private val wakeBuffer = new Array[Byte](8)

  // Set by the first `wakeup` since the loop last saw the eventfd's read complete, so later ones skip the system call.
  private val wakePending = new java.util.concurrent.atomic.AtomicBoolean()

  // Pending timers, and pending ops with a time limit, by deadline (see `fireDueTimers`).
  private val timers = new asyncio.unsafe.TimerHeap[UringOp]
  private var nextTimerOrder = 0L
  private var running = false

  // Promises submitted here whose completion has not run, so `close` can cancel them. Loop thread only.
  private val pendingPromises = scala.collection.mutable.HashSet.empty[UringPromise]

  // Cancelled blockers whose `block` was still running, so their `onCancel` waits for it to return. Loop thread only.
  private val cancellingBlockers = scala.collection.mutable.HashSet.empty[UringBlocking]

  // Guards writing to the eventfd against `close`, after which its descriptor number may be reused.
  private object wakeLock {}

  // The thread that runs the loop: the one that opens the reactor, which owns it, or the one that calls `run`. A promise
  // resolved on it needs no wake-up, because the loop delivers every resolved promise before it waits again.
  @volatile private var loopThread: Thread = Thread.currentThread()
  @volatile private var closed = false
  private var closing = false

  armWake()

  def submit[C <: Op](op: C, completion: Completion[C], timeoutMillis: Int): Unit =
    submitOrFinish(op, completion, finishNow = false, timeoutMillis): Unit

  def trySubmit[C <: Op](op: C, completion: Completion[C], timeoutMillis: Int): Boolean =
    submitOrFinish(op, completion, finishNow = true, timeoutMillis)

  /** Submits `op`. With `finishNow`, a command is first tried at once, and one that finishes takes its completion back
    * off it uncalled, throws a failure, and returns true; otherwise it is issued to the kernel before this returns. A
    * command that fails to prepare fails as one finished at once. An op left waiting, with a time limit, goes into the
    * timer heap to time out when it is due.
    */
  private def submitOrFinish[C <: Op](
      op: C,
      completion: Completion[C],
      finishNow: Boolean,
      timeoutMillis: Int
  ): Boolean = {
    if closed then throw new IllegalStateException("The reactor is closed")
    var finished = false
    var waits = false
    op match {
      case _: Repeatable => ()
      case one: UringOp  =>
        if one.submitted then throw new IllegalStateException(s"$op is not Repeatable and was already submitted")
    }
    require(op.completion == null, s"$op is already pending")
    val pending = completion.asInstanceOf[Completion[UringOp]] // only ever called with `op`, which is a C
    op match {
      case command: Command =>
        if command.cancelling != null then throw new IllegalStateException(s"$op is still being cancelled")
        command.completion = pending
        if command.fd >= 0 then command.generation = DescriptorTable.generation(DescriptorTable.get(command.fd))
        var done =
          finishNow && {
            try command.tryAtOnce()
            catch {
              case t: Throwable =>
                command.atOnceFailure = t
                true
            }
          }
        if !done then
          try {
            enqueue(command)
            waits = true
          } catch {
            case t: Throwable =>
              command.atOnceFailure = t
              done = true
          }
        if done then if finishNow then finished = true else queueReady(command)
        else if finishNow then flush() // the ring's own failure: the command is the kernel's, and stays pending
      case timer: UringTimer =>
        require(timer.heapIndex < 0, "This timer is already pending")
        // A time limit shorter than the timer's own time is when it fires, failed instead of completed.
        timer.expires = timeoutMillis >= 0 && timeoutMillis < timer.milliseconds
        timer.completion = pending
        schedule(timer, if timer.expires then timeoutMillis else timer.milliseconds)
      case promise: UringPromise =>
        promise match {
          case blocking: UringBlocking if blocking.cancelling != null =>
            throw new IllegalStateException(s"$op is still running after being cancelled")
          case _ => ()
        }
        promise.reset() // a `Repeatable` promise's previous outcome
        promise.reactor = this
        promise.completion = pending
        pendingPromises += promise
        promise match {
          case blocking: UringBlocking =>
            blocking.restart()
            asyncio.unsafe.Blockers.run(blocking)
        }
        waits = true
      case atOnce: UringAtOnce =>
        atOnce.completion = pending
        try atOnce.perform()
        catch case t: Throwable => atOnce.atOnceFailure = t
        if finishNow then finished = true else queueReady(atOnce)
    }
    op.submitted = true
    if waits && timeoutMillis >= 0 then schedule(op, timeoutMillis)
    if finished then {
      op.completion = null // finished here, so the completion is never called
      val failure = op.atOnceFailure
      if failure != null then {
        op.atOnceFailure = null
        throw failure
      }
    }
    finished
  }

  /** Queues the completion of `op`, which finished, or failed, without waiting, for the next turn. */
  private def queueReady(op: UringOp): Unit = {
    op.finishedAtOnce = true
    ready.add(op)
  }

  /** Puts `op` in the timer heap, due in `milliseconds`: a timer, to complete then, or an op with a time limit, to time
    * out then (see `fireDueTimers`).
    */
  private def schedule(op: UringOp, milliseconds: Int): Unit = {
    op.deadline = System.nanoTime() + milliseconds * 1_000_000L
    op.order = nextTimerOrder
    nextTimerOrder += 1
    timers.add(op)
  }

  /** Prepares one submission queue entry for `command`, to go to the kernel with the turn's others. If preparing it
    * throws, the entry, already taken, becomes a no-op.
    */
  private def enqueue(command: Command): Unit = {
    val sqe = nextSqe()
    try command.prepareSubmission(sqe)
    catch {
      case t: Throwable =>
        io_uring_prep_nop(sqe)
        sqe.user_data = InternalId.toULong
        unsubmitted = true
        throw t
    }
    val id = nextId
    nextId += 1
    sqe.user_data = id.toULong
    inFlight(id) = command
    command.userData = id
    unsubmitted = true
  }

  /** A free submission queue entry, handing the prepared ones to the kernel first if the queue is full. */
  private def nextSqe(): Ptr[io_uring_sqe] = {
    var sqe = io_uring_get_sqe(ring)
    if sqe == null then {
      flush()
      sqe = io_uring_get_sqe(ring)
    }
    if sqe == null then throw new IOException("The io_uring submission queue is full")
    sqe
  }

  /** Hands the prepared entries to the kernel. One the kernel is too busy to take, or an interrupted call, is left for
    * the next turn to try again.
    */
  private def flush(): Unit =
    if unsubmitted then {
      val res = io_uring_submit(ring)
      unsubmitted = res < 0 || io_uring_sq_ready(ring) != 0.toUInt
      if res < 0 && res != -errno.EBUSY && res != -errno.EAGAIN && res != -errno.EINTR then
        throw Ring.failure("Failed to submit to io_uring", res)
    }

  /** Keeps one read of the eventfd in the ring, so a write to it ends a blocked wait. */
  private def armWake(): Unit =
    if !closing && !wakeInFlight then
      try {
        val sqe = nextSqe()
        io_uring_prep_read(sqe, wakeFd, wakeBuffer.asInstanceOf[ByteArray].at(0), 8.toUInt, 0.toULong)
        sqe.user_data = WakeId.toULong
        wakeInFlight = true
        unsubmitted = true
      } catch case _: IOException => () // the queue is full: the next turn tries again

  def wakeup(): Unit =
    if wakePending.compareAndSet(false, true) then
      wakeLock.synchronized {
        if !closed then {
          val signal = stackalloc[CLongLong]()
          !signal = 1L
          unistd.write(wakeFd, signal.asInstanceOf[Ptr[Byte]], 8.toCSize): Unit
        }
      }

  /** Called from any thread when descriptor `fd`, whose generation in `DescriptorTable` was `generation`, is closed:
    * the commands pending on it here are cancelled, from the loop, and fail, as `Ops.close` says. A command submitted
    * on a later generation, after the number was reused, is left alone.
    */
  private[uring] def closed(fd: Int, generation: Int): Unit =
    if !closed then {
      closedQueue.add((generation.toLong << 32) | (fd.toLong & 0xffffffffL))
      if !(Thread.currentThread() eq loopThread) then wakeup()
    }

  /** Withdraws the commands pending on the descriptors closed since the last turn, failing them once the kernel lets go
    * of them. Returns whether there were any.
    */
  private def deliverClosed(): Boolean = {
    var any = false
    var packed = closedQueue.poll()
    while packed != null do {
      val fd = (packed.longValue & 0xffffffffL).toInt
      val generation = (packed.longValue >>> 32).toInt
      val pending = inFlight.valuesIterator
        .filter(c => c.fd == fd && c.generation == generation && c.completion != null)
        .toList
      for command <- pending do {
        any = true
        val completion = withdraw(command)
        if completion != null then settleWithdrawn(command, completion, new IOException(s"Descriptor $fd was closed"))
      }
      packed = closedQueue.poll()
    }
    any
  }

  /** Called from any thread when a promise has its result: queue it and wake the loop. On the loop thread itself the
    * loop is not waiting, so queueing is enough.
    */
  private[uring] def resolved(promise: UringPromise): Unit =
    if !closed then { // otherwise its completion was dropped by `close`
      resolvedQueue.add(promise)
      if !(Thread.currentThread() eq loopThread) then wakeup()
    }

  /** Runs the completions of the ops that finished without waiting, as many as were queued when it started: a
    * completion that submits an op which finishes at once is left for the next turn, so one busy descriptor cannot keep
    * the loop from the kernel. Returns whether there were any.
    */
  private def deliverReady(): Boolean = {
    var count = ready.size
    val any = count > 0
    while count > 0 do {
      val op = ready.poll().nn
      count -= 1
      op.finishedAtOnce = false
      val failure = op.atOnceFailure
      if failure != null then {
        op.atOnceFailure = null
        fail(op, failure)
      } else complete(op)
    }
    any
  }

  /** Runs the completions of the promises resolved when it starts, on the loop thread. One resolved meanwhile, such as
    * by a completion, waits for the next turn, so completions that keep resolving promises cannot keep the loop from
    * the kernel. Returns whether there were any.
    */
  private def deliverResolved(): Boolean = {
    var remaining = resolvedQueue.size() // traverses the queue, which stays short
    var promise = if remaining > 0 then resolvedQueue.poll() else null
    val any = promise != null
    while promise != null do {
      val completion = promise.completion
      promise.completion = null
      pendingPromises -= promise
      promise match {
        case blocking: UringBlocking if blocking.cancelling != null =>
          // A cancelled blocker's `block` has returned, so its op's buffers and slots are the caller's again. Its
          // outcome is dropped: `cancel` detached it before deferring, and `submit` refuses it until now.
          assert(completion == null, "a blocker awaiting its cancel was resubmitted")
          val cancelled = blocking.cancelling.nn
          val failure = blocking.cancelledBy
          blocking.cancelling = null
          blocking.cancelledBy = null
          cancellingBlockers -= blocking
          if failure == null then cancelled.onCancel(blocking) else cancelled.onFailure(blocking, failure)
        case _ =>
          if completion != null then {
            if promise.heapIndex >= 0 then timers.remove(promise)
            val failure = promise.failure
            if failure != null then completion.onFailure(promise, failure.nn) else completion.onComplete(promise)
          }
      }
      remaining -= 1
      promise = if remaining > 0 then resolvedQueue.poll() else null
    }
    any
  }

  /** Withdraws every pending op, waits for the kernel to let go of every command and for every running blocker, then
    * unmaps the ring. Only then does it run the `onCancel`s, so that none sees memory the kernel or a blocker still
    * uses, and one that throws cannot leak the reactor's resources.
    */
  def close(): Unit = {
    if !closed then {
      closing = true
      val dropped = scala.collection.mutable.ArrayBuffer.empty[(UringOp, Completion[UringOp])]
      for command <- inFlight.values.toList do {
        val completion = if command.completion != null then take(command) else command.cancelling
        command.cancelling = null
        command.cancelledBy = null
        if completion != null then dropped += ((command, completion))
        if !command.cancelQueued then requestCancel(command)
      }
      while !timers.isEmpty do
        timers.poll() match {
          case timer: UringTimer => dropped += ((timer, take(timer)))
          case _                 => () // an op with a time limit, dropped with its kind below
        }
      while !ready.isEmpty do {
        val op = ready.poll().nn // finished, but its completion has not run
        op.finishedAtOnce = false
        op.atOnceFailure = null
        dropped += ((op, take(op)))
      }
      val running = scala.collection.mutable.ArrayBuffer.empty[UringBlocking]
      for promise <- pendingPromises.toList do {
        val completion = detach(promise)
        if completion != null then {
          dropped += ((promise, completion))
          promise match {
            case blocking: UringBlocking if blocking.interrupt() => running += blocking
            case _                                               => ()
          }
        }
      }
      for blocking <- cancellingBlockers do {
        dropped += ((blocking, blocking.cancelling.nn))
        blocking.cancelling = null
        blocking.cancelledBy = null
        running += blocking
      }
      cancellingBlockers.clear()
      resolvedQueue.clear()
      closedQueue.clear()
      drainForClose()
      wakeLock.synchronized {
        closed = true
        unistd.close(wakeFd)
        UringReactor.all.remove(this)
      }
      io_uring_queue_exit(ring)
      running.foreach(_.awaitEnd()) // Until then they may still write to their ops' buffers and slots.
      dropped.foreach((op, completion) => completion.onCancel(op))
    }
  }

  /** Cancels the eventfd's read too, and turns until the kernel has answered every entry it holds. */
  private def drainForClose(): Unit = {
    var wakeCancelQueued = false
    def cancelWake(): Unit =
      if wakeInFlight && !wakeCancelQueued then
        try {
          val sqe = nextSqe()
          io_uring_prep_cancel64(sqe, WakeId.toULong, 0)
          sqe.user_data = InternalId.toULong
          wakeCancelQueued = true
          unsubmitted = true
        } catch case _: IOException => ()
    while inFlight.nonEmpty || wakeInFlight do {
      retryCancels()
      cancelWake()
      try flush()
      catch case _: IOException => ()
      while reapedCursor < reapedCount do {
        val i = reapedCursor
        reapedCursor += 1
        val id = reapedIds(i)
        if id == WakeId then wakeInFlight = false
        else if id != InternalId then
          inFlight.remove(id).foreach { command =>
            command.userData = 0
            command.cancelQueued = false
            command.discardSubmission(reapedResults(i))
          }
      }
      if inFlight.nonEmpty || wakeInFlight then {
        io_uring_wait_cqe_timeout(ring, cqeSlot, waitTimeout(10_000_000L))
        reap()
      }
    }
  }

  /** A command or a promise that finished without waiting counts as finished: its completion still runs, and this
    * returns false. A command in the kernel is cancelled there, synchronously, and settled at once from its entry: one
    * that finished just before counts as finished too, and one stopped, or not finished, such as a read that found
    * nothing yet, is cancelled. Only one the kernel was already running elsewhere, in a worker of its own, as it may a
    * file op, has its `onCancel` run later, from the loop, once its entry arrives, as a blocking task that was running
    * has its run once `block` has returned. A pending timer is deleted, and a promise detached, so completing it later
    * does nothing. Otherwise `onCancel` runs before this returns.
    */
  def cancel(op: Op): Boolean = {
    val completion = withdraw(op)
    if completion == null then false
    else {
      settleWithdrawn(op, completion, null)
      true
    }
  }

  /** Fails `op`, whose time limit ran out, with `TimeoutException`: withdrawn as a cancelled op is, then `onFailure`
    * where that would be `onCancel`. Nothing if it is no longer pending.
    */
  private def timedOut(op: UringOp): Unit = {
    val completion = withdraw(op)
    if completion != null then settleWithdrawn(op, completion, new TimeoutException(s"$op timed out"))
  }

  /** Takes a pending op out of the reactor, as `cancel` describes, and returns its completion: null if it is not
    * pending here, or has finished.
    */
  private def withdraw(op: UringOp): Completion[UringOp] | Null =
    op match {
      case command: Command =>
        if command.completion == null || command.finishedAtOnce then null
        else if command.userData == 0 then take(command)
        else {
          var res = arrivedResult(command.userData)
          if res == NotArrived then {
            flush()
            if !unsubmitted then cancelNow(command)
            res = arrivedResult(command.userData)
          }
          if res == NotArrived then take(command) // still the kernel's: `settleWithdrawn` waits for its entry
          else settleArrived(command, res.toInt)
        }
      case timer: UringTimer     => if timer.heapIndex < 0 || timer.completion == null then null else take(timer)
      case promise: UringPromise => detach(promise)
      case _: UringAtOnce        => null // performed when submitted, so finished
    }

  /** Asks the kernel to cancel `command`'s submission and waits for it, unless the operation is already running
    * elsewhere, in a kernel worker, which the kernel is then told to stop: its entry arrives later. The entry of a
    * submission stopped here, or finished just before, is in the ring when this returns.
    */
  private def cancelNow(command: Command): Unit = {
    val res = io_uring_cancel_now(ring, command.userData.toULong)
    if res == -Linux.ETIME || res == -errno.EALREADY then command.cancelQueued = true
  }

  /** Settles `command`, withdrawn with the entry of its submission already in, whose result is `res`: an operation the
    * cancel stopped, or one that did not finish, such as a read that found nothing yet, ends as cancelled, its
    * completion returned; one that finished, or failed, finishes, its completion queued for the next turn, and this
    * returns null. The entry is dropped when it is dispatched.
    */
  private def settleArrived(command: Command, res: Int): Completion[UringOp] | Null = {
    inFlight.remove(command.userData)
    command.userData = 0
    if res == -errno.ECANCELED then {
      command.abandonRetry()
      take(command)
    } else {
      var failure: Throwable | Null = null
      val done =
        try command.finishSubmission(res)
        catch {
          case t: Throwable =>
            failure = t
            true
        }
      if done then {
        command.atOnceFailure = failure
        queueReady(command)
        null
      } else {
        command.abandonRetry()
        take(command)
      }
    }
  }

  /** Runs the completion of an op `withdraw` took out: `onCancel`, or `onFailure` with `failure` for one that timed out
    * or whose descriptor was closed; for a command still in the kernel, from the loop once its entry arrives, and for a
    * blocking task that was running, once `block` has returned.
    */
  private def settleWithdrawn(op: UringOp, completion: Completion[UringOp], failure: Throwable | Null): Unit =
    op match {
      case blocking: UringBlocking if blocking.interrupt() =>
        blocking.cancelling = completion
        blocking.cancelledBy = failure
        cancellingBlockers += blocking
      case command: Command if command.userData != 0 =>
        command.cancelling = completion
        command.cancelledBy = failure
        if !command.cancelQueued then requestCancel(command)
      case _ => if failure == null then completion.onCancel(op) else completion.onFailure(op, failure)
    }

  /** Detaches a pending promise so its outcome is never delivered. The caller skips or interrupts a blocking task. */
  private def detach(promise: UringPromise): Completion[UringOp] | Null = {
    val completion = promise.completion
    promise.completion = null
    pendingPromises -= promise
    if promise.heapIndex >= 0 then timers.remove(promise)
    completion
  }

  /** Asks the kernel to cancel `command`'s submission. If the queue has no room, the request waits in the backlog that
    * each turn retries.
    */
  private def requestCancel(command: Command): Unit =
    try {
      val sqe = nextSqe()
      try command.prepareCancel(sqe, command.userData)
      catch {
        case t: Throwable =>
          io_uring_prep_nop(sqe)
          sqe.user_data = InternalId.toULong
          unsubmitted = true
          throw t
      }
      sqe.user_data = InternalId.toULong
      command.cancelQueued = true
      unsubmitted = true
    } catch {
      case _: IOException => if !command.cancelQueued then cancelBacklog += command
    }

  /** Asks again for the cancels that did not fit into the queue before. */
  private def retryCancels(): Unit =
    if cancelBacklog.nonEmpty then {
      val retry = cancelBacklog.toList
      cancelBacklog.clear()
      retry.foreach(command => if !command.cancelQueued && command.userData != 0 then requestCancel(command))
    }

  /** The result of submission `id` if its completion queue entry has arrived, copied out or still in the ring, and
    * `NotArrived` otherwise.
    */
  private def arrivedResult(id: Long): Long = {
    var i = reapedCursor
    while i < reapedCount do {
      if reapedIds(i) == id then return reapedResults(i).toLong
      i += 1
    }
    val waiting = io_uring_peek_batch_cqe(ring, scan, scanSize.toUInt).toInt
    i = 0
    while i < waiting do {
      val cqe = !(scan + i)
      if cqe.user_data.toLong == id then return cqe.res.toLong
      i += 1
    }
    NotArrived
  }

  private inline val NotArrived = Long.MinValue

  /** Copies up to a batch of completion queue entries out of the ring and marks them seen, so that dispatching them may
    * submit again without the ring filling up.
    */
  private def reap(): Unit = {
    val count = io_uring_peek_batch_cqe(ring, batch, MaxBatch.toUInt).toInt
    var i = 0
    while i < count do {
      val cqe = !(batch + i)
      reapedIds(i) = cqe.user_data.toLong
      reapedResults(i) = cqe.res
      i += 1
    }
    if count > 0 then io_uring_cq_advance(ring, count.toUInt)
    reapedCount = count
    reapedCursor = 0
  }

  /** Settles submission `id` with its result: completes, fails, or submits again its command, or, for one that was
    * withdrawn, lets it go with `onCancel`, or `onFailure` with what it was withdrawn for. An entry nobody waits for
    * any more, such as one a cancel already settled, is dropped.
    */
  private def dispatch(id: Long, res: Int): Unit =
    if id == WakeId then {
      wakeInFlight = false
      wakePending.set(false) // resolved promises and closed descriptors are delivered by `runOnce` itself
      armWake()
    } else if id != InternalId then {
      val found = inFlight.remove(id)
      if found.isDefined then {
        val command = found.get
        command.userData = 0
        val cancelled = command.cancelling
        if cancelled != null then {
          val failure = command.cancelledBy
          command.cancelling = null
          command.cancelledBy = null
          command.cancelQueued = false
          command.discardSubmission(res)
          if failure == null then cancelled.onCancel(command) else cancelled.onFailure(command, failure)
        } else {
          // A throwing command fails its own op; an exception thrown by the completion itself still escapes.
          var failure: Throwable | Null = null
          val done =
            try command.finishSubmission(res)
            catch {
              case t: Throwable =>
                failure = t
                true
            }
          if failure != null then fail(command, failure.nn)
          else if done then complete(command)
          else
            try enqueue(command) // not done yet: again, without the completion ever seeing it
            catch case t: Throwable => fail(command, t)
        }
      }
    }

  def stop(): Unit = running = false

  /** Issues what was submitted since the last turn, delivers the ops that finished without waiting, closed descriptors
    * and resolved promises, takes in the completion queue entries that have arrived, waiting for some first if asked to
    * and nothing was delivered, and dispatches them; then fires the timers that are due, delivers what arrived from
    * other threads meanwhile, and issues what the completions submitted.
    */
  def runOnce(block: Boolean): Unit = {
    if closed then throw new IllegalStateException("The reactor is closed")
    retryCancels()
    armWake()
    flush()
    val delivered = deliverClosed() | deliverReady() | deliverResolved() // all three, without short-circuiting
    if reapedCursor >= reapedCount then reap()
    if reapedCursor >= reapedCount && !delivered && block then {
      flush() // what the completions just delivered submitted
      waitForEntries()
      reap()
    }
    while reapedCursor < reapedCount do {
      val i = reapedCursor
      reapedCursor += 1
      dispatch(reapedIds(i), reapedResults(i))
    }
    fireDueTimers()
    deliverClosed() // their cancels go to the kernel below
    deliverResolved() // including any whose wake-up was merged into an entry just dispatched
    flush()
  }

  /** Waits for a completion queue entry: no longer than until the earliest timer is due, or not at all if it already
    * is, and no longer than 10 ms while entries the kernel could not take, or the eventfd's read, are still to be
    * issued, so that they are tried again.
    */
  private def waitForEntries(): Unit = {
    var wait = -1L
    if !timers.isEmpty then wait = math.max(timers.peek.deadline - System.nanoTime(), 0L)
    if (unsubmitted || !wakeInFlight) && (wait < 0 || wait > 10_000_000L) then wait = 10_000_000L
    if wait != 0 then io_uring_wait_cqe_timeout(ring, cqeSlot, if wait < 0 then null else waitTimeout(wait)): Unit
  }

  /** Completes the timers that are due, earliest first, and times out the ops whose time limit is. Only entries
    * scheduled before it started fire, so a completion that submits a zero-length timer leaves it for the next turn.
    */
  private def fireDueTimers(): Unit =
    if !timers.isEmpty then {
      val now = System.nanoTime()
      val submittedBefore = nextTimerOrder
      while !timers.isEmpty && timers.peek.deadline <= now && timers.peek.order < submittedBefore do
        timers.poll() match {
          case timer: UringTimer =>
            if timer.expires then fail(timer, new TimeoutException(s"$timer timed out")) else complete(timer)
          case op => timedOut(op)
        }
    }

  def run(): Unit = {
    loopThread = Thread.currentThread()
    running = true
    while running do runOnce(block = true)
  }
}

object UringReactor {

  /** The open reactors of the process, told of every descriptor closed (`UringHandles.close`). */
  private val all = java.util.concurrent.ConcurrentHashMap.newKeySet[UringReactor]()

  /** Tells every open reactor that `fd`, whose generation was `generation`, has been closed (see `closed`). */
  private[uring] def descriptorClosed(fd: Int, generation: Int): Unit = all.forEach(_.closed(fd, generation))

  /** Opens a reactor on a new ring of `entries` submission queue entries. Close it when done. */
  def open(entries: Int = 256): UringReactor = {
    val reactor = new UringReactor(entries)
    all.add(reactor)
    reactor
  }

  /** Opens a reactor for `body` and closes it afterwards. */
  def scoped(entries: Int = 256)(body: UringReactor => Unit): Unit = {
    val reactor = open(entries)
    try body(reactor)
    finally reactor.close()
  }
}
