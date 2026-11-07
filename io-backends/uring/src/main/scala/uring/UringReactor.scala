package gears.async.io
package uring

import gears.async.io.internal.Completion
import gears.async.io.internal.HandleClosedException
import gears.async.io.internal.Interest
import gears.async.io.internal.ReactorCore

import java.io.IOException
import scala.scalanative.linux.eventfd
import scala.scalanative.posix.errno
import scala.scalanative.posix.sys.socket
import scala.scalanative.posix.unistd
import scala.scalanative.runtime.ByteArray
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import uring.*
import uringStructOps.*

/** The io_uring implementation of `Reactor`, for ops built by `UringOps`, on one ring (Linux 6.0 or later). A command
  * becomes a submission queue entry when it is submitted, and the entries a turn prepares go to the kernel together,
  * once per turn, or at once for `trySubmit`, which first tries the command with a non-blocking system call. The kernel
  * owns a command's buffers until its completion queue entry arrives, so withdrawing a command, by `cancel`, a time
  * limit or a close of its handle, cancels it in the kernel synchronously, and waits until the kernel has let go of it:
  * one that finished first counts as finished, its completion run on the next turn, and any other is cancelled, its
  * `onCancel` run before `cancel` returns. The exception is a command a kernel worker is running, such as a read of a
  * regular file, which nothing can stop: it is not waited for, so that a slow disk does not hold up the loop, and
  * counts as committed, `cancel` returning false and the command completing as it finishes. A command holds a use of
  * its handle until the kernel has taken its entry (see `UringBackend.Handle`), so a descriptor's number is never
  * reused under an entry that names it. Handles must be closed through the backend, by the `close` op or
  * `UringHandles.close`, from any thread, which cancels the commands in the kernel on the handle, on every reactor,
  * before the descriptor closes, so that the close frees the file as it returns (`cancelClosed`), and tells every
  * reactor (`handleClosed`), which fails its own on its next turn. Timers, and the ops with a time limit, are kept in a
  * heap on the loop thread, by deadline, and bound how long a turn waits, so setting or cancelling one costs no system
  * call, and an op that times out is withdrawn as a cancelled one is, with `onFailure` instead of `onCancel`. `wakeup`,
  * and blocking tasks finishing on other threads, wake the loop through an eventfd the ring always has a read pending
  * on.
  */
final class UringReactor private (entries: Int) extends ReactorCore[UringOp] {

  type Handle = UringBackend.Handle

  def ops: Ops[UringOp, Handle] = UringOps

  private val ringStorage = new Array[Byte](io_uring_size().toInt)
  private def ring: Ptr[io_uring] = ringStorage.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[io_uring]]
  locally {
    val res = io_uring_queue_init(entries.toUInt, ring, 0.toUInt)
    if res < 0 then throw Results.failure("Failed to set up io_uring", res)
    // A cancel waits for the kernel to let go of a command, which needs the synchronous cancel of Linux 6.0; no command
    // has `InternalId`, so a kernel that has it finds none. The features are older, but a turn relies on them.
    val features = io_uring_features(ring).toInt
    val probe = io_uring_cancel_now(ring, InternalId.toULong, 0, 1)
    if probe != -errno.ENOENT || (features & IORING_FEAT_NODROP) == 0 || (features & IORING_FEAT_EXT_ARG) == 0 then {
      io_uring_queue_exit(ring)
      throw new IOException(s"The io_uring backend needs Linux 6.0 or later (synchronous cancel: $probe)")
    }
  }

  // The user data of the eventfd's read, and of the entries whose completions are ignored: the no-ops that fill an
  // entry taken for a command that then failed to prepare. Commands get the numbers after them.
  private inline val WakeId = 0L
  private inline val InternalId = 1L
  private var nextId = 2L

  // The commands in the kernel, by the user data of their submission, and those on a handle, by its descriptor's
  // number, the first of a list through `Command.nextOnFd`, which may hold a closed handle's beside its number's new
  // one's. Loop thread only.
  private val inFlight = scala.collection.mutable.LongMap.empty[Command]
  private val onFd = scala.collection.mutable.LongMap.empty[Command]

  // This reactor's bit in `Handle.reactorBits`, and its index, -1 if it has none (`UringReactor.claimIndex`).
  private[uring] val index: Int = UringReactor.claimIndex()
  private[uring] val bit: Long = if index < 0 then -1L else 1L << index

  // Whether entries have been prepared that the kernel has not taken yet, and the handles whose uses they hold until it
  // has, with a list to swap in as they are let go. Loop thread only.
  private var unsubmitted = false
  private var submitting = scala.collection.mutable.ArrayBuffer.empty[Handle]
  private var submittingSpare = scala.collection.mutable.ArrayBuffer.empty[Handle]

  // Whether the last turn polled, which issues the entries prepared; if it did not, the next turn's `deliverOwn` does.
  private var polled = true

  // The commands a close fails, gathered before any is settled. Loop thread only.
  private val closedPending = scala.collection.mutable.ArrayBuffer.empty[Command]

  private var wakeInFlight = false

  // Completion queue entries are copied out a batch at a time and dispatched one by one, so a completion that cancels
  // a command of the same batch still finds that command's entry here. A command looked for and not found has the
  // whole queue, and the kernel's overflow list, copied out after them (`drain`), so the copy grows. Loop thread only.
  private inline val MaxBatch = 64
  private val batchStorage = new Array[Byte]((sizeof[Ptr[io_uring_cqe]] * MaxBatch.toUInt).toInt)
  private def batch: Ptr[Ptr[io_uring_cqe]] =
    batchStorage.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[Ptr[io_uring_cqe]]]
  private var reapedIds = new Array[Long](MaxBatch)
  private var reapedResults = new Array[Int](MaxBatch)
  private var reapedCount = 0
  private var reapedCursor = 0

  // Handles closed on any thread while commands were in the kernel on them (`handleClosed`), for the loop to cancel
  // its own (`deliverClosed`).
  private val closedQueue = new java.util.concurrent.ConcurrentLinkedQueue[Handle]()

  private val wakeFd: Int = eventfd.eventfd(0.toUInt, eventfd.EFD_CLOEXEC)
  if wakeFd < 0 then {
    io_uring_queue_exit(ring)
    throw new IOException(s"Failed to create eventfd: ${gears.async.io.internal.PosixErr.cError()}")
  }
  private val wakeBuffer = new Array[Byte](8)

  armWake()

  /** Submits a command, a connect, or a close (see `ReactorCore.submitOwn`). */
  protected def submitOwn(op: UringOp, pending: Completion[UringOp], finishNow: Boolean): Int =
    op match {
      case connect: ConnectTo if !started(connect, pending, finishNow) =>
        if finishNow then ReactorCore.Finished else ReactorCore.Queued
      case command: Command => submitCommand(command, pending, finishNow)
      case close: Close     =>
        // Closed here, as an at-once op is, unless a use of the descriptor is in progress: then pending, as a promise,
        // until the last use ends and closes it.
        close.reset()
        close.reactor = this
        close.completion = pending
        if close.start() then {
          closing.add(close) // completed after the failures it causes, never in trySubmit
          ReactorCore.Queued
        } else {
          pendingPromises += close
          ReactorCore.Waits
        }
      case _ => ReactorCore.NotOwn // a timer, a blocker or an at-once op
    }

  /** Submits a command: done at once if it has nothing to do, or, with `finishNow`, if a non-blocking system call
    * finishes it; otherwise prepared as an entry for the kernel, issued at once with `finishNow`, else with the turn's
    * others. The try and the preparation are a use of the command's handle, which a closed handle refuses.
    */
  private def submitCommand(command: Command, pending: Completion[UringOp], finishNow: Boolean): Int = {
    val handle = command.handle
    if handle != null && !handle.acquire() then refuseNow(command, pending, UringHandles.notOpen(handle), finishNow)
    else
      try {
        command.completion = pending
        var done = command.nothingToDo
        if !done && finishNow && (handle == null || handle.worthTrying(command.interest == Interest.Read)) then {
          done =
            try command.tryAtOnce()
            catch {
              case t: Throwable =>
                command.atOnceFailure = t
                true
            }
          if handle != null then handle.tried(command.interest == Interest.Read, done)
          command.triedFirst = !done
        }
        if done then
          if finishNow then ReactorCore.Finished
          else {
            ready.add(command)
            ReactorCore.Queued
          }
        else {
          val failed =
            try {
              enqueue(command)
              null
            } catch case t: Throwable => t
          if failed != null then refuseNow(command, pending, failed, finishNow)
          else {
            // Pending now, so a failure to issue is the turn's to report: the entry goes with the turn's others.
            if finishNow then
              try flush()
              catch case _: IOException => ()
            ReactorCore.Waits
          }
        }
      } finally if handle != null then handle.release()
  }

  /** Prepares one submission queue entry for `command`, to go to the kernel with the turn's others, holding a use of
    * its handle until the kernel has taken it, which fails once the handle is closed. If preparing it throws, the
    * entry, already taken, becomes a no-op.
    */
  private def enqueue(command: Command): Unit = {
    val sqe = nextSqe()
    try {
      val handle = command.handle
      if handle != null then {
        if !handle.acquire() then throw UringHandles.notOpen(handle)
        submitting += handle
      }
      command.prepareSubmission(sqe)
    } catch {
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
    val handle = command.handle
    if handle != null then {
      handle.entered(bit)
      val fd = handle.fd.toLong
      val first = onFd.getOrNull(fd)
      command.nextOnFd = first
      if first != null then first.prevOnFd = command
      onFd(fd) = command
    }
    unsubmitted = true
  }

  /** Forgets `command`'s submission, which the kernel has let go of. */
  private def settled(command: Command): Unit = {
    inFlight.remove(command.userData)
    command.userData = 0
    val handle = command.handle
    if handle != null then {
      handle.left()
      val prev = command.prevOnFd
      val next = command.nextOnFd
      if next != null then next.prevOnFd = prev
      if prev != null then prev.nextOnFd = next
      else if next != null then onFd(handle.fd.toLong) = next
      else onFd.remove(handle.fd.toLong)
      command.prevOnFd = null
      command.nextOnFd = null
    }
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

  /** Hands the prepared entries to the kernel, and once it has taken them all, ends the uses of their handles. One the
    * kernel is too busy to take, or an interrupted call, is left for the next turn to try again.
    */
  private def flush(): Unit =
    if unsubmitted then submitted(io_uring_submit(ring))

  /** Takes in what a call that submitted the entries prepared returned, `res`. */
  private def submitted(res: Int): Unit = {
    unsubmitted = io_uring_sq_ready(ring) != 0.toUInt
    if !unsubmitted && submitting.nonEmpty then {
      val taken = submitting
      submitting = submittingSpare
      submittingSpare = taken
      try taken.foreach(_.release()) // the last use of a closed handle closes its descriptor
      finally taken.clear()
    }
    if res < 0 && !Results.transient(res) && res != -errno.EBUSY && res != -Linux.ETIME then
      throw Results.failure("Failed to submit to io_uring", res)
  }

  /** Hands every prepared entry to the kernel, for a cancel, which only finds what the kernel has. A kernel that
    * refuses entries while completions overflow takes them once the completions are copied out (`drain`).
    */
  private def flushAll(): Unit = {
    var tries = 0
    flush()
    while unsubmitted do {
      if tries == 1000 then throw new IOException("io_uring does not take the entries prepared")
      tries += 1
      drain()
      Thread.onSpinWait()
      flush()
    }
  }

  /** Keeps one read of the eventfd in the ring, so a write to it ends a blocked wait. */
  private def armWake(): Unit =
    if !closed && !wakeInFlight then
      try {
        val sqe = nextSqe()
        io_uring_prep_read(sqe, wakeFd, wakeBuffer.asInstanceOf[ByteArray].at(0), 8.toUInt, 0.toULong)
        sqe.user_data = WakeId.toULong
        wakeInFlight = true
        unsubmitted = true
      } catch case _: IOException => () // the queue is full: the next turn tries again

  // `signalsWhileAwake` is left off, unlike kqueue's: a wake-up while the loop is awake then costs no eventfd write.
  // kqueue's reason, a macOS blocker thread's hand-off cost, is not measured on Linux yet.

  /** Writes to the eventfd (see `ReactorCore.wakeup`). */
  protected def wakeNow(): Unit = {
    val signal = stackalloc[CLongLong]()
    !signal = 1L
    unistd.write(wakeFd, signal.asInstanceOf[Ptr[Byte]], 8.toCSize): Unit
  }

  /** Called from any thread when `handle` is closed: the commands in the kernel on it here fail, from the loop, as
    * `Ops.close` says (see `deliverClosed`).
    */
  private[uring] def handleClosed(handle: Handle): Unit =
    if !closed then {
      closedQueue.add(handle)
      if !(Thread.currentThread() eq loopThread) then wakeup()
    }

  // Held by a thread cancelling on the ring from outside the loop (`cancelClosed`), so that the ring is not unmapped
  // under it; `ringOpen` is false once it may be.
  private val ringLock = new Object
  private var ringOpen = true

  /** Called from the thread closing `handle`'s descriptor, before it does: cancels the entries in the kernel on the
    * descriptor here, so that the descriptor's file goes when it closes, whether or not the loop is turning; one a
    * kernel worker is running, such as a read of a regular file, cannot be stopped, and keeps the file until it
    * finishes, which this does not wait for. The kernel serializes this with the loop's own submissions and cancels,
    * and posts the entries to the ring, where the loop fails them (`deliverClosed`).
    */
  private[uring] def cancelClosed(handle: Handle): Unit =
    ringLock.synchronized {
      if ringOpen then checkCancel(io_uring_cancel_fd(ring, handle.fd))
    }

  /** Cancels the commands in the kernel on the handles closed since the last turn, and fails them, with a closed
    * handle's failure, once the kernel has let go of them, whatever their result: at once, or, for one a kernel worker
    * is running, as it finishes (`dispatch`). Returns whether there were any.
    */
  protected def deliverClosed(): Boolean = {
    var any = false
    var handle = closedQueue.poll()
    while handle != null do {
      val h = handle
      var on = onFd.getOrNull(h.fd.toLong)
      while on != null do {
        if (on.handle eq h) && on.completion != null then closedPending += on
        on = on.nextOnFd
      }
      if closedPending.nonEmpty then {
        any = true
        flushAll()
        try
          for command <- closedPending do {
            val res = letGo(command)
            if res != StillRunning then {
              settled(command)
              command.abandonRetry()
              command.discardSubmission(res.toInt)
              fail(command, HandleClosedException.pending(h))
            }
          }
        finally closedPending.clear()
      }
      handle = closedQueue.poll()
    }
    any
  }

  /** The commands that finished in the kernel and whose entries have arrived without a poll: io_uring posts them to the
    * ring as the loop's thread runs, so they are dispatched here, with no system call, rather than waiting for the
    * turn's poll, which a busy loop skips (see `ReactorCore.runOnce`). The entries prepared go to the kernel with the
    * turn's poll, in the call that waits, so that a turn that blocks makes one system call; only those left by a turn
    * that skipped its poll go here.
    */
  protected def deliverOwn(): Boolean = {
    if !polled then flush()
    polled = false
    reap()
    dispatchReaped()
  }

  /** Issues the entries prepared, and takes in and dispatches the completion queue entries that have arrived, waiting
    * for one first if `block`, in the same system call: no longer than until the earliest timer is due, or not at all
    * if it already is, and no longer than 10 ms while entries the kernel could not take, or the eventfd's read, are
    * still to be issued, so that they are tried again. The entries the completions prepare go with the next turn's.
    */
  protected def poll(block: Boolean): Unit = {
    polled = true
    armWake()
    reap()
    if block && reapedCursor >= reapedCount && mayBlock() then
      try {
        var wait = -1L
        if !timers.isEmpty then wait = math.max(timers.peek.when - System.nanoTime(), 0L)
        if (unsubmitted || !wakeInFlight) && (wait < 0 || wait > 10_000_000L) then wait = 10_000_000L
        if wait == 0 then flush()
        else submitted(io_uring_submit_and_wait(ring, wait)) // EBUSY, completions overflowing: taken in below
      } finally unpark()
    else flush()
    tookWakeUps()
    reap()
    dispatchReaped()
  }

  /** Dispatches the entries copied out and not yet dispatched; returns whether there were any. */
  private def dispatchReaped(): Boolean = {
    val any = reapedCursor < reapedCount
    while reapedCursor < reapedCount do {
      val i = reapedCursor
      reapedCursor += 1
      dispatch(reapedIds(i), reapedResults(i))
    }
    any
  }

  /** Copies up to a batch of completion queue entries out of the ring and marks them seen, so that dispatching them may
    * submit again without the ring filling up. Nothing while entries copied out before are still to be dispatched.
    */
  private def reap(): Unit =
    if reapedCursor >= reapedCount then {
      reapedCount = 0
      reapedCursor = 0
      copyOut(io_uring_peek_batch_cqe(ring, batch, MaxBatch.toUInt).toInt)
    }

  /** Copies every completion queue entry out of the ring, after those copied out before, and those the kernel keeps in
    * its overflow list while the ring is full, so that a command looked for is found wherever its entry waits.
    */
  private def drain(): Unit = {
    var more = true
    while more do {
      val count = io_uring_peek_batch_cqe(ring, batch, MaxBatch.toUInt).toInt
      copyOut(count)
      if count == 0 then
        if io_uring_cq_overflowing(ring) == 0 then more = false
        else {
          val res = io_uring_get_events(ring)
          if res < 0 && !Results.transient(res) && res != -errno.EBUSY then
            throw Results.failure("Failed to take completions from io_uring", res)
        }
    }
  }

  /** Appends the `count` entries `io_uring_peek_batch_cqe` put in `batch` to those copied out, and marks them seen. */
  private def copyOut(count: Int): Unit =
    if count > 0 then {
      if reapedCount + count > reapedIds.length then {
        val size = math.max(reapedIds.length * 2, reapedCount + count)
        reapedIds = java.util.Arrays.copyOf(reapedIds, size)
        reapedResults = java.util.Arrays.copyOf(reapedResults, size)
      }
      var i = 0
      while i < count do {
        val cqe = !(batch + i)
        reapedIds(reapedCount + i) = cqe.user_data.toLong
        reapedResults(reapedCount + i) = cqe.res
        i += 1
      }
      reapedCount += count
      io_uring_cq_advance(ring, count.toUInt)
    }

  /** Settles submission `id` with its result: completes, fails, or submits again its command. One whose handle was
    * closed meanwhile fails as one on a closed handle does, its result dropped. An entry nobody waits for any more,
    * such as one a cancel already settled, is dropped.
    */
  private def dispatch(id: Long, res: Int): Unit =
    if id == WakeId then {
      wakeInFlight = false // the wake-up was taken as answered once the poll returned (`tookWakeUps`)
      armWake()
    } else if id != InternalId then {
      val found = inFlight.get(id)
      if found.isDefined then {
        val command = found.get
        settled(command)
        val handle = command.handle
        if handle != null && !handle.isOpen then {
          command.discardSubmission(res)
          fail(command, HandleClosedException.pending(handle))
        } else {
          // A throwing command fails its own op; an exception thrown by the completion itself is reported as a
          // `CompletionFailed` (see `Reactor`), not as the op's failure.
          var failure: Throwable | Null = null
          val done =
            try command.finishSubmission(res)
            catch {
              case t: Throwable =>
                failure = t
                true
            }
          if !done then
            try enqueue(command) // not done yet: again, without the completion ever seeing it
            catch case t: Throwable => failure = t
          if failure != null then fail(command, failure.nn) else if done then complete(command)
        }
      }
    }

  /** Asks the kernel to cancel `command`'s submission and waits until it has let go of it, and returns its result: what
    * the operation did if it finished first, else `-ECANCELED`; or `StillRunning` for one a kernel worker is running,
    * such as a read of a regular file, which nothing can stop, and which this does not wait for, so that a disk that is
    * slow to answer does not hold up the loop. The entries prepared must have been issued.
    */
  private def letGo(command: Command): Long = {
    val id = command.userData
    var res = arrivedResult(id)
    if res == NotArrived then {
      val cancelled = io_uring_cancel_now(ring, id.toULong, 0, if command.runsInWorker then 0 else 1)
      checkCancel(cancelled)
      res = arrivedResult(id)
      if res == NotArrived then
        if cancelled == -Linux.ETIME || cancelled == -errno.EALREADY then res = StillRunning
        else throw new IllegalStateException(s"io_uring kept $command after cancelling it ($cancelled)")
    }
    res
  }

  /** Throws unless `res` is what a synchronous cancel returns: how many it cancelled, none found, or one still running.
    */
  private def checkCancel(res: Int): Unit =
    if res < 0 && res != -errno.ENOENT && res != -Linux.ETIME && res != -errno.EALREADY then
      throw Results.failure("Failed to cancel in io_uring", res)

  /** Takes a pending command out of the kernel (see `ReactorCore.withdrawOwn`): one that finished at once, or whose
    * entry says it finished before the cancel, counts as finished, its completion queued for the next turn, and this
    * returns null; so does one a kernel worker is running, which nothing can stop, and which completes as it finishes,
    * as a blocking task that has committed does; any other is cancelled, what it produced discarded, and its completion
    * returned.
    */
  protected def withdrawOwn(op: UringOp): Completion[UringOp] | Null =
    op match {
      case command: Command =>
        if command.completion == null || command.userData == 0 then null // finished at once, in `ready`
        else {
          flushAll()
          val got = letGo(command)
          if got == StillRunning then return null // committed: it completes as it finishes, as a running blocker does
          val res = got.toInt
          settled(command)
          if res == -errno.ECANCELED then {
            command.abandonRetry()
            command.discardSubmission(res)
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
              ready.add(command)
              null
            } else {
              command.abandonRetry()
              command.discardSubmission(Command.NoResult)
              take(command)
            }
          }
        }
      case _ => ReactorCore.NotOwnCompletion
    }

  /** The result of submission `id` if its completion queue entry has arrived, copied out, still in the ring, or in the
    * kernel's overflow list, and `NotArrived` otherwise.
    */
  private def arrivedResult(id: Long): Long = {
    var res = copiedResult(id)
    if res == NotArrived then {
      val from = reapedCount
      drain()
      res = copiedResult(id, from)
    }
    res
  }

  private def copiedResult(id: Long, from: Int = reapedCursor): Long = {
    var i = math.max(from, reapedCursor)
    while i < reapedCount do {
      if reapedIds(i) == id then return reapedResults(i).toLong
      i += 1
    }
    NotArrived
  }

  private inline val NotArrived = Long.MinValue
  private inline val StillRunning = Long.MinValue + 1

  /** Cancels every command in the kernel, and the eventfd's read, waits until the kernel has let go of them all, and
    * drops each command (see `ReactorCore.dropOwn`): one whose entry says it finished first is queued as finished
    * instead, so that it gets its outcome.
    */
  protected def dropOwn(drop: UringOp => Unit): Unit = {
    try flushAll()
    catch case _: IOException => ()
    if inFlight.nonEmpty || wakeInFlight then
      checkCancel(io_uring_cancel_now(ring, 0.toULong, IORING_ASYNC_CANCEL_ANY, 1))
    for (id, command) <- inFlight.toList do {
      val arrived = arrivedResult(id)
      val res = if arrived == NotArrived then -errno.ECANCELED else arrived.toInt
      settled(command)
      var finished = false
      if command.completion != null && res != -errno.ECANCELED then {
        var failure: Throwable | Null = null
        finished =
          try command.finishSubmission(res)
          catch {
            case t: Throwable =>
              failure = t
              true
          }
        if finished then {
          command.atOnceFailure = failure
          ready.add(command)
        }
      }
      if !finished then {
        command.abandonRetry()
        command.discardSubmission(if command.completion == null then res else Command.NoResult)
      }
      if !finished && command.completion != null then drop(command)
    }
    inFlight.clear()
    reapedCount = 0
    reapedCursor = 0
  }

  /** Unmaps the ring and closes the eventfd (see `ReactorCore.closeOwn`). */
  protected def closeOwn(): Unit = {
    closedQueue.clear()
    onFd.clear()
    submitting.foreach(_.release()) // entries the kernel never took, if it would take none
    submitting.clear()
    wakeLock.synchronized {
      closed = true
      unistd.close(wakeFd)
      UringReactor.all.remove(this)
    }
    ringLock.synchronized { ringOpen = false }
    io_uring_queue_exit(ring)
    UringReactor.releaseIndex(index)
  }
}

object UringReactor {

  /** The open reactors of the process: those that have had commands in the kernel on a handle are told when it closes,
    * and cancel them (`UringHandles.closeHandle`).
    */
  private val all = java.util.concurrent.ConcurrentHashMap.newKeySet[UringReactor]()

  /** Tells every open reactor that `handle` was closed, if a command on it may have an entry in the kernel (see
    * `handleClosed`).
    */
  private[uring] def handleClosed(handle: UringBackend.Handle): Unit =
    if handle.inKernel then {
      val bits = handle.reactorBits
      all.forEach(r => if (bits & r.bit) != 0 then r.handleClosed(handle))
    }

  /** Cancels the entries in the kernel on `handle`'s descriptor on every open reactor, from the thread about to close
    * it, if any command on it may have one (see `cancelClosed`): a close with none costs no system call. The kernel
    * drops a cancelled entry's hold on the file on the thread of the entry's reactor, as that thread next runs, so a
    * socket's descriptor is shut down first: a listener's port is free, and a peer sees the end of the stream, as the
    * close returns, as `Ops.close` says, even while that reactor's thread waits on something else.
    */
  private[uring] def cancelClosed(handle: UringBackend.Handle): Unit =
    if handle.inKernel then {
      socket.shutdown(handle.fd, socket.SHUT_RDWR): Unit // ENOTSOCK for a pipe or file, ENOTCONN unconnected: ignored
      val bits = handle.reactorBits
      all.forEach(r => if (bits & r.bit) != 0 then r.cancelClosed(handle))
    }

  // The indices taken by open reactors, one bit each: 64 reactors have one, any beyond have none, and count as all.
  private val indices = new java.util.concurrent.atomic.AtomicLong(0L)

  /** Takes the lowest index no open reactor has, or -1 if none is free. */
  private def claimIndex(): Int = {
    var taken = indices.get()
    var claimed = -1
    while claimed < 0 && taken != -1L do {
      val free = java.lang.Long.numberOfTrailingZeros(~taken)
      if indices.compareAndSet(taken, taken | (1L << free)) then claimed = free else taken = indices.get()
    }
    claimed
  }

  private def releaseIndex(index: Int): Unit =
    if index >= 0 then indices.getAndAccumulate(~(1L << index), _ & _): Unit

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
