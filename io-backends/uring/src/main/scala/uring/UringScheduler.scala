package gears.async.io
package uring

import gears.async.io.shareable.WorkpoolScheduler
import gears.async.native.NativeSuspend

import scala.scalanative.posix.errno
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*
import scala.util.Failure

import uring.*
import uringStructOps.*

/** The io_uring backend's runtime: the shared native runtime (`WorkpoolScheduler`) with a ring for each worker, which
  * is the worker's poller. An op is performed on the performer's own worker, which tries it at once, else prepares its
  * entry on its own ring; it waits on the ring, until its next timer at the latest, once it has nothing else to run,
  * one `io_uring_enter` submitting what it prepared and waiting; and it applies each completion and fires the op.
  *
  * Taking an op back (`withdraw`, on a cancel; `limitReached`, on a time limit) is done on the op's worker, whose ring
  * alone holds its entry: an entry not yet submitted becomes a no-op, and one in the kernel is cancelled synchronously,
  * which returns once the kernel has let go of it (Linux 6.0). A performer that resumed on another worker hands the
  * withdrawal to the op's, and waits for the op to settle.
  */
private[uring] final class UringScheduler private (rings: Array[Ring])
    extends WorkpoolScheduler[UringOp, UringBackend.type](UringBackend, rings.length)
    with NativeSuspend {
  override protected def selectWorker() = workerOfCurrentThread()

  // A class parameter is set before the superclass's constructor runs, which makes the workers.
  override def makeWorker(id: Int): UringWorker = new UringWorker(id, rings(id))

  private def isWorkerOf(worker: UringWorker): Boolean = worker.isCurrent

  final class UringWorker(id: Int, givenRing: Ring)
      extends WorkpoolScheduler.Worker[UringOp, UringBackend.type](id, UringBackend) {

    // Lazy: `Worker` starts its thread in its constructor, which uses these before this class's fields are set.
    private lazy val own = new Own

    /** What only the worker's thread touches, but for `ring.wake` and `ring.cancelFd`. */
    private final class Own {
      val ring = givenRing

      /** The commands in the kernel or prepared, by the user data of their entry. */
      val inFlight = scala.collection.mutable.LongMap.empty[Command]

      /** The commands whose entries are prepared and not yet submitted. */
      val prepared = scala.collection.mutable.ArrayBuffer.empty[Command]

      var nextId = Ring.InternalId + 1

      /** The commands whose entries went to the kernel in the last submission. */
      val submitted = scala.collection.mutable.ArrayBuffer.empty[Command]

      // A close on another thread cancels the entries in the kernel on its descriptor, then wakes this ring. An entry
      // still prepared as the close begins is turned into a no-op here and its command failed; one submitted as the
      // close begins, which the close's cancel may have missed, is cancelled after the submission, once woken.
      ring.onSubmit = () => {
        var closed: scala.collection.mutable.ArrayBuffer[Command] | Null = null // rarely any
        prepared.foreach { c =>
          if c.state == Command.Prepared then {
            if c.handle.isClosing then {
              ring.nop(c.sqe)
              inFlight.remove(c.id)
              c.state = Command.Idle
              if closed == null then closed = scala.collection.mutable.ArrayBuffer.empty[Command]
              closed.nn += c
            } else {
              c.state = Command.Submitted
              submitted += c
            }
          }
        }
        prepared.clear()
        if closed != null then
          closed.nn.foreach { c =>
            c.discardSubmission()
            settle(c, Failure(c.handle.closedFailure))
          }
      }

      ring.afterSubmit = () => {
        submitted.foreach(c => if c.state == Command.Submitted && c.handle.isClosing then ring.cancelNow(c.id): Unit)
        submitted.clear()
      }
    }

    override def tryPerform(op: UringOp): Boolean = op match {
      case timer: (UringTimer | UringDeadline) =>
        addTimer(timer)
        false
      case atOnce: AtOnce =>
        atOnce.run()
        true
      case close: CloseHandle    => close.start()
      case task: BlockingTask[?] =>
        task.check()
        task.owner = this
        if task.hasDeadline then addTimer(task)
        task.start()
        false
      case command: Command => perform(command)
    }

    /** Tries `command` at once, else prepares its entry, in a use of its handle that its completion ends. */
    private def perform(command: Command): Boolean = {
      command.check()
      command.begin()
      val handle = command.handle
      if !handle.enter() then throw handle.closedFailure
      if !command.claim() then {
        handle.leave()
        throw Command.alreadyPending(command)
      }
      command.owner = this
      val turns = command.turns
      if turns != null && !turns.enter(command) then {
        // Started by the command before it as that one ends (`startTurn`).
        if command.hasDeadline then addTimer(command)
        return false
      }
      val finished =
        try command.nothingToDo || (handle.triesAtOnce && command.tryAtOnce())
        catch {
          case failure: Throwable =>
            command.release()
            handle.leave()
            passTurn(command)
            throw failure
        }
      if finished then {
        command.release()
        handle.leave()
        passTurn(command)
        true
      } else {
        command.onRing(own.ring)
        try submit(command)
        catch {
          case failure: Throwable =>
            command.discardSubmission() // what it staged, and a connecting socket
            command.release()
            handle.leave()
            passTurn(command)
            throw failure
        }
        if command.hasDeadline then addTimer(command)
        false
      }
    }

    /** Prepares an entry for `command`, which the next wait submits. */
    private def submit(command: Command): Unit = {
      val b = broken
      if b != null then throw new java.io.IOException("The io_uring worker's ring failed", b)
      val o = own
      val sqe = o.ring.nextSqe()
      try command.prepareSubmission(sqe)
      catch {
        case failure: Throwable =>
          o.ring.nop(sqe)
          throw failure
      }
      val id = o.nextId
      o.nextId += 1
      sqe.user_data = id.toULong
      command.id = id
      command.sqe = sqe
      command.state = Command.Prepared
      o.inFlight(id) = command
      o.prepared += command
    }

    /** Set once the ring has failed for good: the worker then runs its timers and inbox without it, parking between
      * them, and fails every op that would use it.
      */
    @volatile private var broken: Throwable | Null = null

    override protected def wake(): Unit =
      if broken == null then own.ring.wake() else java.util.concurrent.locks.LockSupport.unpark(workerThread)

    @volatile private var workerThread: Thread | Null = null

    override def waitForDeadline(deadlineTask: Long): Unit =
      if broken != null then {
        if isEmpty() then
          if deadlineTask < 0 then java.util.concurrent.locks.LockSupport.park()
          else java.util.concurrent.locks.LockSupport.parkNanos(math.max(deadlineTask - System.nanoTime(), 0L))
      } else
        try own.ring.await(deadlineTask, !isEmpty())(dispatch)
        catch case failure: Throwable => fail(failure)

    /** The ring failed: it is torn down, so that the kernel lets go of every entry, and the ops it had are failed. */
    private def fail(cause: Throwable): Unit = {
      workerThread = Thread.currentThread()
      broken = cause
      System.err.println(s"[gears-io-uring] a worker's ring failed; its ops fail from now on: $cause")
      val o = own
      o.ring.close()
      val held = o.inFlight.values.toList
      o.inFlight.clear()
      o.prepared.clear()
      o.submitted.clear()
      for command <- held do {
        command.state = Command.Idle
        command.discardSubmission()
        try settle(command, Failure(new java.io.IOException("The io_uring worker's ring failed", cause)))
        catch case e: Throwable => e.printStackTrace()
      }
    }

    /** Applies the completion of the entry with user data `id`, unless the entry was withdrawn, and fires its op, or
      * submits it again.
      */
    private val dispatch: (Long, Int) => Unit = (id, res) => {
      val command = own.inFlight.getOrNull(id)
      if command != null then {
        own.inFlight -= id
        command.state = Command.Idle
        val handle = command.handle
        // The outcome, or null if it was submitted again; settled outside the `try`, so that a settle that throws is
        // not settled a second time. None succeeds once a close has begun, as `Ops.close` has it, even having
        // finished: its buffer and a file's offset are left as they were, and what it made, an accepted connection,
        // is let go of.
        val outcome: scala.util.Try[Unit] | Null =
          if handle.isClosing then {
            command.dropCompletion(res)
            command.discardSubmission()
            Failure(handle.closedFailure)
          } else if res == -errno.ECANCELED && command.wantOut then {
            command.discardSubmission()
            Failure(gears.async.cancellation())
          } else
            try
              if command.finishSubmission(res) then {
                command.done = true
                WorkpoolScheduler.SuccessUnit
              } else if handle.isClosing then {
                command.discardSubmission()
                Failure(handle.closedFailure)
              } else if command.wantOut then {
                command.discardSubmission()
                Failure(gears.async.cancellation())
              } else
                try {
                  submit(command)
                  null
                } catch {
                  case failure: Throwable =>
                    command.discardSubmission()
                    Failure(failure)
                }
            catch
              case failure: Throwable => Failure(failure) // the command let go of what it held as it failed
        // What a settle throws, a turn that cannot be passed on, is the next command's, not the ring's.
        if outcome != null then
          try settle(command, outcome)
          catch case e: Throwable => e.printStackTrace()
      }
    }

    /** Ends `command`'s use of its handle, and fires it with `outcome`, unless its time limit fired it already. */
    private def settle(command: Command, outcome: scala.util.Try[Unit]): Unit = {
      command.outcome = outcome
      command.release()
      command.handle.leave()
      passTurn(command)
      command.applied = true
      fire(command)
    }

    /** Ends `command`'s turn on its handle, if it takes turns, starting the next command's on that one's worker. */
    private def passTurn(command: Command): Unit = {
      val turns = command.turns
      if turns != null then {
        val next = turns.pass(command)
        if next != null then next.owner.nn.startTurn(next)
      }
    }

    /** Starts `command`, whose turn it is now, or which a close ended the wait of, on this worker: from any thread,
      * through the inbox. One withdrawn meanwhile, or whose handle is closing, is fired as such.
      */
    private[uring] def startTurn(command: Command): Unit = {
      val run: Runnable = () => {
        val handle = command.handle
        if command.wantOut || handle.isClosing then
          settle(command, Failure(if handle.isClosing then handle.closedFailure else gears.async.cancellation()))
        else
          try {
            val finished = command.nothingToDo || (handle.triesAtOnce && command.tryAtOnce())
            if finished then {
              command.done = true
              settle(command, WorkpoolScheduler.SuccessUnit)
            } else {
              command.onRing(own.ring)
              submit(command)
            }
          } catch case failure: Throwable => settle(command, Failure(failure))
      }
      if !submit(run) then throw new IllegalStateException("The io_uring worker has stopped")
    }

    private def fire(command: Command): Unit =
      if !command.hasDeadline || command.offHeap || removeTimer(command) then command.timerWakeup(System.nanoTime())

    /** Takes `command` back on this worker's thread: true if it never reached the kernel, or the kernel has let go of
      * it without doing it, its use of its handle then ended; false if it finished first, its completion in the ring.
      */
    private def withdrawHere(command: Command): Boolean = command.state match {
      case _ if command.turns != null && command.turns.nn.leaveWaiting(command) =>
        command.release()
        command.handle.leave()
        true
      case Command.Prepared =>
        own.ring.nop(command.sqe)
        withdrawn(command)
        true
      case Command.Submitted =>
        val res = own.ring.cancelNow(command.id)
        if res == 0 then {
          withdrawn(command)
          true
        } else false
      case _ => false
    }

    private def withdrawn(command: Command): Unit = {
      own.inFlight.remove(command.id)
      command.state = Command.Idle
      command.discardSubmission()
      command.release()
      command.handle.leave()
      passTurn(command)
    }

    /** As `Worker.withdraw`, and takes a command back from the ring: here, if this is its worker's thread, else by its
      * worker, which then fires it as cancelled (false: the performer waits for that).
      */
    override def withdraw(op: UringOp): Boolean = op match {
      case command: Command =>
        if command.hasDeadline then {
          if !removeTimer(command) then return false // it left the heap to fire
          command.offHeap = true
        }
        command.wantOut = true
        if isWorkerOf(this) then withdrawHere(command)
        else {
          val handedOver = submit(() =>
            if withdrawHere(command) then {
              command.outcome = Failure(gears.async.cancellation())
              fire(command)
            }
          )
          if !handedOver then throw new IllegalStateException("The io_uring worker has stopped")
          false
        }
      case _: CloseHandle        => false // it ends once begun
      case task: BlockingTask[?] =>
        if task.hasDeadline then {
          if !removeTimer(task) then return false // it left the heap to fire
          task.offHeap = true
        }
        task.stop()
      case _ => super.withdraw(op)
    }

    /** For a command whose time limit has passed, on this worker's thread (`AbstractTimerHeapOp.limitReached`). */
    private[uring] def limitReached(command: Command): Boolean = {
      command.wantOut = true
      // One whose outcome was set as its time ran out, with a failure, is out of use: it fails with `TimeoutException`.
      if command.applied || withdrawHere(command) then true
      else {
        command.offHeap = true // its completion, in the ring, fires it
        false
      }
    }

    override def loop(): Unit =
      try super.loop()
      finally own.ring.close()
  }
}

private[uring] object UringScheduler {

  /** A runtime of `workers` workers, each with its ring, made here, so that one that cannot be set up, past a limit on
    * locked memory or on rings, fails this call rather than its worker's thread.
    */
  def apply(workers: Int): UringScheduler = {
    val rings = new Array[Ring](workers)
    var made = 0
    try
      while made < workers do {
        rings(made) = new Ring(Entries)
        made += 1
      }
    catch {
      case e: Throwable =>
        for i <- 0 until made do rings(i).close()
        throw e
    }
    new UringScheduler(rings)
  }

  /** Submission queue entries of each worker's ring. */
  inline val Entries = 256
}
