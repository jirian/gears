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

private[uring] final class UringScheduler private (rings: Array[Ring])
    extends WorkpoolScheduler[UringOp, UringOps.type](UringBackend, UringOps, rings.length)
    with NativeSuspend {
  override protected def selectWorker() = workerOfCurrentThread()

  override def makeWorker(id: Int): UringWorker = new UringWorker(id, rings(id))

  private def isWorkerOf(worker: UringWorker): Boolean = worker.isCurrent

  private[uring] def failRing(cause: Throwable): Unit = workerOfCurrentThread().asInstanceOf[UringWorker].fail(cause)

  final class UringWorker(id: Int, givenRing: Ring)
      extends WorkpoolScheduler.Worker[UringOp, UringOps.type](id, UringOps) {

    private lazy val own = new Own

    private final class Own {
      val ring = givenRing

      val inFlight = scala.collection.mutable.LongMap.empty[Command]

      val prepared = scala.collection.mutable.ArrayBuffer.empty[Command]

      var nextId = Ring.InternalId + 1

      val submitted = scala.collection.mutable.ArrayBuffer.empty[Command]

      ring.onSubmit = () => {
        var closed: scala.collection.mutable.ArrayBuffer[Command] | Null = null
        prepared.foreach { c =>
          if c.state == Command.Stage.Prepared then {
            if c.handle.isClosing then {
              ring.nop(c.sqe)
              inFlight.remove(c.id)
              c.outOfRing()
              if closed == null then closed = scala.collection.mutable.ArrayBuffer.empty[Command]
              closed.nn += c
            } else {
              c.submitted()
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
        submitted.foreach(c =>
          if c.state == Command.Stage.Submitted && c.handle.isClosing then
            if ring.pending(c.sqe, c.id) then {
              ring.nop(c.sqe)
              inFlight.remove(c.id)
              c.outOfRing()
              c.discardSubmission()
              settle(c, Failure(c.handle.closedFailure))
            } else ring.cancelNow(c.id): Unit
        )
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
        if command.hasDeadline then addTimer(command)
        return false
      }
      var parked = false
      val finished =
        try command.doneAtOnce() || { parked = !command.readyToSubmit(); false }
        catch {
          case failure: Throwable =>
            endUse(command)
            throw failure
        }
      if finished then {
        endUse(command)
        true
      } else if parked then {
        if command.hasDeadline then addTimer(command)
        false
      } else {
        command.onRing(own.ring)
        try submit(command)
        catch {
          case failure: Throwable =>
            command.discardSubmission()
            endUse(command)
            throw failure
        }
        if command.hasDeadline then addTimer(command)
        false
      }
    }

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
      command.prepared(id, sqe)
      o.inFlight(id) = command
      o.prepared += command
    }

    @volatile private var broken: Throwable | Null = null

    override protected def wake(): Unit =
      if !isCurrent then
        if broken == null then own.ring.wake() else java.util.concurrent.locks.LockSupport.unpark(workerThread)

    @volatile private var workerThread: Thread | Null = null

    override def waitForDeadline(deadlineTask: Long): Unit =
      if broken != null then {
        if isEmpty() then
          if deadlineTask < 0 then java.util.concurrent.locks.LockSupport.park()
          else java.util.concurrent.locks.LockSupport.parkNanos(math.max(deadlineTask - System.nanoTime(), 0L))
      } else
        try own.ring.await(deadlineTask, !isEmpty(), spin = own.inFlight.nonEmpty)(dispatch)
        catch case failure: Throwable => fail(failure)

    private[uring] def fail(cause: Throwable): Unit = {
      workerThread = Thread.currentThread()
      broken = cause
      System.err.println(s"[gears-io-uring] a worker's ring failed; its ops fail from now on: $cause")
      val o = own
      val held = o.inFlight.values.toList
      val letGo = held.map(c =>
        c.state != Command.Stage.Submitted || o.ring.pending(c.sqe, c.id) || {
          val res = o.ring.cancelNow(c.id)
          res == 0 || res == -errno.ENOENT
        }
      )
      o.ring.close()
      o.inFlight.clear()
      o.prepared.clear()
      o.submitted.clear()
      for (command, free) <- held.zip(letGo) do {
        command.outOfRing()
        if free then command.discardSubmission() else UringScheduler.kept.add(command)
        try settle(command, Failure(new java.io.IOException("The io_uring worker's ring failed", cause)))
        catch case e: Throwable => e.printStackTrace()
      }
    }

    private val dispatch: (Long, Int) => Unit = (id, res) => {
      val command = own.inFlight.getOrNull(id)
      if command != null then {
        own.inFlight -= id
        command.outOfRing()
        val handle = command.handle
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
                  if command.readyToSubmit() then submit(command)
                  null
                } catch {
                  case failure: Throwable =>
                    command.discardSubmission()
                    Failure(failure)
                }
            catch
              case failure: Throwable => Failure(failure)
        if outcome != null then
          try settle(command, outcome)
          catch case e: Throwable => e.printStackTrace()
      }
    }

    private def settle(command: Command, outcome: scala.util.Try[Unit]): Unit = {
      command.outcome = outcome
      endUse(command)
      command.applied = true
      fire(command)
    }

    private def endUse(command: Command): Unit = {
      command.release()
      command.handle.leave()
      passTurn(command)
    }

    private def passTurn(command: Command): Unit = {
      val turns = command.turns
      if turns != null then {
        val next = turns.pass(command)
        if next != null then next.owner.nn.startTurn(next)
      }
    }

    private[uring] def startTurn(command: Command): Unit = {
      val run: Runnable = () => {
        command.resume()
        val handle = command.handle
        if command.wantOut || handle.isClosing then
          settle(command, Failure(if handle.isClosing then handle.closedFailure else gears.async.cancellation()))
        else
          try {
            val finished = command.doneAtOnce()
            if finished then {
              command.done = true
              settle(command, WorkpoolScheduler.SuccessUnit)
            } else if command.readyToSubmit() then {
              command.onRing(own.ring)
              submit(command)
            }
          } catch case failure: Throwable => settle(command, Failure(failure))
      }
      if !submit(run) then throw new IllegalStateException("The io_uring worker has stopped")
    }

    private def fire(command: Command): Unit =
      if !command.hasDeadline || claimFiring(command) then
        command.timerWakeup(System.nanoTime())

    private def withdrawHere(command: Command): Boolean = command.state match {
      case _ if command.turns != null && command.turns.nn.leaveWaiting(command) =>
        command.release()
        command.handle.leave()
        true
      case Command.Stage.Parked =>
        if command.leaveParking() then {
          command.resume()
          command.discardSubmission()
          endUse(command)
          true
        } else false
      case Command.Stage.Prepared =>
        own.ring.nop(command.sqe)
        withdrawn(command)
        true
      case Command.Stage.Submitted if own.ring.pending(command.sqe, command.id) =>
        own.ring.nop(command.sqe)
        withdrawn(command)
        true
      case Command.Stage.Submitted =>
        val res = own.ring.cancelNow(command.id)
        if res == 0 then {
          withdrawn(command)
          true
        } else false
      case _ => false
    }

    private def withdrawn(command: Command): Unit = {
      own.inFlight.remove(command.id)
      command.outOfRing()
      command.discardSubmission()
      endUse(command)
    }

    override def withdraw(op: UringOp): Boolean = op match {
      case command: Command =>
        if command.hasDeadline && !takeOff(command) then return false
        command.wantOut = true
        if isWorkerOf(this) then withdrawHere(command)
        else {
          // - by the time the op's worker runs this, the op may have been performed again: act only on this performance
          val performance = command.performance
          val handedOver = submit(() =>
            if command.performance == performance && (command.owner eq this) && !command.isSettled &&
              withdrawHere(command)
            then {
              command.outcome = Failure(gears.async.cancellation())
              fire(command)
            }
          )
          if !handedOver then throw new IllegalStateException("The io_uring worker has stopped")
          false
        }
      case _: CloseHandle        => false
      case task: BlockingTask[?] =>
        if task.hasDeadline && !takeOff(task) then return false
        task.stop()
      case _ => super.withdraw(op)
    }

    private[uring] def limitReached(command: Command): Boolean = {
      command.wantOut = true
      if command.applied || withdrawHere(command) then true
      else {
        command.offHeap = true
        false
      }
    }

    override def loop(): Unit =
      try super.loop()
      finally own.ring.close()
  }
}

private[uring] object UringScheduler {

  private[uring] val kept = new java.util.concurrent.ConcurrentLinkedQueue[Command]()

  def apply(workers: Int): UringScheduler = {
    val rings = new Array[Ring](workers)
    var made = 0
    try
      while made < workers do {
        rings(made) = new Ring(Entries, Completions)
        made += 1
      }
    catch {
      case e: Throwable =>
        for i <- 0 until made do rings(i).close()
        throw e
    }
    new UringScheduler(rings)
  }

  inline val Entries = 256

  // - rings are locked memory (8 MiB by default), and the kernel frees a closed one late
  inline val Completions = 2048
}
