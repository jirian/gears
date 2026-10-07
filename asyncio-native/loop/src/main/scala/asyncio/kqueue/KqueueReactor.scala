package asyncio.kqueue

import java.io.IOException
import scala.scalanative.posix.unistd
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import asyncio.Completion
import asyncio.Handles
import asyncio.Interest
import asyncio.Ops
import asyncio.Reactor
import asyncio.Repeatable
import asyncio.unsafe.KqueueLoop
import asyncio.unsafe.KqueueLoop.Event
import asyncio.unsafe.PosixErr.cError
import asyncio.unsafe.PosixSockets

/** The kqueue implementation of `Reactor`, for ops built by `KqueueOps`. A command waits for readiness with a one-shot
  * filter and is performed on the loop thread; a timer is a kqueue timer; resolved promises wake the loop through a
  * pipe. Each submission is its own kevent call, and each poll returns up to `maxEvents` events.
  */
final class KqueueReactor private (val kq: Int, maxEvents: Int) extends Reactor {

  /** Every kqueue reactor runs the same kinds of op, so kqueue-specific code may build them directly. Code that only
    * sees the `Reactor` interface still cannot mix ops between reactors.
    */
  type Op = KqueueOp

  def ops: Ops[Op] = KqueueOps

  /** Kqueue watches POSIX file descriptors, so a handle is the descriptor itself. */
  def handles: Handles = KqueueHandles

  private final class Pending[C <: Op](val op: C, val completion: Completion[C]) {
    def complete(): Unit = completion.onComplete(op)
    def fail(failure: Throwable): Unit = completion.onFailure(op, failure)
    def cancelled(): Unit = completion.onCancel(op)
  }

  // Promises resolve on other threads; they queue here and a byte on the pipe interrupts a blocked poll.
  private val resolvedQueue = new java.util.concurrent.ConcurrentLinkedQueue[KqueuePromise]()
  private val (wakeRead, wakeWrite) = {
    val ends = stackalloc[CInt](2)
    if unistd.pipe(ends) < 0 then throw new IOException(s"Failed to create wake pipe: ${cError()}")
    PosixSockets.setNonBlocking(ends(0))
    PosixSockets.setNonBlocking(ends(1))
    (ends(0), ends(1))
  }
  KqueueLoop.createAndRegisterEvents(kq, 1) { events =>
    KqueueLoop.addFile(events(0), wakeRead, read = true, clear = true) // edge-triggered, registered once
  }

  private final class Slot {
    var read: Pending[?] | Null = null
    var write: Pending[?] | Null = null
  }

  private var slots = new Array[Slot | Null](64)
  private val timers = scala.collection.mutable.HashMap.empty[Int, Pending[?]]
  private var nextTimerId = 1
  private var running = false

  // Promises submitted here whose completion has not run, so `close` can cancel them. Loop thread only.
  private val pendingPromises = scala.collection.mutable.HashSet.empty[KqueuePromise]

  // Cancelled blockers whose `block` was still running, so their `onCancel` waits for it to return. Loop thread only.
  private val cancellingBlockers = scala.collection.mutable.HashSet.empty[KqueueBlocking]

  // Guards the wake pipe against a blocker finishing after `close`, when its descriptor number may already be reused.
  private object wakeLock {}
  private var closed = false

  def submit[C <: Op](op: C, completion: Completion[C]): Unit = {
    op match {
      case _: Repeatable => ()
      case one: KqueueOp =>
        if one.submitted then throw new IllegalStateException(s"$op is not Repeatable and was already submitted")
    }
    val pending = new Pending(op, completion)
    op match {
      case command: Command =>
        val slot = slotFor(command.fd)
        command.interest match {
          case Interest.Read =>
            require(slot.read == null, s"A read command is already pending on descriptor ${command.fd}")
            slot.read = pending
          case Interest.Write =>
            require(slot.write == null, s"A write command is already pending on descriptor ${command.fd}")
            slot.write = pending
        }
        arm(command.fd, read = command.interest == Interest.Read)
      case timer: KqueueTimer =>
        require(timer.id < 0 || !timers.contains(timer.id), "This timer is already pending")
        if timer.id < 0 then
          timer.assign(nextTimerId)
          nextTimerId += 1
        timers(timer.id) = pending
        KqueueLoop.createAndRegisterEvents(kq, 1) { events =>
          KqueueLoop.addTimerOneShot(events(0), timer.id.toUSize, timer.milliseconds)
        }
      case promise: KqueuePromise =>
        promise match {
          case blocking: KqueueBlocking if blocking.cancelling != null =>
            throw new IllegalStateException(s"$op is still running after being cancelled")
          case _ => ()
        }
        promise.reactor = this
        promise.pending = pending
        pendingPromises += promise
        promise match {
          case blocking: KqueueBlocking => Blockers.run(blocking)
          case _                        => () // Completed by whoever holds it.
        }
    }
    op.submitted = true
  }

  /** Called from any thread when a promise has its result: queue it and wake the loop. */
  private[kqueue] def resolved(promise: KqueuePromise): Unit = wakeLock.synchronized {
    if closed then return // Its completion was dropped by `close`.
    resolvedQueue.add(promise)
    val signal = stackalloc[Byte]()
    !signal = 1
    unistd.write(wakeWrite, signal, 1.toCSize) // A full pipe already guarantees a wake-up.
    ()
  }

  /** Runs the completions of every promise resolved so far, on the loop thread. */
  private def deliverResolved(): Unit = {
    var promise = resolvedQueue.poll()
    while promise != null do {
      val pending = promise.pending
      promise.pending = null
      pendingPromises -= promise
      promise match {
        case blocking: KqueueBlocking if blocking.cancelling != null =>
          // A cancelled blocker's `block` has returned, so its op's buffers and slots are the caller's again. Its
          // outcome is dropped: `cancel` detached it before deferring, and `submit` refuses it until now.
          assert(pending == null, "a blocker awaiting its cancel was resubmitted")
          val cancelled = blocking.cancelling.asInstanceOf[Pending[?]]
          blocking.cancelling = null
          cancellingBlockers -= blocking
          cancelled.cancelled()
        case _ =>
          if pending != null then {
            val p = pending.asInstanceOf[Pending[?]]
            val failure = promise.failure
            if failure != null then p.fail(failure.nn) else p.complete()
          }
      }
      promise = resolvedQueue.poll()
    }
  }

  /** Empties the wake pipe after its edge fired. */
  private def drainWakePipe(): Unit = {
    val signals = stackalloc[Byte](64)
    while unistd.read(wakeRead, signals, 64.toCSize) > 0 do ()
  }

  /** Cancels every pending promise, interrupting blockers, then closes the wake pipe and the kqueue. The kqueue's own
    * filters and timers go with it. A blocker still running is waited for before its `onCancel`.
    */
  def close(): Unit = {
    if !closed then {
      // Detach every pending op first, so nothing can be delivered while the reactor shuts down.
      val dropped = scala.collection.mutable.ArrayBuffer.empty[Pending[?]]
      for slot <- slots if slot != null do {
        if slot.nn.read != null then dropped += slot.nn.read.nn
        if slot.nn.write != null then dropped += slot.nn.write.nn
      }
      java.util.Arrays.fill(slots.asInstanceOf[Array[AnyRef]], null)
      dropped ++= timers.values
      timers.clear()
      val running = scala.collection.mutable.ArrayBuffer.empty[KqueueBlocking]
      for promise <- pendingPromises.toList do {
        val pending = detach(promise)
        if pending != null then {
          dropped += pending.nn
          promise match {
            case blocking: KqueueBlocking if blocking.interrupt() => running += blocking
            case _                                                => ()
          }
        }
      }
      for blocking <- cancellingBlockers do {
        dropped += blocking.cancelling.asInstanceOf[Pending[?]]
        blocking.cancelling = null
        running += blocking
      }
      cancellingBlockers.clear()
      resolvedQueue.clear()
      wakeLock.synchronized {
        closed = true
        PosixSockets.close(wakeRead)
        PosixSockets.close(wakeWrite)
      }
      KqueueLoop.close(kq) // Its filters and timers go with it.
      running.foreach(_.awaitEnd()) // Until then they may still write to their ops' buffers and slots.
      // Only now tell the completions, so one that throws cannot leak the reactor's resources.
      dropped.foreach(_.cancelled())
    }
  }

  // Polled events, only meaningful while the reactor dispatches them.

  private def isReadEvent(event: Event): Boolean = KqueueLoop.isReadEvent(event)
  private def isWriteEvent(event: Event): Boolean = KqueueLoop.isWriteEvent(event)
  private def fileIdent(event: Event): Int = KqueueLoop.fileIdent(event)

  /** A pending command is removed from its slot and its filter deleted; a pending timer is deleted; a promise is
    * detached, so completing it later does nothing, and a blocking task is skipped or interrupted. The completion's
    * `onCancel` then runs before this returns, except for a blocking task that was running: its `onCancel` runs from
    * the loop once `block` has returned.
    */
  def cancel(op: Op): Boolean = {
    val pending: Pending[?] | Null = op match {
      case command: Command =>
        val slot = if command.fd >= 0 && command.fd < slots.length then slots(command.fd) else null
        if slot == null then null
        else {
          val read = command.interest == Interest.Read
          val p = if read then slot.read else slot.write
          if p == null || !(p.nn.op.asInstanceOf[AnyRef] eq command) then null
          else {
            if read then slot.read = null else slot.write = null
            deleteFilter(command.fd, read)
            p
          }
        }
      case timer: KqueueTimer =>
        val p = if timer.id < 0 then None else timers.remove(timer.id)
        p.foreach { _ =>
          KqueueLoop.createAndRegisterEvents(kq, 1) { events =>
            KqueueLoop.deleteTimer(events(0), timer.id.toUSize)
          }
        }
        p.orNull
      case promise: KqueuePromise => detach(promise)
    }
    if pending == null then false
    else {
      op match {
        case blocking: KqueueBlocking if blocking.interrupt() =>
          blocking.cancelling = pending
          cancellingBlockers += blocking
        case _ => pending.nn.cancelled()
      }
      true
    }
  }

  /** Deletes a pending command's one-shot filter so it cannot wake the loop for nothing. If the caller already closed
    * the descriptor, kqueue removed the filter with it and reports `ENOENT` or `EBADF`; the op is cancelled all the
    * same.
    */
  private def deleteFilter(fd: Int, read: Boolean): Unit = KqueueLoop.deleteFileIfOpen(kq, fd, read)

  /** Detaches a pending promise so its outcome is never delivered. The caller skips or interrupts a blocking task. */
  private def detach(promise: KqueuePromise): Pending[?] | Null = {
    val pending = promise.pending
    promise.pending = null
    pendingPromises -= promise
    pending.asInstanceOf[Pending[?] | Null]
  }

  def stop(): Unit = running = false

  private def slotFor(fd: Int): Slot = {
    require(fd >= 0, "Descriptor must be non-negative")
    if fd >= slots.length then slots = java.util.Arrays.copyOf(slots, math.max(slots.length * 2, fd + 1))
    var slot = slots(fd)
    if slot == null then {
      slot = new Slot
      slots(fd) = slot
    }
    slot.nn
  }

  /** One kevent call per command: the filter is one-shot, so nothing fires without a command waiting for it. */
  private def arm(fd: Int, read: Boolean): Unit =
    KqueueLoop.createAndRegisterEvents(kq, 1) { events =>
      KqueueLoop.addFile(events(0), fd, read = read, clear = false, oneShot = true)
    }

  /** Finds the command an event is for, performs it, and hands its completion the result. The slot is cleared first so
    * the completion may submit the next command.
    */
  private def dispatch(event: Event): Unit = {
    val ident = fileIdent(event)
    if ident == wakeRead && isReadEvent(event) then drainWakePipe() // promises are delivered by the loop itself
    else if KqueueLoop.isTimerEvent(event) then timers.remove(ident).foreach(_.complete())
    else {
      val slot = if ident < slots.length then slots(ident) else null
      if slot != null then {
        val write = isWriteEvent(event)
        val pending = if write then slot.write else slot.read
        if pending != null then {
          if write then slot.write = null else slot.read = null
          val p = pending.nn
          p.op match {
            case command: Command =>
              // A throwing command fails its own op; an exception thrown by the completion itself still escapes.
              var failure: Throwable | Null = null
              val done =
                try command.perform()
                catch {
                  case t: Throwable =>
                    failure = t
                    true
                }
              if failure != null then p.fail(failure.nn)
              else if done then p.complete()
              else {
                // Not ready after all: wait again, without the completion ever seeing it.
                if write then slot.write = p else slot.read = p
                arm(ident, read = !write)
              }
            case _ => () // Only commands live in descriptor slots.
          }
        }
      }
    }
  }

  /** Error events throw. */
  def run(): Unit =
    KqueueLoop.pollQueue(maxEvents) { events =>
      running = true
      while running do {
        deliverResolved() // Interleave resolved promises with polled events.
        if running then {
          val polled = KqueueLoop.pollEventsForever(kq, events, maxEvents)
          var i = 0
          while i < polled && running do {
            val event = events(i)
            if KqueueLoop.isError(event) then throw new IOException(s"Event error: ${KqueueLoop.errno(event)}")
            dispatch(event)
            i += 1
          }
        }
      }
    }
}

object KqueueReactor {

  /** Opens a reactor on a new kqueue; each poll returns up to `maxEvents` events. Close it when done. */
  def open(maxEvents: Int = 255): KqueueReactor = new KqueueReactor(KqueueLoop.open(), maxEvents)

  /** Opens a reactor for `body` and closes it afterwards. */
  def scoped(maxEvents: Int = 255)(body: KqueueReactor => Unit): Unit = {
    val reactor = open(maxEvents)
    try body(reactor)
    finally reactor.close()
  }
}
