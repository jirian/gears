package gears.io.reactor

import scala.collection.mutable

/** The open/closed state of one handle and the ops in flight on it. `close` cancels those ops and defers the actual
  * close until the last one has finished.
  */
// The descriptor is closed only after every registered op has finished, so its number cannot be reused under one.
final class IoGuard:
  private var closed = false
  private val inFlight = mutable.ArrayBuffer.empty[IoGuard.Registration]
  private var closeHandle: (() => Unit) | Null = null

  /** Registers an op with its cancel action and runs `submit`, or returns null if the guard is already closed. */
  def submit(cancel: () => Unit)(submit: IoGuard.Registration => Unit): IoGuard.Registration | Null = synchronized:
    if closed then null
    else
      val registration = IoGuard.Registration(cancel)
      inFlight += registration
      try submit(registration)
      catch
        case t: Throwable =>
          inFlight -= registration
          throw t
      registration

  /** The op has finished. If the guard is closed and this was the last op, the deferred close runs now. */
  def release(registration: IoGuard.Registration): Unit =
    val action = synchronized:
      inFlight -= registration
      if closed && inFlight.isEmpty then
        val action = closeHandle
        closeHandle = null
        action
      else null
    // Outside the lock: the action closes the descriptor and may run other callbacks.
    if action != null then action()

  def isClosed: Boolean = synchronized(closed)

  /** Runs `body` if the guard is open, else throws `ClosedChannelException`. */
  def whileOpen[A](body: => A): A = synchronized:
    if closed then throw java.nio.channels.ClosedChannelException()
    body

  /** Closes the guard: runs `releaseHandle` now if no op is in flight, else cancels them all and runs it after the last
    * has finished. Closing twice does nothing.
    */
  def close(releaseHandle: => Unit): Unit =
    val (toCancel, immediate) = synchronized:
      if closed then (Nil, false)
      else
        closed = true
        if inFlight.isEmpty then (Nil, true)
        else
          closeHandle = () => releaseHandle
          (inFlight.toList, false)
    if immediate then releaseHandle
    toCancel.foreach(_.cancel())

object IoGuard:
  final class Registration private[io] (val cancel: () => Unit)
