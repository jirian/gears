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

  /** Registers an op with its cancel action and runs `submit`.
    *
    * @param cancel
    *   cancels the op, for `close`
    * @param submit
    *   submits the op, given its registration
    * @return
    *   the registration, or null if this guard is already closed
    */
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

  /** Records that an op has finished. If this guard is closed and this was the last op, the deferred close runs now.
    *
    * @param registration
    *   what `submit` returned for the op
    */
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

  /** Returns whether this guard is closed. */
  def isClosed: Boolean = synchronized(closed)

  /** Runs `body` if this guard is open, else throws `ClosedChannelException`.
    *
    * @tparam A
    *   the result type
    * @param body
    *   what to run
    * @return
    *   the result of `body`
    */
  def whileOpen[A](body: => A): A = synchronized:
    if closed then throw java.nio.channels.ClosedChannelException()
    body

  /** Closes this guard: runs `releaseHandle` now if no op is in flight, else cancels them all and runs it after the
    * last has finished. Closing twice does nothing.
    *
    * @param releaseHandle
    *   closes the handle
    */
  def close(releaseHandle: => Unit): Unit = close((), releaseHandle)

  /** Closes this guard like the other `close`, but first runs `closing` at once, in the call that closes this guard. It
    * is for cleanup that must not wait for the ops in flight, such as removing a socket's path before another socket
    * can be bound to it. If `closing` throws, this guard still closes, and the exception is rethrown afterwards.
    *
    * @param closing
    *   the cleanup to run at once
    * @param releaseHandle
    *   closes the handle
    */
  def close(closing: => Unit, releaseHandle: => Unit): Unit =
    var failure: Throwable | Null = null
    val (toCancel, immediate) = synchronized:
      if closed then (Nil, false)
      else
        closed = true
        // Under the lock, so the last op's release cannot close the handle before this has run.
        try closing
        catch case t: Throwable => failure = t
        if inFlight.isEmpty then (Nil, true)
        else
          closeHandle = () => releaseHandle
          (inFlight.toList, false)
    if immediate then releaseHandle
    toCancel.foreach(_.cancel())
    if failure != null then throw failure.nn

object IoGuard:
  final class Registration private[io] (val cancel: () => Unit)
