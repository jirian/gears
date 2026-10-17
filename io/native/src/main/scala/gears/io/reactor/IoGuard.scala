package gears.io.reactor

import scala.collection.mutable

// The descriptor is closed only after every registered op has finished, so its number cannot be reused under one.
final class IoGuard:
  private var closed = false
  private val inFlight = mutable.HashSet.empty[IoGuard.Registration]
  private var closeHandle: (() => Unit) | Null = null

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

  def release(registration: IoGuard.Registration): Unit =
    val action = synchronized:
      inFlight -= registration
      if closed && inFlight.isEmpty then
        val action = closeHandle
        closeHandle = null
        action
      else null
    if action != null then action()

  def isClosed: Boolean = synchronized(closed)

  def whileOpen[A](body: => A): A = synchronized:
    if closed then throw java.nio.channels.ClosedChannelException()
    body

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
