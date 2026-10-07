package gears.async.asyncio.uring

import scala.collection.mutable

/** The open/closed state of one descriptor-owning resource (a stream, listener, socket or file) and the ops in flight
  * on it.
  *
  * Closing a descriptor does not cancel io_uring ops already using it: the kernel holds its own reference to the file,
  * so a pending accept or read would never finish. And an op submitted after the close would name a descriptor number
  * the process may already have reused for something else. So every op on the resource is registered and submitted
  * through [[submit]], under the guard's lock, and [[close]] takes that lock, refuses all later submissions, and
  * cancels the ops still in flight.
  */
private[uring] final class IoGuard:
  private var closed = false
  private val inFlight = mutable.HashSet.empty[IoGuard.Registration]

  /** Registers an op and runs `submit`, which hands it to the kernel, unless the resource is closed. Returns the
    * registration to [[release]] once the op finishes, or null if the resource is closed and nothing was submitted.
    * `cancel` asks the op's reactor to cancel it; it is called at most once, by [[close]].
    */
  def submit(cancel: () => Unit)(submit: => Unit): IoGuard.Registration | Null = synchronized:
    if closed then null
    else
      val registration = IoGuard.Registration(cancel)
      inFlight += registration
      try submit
      catch
        case t: Throwable =>
          inFlight -= registration
          throw t
      registration

  /** Forgets an op that finished, failed or was cancelled. */
  def release(registration: IoGuard.Registration): Unit = synchronized:
    inFlight -= registration

  def isClosed: Boolean = synchronized(closed)

  /** Runs a synchronous call on the descriptor, such as `shutdown(2)`, unless the resource is closed: it holds the lock
    * `close` takes, so the descriptor cannot be closed, and its number reused, during the call.
    */
  def whileOpen[A](body: => A): A = synchronized:
    if closed then throw java.nio.channels.ClosedChannelException()
    body

  /** Marks the resource closed and cancels every op in flight on it. Returns true the first time only, when the caller
    * should then close the descriptor: any op the kernel still holds keeps its own reference to the file.
    */
  def close(): Boolean =
    val toCancel = synchronized:
      if closed then null
      else
        closed = true
        val ops = inFlight.toList
        inFlight.clear()
        ops
    if toCancel == null then false
    else
      toCancel.foreach(_.cancel())
      true

private[uring] object IoGuard:
  final class Registration(val cancel: () => Unit)
