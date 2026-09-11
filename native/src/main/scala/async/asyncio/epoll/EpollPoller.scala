package gears.async.asyncio.epoll

import gears.async.Async
import gears.async.Async.Source
import gears.async.Cancellable
import gears.async.Listener
import gears.async.asyncio.epoll.unsafe.epoll._
import gears.async.asyncio.epoll.unsafe.epollImplicits._

import java.io.Closeable
import java.nio.channels.ReadPendingException
import java.util.concurrent.CancellationException
import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.duration._
import scala.scalanative.libc.errno
import scala.scalanative.posix.errno.EINTR
import scala.scalanative.posix.fcntl
import scala.scalanative.posix.string
import scala.scalanative.posix.unistd
import scala.scalanative.runtime.Intrinsics
import scala.scalanative.runtime._
import scala.scalanative.unsafe.Zone
import scala.scalanative.unsafe._
import scala.scalanative.unsigned._
import scala.util.Failure
import scala.util.Success
import scala.util.Try

class IOException(error: Int) extends Exception:
  override def toString(): String =
    "IO Error: " + fromCString(string.strerror(error))

class EpollPoller(epfd: Int) extends Closeable, Poller:

  override def close(): Unit =
    if unistd.close(epfd) != 0 then throw IOException(errno.errno)

  // store handles only for GC purposes
  val handles = mutable.Set[PollHandle]()
  var interruptHandle: Option[CInt] = None

  override def wake() = interruptHandle.foreach: fd =>
    val size = 8.toUInt
    val buf = stackalloc[Byte](size)
    !buf = 1
    unistd.write(fd, buf, size)

  override def poll(timeout: Duration) =
    Zone.acquire: zone =>
      if timeout.toNanos != 0 then
        val pipeFds = alloc[CArray[CInt, Nat._2]]()(using zone)
        if unistd.pipe(pipeFds.at(0)) < 0 then throw IOException(errno.errno)
        val (_, cancel) = registerFd(!pipeFds.at(0), true, false)
        interruptHandle = Some(!pipeFds.at(1))
        try pollImpl(timeout)(using zone)
        finally
          cancel.cancel()
          interruptHandle = None
          unistd.close(!pipeFds.at(0))
          unistd.close(!pipeFds.at(1))
      else pollImpl(timeout)(using zone)

  private def pollImpl(timeout: Duration)(using Zone) =
    val MAX_EVENTS = 64
    val events = alloc[epoll_event](MAX_EVENTS)
    @tailrec def loop(timeoutSecs: Int): Unit =
      val count = epoll_wait(epfd, events, MAX_EVENTS, timeoutSecs)
      if count < 0 then
        if errno.errno == EINTR then ()
        else throw IOException(errno.errno)
      else
        for i <- 0 until count do
          val event = events + i
          val handle = Intrinsics
            .castRawPtrToObject(toRawPtr(event.data))
            .asInstanceOf[PollHandle]
          handle.notify(event.events.toInt)
          // println(s"Notifying ${handle.fd} with ${event.events}")
        if count == MAX_EVENTS then loop(0)

    val timeoutSecs =
      if timeout < 0.seconds then -1 else timeout.toSeconds.toInt
    loop(timeoutSecs)

  def registerFd(
      fd: Int,
      read: Boolean,
      write: Boolean
  ): (PollHandle, Cancellable) =
    val handle = PollHandle(fd)
    Zone.acquire: zone =>
      val event = alloc[epoll_event]()(using zone)
      event.events =
        (EPOLLET | (if read then EPOLLIN else 0) | (if write then EPOLLOUT
                                                    else 0)).toUInt
      event.data = fromRawPtr(Intrinsics.castObjectToRawPtr(handle))
      // get the fd flags and modify it if needed
      val statusFlags = fcntl.fcntl(fd, fcntl.F_GETFL, 0)
      if statusFlags < 0 then throw IOException(errno.errno)
      if (statusFlags & fcntl.O_NONBLOCK) == 0 then
        if fcntl.fcntl(
            fd,
            fcntl.F_SETFL,
            (statusFlags | fcntl.O_NONBLOCK)
          ) < 0
        then throw IOException(errno.errno)
      if epoll_ctl(epfd, EPOLL_CTL_ADD, fd, event) == -1 then
        throw IOException(errno.errno)
      handles += handle

      val cancel: Cancellable = () =>
        epoll_ctl(epfd, EPOLLONESHOT, fd, null)
        handle.cancelMonitors()
        handles -= handle
      (handle, cancel)

class MonitorChange() extends Source[Try[Int]], Cancellable:
  // Increases every time the monitor is updated.
  @volatile private var _counter = 0
  private val listeners: mutable.Set[Listener[Try[Int]]] =
    mutable.Set()

  def counter = _counter

  override def poll(k: Listener[Try[Int]]): Boolean =
    false // assume nothing is coming until update()
  override def onComplete(k: Listener[Try[Int]]): Unit = synchronized:
    listeners += k
  override def dropListener(k: Listener[Try[Int]]): Unit = synchronized:
    listeners -= k

  /* Returns whether the counter has been updated from the current known state. */
  def poll(current: Int = 0) = _counter != current

  def onUpdate(listener: Listener[Try[Int]], current: Int): Unit =
    val runNow = synchronized:
      if current != _counter then true
      else
        listeners += listener
        false
    if runNow then listener.completeNow(Success(_counter), this)

  def onUpdate(current: Int)(using Async): Int =
    if current != _counter then _counter
    else this.await

  def cancel() =
    val toLoop = synchronized:
      val ls = listeners.toSeq
      listeners.clear()
      ls
    for listener <- toLoop do
      listener.completeNow(Failure(CancellationException()), this)

  // Increment the counter and trigger the listener if it exists.
  def update() =
    val n = _counter + 1
    _counter = n
    val toLoop = synchronized:
      val ls = listeners.toSeq
      listeners.clear()
      ls
    for listener <- toLoop do listener.completeNow(Success(n), this)

class PollHandle(val fd: Int):
  val read = MonitorChange()
  val write = MonitorChange()

  def notify(mask: Int) =
    if ((mask & EPOLLIN) > 0) then read.update()
    if ((mask & EPOLLOUT) > 0) then write.update()

  def cancelMonitors() =
    read.cancel()
    write.cancel()
