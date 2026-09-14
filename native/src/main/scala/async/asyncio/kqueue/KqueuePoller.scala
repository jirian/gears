package gears.async.asyncio.kqueue

import gears.async.Cancellable
import gears.async.asyncio.epoll.{IOException, PollHandle, Poller}
import gears.async.asyncio.kqueue.unsafe.kqueue._
import gears.async.asyncio.kqueue.unsafe.kqueueImplicits._

import java.io.Closeable
import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.duration._
import scala.scalanative.libc.errno
import scala.scalanative.posix.errno.EINTR
import scala.scalanative.posix.fcntl
import scala.scalanative.posix.time.timespec
import scala.scalanative.posix.unistd
import scala.scalanative.runtime.Intrinsics
import scala.scalanative.runtime._
import scala.scalanative.unsafe.Zone
import scala.scalanative.unsafe._
import scala.scalanative.unsigned._

/** UNVERIFIED (see unsafe/kqueue.scala) - structural port of
  * [[gears.async.asyncio.epoll.EpollPoller]] onto kqueue/kevent. Reuses
  * [[PollHandle]]/[[IOException]]/[[Poller]] from the epoll package since
  * none of those are actually epoll-specific: they're generic "readiness
  * event source" bookkeeping that both backends need identically.
  */
class KqueuePoller(kq: Int) extends Closeable, Poller:

  override def close(): Unit =
    if unistd.close(kq) != 0 then throw IOException(errno.errno)

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
    val events = alloc[kevent](MAX_EVENTS)

    val zeroTs = alloc[timespec]()
    zeroTs._1 = 0
    zeroTs._2 = 0

    val initialTs: Ptr[timespec] =
      if timeout < 0.seconds then null
      else
        val ts = alloc[timespec]()
        ts._1 = timeout.toSeconds.toInt
        ts._2 = (timeout - timeout.toSeconds.seconds).toNanos.toInt
        ts

    @tailrec def loop(ts: Ptr[timespec]): Unit =
      val count = kevent(kq, null, 0, events, MAX_EVENTS, ts)
      if count < 0 then
        if errno.errno == EINTR then ()
        else throw IOException(errno.errno)
      else
        var i = 0
        while i < count do
          val event = events + i
          val handle = Intrinsics
            .castRawPtrToObject(toRawPtr(event.udata))
            .asInstanceOf[PollHandle]
          // kqueue reports read/write readiness as separate events (unlike
          // epoll's single bitmask), so route directly instead of going
          // through PollHandle.notify.
          if event.filter == EVFILT_READ.toShort then handle.read.update()
          else if event.filter == EVFILT_WRITE.toShort then handle.write.update()
          i += 1
        if count == MAX_EVENTS then loop(zeroTs)

    loop(initialTs)

  def registerFd(
      fd: Int,
      read: Boolean,
      write: Boolean
  ): (PollHandle, Cancellable) =
    val handle = PollHandle(fd)
    Zone.acquire: zone =>
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

      val nchanges = (if read then 1 else 0) + (if write then 1 else 0)
      if nchanges > 0 then
        val changes = alloc[kevent](nchanges)(using zone)
        val udata = fromRawPtr[Byte](Intrinsics.castObjectToRawPtr(handle))
        var i = 0
        if read then
          val e = changes + i
          e.ident = fd.toUSize; e.filter = EVFILT_READ.toShort
          e.flags = (EV_ADD | EV_CLEAR).toUShort; e.fflags = 0.toUInt
          e.data = 0; e.udata = udata
          i += 1
        if write then
          val e = changes + i
          e.ident = fd.toUSize; e.filter = EVFILT_WRITE.toShort
          e.flags = (EV_ADD | EV_CLEAR).toUShort; e.fflags = 0.toUInt
          e.data = 0; e.udata = udata
          i += 1
        if kevent(kq, changes, nchanges, null, 0, null) < 0 then
          throw IOException(errno.errno)
      handles += handle

      val cancel: Cancellable = () =>
        Zone.acquire: zone =>
          val nchanges = (if read then 1 else 0) + (if write then 1 else 0)
          if nchanges > 0 then
            val changes = alloc[kevent](nchanges)(using zone)
            var i = 0
            if read then
              val e = changes + i
              e.ident = fd.toUSize; e.filter = EVFILT_READ.toShort; e.flags = EV_DELETE.toUShort
              i += 1
            if write then
              val e = changes + i
              e.ident = fd.toUSize; e.filter = EVFILT_WRITE.toShort; e.flags = EV_DELETE.toUShort
              i += 1
            // best-effort: fd may already be closed by the caller, ignore errors
            kevent(kq, changes, nchanges, null, 0, null)
        handle.cancelMonitors()
        handles -= handle
      (handle, cancel)
