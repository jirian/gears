package asyncio.unsafe

import java.io.IOException
import scala.scalanative.annotation.alwaysinline
import scala.scalanative.bsd.kevent
import scala.scalanative.meta.LinktimeInfo
import scala.scalanative.posix.time.timespec
import scala.scalanative.posix.timeOps.*
import scala.scalanative.posix.unistd
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.USize
import scala.scalanative.unsigned.{*, given}

import PosixErr.cError
import asyncio.unsafe.Bracket.FileOperation

object KqueueLoop {

  // posixlib owns the native layout. Keep buffers opaque so callers cannot
  // accidentally advance by one byte instead of one event.
  opaque type Event = Ptr[Byte]
  opaque type EventQueue = Ptr[Byte]

  object EventQueue {
    extension (queue: EventQueue) inline def apply(inline index: Int): Event = eventAt(queue, index)
  }

  private val eventSize = kevent.scalanative_kevent_size()

  @alwaysinline def eventAt(events: EventQueue, index: Int): Event =
    events + eventSize * index.toCSize

  def debugEvent(evt: Event): Unit = {
    val ident = stackalloc[USize]()
    val filter = stackalloc[CShort]()
    val flags = stackalloc[CUnsignedShort]()
    val fflags = stackalloc[CUnsignedInt]()
    val data = stackalloc[Size]()
    val udata = stackalloc[CVoidPtr]()
    kevent.scalanative_kevent_get(evt, 0, ident, filter, flags, fflags, data, udata)
    println(
      s"kevent{ident=${!ident}, filter=${!filter}, flags=${!flags}, fflags=${!fflags}, data=${!data}, udata=${!udata}}"
    )
  }

  @alwaysinline
  def ident(evt: Event): USize = {
    val value = stackalloc[USize]()
    kevent.scalanative_kevent_get(evt, 0, value, null, null, null, null, null)
    !value
  }

  @alwaysinline
  def filter(evt: Event): CShort = {
    val value = stackalloc[CShort]()
    kevent.scalanative_kevent_get(evt, 0, null, value, null, null, null, null)
    !value
  }

  @alwaysinline
  def flags(evt: Event): CUnsignedShort = {
    val value = stackalloc[CUnsignedShort]()
    kevent.scalanative_kevent_get(evt, 0, null, null, value, null, null, null)
    !value
  }

  @alwaysinline
  def data(evt: Event): Size = {
    val value = stackalloc[Size]()
    kevent.scalanative_kevent_get(evt, 0, null, null, null, null, value, null)
    !value
  }

  @alwaysinline
  def isError(evt: Event): Boolean = (flags(evt).toInt & kevent.EV_ERROR) != 0

  @alwaysinline
  def isEOF(evt: Event): Boolean = (flags(evt).toInt & kevent.EV_EOF) != 0

  @alwaysinline
  def errno(evt: Event): CInt = data(evt).toInt

  @alwaysinline
  def rwAvailable(evt: Event): CInt = data(evt).toInt

  @alwaysinline
  def isReadEvent(evt: Event): Boolean = filter(evt) == kevent.EVFILT_READ

  @alwaysinline
  def isWriteEvent(evt: Event): Boolean = filter(evt) == kevent.EVFILT_WRITE

  def fileIdent(evt: Event): Int = ident(evt).toInt

  @alwaysinline
  def pollQueue(nevents: Int)(op: EventQueue => Unit): Unit = {
    require(nevents > 0, "Event buffer capacity must be positive")
    // Keep the multiplication outside stackalloc: Scala Native 0.5.11 can
    // discard it when lowering an inline allocation-size expression.
    val byteCount = eventSize * nevents.toCSize
    val events = stackalloc[Byte](byteCount)
    op(events)
  }

  /** posixlib does not expose EVFILT_TIMER. Its value is -7 on macOS and FreeBSD. */
  val EVFILT_TIMER: CShort = -7

  @alwaysinline
  def isTimerEvent(evt: Event): Boolean = filter(evt) == EVFILT_TIMER

  /** Register a one-shot timer using kqueue's default millisecond units. */
  def addTimerOneShot(evt: Event, id: USize, milliseconds: Size): Unit = {
    if (LinktimeInfo.isMac || LinktimeInfo.isFreeBSD) {
      kevent.scalanative_kevent_set(
        evt,
        0, // index within the event buffer
        id, // timer ID
        EVFILT_TIMER, // filter type
        (kevent.EV_ADD | kevent.EV_ONESHOT).toUShort, // flags
        0.toUInt, // default units: milliseconds
        milliseconds, // timeout
        null // user data
      )
    } else {
      throw new UnsupportedOperationException("Kqueue timers require macOS or FreeBSD")
    }
  }

  def addFile(evt: Event, fd: Int, read: Boolean, clear: Boolean, oneShot: Boolean = false): Unit = {
    var flags = kevent.EV_ADD | kevent.EV_ENABLE
    if (clear) {
      flags |= kevent.EV_CLEAR
    }
    if (oneShot) {
      flags |= kevent.EV_ONESHOT
    }
    kevent.scalanative_kevent_set(
      evt,
      0, // index within the event buffer
      fd.toUSize, // file descriptor
      (if (read) kevent.EVFILT_READ else kevent.EVFILT_WRITE).toShort, // filter type
      flags.toUShort, // flags
      0.toUInt, // fflags
      0, // filter-specific data
      null // user data
    )
  }

  /** Removes a timer registered with `addTimerOneShot` before it fires. */
  def deleteTimer(evt: Event, id: USize): Unit =
    kevent.scalanative_kevent_set(
      evt,
      0, // index within the event buffer
      id, // timer ID
      EVFILT_TIMER, // filter type
      kevent.EV_DELETE.toUShort, // flags
      0.toUInt, // fflags
      0, // filter-specific data
      null // user data
    )

  def deleteFile(evt: Event, fd: Int, read: Boolean): Unit = {
    kevent.scalanative_kevent_set(
      evt,
      0, // index within the event buffer
      fd.toUSize, // file descriptor
      (if (read) kevent.EVFILT_READ else kevent.EVFILT_WRITE).toShort, // filter type
      kevent.EV_DELETE.toUShort, // flags
      0.toUInt, // fflags
      0, // filter-specific data
      null // user data
    )
  }

  @alwaysinline
  def createAndRegisterEvents(
      kq: Int,
      nEvents: Int
  )(f: EventQueue => Unit): Unit = {
    pollQueue(nEvents) { events =>
      f(events)
      registerEvents(kq, events, nEvents)
    }
  }

  /** Deletes `fd`'s read or write filter. A filter that is already gone because `fd` was closed, reported as `ENOENT`
    * or `EBADF`, is not an error: kqueue removes a descriptor's filters when it is closed.
    */
  def deleteFileIfOpen(kq: Int, fd: Int, read: Boolean): Unit =
    pollQueue(1) { events =>
      deleteFile(eventAt(events, 0), fd, read)
      if (kevent.kevent(kq, events, 1, null, 0, null) < 0) {
        val code = scala.scalanative.posix.errno.errno
        if (code != scala.scalanative.posix.errno.ENOENT && code != scala.scalanative.posix.errno.EBADF)
          throw new IOException(s"Failed to delete filter: ${cError()}")
      }
    }

  def registerEvents(
      kq: Int,
      events: EventQueue,
      nEvents: Int
  ): Unit = {
    if (kevent.kevent(kq, events, nEvents, null, 0, null) < 0) {
      throw new IOException(
        s"Failed to register events: ${cError()}"
      )
    }
  }

  def pollEventsNow(
      kq: Int,
      events: EventQueue,
      nEvents: Int
  ): Int = pollEventsTimeout(kq, events, nEvents, 0, 0)

  def pollEventsForever(
      kq: Int,
      events: EventQueue,
      nEvents: Int
  ): Int = {
    val polledEvents = kevent.kevent(kq, null, 0, events, nEvents, null)
    if (polledEvents < 0) {
      throw new IOException(
        s"Failed to poll events: ${cError()}"
      )
    }
    polledEvents
  }

  def pollEventsTimeout(
      kq: Int,
      events: EventQueue,
      nEvents: Int,
      seconds: Int,
      nanoseconds: Int
  ): Int = {
    val timeout = stackalloc[timespec]()
    timeout.tv_sec = seconds
    timeout.tv_nsec = nanoseconds
    val polledEvents = kevent.kevent(kq, null, 0, events, nEvents, timeout)
    if (polledEvents < 0) {
      throw new IOException(
        s"Failed to poll events: ${cError()}"
      )
    }
    polledEvents
  }

  /** Opens a kqueue.
    */
  def open(): Int = {
    val kq = kevent.kqueue()
    if (kq < 0) {
      throw new IOException(
        s"Failed to create kqueue: ${cError()}"
      )
    }
    kq
  }

  def close(kq: Int): Unit = {
    if (unistd.close(kq) < 0) {
      throw new IOException(
        s"Failed to close kqueue: ${cError()}"
      )
    }
  }

  def scoped(f: FileOperation): Unit =
    Bracket.fileResource(open())(close)(f)
}
