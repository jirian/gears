package gears.async.asyncio.kqueue.unsafe

import scala.scalanative.unsafe._
import scala.scalanative.unsigned._

/** Raw kqueue/kevent bindings (Darwin/macOS ABI).
  *
  * UNVERIFIED: written without access to a macOS toolchain (this repo is
  * developed on Linux) - the struct layout and constants below are
  * transcribed from <sys/event.h> but have not been compiled or linked
  * against the real headers. Treat as a first draft to validate on macOS.
  */
@extern
private[kqueue] object kqueue {

  final val EVFILT_READ = -1
  final val EVFILT_WRITE = -2

  final val EV_ADD = 0x0001
  final val EV_DELETE = 0x0002
  final val EV_CLEAR = 0x0020

  // struct kevent (32 bytes on 64-bit Darwin):
  //   uintptr_t ident; int16_t filter; uint16_t flags;
  //   uint32_t fflags; intptr_t data; void *udata;
  type kevent = CStruct6[
    CUnsignedLong, // ident
    CShort, // filter
    CUnsignedShort, // flags
    CUnsignedInt, // fflags
    CLong, // data
    Ptr[Byte] // udata
  ]

  def kqueue(): CInt = extern

  def kevent(
      kq: CInt,
      changelist: Ptr[kevent],
      nchanges: CInt,
      eventlist: Ptr[kevent],
      nevents: CInt,
      timeout: Ptr[scala.scalanative.posix.time.timespec]
  ): CInt = extern
}

private[kqueue] object kqueueImplicits {
  import kqueue._

  implicit final class keventOps(val ev: Ptr[kevent]) extends AnyVal {
    inline def ident: CUnsignedLong = ev._1
    inline def ident_=(v: CUnsignedLong): Unit = ev._1 = v
    inline def filter: CShort = ev._2
    inline def filter_=(v: CShort): Unit = ev._2 = v
    inline def flags: CUnsignedShort = ev._3
    inline def flags_=(v: CUnsignedShort): Unit = ev._3 = v
    inline def fflags: CUnsignedInt = ev._4
    inline def fflags_=(v: CUnsignedInt): Unit = ev._4 = v
    inline def data: CLong = ev._5
    inline def data_=(v: CLong): Unit = ev._5 = v
    inline def udata: Ptr[Byte] = ev._6
    inline def udata_=(v: Ptr[Byte]): Unit = ev._6 = v
  }
}
