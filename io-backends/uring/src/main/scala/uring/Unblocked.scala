// From the old shared native runtime (gears.async.io.internal on async-io, 1d7add2), now the io_uring backend's.
package gears.async.io
package uring

import scala.scalanative.posix.sys.socket
import scala.scalanative.unsafe.*

/** The system calls the worker makes where they cannot block, bound without posixlib's `@blocking`: reads, writes and
  * datagram calls on non-blocking descriptors, tried before an op goes to the ring.
  *
  * A `@blocking` call tells the GC the thread is leaving managed code, so that a collection need not wait for it, and
  * to that end captures the thread's registers, which costs more than a call that returns at once: one that never waits
  * never keeps a collection waiting, so it needs none of that. The signatures match posixlib's declarations of the same
  * C functions.
  */
@extern
private[uring] object Unblocked {
  def read(fildes: CInt, buf: CVoidPtr, nbyte: CSize): CSSize = extern

  def write(fildes: CInt, buf: CVoidPtr, nbyte: CSize): CSSize = extern

  def readv(fildes: CInt, iov: Ptr[scala.scalanative.posix.sys.uio.iovec], iovcnt: CInt): CSSize = extern

  def writev(fildes: CInt, iov: Ptr[scala.scalanative.posix.sys.uio.iovec], iovcnt: CInt): CSSize = extern

  def sendmsg(fd: CInt, message: Ptr[socket.msghdr], flags: CInt): CSSize = extern

  def recvmsg(fd: CInt, message: Ptr[socket.msghdr], flags: CInt): CSSize = extern

}
