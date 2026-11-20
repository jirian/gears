package gears.async.io
package uring

import scala.scalanative.posix.sys.socket
import scala.scalanative.unsafe.*

@extern
// - no `@blocking`: these never block on non-blocking descriptors, and `@blocking` costs a GC state change per call
private[uring] object Unblocked {
  def read(fildes: CInt, buf: CVoidPtr, nbyte: CSize): CSSize = extern

  def write(fildes: CInt, buf: CVoidPtr, nbyte: CSize): CSSize = extern

  def readv(fildes: CInt, iov: Ptr[scala.scalanative.posix.sys.uio.iovec], iovcnt: CInt): CSSize = extern

  def writev(fildes: CInt, iov: Ptr[scala.scalanative.posix.sys.uio.iovec], iovcnt: CInt): CSSize = extern

  def sendmsg(fd: CInt, message: Ptr[socket.msghdr], flags: CInt): CSSize = extern

  def recvmsg(fd: CInt, message: Ptr[socket.msghdr], flags: CInt): CSSize = extern

}
