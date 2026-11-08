package gears.async.io
package uring

import scala.scalanative.posix.sys.socket
import scala.scalanative.unsafe.*

/** Linux values that posixlib does not provide. */
private[uring] object Linux {
  inline val SOCK_NONBLOCK = 0x800
  inline val SOCK_CLOEXEC = 0x80000
  inline val MSG_DONTWAIT = 0x40
  inline val MSG_NOSIGNAL = 0x4000
  inline val O_CLOEXEC = 0x80000
  inline val POLLIN = 0x001
  inline val POLLOUT = 0x004
  // posixlib's ETIME is 0 before Scala Native 0.5.13 (scala-native#5050).
  inline val ETIME = 62
  inline val SO_DOMAIN = 39
}

/** Linux calls that posixlib does not bind, or binds with `@blocking`, which these never need: each returns at once. */
@extern
private[uring] object LinuxCalls {
  def pipe2(fds: Ptr[CInt], flags: CInt): CInt = extern

  def accept4(fd: CInt, address: Ptr[socket.sockaddr], addressLength: Ptr[socket.socklen_t], flags: CInt): CInt =
    extern
}
