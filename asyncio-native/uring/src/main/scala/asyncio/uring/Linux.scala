package asyncio.uring

import java.io.IOException
import scala.scalanative.posix.errno
import scala.scalanative.posix.string
import scala.scalanative.posix.sys.socket
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import uring.__u64

/** Linux values that posixlib does not provide. */
private[uring] object Linux {
  inline val ETIME = 62
  inline val SOCK_NONBLOCK = 0x800
  inline val SOCK_CLOEXEC = 0x80000
  inline val O_CLOEXEC = 0x80000
  inline val POLLIN = 0x001
  inline val POLLOUT = 0x004
}

/** Linux calls that posixlib does not bind, or binds with `@blocking`, which these never need: each returns at once. */
@extern
private[uring] object LinuxCalls {
  def pipe2(fds: Ptr[CInt], flags: CInt): CInt = extern

  def accept4(fd: CInt, address: Ptr[socket.sockaddr], addressLength: Ptr[socket.socklen_t], flags: CInt): CInt =
    extern

  def send(fd: CInt, buffer: CVoidPtr, length: CSize, flags: CInt): CSSize = extern
}

/** What the commands share: reading a completion's result, and the poll events for an interest. */
private[uring] object Ring {

  /** The offset that makes a read or write use and advance the file position, as `read(2)` and `write(2)` do. */
  val currentPosition: __u64 = (-1L).toULong

  /** Whether a negative result only means "try again". */
  def transient(res: Int): Boolean = res == -errno.EINTR || res == -errno.EAGAIN

  /** Whether the calling thread's last system call would have blocked, or was interrupted. */
  def wouldBlock: Boolean = {
    val err = errno.errno
    err == errno.EAGAIN || err == errno.EWOULDBLOCK || err == errno.EINTR
  }

  /** The failure of a submission whose result is `res`, a negated `errno`. */
  def failure(what: String, res: Int): IOException =
    new IOException(s"$what: ${fromCString(string.strerror(-res))}")

  /** The poll events that `interest` waits for. */
  def pollMask(interest: asyncio.Interest): CUnsignedInt =
    (if interest == asyncio.Interest.Read then Linux.POLLIN else Linux.POLLOUT).toUInt
}
