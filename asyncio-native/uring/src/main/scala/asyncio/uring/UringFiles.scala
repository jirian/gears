package asyncio.uring

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.scalanative.posix.errno
import scala.scalanative.posix.fcntl
import scala.scalanative.posix.sys.stat
import scala.scalanative.posix.sys.statOps.*
import scala.scalanative.posix.unistd
import scala.scalanative.runtime.ByteArray
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import asyncio.Handle
import asyncio.Repeatable
import asyncio.unsafe.NativeBuffer
import uring.*
import uringStructOps.*

/** The file ops (see `FileOps`), each run by the kernel: `openat`, reads and writes at an offset, and `fsync`, which
  * io_uring does asynchronously at the device, with no thread of ours blocked for the disk. A cancel reaches a file op
  * as it does any other command: the kernel stops it if it has not started, and otherwise its result arrives and is
  * discarded before `onCancel` runs.
  */

/** Opens a regular file and writes its handle into `into` (see `FileOps.open`). It opens without blocking, so that a
  * FIFO, which it then refuses, cannot keep it waiting for a writer, and turns blocking off again for the regular file:
  * io_uring would otherwise report a read of uncached data as would-block instead of waiting for the disk. A file
  * opened for a submission cancelled meanwhile is closed again, since its handle never reaches the caller.
  */
final class OpenFile(val path: String, write: Boolean, into: Handle.Slot) extends Command {
  def fd: Int = -1

  private val cPath = {
    val utf8 = path.getBytes(StandardCharsets.UTF_8)
    java.util.Arrays.copyOf(utf8, utf8.length + 1) // the kernel reads it after the submission, NUL-terminated
  }

  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit = {
    val access = if write then fcntl.O_WRONLY else fcntl.O_RDONLY
    io_uring_prep_openat(
      sqe,
      fcntl.AT_FDCWD,
      cPath.asInstanceOf[ByteArray].at(0),
      access | fcntl.O_NONBLOCK | Linux.O_CLOEXEC,
      0.toUInt
    )
  }

  protected def finish(res: Int): Boolean =
    if res == -errno.EINTR then false
    else if res < 0 then throw Ring.failure(s"Failed to open $path", res)
    else {
      try {
        val info = stackalloc[stat.stat]()
        if stat.fstat(res, info) < 0 then
          throw new IOException(s"Failed to inspect $path: ${asyncio.unsafe.PosixErr.cError()}")
        if stat.S_ISREG(info.st_mode) == 0 then throw new IOException(s"Not a regular file: $path")
        val flags = fcntl.fcntl(res, fcntl.F_GETFL, 0)
        if flags < 0 || fcntl.fcntl(res, fcntl.F_SETFL, flags & ~fcntl.O_NONBLOCK) < 0 then
          throw new IOException(s"Failed to make $path blocking: ${asyncio.unsafe.PosixErr.cError()}")
      } catch {
        case e: IOException =>
          unistd.close(res)
          throw e
      }
      UringHandles.opened(res)
      into.set(Handle.wrap(res))
      true
    }

  override protected def discard(res: Int): Unit = if res >= 0 then unistd.close(res): Unit
}

/** A read at an offset (see `FileOps.read`): fills `buf` from the file at the op's offset, which each completion
  * advances by the bytes read, so that submitting the op again goes on from there.
  */
final class PositionalRead(val fd: Int, buf: ByteBuffer, private var offset: Long) extends Command with Repeatable {
  require(offset >= 0, "A file offset must not be negative")

  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit =
    io_uring_prep_read(sqe, fd, NativeBuffer.atPosition(buf), buf.remaining().toUInt, offset.toULong)

  protected def finish(res: Int): Boolean =
    if res == -errno.EINTR then false
    else if res < 0 then throw Ring.failure(s"Failed to read descriptor $fd at $offset", res)
    else {
      NativeBuffer.advance(buf, res)
      offset += res
      true
    }
}

/** A write at an offset (see `FileOps.write`): drains `buf` into the file at the op's offset, which each completion
  * advances by the bytes written.
  */
final class PositionalWrite(val fd: Int, buf: ByteBuffer, private var offset: Long) extends Command with Repeatable {
  require(offset >= 0, "A file offset must not be negative")

  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit =
    io_uring_prep_write(sqe, fd, NativeBuffer.atPosition(buf), buf.remaining().toUInt, offset.toULong)

  protected def finish(res: Int): Boolean =
    if res == -errno.EINTR then false
    else if res < 0 then throw Ring.failure(s"Failed to write descriptor $fd at $offset", res)
    else {
      NativeBuffer.advance(buf, res)
      offset += res
      true
    }
}

/** `fsync` (see `FileOps.sync`). */
final class FileSync(val fd: Int) extends Command with Repeatable {
  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit = io_uring_prep_fsync(sqe, fd)

  protected def finish(res: Int): Boolean =
    if res == -errno.EINTR then false
    else if res < 0 then throw Ring.failure(s"Failed to sync descriptor $fd", res)
    else true
}
