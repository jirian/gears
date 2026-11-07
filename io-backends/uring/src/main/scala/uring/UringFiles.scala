package gears.async.io
package uring

import gears.async.io.Slot
import gears.async.io.internal.Interest
import gears.async.io.internal.NativeBuffer
import gears.async.io.internal.PosixErr.cError

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

import uring.*
import uringStructOps.*

// The file ops (see `FileOps`), each run by the kernel: `openat`, reads and writes at an offset, and `fsync`, which
// io_uring does asynchronously at the device, with no thread of ours blocked for the disk. Once a kernel worker has
// started one, nothing stops it: a cancel, a time limit or a close does not wait for it, so that a slow disk does not
// hold up the loop, and it completes as it finishes, `cancel` returning false (see `UringReactor.withdrawOwn`), or, if
// its handle was closed meanwhile, fails, its buffer and offset left as they were, as on NIO. One not yet started is
// cancelled as any command is. A stream read or write of a regular file's handle runs the same way.

/** Opens a regular file and writes its handle into `into` (see `FileOps.open`). It opens without blocking, so that a
  * FIFO, which it then refuses, cannot keep it waiting for a writer, and turns blocking off again for the regular file:
  * io_uring would otherwise report a read of uncached data as would-block instead of waiting for the disk. A file
  * opened for a submission withdrawn meanwhile is closed again, since its handle never reaches the caller. One-shot.
  */
final class OpenFile(path: String, write: Boolean, into: Slot[UringKinds.File]) extends Command {
  gears.async.io.internal.Checks.checkPath(path)
  override private[io] def slotRefusal: Throwable | Null = gears.async.io.internal.Checks.notEmpty(into)
  override private[io] def oneShot: Boolean = true
  def fd: Int = -1
  def interest: Interest = Interest.Read
  override protected def takesTurn: Boolean = false
  override private[uring] def runsInWorker: Boolean = true

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
    else if res < 0 then throw Results.failure(s"Failed to open $path", res)
    else {
      try {
        val info = stackalloc[stat.stat]()
        if stat.fstat(res, info) < 0 then throw new IOException(s"Failed to inspect $path: ${cError()}")
        if stat.S_ISREG(info.st_mode) == 0 then throw new IOException(s"Not a regular file: $path")
        val flags = fcntl.fcntl(res, fcntl.F_GETFL, 0)
        if flags < 0 || fcntl.fcntl(res, fcntl.F_SETFL, flags & ~fcntl.O_NONBLOCK) < 0 then
          throw new IOException(s"Failed to make $path blocking: ${cError()}")
      } catch {
        case e: IOException =>
          unistd.close(res)
          throw e
      }
      UringHandles.opened(res)
      into.set(UringKinds.file(UringHandles.handle(res)))
      true
    }

  override protected def discard(res: Int): Unit = if res >= 0 then unistd.close(res): Unit
}

/** A read at an offset (see `FileOps.read`): fills `buf` from the file at the op's offset, which each completion
  * advances by the bytes read, so that submitting the op again goes on from there.
  */
final class PositionalRead(val fd: Int, buf: ByteBuffer, private var offset: Long) extends Command {
  gears.async.io.internal.Checks.checkFillable(buf)
  gears.async.io.internal.Checks.checkOffset(offset)
  override private[io] def refusal: Throwable | Null =
    if buf.hasRemaining() then null else gears.async.io.internal.Checks.bufferFull(buf)
  def interest: Interest = Interest.Read
  override protected def takesTurn: Boolean = false
  override private[uring] def runsInWorker: Boolean = true
  private val staging = new Staging(Array(buf), copyIn = false, copyOut = true)

  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit = {
    val b = staging.begin()(0)
    io_uring_prep_read(sqe, fd, NativeBuffer.atPosition(b), b.remaining().toUInt, offset.toULong)
  }

  protected def finish(res: Int): Boolean =
    if res == -errno.EINTR then false
    else if res < 0 then {
      staging.end()
      throw Results.failure(s"Failed to read descriptor $fd at $offset", res)
    } else {
      staging.commit(res)
      offset += res
      true
    }

  override protected def discard(res: Int): Unit = staging.end()
}

/** A write at an offset (see `FileOps.write`): drains `buf` into the file at the op's offset, which each completion
  * advances by the bytes written; with nothing remaining, it completes at once, writing nothing.
  */
final class PositionalWrite(val fd: Int, buf: ByteBuffer, private var offset: Long) extends Command {
  gears.async.io.internal.Checks.checkOffset(offset)
  def interest: Interest = Interest.Write
  override protected def takesTurn: Boolean = false
  override private[uring] def runsInWorker: Boolean = true
  override private[uring] def nothingToDo: Boolean = !buf.hasRemaining()
  private val staging = new Staging(Array(buf), copyIn = true, copyOut = false)

  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit = {
    val b = staging.begin()(0)
    io_uring_prep_write(sqe, fd, NativeBuffer.atPosition(b), b.remaining().toUInt, offset.toULong)
  }

  protected def finish(res: Int): Boolean =
    if res == -errno.EINTR then false
    else if res < 0 then {
      staging.end()
      throw Results.failure(s"Failed to write descriptor $fd at $offset", res)
    } else {
      staging.commit(res)
      offset += res
      true
    }

  override protected def discard(res: Int): Unit = staging.end()
}

/** `fsync` (see `FileOps.sync`). */
final class FileSync(val fd: Int) extends Command {
  def interest: Interest = Interest.Write
  override protected def takesTurn: Boolean = false
  override private[uring] def runsInWorker: Boolean = true

  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit = io_uring_prep_fsync(sqe, fd)

  protected def finish(res: Int): Boolean =
    if res == -errno.EINTR then false
    else if res < 0 then throw Results.failure(s"Failed to sync descriptor $fd", res)
    else true
}
