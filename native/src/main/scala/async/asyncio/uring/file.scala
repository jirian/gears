package gears.async.asyncio.uring

import gears.async.Async
import gears.async.asyncio.{Buffer, Error, Result}
import gears.async.file.{FileReader, FileSupport, FileWriter}
import gears.util.either

import scala.scalanative.posix.fcntl.{O_CREAT, O_RDONLY, O_TRUNC, O_WRONLY}
import scala.scalanative.posix.unistd

import _root_.asyncio.HandleSlot
import _root_.asyncio.uring.LinuxConstants.O_CLOEXEC

final class UringFileReader private[uring] (
    private val fd: Int,
    scheduler: UringPerThreadScheduler
) extends FileReader:
  // Closing cancels a pending read, which then fails with `AsynchronousCloseException`.
  private val guard = IoGuard()

  /** Reads at the descriptor's own position, which each read advances. */
  override def readBuf(buf: Buffer)(using Async): Result[Unit] = either:
    val before = buf.position()
    scheduler.await(guard)(reactor => reactor.ops.read(fd, buf))
    if buf.position() == before then either.error(Error.EOF)

  override def close(): Unit = if guard.close() then unistd.close(fd)

final class UringFileWriter private[uring] (
    private val fd: Int,
    scheduler: UringPerThreadScheduler
) extends FileWriter:
  // Closing cancels a pending write, which then fails with `AsynchronousCloseException`.
  private val guard = IoGuard()

  /** Writes at the descriptor's own position, which each write advances. */
  override def writeBuf(buf: Buffer)(using Async): Result[Unit] = either:
    while buf.hasRemaining() do scheduler.await(guard)(reactor => reactor.ops.writeFile(fd, buf))

  override def close(): Unit = if guard.close() then unistd.close(fd)

trait UringFileSupport(scheduler: UringPerThreadScheduler) extends FileSupport:
  type File = UringFileReader
  type FileOut = UringFileWriter

  /** A missing file throws `FileNotFoundException`. */
  override def openRead(path: String)(using Async): Result[File] = either:
    UringFileReader(open(path, O_RDONLY | O_CLOEXEC, 0), scheduler)

  override def openWrite(path: String)(using Async): Result[FileOut] = either:
    val mode = 420 // 0644 octal (rw-r--r--) - Scala has no octal literal syntax
    UringFileWriter(open(path, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, mode), scheduler)

  private def open(path: String, flags: Int, mode: Int)(using Async): Int =
    val opened = HandleSlot()
    scheduler.await(null)(reactor => reactor.ops.openAt(path, flags, mode, opened))
    opened.clear()
