package gears.async.asyncio.uring

import asyncio.Slot
import gears.async.Async
import gears.async.asyncio.{Buffer, Error, Result}
import gears.async.file.{FileReader, FileSupport, FileWriter}
import gears.util.either

import java.nio.channels.ClosedChannelException
import scala.scalanative.posix.fcntl.{O_CREAT, O_RDONLY, O_TRUNC, O_WRONLY}
import scala.scalanative.posix.unistd

/** `O_CLOEXEC` on Linux; Scala Native's posixlib does not expose it. */
private final val O_CLOEXEC = 0x80000

final class UringFileReader private[uring] (
    private val fd: Int,
    scheduler: UringPerThreadScheduler
) extends FileReader:
  private var closed = false

  private def checkOpen(): Unit = if closed then throw new ClosedChannelException()

  /** Reads at the descriptor's own position, which each read advances. */
  override def readBuf(buf: Buffer)(using Async): Result[Unit] = either:
    checkOpen()
    val before = buf.position()
    scheduler.await(reactor => reactor.ops.read(fd, buf))
    if buf.position() == before then either.error(Error.EOF)

  override def close(): Unit = synchronized:
    if !closed then
      closed = true
      unistd.close(fd)

final class UringFileWriter private[uring] (
    private val fd: Int,
    scheduler: UringPerThreadScheduler
) extends FileWriter:
  private var closed = false

  private def checkOpen(): Unit = if closed then throw new ClosedChannelException()

  /** Writes at the descriptor's own position, which each write advances. */
  override def writeBuf(buf: Buffer)(using Async): Result[Unit] = either:
    checkOpen()
    while buf.hasRemaining() do scheduler.await(reactor => reactor.ops.writeFile(fd, buf))

  override def close(): Unit = synchronized:
    if !closed then
      closed = true
      unistd.close(fd)

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
    val opened = Slot[Integer]()
    scheduler.await(reactor => reactor.ops.openAt(path, flags, mode, opened))
    opened.clear().intValue
