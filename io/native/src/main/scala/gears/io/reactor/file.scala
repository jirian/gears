package gears.io.reactor

import gears.async.{Async, Semaphore}
import gears.io.either
import gears.io.file.{FileHandle, FileInfo, FileReader, FileSupport, FileWriter, WriteOptions}
import gears.io.reactor.Slot
import gears.io.{Buffer, Error, IoConcurrency, Result}

import scala.scalanative.posix.sys.stat
import scala.scalanative.posix.{fcntl, unistd}
import scala.scalanative.unsafe.*

private[reactor] trait PosixFile extends FileHandle:
  protected def fd: Int
  protected def driver: ReactorDriver
  protected val guard = IoGuard()
  protected val access = IoConcurrency()
  private val cursor = Semaphore(1)

  protected def atCursor[A](body: => A)(using Async): A =
    val permit = cursor.acquire()
    try body
    finally permit.release()

  private case class Value[A](value: A)

  protected def call[A](body: Int => A)(using Async): A =
    val copy = guard.whileOpen(ReactorDriver.duplicate(fd))
    val result = Slot[Value[A]]()
    try
      driver.await(guard)(r => r.ops.blocking(() => Value(body(copy)), result))
      result.clear().value
    finally unistd.close(copy)

  override def close(): Unit = guard.close { unistd.close(fd); () }

  def position()(using Async): Long = atCursor:
    call: fd =>
      val position = unistd.lseek(fd, 0L.toSize, unistd.SEEK_CUR)
      if position < 0 then throw posixError("Failed to read file position")
      position.toLong

  def seek(offset: Long)(using Async): Unit = atCursor:
    require(offset >= 0, "offset must be nonnegative")
    call: fd =>
      if unistd.lseek(fd, offset.toSize, unistd.SEEK_SET) < 0 then throw posixError("Failed to seek file")

  def metadata()(using Async): FileInfo = call: fd =>
    Zone.acquire: zone =>
      val info = alloc[stat.stat]()(using zone)
      if stat.fstat(fd, info) < 0 then throw posixError("Failed to stat file")
      val value = !info
      FileInfo(
        value._6,
        value._8._1 * 1000L + value._8._2 / 1000000L,
        value._13.toInt & 511,
        stat.S_ISREG(value._13) != 0,
        stat.S_ISDIR(value._13) != 0
      )

final class ReactorFileReader private[io] (
    protected val fd: Int,
    protected val driver: ReactorDriver
) extends FileReader
    with PosixFile:

  override def readBuf(buf: Buffer)(using Async): Result[Unit] = access.reading:
    atCursor(read(buf, None))

  override def readAt(offset: Long, buf: Buffer)(using Async): Result[Unit] = access.reading:
    require(offset >= 0, "offset must be nonnegative")
    read(buf, Some(offset))

  private def read(buf: Buffer, offset: Option[Long])(using Async): Result[Unit] = either:
    guard.whileOpen(())
    require(!buf.isReadOnly, "Read buffers must be writable")
    if buf.hasRemaining() then
      val before = buf.position()
      offset match
        case None     => driver.readFile(guard, fd, buf)
        case Some(at) => driver.readAt(guard, fd, buf, at)
      if buf.position() == before then either.error(Error.EOF)

final class ReactorFileWriter private[io] (
    protected val fd: Int,
    protected val driver: ReactorDriver,
    append: Boolean = false
) extends FileWriter
    with PosixFile:

  override def writeBuf(buf: Buffer)(using Async): Result[Unit] = access.writing:
    atCursor:
      guard.whileOpen(())
      while buf.hasRemaining() do driver.writeFile(guard, fd, buf)
      Right(())

  override def writeAt(offset: Long, buf: Buffer)(using Async): Result[Unit] = access.writing:
    require(offset >= 0, "offset must be nonnegative")
    require(!append, "Positioned writes are unavailable on an append-only descriptor")
    guard.whileOpen(())
    var at = offset
    while buf.hasRemaining() do
      val before = buf.position()
      driver.writeAt(guard, fd, buf, at)
      val written = buf.position() - before
      if written == 0 then throw java.io.IOException("Positioned write made no progress")
      at += written
    Right(())

  def truncate(size: Long)(using Async): Unit = access.writing:
    atCursor:
      require(size >= 0, "size must be nonnegative")
      call(fd => if unistd.ftruncate(fd, size.toSize) < 0 then throw posixError("Failed to truncate file"))

  def sync()(using Async): Unit = access.writing:
    call(fd => if unistd.fsync(fd) < 0 then throw posixError("Failed to sync file"))

trait ReactorFileSupport(driver: ReactorDriver) extends FileSupport:
  type File = ReactorFileReader
  type FileOut = ReactorFileWriter

  override def openRead(path: String)(using Async): Result[File] = either:
    ReactorFileReader(driver.openFile(path, fcntl.O_RDONLY, 0), driver)

  override def openWrite(path: String)(using Async): Result[FileOut] = openWrite(path, WriteOptions())

  override def openWrite(path: String, options: WriteOptions)(using Async): Result[FileOut] = either:
    val flags = fcntl.O_WRONLY |
      (if options.create then fcntl.O_CREAT else 0) |
      (if options.truncate && !options.append then fcntl.O_TRUNC else 0) |
      (if options.append then fcntl.O_APPEND else 0) |
      (if options.exclusive then fcntl.O_EXCL else 0)
    ReactorFileWriter(driver.openFile(path, flags, options.permissions), driver, options.append)
