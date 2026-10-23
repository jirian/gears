package gears.io.file

import gears.async.{Async, Resource}
import gears.io.{Buffer, Reader, Result, Writer}

import java.io.Closeable

/** What every open file can do besides reading or writing. */
trait FileHandle extends Closeable:

  /** The current file position, used by `readBuf` and `writeBuf`. */
  def position()(using Async): Long

  /** Moves the file position to `offset`. */
  def seek(offset: Long)(using Async): Unit

  /** The file's size, modification time and type, like `fstat(2)`. */
  def metadata()(using Async): FileInfo

/** File metadata. `permissions` are the Unix mode bits 0-0777. */
final case class FileInfo(size: Long, modifiedMillis: Long, permissions: Int, isRegular: Boolean, isDirectory: Boolean)

/** How `openWrite` opens a file: create it if missing (`exclusive`: fail if it exists), truncate it, or append to it.
  * `permissions` applies to a newly created file; the default 420 is 0644.
  */
final case class WriteOptions(
    create: Boolean = true,
    truncate: Boolean = true,
    append: Boolean = false,
    exclusive: Boolean = false,
    permissions: Int = 420
):
  require(!exclusive || create, "exclusive requires create")
  require(permissions >= 0 && permissions <= 511, "permissions must be in 0000..0777")

/** A file open for reading. Only one read may be pending at a time. */
abstract class FileReader extends Reader, FileHandle:

  /** Reads at `offset` without moving the file position. */
  def readAt(offset: Long, buf: Buffer)(using Async): Result[Unit]

/** A file open for writing. Writes are queued and run one at a time. */
abstract class FileWriter extends Writer, FileHandle:

  /** Writes all of `buf` at `offset` without moving the file position. Not allowed in append mode. */
  def writeAt(offset: Long, buf: Buffer)(using Async): Result[Unit]

  /** Cuts or extends the file to `size` bytes. */
  def truncate(size: Long)(using Async): Unit

  /** Waits until the file's data is on disk, like `fsync(2)`. */
  def sync()(using Async): Unit

/** Opens files. A missing file fails with `FileNotFoundException`. */
trait FileSupport:
  type File <: FileReader
  type FileOut <: FileWriter

  def openRead(path: String)(using Async): Result[File]

  def openWrite(path: String)(using Async): Result[FileOut]
  def openWrite(path: String, options: WriteOptions)(using Async): Result[FileOut]

object FileSupport:
  def openRead(path: String)(using fs: FileSupport, async: Async): Result[fs.File] =
    fs.openRead(path)
  def openWrite(path: String)(using fs: FileSupport, async: Async): Result[fs.FileOut] =
    fs.openWrite(path)
  def openWrite(path: String, options: WriteOptions)(using fs: FileSupport, async: Async): Result[fs.FileOut] =
    fs.openWrite(path, options)

  /** `openRead` as a gears `Resource`, closed when released. */
  def read(path: String)(using fs: FileSupport): Resource[fs.File] =
    Resource(fs.openRead(path).toOption.get, _.close())

  /** `openWrite` as a gears `Resource`, closed when released. */
  def write(path: String, options: WriteOptions = WriteOptions())(using fs: FileSupport): Resource[fs.FileOut] =
    Resource(fs.openWrite(path, options).toOption.get, _.close())
