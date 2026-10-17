package gears.async.file

import gears.async.asyncio.{Buffer, Reader, Result, Writer}
import gears.async.{Async, Resource}

import java.io.Closeable

trait FileHandle extends Closeable:
  def position()(using Async): Long
  def seek(offset: Long)(using Async): Unit
  def metadata()(using Async): FileInfo

final case class FileInfo(size: Long, modifiedMillis: Long, permissions: Int, isRegular: Boolean, isDirectory: Boolean)

final case class WriteOptions(
    create: Boolean = true,
    truncate: Boolean = true,
    append: Boolean = false,
    exclusive: Boolean = false,
    permissions: Int = 420
):
  require(!exclusive || create, "exclusive requires create")
  require(permissions >= 0 && permissions <= 511, "permissions must be in 0000..0777")

abstract class FileReader extends Reader, FileHandle:
  def readAt(offset: Long, buf: Buffer)(using Async): Result[Unit]

abstract class FileWriter extends Writer, FileHandle:
  def writeAt(offset: Long, buf: Buffer)(using Async): Result[Unit]
  def truncate(size: Long)(using Async): Unit
  def sync()(using Async): Unit

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
  def read(path: String)(using fs: FileSupport): Resource[fs.File] =
    Resource(fs.openRead(path).toOption.get, _.close())
  def write(path: String, options: WriteOptions = WriteOptions())(using fs: FileSupport): Resource[fs.FileOut] =
    Resource(fs.openWrite(path, options).toOption.get, _.close())
