package gears.async.asyncio.uring

import uring._
import uringOps._

import gears.async.Async
import gears.async.asyncio.{Buffer, Error, Result}
import gears.async.file.{FileReader, FileSupport, FileWriter}
import gears.util.either

import java.io.FileNotFoundException
import java.nio.channels.ClosedChannelException
import scala.scalanative.posix.errno
import scala.scalanative.posix.fcntl.{AT_FDCWD, O_CREAT, O_RDONLY, O_TRUNC, O_WRONLY}
import scala.scalanative.posix.unistd
import scala.scalanative.runtime.ByteArray
import scala.scalanative.unsafe._
import scala.scalanative.unsigned._

private def toNulTerminatedBytes(s: String): Array[Byte] =
  val utf8 = s.getBytes(java.nio.charset.StandardCharsets.UTF_8)
  val arr = new Array[Byte](utf8.length + 1)
  System.arraycopy(utf8, 0, arr, 0, utf8.length)
  arr

final class UringFileReader private[uring] (
    private val fd: Int,
    scheduler: UringPerThreadScheduler
) extends FileReader:
  private var closed = false
  private var offset: Long = 0

  private def checkOpen(): Unit = if closed then throw new ClosedChannelException()

  override def readBuf(buf: Buffer)(using Async): Result[Unit] = either:
    checkOpen()
    val len = buf.remaining()
    nativePtrInto(buf) match
      case Some(ptr) =>
        val res =
          submitAwait(scheduler)(buf.array())(sqe => io_uring_prep_read(sqe, fd, ptr, len.toUInt, offset.toULong))
        if res < 0 then throw IOException(-res)
        else if res == 0 then either.error(Error.EOF)
        else
          offset += res
          buf.position(buf.position() + res)
      case None =>
        val scratchArr = new Array[Byte](len)
        val scratch = scratchArr.asInstanceOf[ByteArray].at(0)
        val res =
          submitAwait(scheduler)(scratchArr)(sqe => io_uring_prep_read(sqe, fd, scratch, len.toUInt, offset.toULong))
        if res < 0 then throw IOException(-res)
        else if res == 0 then either.error(Error.EOF)
        else
          offset += res
          copyFromNative(scratch, res, buf)

  override def close(): Unit = synchronized:
    if !closed then
      closed = true
      unistd.close(fd)

final class UringFileWriter private[uring] (
    private val fd: Int,
    scheduler: UringPerThreadScheduler
) extends FileWriter:
  private var closed = false
  private var offset: Long = 0

  private def checkOpen(): Unit = if closed then throw new ClosedChannelException()

  override def writeBuf(buf: Buffer)(using Async): Result[Unit] = either:
    checkOpen()
    val len = buf.remaining()
    nativePtrInto(buf) match
      case Some(ptr) =>
        var sent = 0
        while sent < len do
          val res = submitAwait(scheduler)(buf.array())(sqe =>
            io_uring_prep_write(sqe, fd, ptr + sent, (len - sent).toUInt, (offset + sent).toULong)
          )
          if res < 0 then throw IOException(-res)
          else sent += res
        offset += sent
        buf.position(buf.position() + sent)
      case None =>
        val scratchArr = new Array[Byte](len)
        val scratch = scratchArr.asInstanceOf[ByteArray].at(0)
        copyToNative(buf, scratch, len)
        var sent = 0
        while sent < len do
          val res = submitAwait(scheduler)(scratchArr)(sqe =>
            io_uring_prep_write(sqe, fd, scratch + sent, (len - sent).toUInt, (offset + sent).toULong)
          )
          if res < 0 then throw IOException(-res)
          else sent += res
        offset += sent

  override def close(): Unit = synchronized:
    if !closed then
      closed = true
      unistd.close(fd)

trait UringFileSupport(scheduler: UringPerThreadScheduler) extends FileSupport:
  type File = UringFileReader
  type FileOut = UringFileWriter

  override def openRead(path: String)(using Async): Result[File] = either:
    val pathArr = toNulTerminatedBytes(path)
    val pathPtr = pathArr.asInstanceOf[ByteArray].at(0).asInstanceOf[CString]
    val fd = submitAwait(scheduler)(pathArr)(sqe => io_uring_prep_openat(sqe, AT_FDCWD, pathPtr, O_RDONLY, 0.toUInt))
    if fd < 0 then
      if -fd == errno.ENOENT then throw new FileNotFoundException(path)
      else throw IOException(-fd)
    else UringFileReader(fd, scheduler)

  override def openWrite(path: String)(using Async): Result[FileOut] = either:
    val pathArr = toNulTerminatedBytes(path)
    val pathPtr = pathArr.asInstanceOf[ByteArray].at(0).asInstanceOf[CString]
    val flags = O_WRONLY | O_CREAT | O_TRUNC
    val mode = 420.toUInt // 0644 octal (rw-r--r--) - Scala has no octal literal syntax
    val fd = submitAwait(scheduler)(pathArr)(sqe => io_uring_prep_openat(sqe, AT_FDCWD, pathPtr, flags, mode))
    if fd < 0 then throw IOException(-fd)
    else UringFileWriter(fd, scheduler)
