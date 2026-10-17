package gears.io.examples

import gears.async._
import gears.io.Error
import gears.io.file.FileSupport
import gears.io.uring.UringPerThreadSupport

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

def fileWriteExample(): Unit =
  given support: UringPerThreadSupport = UringPerThreadSupport()
  given fs: FileSupport = support.fileSupport

  val path = s"/tmp/gears-file-write-example-${scala.scalanative.posix.unistd.getpid()}.txt"
  val content = "the quick brown fox jumps over the lazy dog\n" * 50

  Async.blocking:
    fs.openWrite(path) match
      case Left(e)     => fail(s"could not open for writing: $e")
      case Right(file) =>
        try
          val bytes = content.getBytes(StandardCharsets.UTF_8)
          var offset = 0
          while offset < bytes.length do
            val chunk = math.min(37, bytes.length - offset)
            file.writeBuf(ByteBuffer.wrap(bytes, offset, chunk)) match
              case Right(()) => offset += chunk
              case Left(e)   => throw new RuntimeException(s"write failed: $e")
          println(s"wrote ${bytes.length} bytes to $path")
        finally file.close()

    fs.openRead(path) match
      case Left(e)     => fail(s"could not reopen for reading: $e")
      case Right(file) =>
        try
          val buf = ByteBuffer.allocate(4096)
          val out = new StringBuilder
          var reading = true
          while reading do
            buf.clear()
            file.readBuf(buf) match
              case Right(()) =>
                buf.flip()
                val bytes = new Array[Byte](buf.remaining())
                buf.get(bytes)
                out ++= new String(bytes, StandardCharsets.UTF_8)
              case Left(Error.EOF) => reading = false
          check(out.toString == content, s"read back ${out.length} bytes that round-trip")
        finally file.close()

  Files.deleteIfExists(Paths.get(path))
