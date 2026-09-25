package gears.async.asyncio.examples

import gears.async._
import gears.async.asyncio.Error
import gears.async.asyncio.uring.UringPerThreadSupport
import gears.async.file.FileSupport

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

@main def fileWriteExample(): Unit =
  given support: UringPerThreadSupport = UringPerThreadSupport()
  given fs: FileSupport = support.fileSupport

  val path = "native/src/main/scala/async/asyncio/examples/file-written-by-example.txt"
  val content = "the quick brown fox jumps over the lazy dog\n" * 50

  Async.blocking:
    fs.openWrite(path) match
      case Left(e) => println(s"FAILED to open for writing: $e")
      case Right(file) =>
        try
          val bytes = content.getBytes(StandardCharsets.UTF_8)
          var offset = 0
          while offset < bytes.length do
            val chunk = math.min(37, bytes.length - offset) // small, to force several writeBuf calls
            file.writeBuf(ByteBuffer.wrap(bytes, offset, chunk)) match
              case Right(()) => offset += chunk
              case Left(e)    => throw new RuntimeException(s"write failed: $e")
          println(s"wrote ${bytes.length} bytes to $path")
        finally file.close()

    fs.openRead(path) match
      case Left(e) => println(s"FAILED to reopen for reading: $e")
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
          println(s"read back ${out.length} bytes, round-trips correctly: ${out.toString == content}")
        finally file.close()

  Files.deleteIfExists(Paths.get(path))
  System.exit(0)
