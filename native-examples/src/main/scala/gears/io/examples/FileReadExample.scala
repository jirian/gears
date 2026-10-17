package gears.io.examples

import gears.async._
import gears.io.file.FileSupport
import gears.io.uring.UringPerThreadSupport
import gears.io.{Error, Result}

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

def fileReadExample(): Unit =
  given support: UringPerThreadSupport = UringPerThreadSupport()
  given fs: FileSupport = support.fileSupport

  val path = "native-examples/file-to-read.txt"
  val expected = new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8)
  println(s"expecting to read ${expected.length} bytes from $path")

  def readAll(read: ByteBuffer => Result[Unit]): String =
    val buf = ByteBuffer.allocate(37)
    val out = new StringBuilder
    var reading = true
    while reading do
      buf.clear()
      read(buf) match
        case Right(()) =>
          buf.flip()
          val bytes = new Array[Byte](buf.remaining())
          buf.get(bytes)
          out ++= new String(bytes, StandardCharsets.UTF_8)
        case Left(Error.EOF) => reading = false
    out.toString

  Async.blocking:
    fs.openRead(path) match
      case Left(e)     => fail(s"could not open: $e")
      case Right(file) =>
        try
          val text = readAll(file.readBuf)
          check(text == expected, s"manual readBuf loop read ${text.length} bytes matching the file")
        finally file.close()

    fs.openRead(path) match
      case Left(e)     => fail(s"could not open a second time: $e")
      case Right(file) =>
        try
          val text = readAll(file.buffered(4096).readBuf)
          check(text == expected, s"BufferedReader.readBuf read ${text.length} bytes matching the file")
        finally file.close()

    try
      fs.openRead("native-examples/does-not-exist.txt")
      fail("opened a missing file")
    catch case e: java.io.FileNotFoundException => println(s"correctly failed to open a missing file: $e")
