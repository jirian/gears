package gears.async.asyncio.examples

import gears.async._
import gears.async.asyncio.{Error, Result}
import gears.async.asyncio.uring.UringPerThreadSupport
import gears.async.file.FileSupport

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

@main def fileReadExample(): Unit =
  given support: UringPerThreadSupport = UringPerThreadSupport()
  given fs: FileSupport = support.fileSupport

  val path = "native/src/main/scala/async/asyncio/examples/file-to-read.txt"
  val expected = new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8)
  println(s"expecting to read ${expected.length} bytes from $path")

  def readAll(read: ByteBuffer => Result[Unit]): String =
    val buf = ByteBuffer.allocate(37) // smaller than the file, to force more than one read/EOF cycle
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
      case Left(e) => println(s"FAILED to open: $e")
      case Right(file) =>
        try
          val text = readAll(file.readBuf)
          println(s"manual readBuf loop: read ${text.length} bytes, matches file: ${text == expected}")
        finally file.close()

    fs.openRead(path) match
      case Left(e) => println(s"FAILED to open (second time): $e")
      case Right(file) =>
        try
          // Wrap in the shared BufferedReader and read through *its*
          // readBuf - proves the wrapper is genuinely re-fetching from
          // UringFileReader.readBuf via readToInternal, not just
          // special-casing the underlying reader.
          val text = readAll(file.buffered(4096).readBuf)
          println(s"BufferedReader.readBuf: read ${text.length} bytes, matches file: ${text == expected}")
        finally file.close()

    // A missing file is a genuine open-time error, not an in-band `Result`
    // outcome - it's surfaced as a thrown exception, matching
    // `TcpSupport.connect`/`listen`'s own convention (see FileSupport's doc).
    try
      fs.openRead("native/src/main/scala/async/asyncio/examples/does-not-exist.txt")
      println("unexpectedly opened a missing file")
    catch case e: java.io.FileNotFoundException => println(s"correctly failed to open a missing file: $e")

  System.exit(0)
