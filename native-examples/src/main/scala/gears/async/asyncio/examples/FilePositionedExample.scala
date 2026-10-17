package gears.async.asyncio.examples

import gears.async.*
import gears.async.asyncio.uring.UringPerThreadSupport
import gears.async.file.{FileSupport, WriteOptions}

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

def filePositionedExample(): Unit = withUring:
  given FileSupport = summon[UringPerThreadSupport].fileSupport
  val path = s"/tmp/gears-positioned-example-${scala.scalanative.posix.unistd.getpid()}.txt"
  def text(buf: ByteBuffer): String =
    buf.flip()
    StandardCharsets.UTF_8.decode(buf).toString
  try
    Async.blocking:
      val out = orFail("create")(FileSupport.openWrite(path, WriteOptions(exclusive = true, permissions = 0x180)))
      try
        orFail("writeAt")(out.writeAt(6, bytes("world")))
        orFail("writeAt")(out.writeAt(0, bytes("hello ")))
        out.sync()
        println(s"wrote out of order with writeAt; metadata: ${out.metadata()}")
        out.truncate(5)
        println(s"after truncate(5) the size is ${out.metadata().size}")
      finally out.close()

      try
        FileSupport.openWrite(path, WriteOptions(exclusive = true)) match
          case Right(again) =>
            again.close()
            fail("created the file a second time")
          case Left(e) => println(s"exclusive create of an existing file fails: $e")
      catch case e: java.io.IOException => println(s"exclusive create of an existing file fails: ${e.getMessage}")

      val appender =
        orFail("open for append")(FileSupport.openWrite(path, WriteOptions(append = true)))
      try orFail("append")(appender.writeBuf(bytes(", appended")))
      finally appender.close()

      val in = orFail("open")(FileSupport.openRead(path))
      try
        val tail = ByteBuffer.allocate(8)
        orFail("readAt")(in.readAt(7, tail))
        println(s"readAt(7) reads '${text(tail)}' and leaves the cursor at ${in.position()}")
        in.seek(5)
        val rest = ByteBuffer.allocate(32)
        orFail("read")(in.readBuf(rest))
        println(s"after seek(5) a cursor read gives '${text(rest)}', cursor now at ${in.position()}")
      finally in.close()
      println(s"file now reads '${new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8)}'")
  finally Files.deleteIfExists(Paths.get(path))
