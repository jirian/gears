import gears.async.*
import gears.async.asyncio.Error
import gears.async.asyncio.reactor.{ReactorDriver, SingleReactorDriver, fileSupport}
import gears.async.default.given
import gears.async.file.{FileSupport, WriteOptions}

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files

abstract class FileBehavior extends munit.FunSuite:
  protected def driver: ReactorDriver
  private def withFile(body: (String, FileSupport) => Async ?=> Unit): Unit = Async.blocking:
    val path = Files.createTempFile("gears-file-", ".dat")
    try body(path.toString, driver.fileSupport)
    finally Files.deleteIfExists(path)

  test("sequential and positioned I/O, seeking, metadata, truncation, and sync"):
    withFile: (path, fs) =>
      val out = fs.openWrite(path).toOption.get
      try
        out.writeBuf(ByteBuffer.wrap("abcdef".getBytes(UTF_8)))
        assertEquals(out.position(), 6L)
        out.writeAt(2, ByteBuffer.wrap("XY".getBytes(UTF_8)))
        assertEquals(out.position(), 6L)
        out.seek(1)
        out.writeBuf(ByteBuffer.wrap(Array[Byte](33)))
        out.truncate(5)
        out.sync()
        assertEquals(out.metadata().size, 5L)
        assert(out.metadata().isRegular)
      finally out.close()
      val in = fs.openRead(path).toOption.get
      try
        val buf = ByteBuffer.allocate(8)
        in.readAt(2, buf)
        assertEquals(in.position(), 0L)
        assertEquals(new String(buf.array(), 0, buf.position(), UTF_8), "XYe")
        in.seek(1)
        buf.clear()
        in.readBuf(buf)
        assertEquals(new String(buf.array(), 0, buf.position(), UTF_8), "!XYe")
        assertEquals(in.readBuf(ByteBuffer.allocate(1)), Left(Error.EOF))
        assertEquals(in.readBuf(ByteBuffer.allocate(0)), Right(()))
        assertEquals(in.buffered(2).readAll().toOption.get.toSeq, Seq.empty)
      finally in.close()

  test("append preserves content, exclusive create refuses an existing file, and closing refuses later I/O"):
    withFile: (path, fs) =>
      val first = fs.openWrite(path).toOption.get
      first.writeBuf(ByteBuffer.wrap("one".getBytes(UTF_8)))
      first.close()
      val append = fs.openWrite(path, WriteOptions(append = true)).toOption.get
      try
        append.writeBuf(ByteBuffer.wrap("two".getBytes(UTF_8)))
        intercept[IllegalArgumentException](append.writeAt(0, ByteBuffer.wrap(Array[Byte](1))))
      finally append.close()
      intercept[java.io.IOException](fs.openWrite(path, WriteOptions(exclusive = true)))
      assertEquals(new String(Files.readAllBytes(java.nio.file.Paths.get(path)), UTF_8), "onetwo")
      intercept[java.nio.channels.ClosedChannelException](first.writeBuf(ByteBuffer.allocate(1)))
      intercept[java.io.FileNotFoundException](fs.openRead(path + ".missing"))

  test("buffered reads preserve unread bytes after a partial consumption"):
    withFile: (path, fs) =>
      Files.write(java.nio.file.Paths.get(path), "abcdef".getBytes(UTF_8))
      val in = fs.openRead(path).toOption.get
      try
        val reader = in.buffered(4)
        val first = ByteBuffer.allocate(1)
        reader.readBuf(first)
        assertEquals(first.array().toSeq, Seq('a'.toByte))
        assertEquals(new String(reader.readAll().toOption.get.toArray, UTF_8), "bcdef")
      finally in.close()

class ReactorDriverFileSuite extends FileBehavior:
  private lazy val backend = SingleReactorDriver(() => TestReactor.open())
  protected def driver: ReactorDriver = backend
  override def afterAll(): Unit = backend.close()
