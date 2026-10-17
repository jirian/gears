package gears.io.http

import gears.async.default.given
import gears.async.{Async, AsyncOperations, Future, SyncChannel}
import gears.io.net.TcpStream
import gears.io.{Error, Result}

import java.io.{ByteArrayOutputStream, EOFException}
import java.net.{InetSocketAddress, SocketAddress}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8

class WebSocketSuite extends munit.FunSuite:
  private class MemoryStream(bytes: Array[Byte], chunk: Int = 1) extends TcpStream:
    private val input = ByteBuffer.wrap(bytes)
    val output = ByteArrayOutputStream()
    var closed = false
    def localAddress: SocketAddress = InetSocketAddress("127.0.0.1", 1)
    def remoteAddress: SocketAddress = InetSocketAddress("127.0.0.1", 2)
    def close(): Unit = closed = true
    def shutdownInput()(using Async): Unit = ()
    def shutdownOutput()(using Async): Unit = ()
    def readBuf(buf: ByteBuffer)(using Async): Result[Unit] =
      if !input.hasRemaining then Left(Error.EOF)
      else
        val n = math.min(chunk, math.min(input.remaining(), buf.remaining()))
        for _ <- 0 until n do buf.put(input.get())
        Right(())
    def writeBuf(buf: ByteBuffer)(using Async): Result[Unit] =
      while buf.hasRemaining do output.write(buf.get() & 255)
      Right(())

  private def socket(wire: Array[Byte], client: Boolean = true, limit: Int = 1024): (WebSocket, MemoryStream) =
    val stream = MemoryStream(wire)
    (new WebSocket(stream, stream, client, None, limit), stream)

  private def frame(opcode: Int, bytes: Array[Byte], fin: Boolean = true, masked: Boolean = false): Array[Byte] =
    require(bytes.length < 126)
    val head = Array((opcode | (if fin then 0x80 else 0)).toByte, (bytes.length | (if masked then 0x80 else 0)).toByte)
    if !masked then head ++ bytes
    else
      val mask = Array[Byte](0x37, 0xfa.toByte, 0x21, 0x3d)
      head ++ mask ++ bytes.zipWithIndex.map((b, i) => (b ^ mask(i & 3)).toByte)

  test("RFC 6455 handshake example and key validation"):
    assertEquals(WebSocketProtocol.accept("dGhlIHNhbXBsZSBub25jZQ=="), "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=")
    assert(WebSocketProtocol.validKey("dGhlIHNhbXBsZSBub25jZQ=="))
    assert(!WebSocketProtocol.validKey("dGhlIHNhbXBsZSBub25jZR=="))
    assert(!WebSocketProtocol.validKey("short"))

  test("masked RFC Hello example is decoded on the server, even with one-byte reads"):
    Async.fromSync:
      val wire =
        Array[Byte](0x81.toByte, 0x85.toByte, 0x37, 0xfa.toByte, 0x21, 0x3d, 0x7f, 0x9f.toByte, 0x4d, 0x51, 0x58)
      val (ws, _) = socket(wire, client = false)
      assertEquals(ws.receive(), Some(WebSocket.Message.Text("Hello")))

  test("fragmented text permits an interleaved ping and UTF-8 split across fragments"):
    Async.fromSync:
      val text = "a€z".getBytes(UTF_8)
      val wire = frame(1, text.take(2), fin = false) ++ frame(9, Array[Byte](42)) ++ frame(0, text.drop(2))
      val (ws, stream) = socket(wire)
      assertEquals(ws.receive(), Some(WebSocket.Message.Text("a€z")))
      val pong = stream.output.toByteArray
      assertEquals(pong(0) & 255, 0x8a)
      assertEquals(pong(1) & 255, 0x81)
      assertEquals((pong(6) ^ pong(2)).toByte, 42.toByte)

  test("server sends unmasked text; client sends fresh masked binary frames"):
    Async.fromSync:
      val (server, output) = socket(Array.emptyByteArray, client = false)
      server.sendText("Hello")
      assertEquals(output.output.toByteArray.toSeq, (Array[Byte](0x81.toByte, 5) ++ "Hello".getBytes(UTF_8)).toSeq)
      val (client, masked) = socket(Array.emptyByteArray)
      client.sendBinary(Array[Byte](1, 2, 3))
      val bytes = masked.output.toByteArray
      assertEquals(bytes.take(2).toSeq, Seq(0x82.toByte, 0x83.toByte))
      assertEquals((0 until 3).map(i => (bytes(6 + i) ^ bytes(2 + i)).toByte), Seq[Byte](1, 2, 3))

  for size <- Seq(0, 125, 126, 65535, 65536) do
    test(s"binary payload of $size bytes uses the correct length encoding"):
      Async.fromSync:
        val data = Array.tabulate[Byte](size)(_.toByte)
        val (sender, output) = socket(Array.emptyByteArray, client = false, limit = 100000)
        sender.sendBinary(data)
        val bytes = output.output.toByteArray
        assertEquals(bytes(1) & 127, if size < 126 then size else if size <= 65535 then 126 else 127)
        val stream = MemoryStream(bytes, chunk = 4096)
        val receiver = new WebSocket(stream, stream, true, None, 100000)
        receiver.receive() match
          case Some(WebSocket.Message.Binary(actual)) => assertEquals(actual.toSeq, data.toSeq)
          case other                                  => fail(s"Expected binary message, got $other")

  test("a close frame is acknowledged, exposes its status, and releases the stream"):
    Async.fromSync:
      val payload = Array[Byte](3, 0xe8.toByte) ++ "bye".getBytes(UTF_8)
      val (ws, stream) = socket(frame(8, payload, masked = true), client = false)
      assertEquals(ws.receive(), None)
      assertEquals(ws.peerClose, Some(WebSocket.Close(Some(1000), "bye")))
      assertEquals(stream.output.toByteArray.toSeq, frame(8, payload).toSeq)
      assert(stream.closed)
      assertEquals(ws.receive(), None)

  test("an empty close frame is echoed without inventing a status code"):
    Async.fromSync:
      val (ws, stream) = socket(frame(8, Array.emptyByteArray, masked = true), client = false)
      assertEquals(ws.receive(), None)
      assertEquals(ws.peerClose, Some(WebSocket.Close(None, "")))
      assertEquals(stream.output.toByteArray.toSeq, Seq[Byte](0x88.toByte, 0))

  val invalidFrames = Seq(
    "masked server frame" -> frame(1, Array[Byte](65), masked = true),
    "reserved bits" -> Array[Byte](0xc1.toByte, 0),
    "unknown opcode" -> Array[Byte](0x83.toByte, 0),
    "unexpected continuation" -> frame(0, Array[Byte](65)),
    "overlapping fragments" -> (frame(1, Array[Byte](65), fin = false) ++ frame(1, Array[Byte](66))),
    "fragmented control" -> frame(9, Array.emptyByteArray, fin = false),
    "oversized control" -> Array[Byte](0x89.toByte, 126),
    "non-minimal length" -> Array[Byte](0x82.toByte, 126, 0, 1),
    "negative length" -> (Array[Byte](0x82.toByte, 127) ++ Array.fill[Byte](8)(-1)),
    "one-byte close" -> frame(8, Array[Byte](1)),
    "reserved close code" -> frame(8, Array[Byte](3, 0xed.toByte))
  )
  for (name, wire) <- invalidFrames do
    test(s"rejects $name and sends protocol-error close"):
      Async.fromSync:
        val (ws, stream) = socket(wire)
        assertEquals(intercept[WebSocket.ProtocolException](ws.receive()).closeCode, 1002)
        assertEquals(stream.output.toByteArray.head & 255, 0x88)
        assert(stream.closed)

  test("invalid UTF-8 in a message or close reason sends close code 1007"):
    Async.fromSync:
      for wire <- Seq(frame(1, Array[Byte](0xc0.toByte, 0xaf.toByte)), frame(8, Array[Byte](3, 0xe8.toByte, -1))) do
        val (ws, stream) = socket(wire)
        assertEquals(intercept[WebSocket.ProtocolException](ws.receive()).closeCode, 1007)
        assert(stream.closed)

  test("the message limit applies to all fragments together, before allocating their payload"):
    Async.fromSync:
      val (ws, stream) = socket(frame(2, Array[Byte](1, 2), fin = false) ++ frame(0, Array[Byte](3, 4)), limit = 3)
      assertEquals(intercept[WebSocket.ProtocolException](ws.receive()).closeCode, 1009)
      assert(stream.closed)

  test("TCP EOF without a close frame is an abnormal termination"):
    Async.fromSync:
      val (ws, stream) = socket(Array[Byte](0x81.toByte, 4, 65))
      intercept[EOFException](ws.receive())
      assert(stream.closed)

  test("HTTP upgrade reader preserves bytes read beyond the head"):
    Async.fromSync:
      val wire = frame(1, "early".getBytes(UTF_8))
      val http = "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n\r\n".getBytes(UTF_8)
      val stream = MemoryStream(http ++ wire, chunk = 8192)
      val reader = RequestReader(stream)
      assert(reader.readHead().nonEmpty)
      val ws = new WebSocket(stream, reader, true, None, 100)
      assertEquals(ws.receive(), Some(WebSocket.Message.Text("early")))

  test("overlapping receives are rejected without closing the active reader"):
    Async.fromSync:
      val entered = SyncChannel[Unit]()
      val resume = SyncChannel[Unit]()
      val stream = new MemoryStream(frame(1, "ok".getBytes(UTF_8))):
        private var first = true
        override def readBuf(buf: ByteBuffer)(using Async): Result[Unit] =
          if first then
            first = false
            entered.send(())
            resume.read()
          super.readBuf(buf)
      val ws = new WebSocket(stream, stream, true, None, 100)
      val first = Future(ws.receive())
      entered.read()
      intercept[WebSocket.ConcurrentReceiveException](ws.receive())
      assert(!stream.closed)
      resume.send(())
      assertEquals(first.await, Some(WebSocket.Message.Text("ok")))

  test("cancelling a receive closes the connection and waits for the read to unwind"):
    Async.fromSync:
      val entered = SyncChannel[Unit]()
      val blocked = SyncChannel[Unit]()
      val stream = new MemoryStream(Array.emptyByteArray):
        override def readBuf(buf: ByteBuffer)(using Async): Result[Unit] =
          entered.send(())
          blocked.read()
          Right(())
      val ws = new WebSocket(stream, stream, true, None, 100)
      val receiving = Future(ws.receive())
      entered.read()
      receiving.cancel()
      assert(receiving.awaitResult.isFailure)
      assert(stream.closed)

  test("concurrent senders serialize whole frames across suspending writes"):
    Async.fromSync:
      val active = new java.util.concurrent.atomic.AtomicInteger()
      val stream = new MemoryStream(Array.emptyByteArray):
        override def writeBuf(buf: ByteBuffer)(using Async): Result[Unit] =
          assertEquals(active.incrementAndGet(), 1, "Two frame writes overlapped")
          try
            while buf.hasRemaining do
              output.write(buf.get() & 255)
              AsyncOperations.sleep(1)
            Right(())
          finally active.decrementAndGet()
      val ws = new WebSocket(stream, stream, false, None, 100)
      val sends = (1 to 8).map(i => Future(ws.sendText(i.toString)))
      sends.foreach(_.await)
      val input = MemoryStream(stream.output.toByteArray)
      val peer = new WebSocket(input, input, true, None, 100)
      val messages = (1 to 8).map(_ => peer.receive().get).toSet
      assertEquals(messages, (1 to 8).map(i => WebSocket.Message.Text(i.toString)).toSet)
