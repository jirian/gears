package gears.io.http

import gears.async.Async
import gears.async.io.IpAddress
import gears.async.io.IpSocketAddress
import gears.io.net.BufferedInput
import gears.io.net.Stream

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt

import munit.FunSuite

class WireSuite extends FunSuite {

  private final class InMemory(text: String) extends Stream {
    private val bytes = ByteBuffer.wrap(text.getBytes(StandardCharsets.ISO_8859_1))
    def read(buf: ByteBuffer, timeout: Duration)(using Async): Int =
      if !bytes.hasRemaining then -1
      else {
        val n = math.min(buf.remaining(), bytes.remaining())
        val slice = bytes.duplicate()
        slice.limit(slice.position() + n)
        buf.put(slice)
        bytes.position(bytes.position() + n)
        n
      }
    def write(buf: ByteBuffer, timeout: Duration)(using Async): Unit = buf.position(buf.limit()): Unit
    def shutdownOutput()(using Async): Unit = ()
    def close()(using Async): Unit = ()
    def localAddress = IpSocketAddress(IpAddress.IPv4Loopback, 0)
    def peerAddress = IpSocketAddress(IpAddress.IPv4Loopback, 0)
  }

  private def head(text: String)(using Async): Wire.RequestHead | Null =
    Wire.readRequestHead(new BufferedInput(new InMemory(text)), Limits(), 1.second, 1.second)

  private def refusal(text: String)(using Async): Int =
    try {
      head(text)
      0
    } catch case e: Wire.Malformed => e.status.code

  test("a request head is parsed into its method, target, version and fields") {
    Http.run {
      val h = head("POST /a?b=c HTTP/1.0\r\nHost: x\r\nX-Two: 1\r\nX-Two: 2\r\n\r\n").nn
      assertEquals(h.method, Method.Post)
      assertEquals(h.target, "/a?b=c")
      assertEquals(h.version, HttpVersion.Http10)
      assertEquals(h.headers.getAll("X-Two"), Seq("1", "2"))
      assertEquals(head(""), null, "a stream that ends before a byte is no request")
    }
  }

  test("a malformed request head is refused with its status") {
    Http.run {
      for (text, status) <- Seq(
          "GET / HTTP/1.1\nHost: x\r\n\r\n" -> 400,
          "GET / HTTP/1.1\r\nHost: x\rY: z\r\n\r\n" -> 400,
          "GET / HTTP/1.1\r\nHost : x\r\n\r\n" -> 400,
          "GET / HTTP/1.1\r\nHost: x\r\n folded\r\n\r\n" -> 400,
          "GET /  HTTP/1.1\r\nHost: x\r\n\r\n" -> 400,
          "G(T / HTTP/1.1\r\nHost: x\r\n\r\n" -> 400,
          "GET / HTTP/1.1x\r\nHost: x\r\n\r\n" -> 400,
          "GET / HTTP/2.0\r\nHost: x\r\n\r\n" -> 505,
          s"GET / HTTP/1.1\r\n${(1 to 101).map(i => s"X-$i: v\r\n").mkString}\r\n" -> 431,
          "GET / HTTP/1.1\r\nHost: x\r\n" -> 400,
          "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: \u000bchunked\r\n\r\n" -> 400,
          "GET / HTTP/1.1\r\nHost: x\u000c\r\n\r\n" -> 400
        )
      do assertEquals(refusal(text), status, text)
    }
  }

  private def chunked(text: String)(using Async): Either[Int, String] = {
    val source = new Wire.Source(
      new BufferedInput(new InMemory(text)),
      Wire.Framing.Chunked,
      1 << 20,
      1.second,
      () => (),
      n => new java.io.IOException(s"over $n")
    )
    val buf = ByteBuffer.allocate(256)
    try {
      while source.read(buf) >= 0 do ()
      buf.flip()
      Right(StandardCharsets.ISO_8859_1.decode(buf).toString)
    } catch case e: Wire.Malformed => Left(e.status.code)
  }

  test("a chunked body: sizes in hex, extensions and trailers allowed as RFC 9112 has them") {
    Http.run {
      assertEquals(chunked("5\r\nhello\r\n0\r\n\r\n"), Right("hello"))
      assertEquals(chunked("A\r\n0123456789\r\n0\r\n\r\n"), Right("0123456789"))
      assertEquals(chunked("5;a=b;c\r\nhello\r\n0\r\n\r\n"), Right("hello"))
      assertEquals(chunked("5 ; a = \"x \\\" y\"\r\nhello\r\n0\r\n\r\n"), Right("hello"), "whitespace around ; and =")
      assertEquals(chunked("5\r\nhello\r\n0\r\nX-Sum: 1\r\nX-Other: two\r\n\r\n"), Right("hello"), "trailers")
    }
  }

  test("a chunk line or trailer that another parser could end elsewhere is refused") {
    Http.run {
      for text <- Seq(
          "2;\nxx\r\nhi\r\n0\r\n\r\n",
          "2;a\rb\r\nhi\r\n0\r\n\r\n",
          "2\nhi\r\n0\r\n\r\n",
          " 2\r\nhi\r\n0\r\n\r\n",
          "2 \r\nhi\r\n0\r\n\r\n",
          "2;\r\nhi\r\n0\r\n\r\n",
          "2;a=\r\nhi\r\n0\r\n\r\n",
          "2;a=\"x\r\nhi\r\n0\r\n\r\n",
          "2;a=b c\r\nhi\r\n0\r\n\r\n",
          "+2\r\nhi\r\n0\r\n\r\n",
          "0x2\r\nhi\r\n0\r\n\r\n",
          "\u00b2\r\nhi\r\n0\r\n\r\n",
          "1234567890abcdef\r\n",
          "2\r\nhi\r\n0\r\nX-A: 1\nX-B: 2\r\n\r\n",
          "2\r\nhi\r\n0\r\nno colon\r\n\r\n",
          "2\r\nhi\r\n0\r\nX-A: 1\r\n folded\r\n\r\n",
          "2\r\nhiX\r\n0\r\n\r\n"
        )
      do assertEquals(chunked(text), Left(400), text)
    }
  }

  test("a message's framing: chunked, a length, or none, and the ambiguous ones refused") {
    def framing(fields: (String, String)*): Wire.Framing | Int =
      try Wire.framing(Headers(fields*), Wire.Framing.NoBody)
      catch case e: Wire.Malformed => e.status.code
    assertEquals(framing(), Wire.Framing.NoBody)
    assertEquals(framing("Transfer-Encoding" -> "chunked"), Wire.Framing.Chunked)
    assertEquals(framing("Transfer-Encoding" -> "Chunked"), Wire.Framing.Chunked)
    assertEquals(framing("Content-Length" -> "5"), Wire.Framing.Length(5))
    assertEquals(framing("Content-Length" -> "0"), Wire.Framing.NoBody)
    assertEquals(framing("Content-Length" -> "5", "Content-Length" -> "5"), Wire.Framing.Length(5), "the same twice")
    assertEquals(framing("Content-Length" -> "5, 5"), Wire.Framing.Length(5))
    assertEquals(framing("Content-Length" -> "5", "Content-Length" -> "6"), 400, "lengths that disagree")
    assertEquals(framing("Content-Length" -> "5, 6"), 400)
    assertEquals(framing("Content-Length" -> "+5"), 400)
    assertEquals(framing("Content-Length" -> "5 5"), 400)
    assertEquals(framing("Transfer-Encoding" -> "chunked", "Content-Length" -> "5"), 400)
    assertEquals(framing("Transfer-Encoding" -> ""), 400)
    assertEquals(framing("Transfer-Encoding" -> "gzip"), 501, "a coding other than chunked")
    assertEquals(framing("Transfer-Encoding" -> "chunked, gzip"), 501, "chunked not last")
    assertEquals(framing("Transfer-Encoding" -> "gzip, chunked"), 501)
    assertEquals(framing("Transfer-Encoding" -> "chunked, chunked"), 501)
  }

  test("a body in memory, or one a function writes, is read from its start by each of its readers") {
    Http.run {
      def all(reader: Body.Reader): String = {
        val buf = ByteBuffer.allocate(2)
        val out = new StringBuilder
        while reader.read(buf) >= 0 do {
          out ++= new String(buf.array(), 0, buf.position(), StandardCharsets.UTF_8)
          buf.clear()
        }
        out.result()
      }
      for body <- Seq(Body.text("hello"), Body.stream("text/plain")(_.write("hello"))) do {
        val first = body.reader()
        val second = body.reader()
        assertEquals(all(first), "hello")
        assertEquals(all(second), "hello", "the second reader has a position of its own")
        assertEquals(all(body.reader()), "hello")
      }
    }
  }

  test("a known method is only its own case, never Other") {
    assertEquals(Method.parse("GET"), Method.Get)
    assertEquals(Method.parse("PURGE"), Method.Other("PURGE"))
    intercept[IllegalArgumentException](Method.Other("GET"))
  }

  test("header names compare ignoring case, as equalsIgnoreCase does, and a value's elements are found trimmed") {
    val h = Headers("Content-Type" -> "text/plain", "connection" -> "Upgrade ,  Close", "X-Id" -> "1")
    assertEquals(h.get("CONTENT-TYPE"), Some("text/plain"))
    assertEquals(h.get("Content-Typf"), None)
    assertEquals(h.get("Content_Type"), None)
    assert(h.hasToken("Connection", "close"))
    assert(h.hasToken("Connection", "UPGRADE"))
    assert(!h.hasToken("Connection", "clos"))
    assert(!h.hasToken("Connection", ""))
    for (a, b) <- Seq("@" -> "`", "[" -> "{", "\u00e9" -> "\u00c9", "\u0131" -> "I", "k" -> "\u212a") do
      assertEquals(Text.sameName(a, b), a.equalsIgnoreCase(b), s"$a vs $b")
    assertEquals(h.set("CONNECTION", "close").getAll("connection"), Seq("close"))
    assertEquals(h.remove("x-ID").size, 2)
    assertEquals((h ++ Headers("A" -> "b")).iterator.toList.last, "A" -> "b")
    assertEquals(h, Headers("Content-Type" -> "text/plain", "connection" -> "Upgrade ,  Close", "X-Id" -> "1"))
  }

  private final class Recording extends Stream {
    val written = new java.io.ByteArrayOutputStream()
    val buffers = scala.collection.mutable.ArrayBuffer.empty[ByteBuffer]
    val arrays = scala.collection.mutable.ArrayBuffer.empty[Array[ByteBuffer]]
    private def drain(buf: ByteBuffer): Unit = {
      val bytes = new Array[Byte](buf.remaining())
      buf.get(bytes)
      written.write(bytes)
    }
    def read(buf: ByteBuffer, timeout: Duration)(using Async): Int = -1
    def write(buf: ByteBuffer, timeout: Duration)(using Async): Unit = {
      buffers += buf
      drain(buf)
    }
    override def write(bufs: Array[ByteBuffer], timeout: Duration)(using Async): Unit = {
      arrays += bufs
      bufs.foreach(drain)
    }
    def shutdownOutput()(using Async): Unit = ()
    def close()(using Async): Unit = ()
    def localAddress = IpSocketAddress(IpAddress.IPv4Loopback, 0)
    def peerAddress = IpSocketAddress(IpAddress.IPv4Loopback, 0)
  }

  test("a streamed body's large writes from one array go out in the same buffers each time, framed as before") {
    Http.run {
      for chunked <- Seq(true, false) do {
        val stream = Recording()
        val out = new Wire.OutBuffer(16 * 1024)
        out.putLatin1("HEAD\r\n")
        val chunk = new Array[Byte](40 * 1024)
        val declared = if chunked then -1L else 3L * chunk.length
        val body = new Wire.Out(stream, out, chunked, declared, suppress = false, 1.second)
        val expected = new java.io.ByteArrayOutputStream()
        expected.write("HEAD\r\n".getBytes(StandardCharsets.ISO_8859_1))
        for i <- 0 until 3 do {
          java.util.Arrays.fill(chunk, ('a' + i).toByte)
          body.write(chunk, 0, chunk.length)
          val size = s"${Integer.toHexString(chunk.length)}\r\n"
          if chunked then expected.write(size.getBytes(StandardCharsets.ISO_8859_1))
          expected.write(chunk)
          if chunked then expected.write(Wire.Crlf)
        }
        body.finish()
        if chunked then expected.write("0\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1))
        assert(java.util.Arrays.equals(stream.written.toByteArray, expected.toByteArray), s"chunked: $chunked")
        if chunked then {
          assertEquals(stream.arrays.size, 3)
          assert(stream.arrays.forall(_ eq stream.arrays.head), "a new array for a write")
        } else {
          assertEquals((stream.arrays.size, stream.buffers.size), (1, 2))
          assert(stream.buffers(1) eq stream.buffers(0), "a new buffer for a write")
        }
      }
    }
  }
}
