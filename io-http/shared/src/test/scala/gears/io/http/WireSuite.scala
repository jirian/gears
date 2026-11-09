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

/** The wire format on its own, over a stream in memory: how a request head is parsed and refused, and how a message's
  * headers frame its body.
  */
class WireSuite extends FunSuite {

  /** A stream that reads `text`, ISO-8859-1, then ends, and drops what is written to it. */
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

  /** The status a head is refused with, or 0 if it is not. */
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
      assertEquals(h.version, "HTTP/1.0")
      assertEquals(h.headers.getAll("X-Two"), Seq("1", "2"))
      assertEquals(head(""), null, "a stream that ends before a byte is no request")
    }
  }

  test("a malformed request head is refused with its status") {
    Http.run {
      for (text, status) <- Seq(
          "GET / HTTP/1.1\nHost: x\r\n\r\n" -> 400, // a bare LF
          "GET / HTTP/1.1\r\nHost: x\rY: z\r\n\r\n" -> 400, // a bare CR
          "GET / HTTP/1.1\r\nHost : x\r\n\r\n" -> 400, // a space before the colon
          "GET / HTTP/1.1\r\nHost: x\r\n folded\r\n\r\n" -> 400, // an obsolete line folding
          "GET /  HTTP/1.1\r\nHost: x\r\n\r\n" -> 400, // three spaces in the request line
          "G(T / HTTP/1.1\r\nHost: x\r\n\r\n" -> 400, // a method that is not a token
          "GET / HTTP/1.1x\r\nHost: x\r\n\r\n" -> 400,
          "GET / HTTP/2.0\r\nHost: x\r\n\r\n" -> 505,
          s"GET / HTTP/1.1\r\n${(1 to 101).map(i => s"X-$i: v\r\n").mkString}\r\n" -> 431, // over 100 fields
          "GET / HTTP/1.1\r\nHost: x\r\n" -> 400 // the stream ends within the head
        )
      do assertEquals(refusal(text), status, text)
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
        val buf = ByteBuffer.allocate(2) // a few bytes at a time
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
}
