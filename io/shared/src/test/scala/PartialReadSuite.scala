package gears.io.http

import gears.async.Async
import gears.async.default.given
import gears.io.net.TcpStream
import gears.io.{Error, Reader, Result}

import java.net.{InetSocketAddress, SocketAddress}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.concurrent.CancellationException

class PartialReadSuite extends munit.FunSuite:
  private def interruptedThenComplete(first: String, second: String): Reader = new Reader:
    private var reads = 0
    def readBuf(buf: ByteBuffer)(using Async): Result[Unit] =
      reads += 1
      reads match
        case 1 =>
          buf.put(first.getBytes(StandardCharsets.ISO_8859_1))
          throw CancellationException()
        case 2 =>
          buf.put(second.getBytes(StandardCharsets.ISO_8859_1))
          Right(())
        case _ => Left(Error.EOF)

  private def stream(reader: Reader): TcpStream = new TcpStream:
    def readBuf(buf: ByteBuffer)(using Async): Result[Unit] = reader.readBuf(buf)
    def writeBuf(buf: ByteBuffer)(using Async): Result[Unit] = Right(())
    def close(): Unit = ()
    def shutdownOutput()(using Async): Unit = ()
    def shutdownInput()(using Async): Unit = ()
    def localAddress: SocketAddress = InetSocketAddress(0)
    def remoteAddress: SocketAddress = InetSocketAddress(0)

  test("a request read cancelled after the stream delivered bytes keeps them"):
    Async.fromSync:
      val reader = RequestReader(stream(interruptedThenComplete("GET / HTTP/1.1\r\nHo", "st: x\r\n\r\n")))
      intercept[CancellationException](reader.readHead())
      val head = reader.readHead().map(new String(_, StandardCharsets.ISO_8859_1))
      assertEquals(head, Some("GET / HTTP/1.1\r\nHost: x"))

  test("a buffered read cancelled after the source delivered bytes keeps them"):
    Async.fromSync:
      val buffered = interruptedThenComplete("abc", "def").buffered(16)
      val out = ByteBuffer.allocate(16)
      intercept[CancellationException](buffered.readBuf(out))
      while out.position() < 6 && buffered.readBuf(out) == Right(()) do ()
      assertEquals(new String(out.array(), 0, out.position(), StandardCharsets.ISO_8859_1), "abcdef")
