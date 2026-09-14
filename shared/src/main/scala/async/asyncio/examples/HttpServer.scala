package gears.async.asyncio.examples

import gears.async._
import gears.async.net.TcpListener
import gears.async.net.TcpStream
import gears.async.asyncio.Error

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

/** Minimal single-request-per-connection HTTP/1.1 server: no HTTP framing/
  * keep-alive/chunked bodies, just enough to answer a GET with a fixed
  * response. Written entirely against `net.TcpListener`/`TcpStream` (the
  * shared, backend-agnostic interface), so it's the same code regardless of
  * which `net.TcpSupport` (uring/epoll/kqueue/...) is plugged in - each
  * backend only needs a few-line entry point that constructs its own
  * `Scheduler`/`TcpSupport` and calls [[serve]].
  */
def serve(listener: TcpListener)(using Async): Unit =
  Async.group:
    while true do
      listener.accept() match
        case Right(stream) => Future(handleConnection(stream))
        case Left(e)        => System.err.println(s"accept failed: $e")

private def handleConnection(stream: TcpStream)(using Async): Unit =
  try
    val request = readRequestHead(stream)
    val (method, path) = parseRequestLine(request)
    val body = s"Hello from gears! You requested $method $path\n"
    val bodyBytes = body.getBytes(StandardCharsets.UTF_8)
    val response =
      s"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: ${bodyBytes.length}\r\nConnection: close\r\n\r\n$body"
    stream.writeBuf(ByteBuffer.wrap(response.getBytes(StandardCharsets.UTF_8)))
  catch case e: Exception => System.err.println(s"connection error: $e")
  finally stream.close()

/** Reads (and discards) bytes until the end of the request headers
  * (`\r\n\r\n`) or EOF - no attempt to parse/consume a request body.
  */
private def readRequestHead(stream: TcpStream)(using Async): String =
  val buf = ByteBuffer.allocate(4096)
  val acc = new StringBuilder
  var done = false
  while !done do
    buf.clear()
    stream.readBuf(buf) match
      case Right(()) =>
        buf.flip()
        val bytes = new Array[Byte](buf.remaining())
        buf.get(bytes)
        acc ++= new String(bytes, StandardCharsets.UTF_8)
        if acc.indexOf("\r\n\r\n") >= 0 then done = true
      case Left(Error.EOF) => done = true
  acc.toString

private def parseRequestLine(request: String): (String, String) =
  request.linesIterator.nextOption().getOrElse("").split(" ").toList match
    case method :: path :: _ => (method, path)
    case _                   => ("?", "?")
