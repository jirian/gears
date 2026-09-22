package gears.async.http

import gears.async._
import gears.async.net.TcpListener
import gears.async.net.TcpStream
import gears.async.asyncio.Error

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeoutException
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.FiniteDuration

/** A server's configuration - Go's `http.Server`. [[readTimeout]]/
  * [[writeTimeout]] bound, respectively, how long reading a request's body
  * and running the handler (headers included - streamed writes each reset
  * nothing, so a slow handler racing a fast one can still blow this budget,
  * same as Go's own non-per-write `WriteTimeout`) may take once a request's
  * head has arrived; [[idleTimeout]] bounds how long a keep-alive
  * connection may sit idle waiting for the *next* request's head to start
  * arriving (Go's own `IdleTimeout`). All three default to `None`
  * (unbounded), matching `http.Server`'s zero-value defaults.
  */
final case class Server(
    handler: Handler,
    readTimeout: Option[FiniteDuration] = None,
    writeTimeout: Option[FiniteDuration] = None,
    idleTimeout: Option[FiniteDuration] = None
):
  /** Equivalent to `serve(listener, this)` - Go's `Server.Serve`. */
  def serve(listener: TcpListener)(using Async, AsyncOperations): Unit = gears.async.http.serve(listener, this)

/** A backend-agnostic HTTP/1.0 and HTTP/1.1 server, written entirely
  * against `net.TcpListener`/`TcpStream` - the same code runs regardless of
  * which `net.TcpSupport` (uring/epoll/kqueue/...) is plugged in. Each
  * backend only needs a few-line entry point that constructs its own
  * `Scheduler`/`TcpSupport`, builds a [[Handler]] (a [[Router]], typically),
  * and calls [[serve]] - or wraps it in a [[Server]] for timeouts.
  *
  * Supported: request-line/header parsing, `Content-Length` request and
  * response bodies, streaming responses via [[ResponseWriter]], `HEAD`
  * (dispatched to the matching `GET` handler, body suppressed once the real
  * `Content-Length` is known), persistent (keep-alive) connections -
  * HTTP/1.1 defaults to keep-alive and HTTP/1.0 to close, either
  * overridable by a `Connection` request header, and a streamed response
  * with no known length forces `Connection: close` regardless (see
  * [[ResponseWriter]]'s own doc) - and [[Server]]'s per-phase timeouts. Not
  * supported: chunked request or response bodies (`Transfer-Encoding:
  * chunked` on a request is answered with 501) and pipelined responses to
  * unread requests beyond the one in flight.
  */
def serve(listener: TcpListener, handler: Handler)(using Async, AsyncOperations): Unit =
  serve(listener, Server(handler))

def serve(listener: TcpListener, server: Server)(using Async, AsyncOperations): Unit =
  Async.group:
    while true do
      listener.accept() match
        case Right(stream) => Future(handleConnection(stream, server))
        case Left(e)         => System.err.println(s"accept failed: $e")

private[http] def handleConnection(stream: TcpStream, server: Server)(using Async, AsyncOperations): Unit =
  val reader = RequestReader(stream)
  try
    var keepAlive = true
    while keepAlive do keepAlive = handleOneRequest(stream, reader, server)
  catch case e: Exception => System.err.println(s"connection error: $e")
  finally stream.close()

/** Handles one request/response cycle on `stream`, returning whether the
  * connection should stay open for another request. A phase that exceeds
  * its configured [[Server]] timeout ends the connection (`false`) rather
  * than propagating [[TimeoutException]] up through [[handleConnection]]'s
  * generic `catch`, since a half-read request or half-written response
  * leaves the connection in no state to carry another one.
  */
private def handleOneRequest(stream: TcpStream, reader: RequestReader, server: Server)(using Async, AsyncOperations): Boolean =
  val headBytesOpt =
    try server.idleTimeout.fold(reader.readHead())(t => withTimeoutOption(t)(reader.readHead()).flatten)
    catch case _: TimeoutException => None
  headBytesOpt match
    case None => false // client closed the connection cleanly between requests, or the idle timeout elapsed
    case Some(headBytes) =>
      parseHead(headBytes) match
        case Left(errorResponse) =>
          writeSimpleResponse(stream, errorResponse.withHeader("Connection", "close"))
          false
        case Right(head) =>
          determineBodyLength(head.headers) match
            case Left(errorResponse) =>
              writeSimpleResponse(stream, errorResponse.withHeader("Connection", "close"))
              false
            case Right(bodyLength) =>
              try
                val body = server.readTimeout.fold(reader.readBody(bodyLength))(t => withTimeout(t)(reader.readBody(bodyLength)))
                val request = HttpRequest(head.method, head.path, head.query, head.version, head.headers, body)
                val requestWantsKeepAlive = wantsKeepAlive(head.version, head.headers)
                val w = StreamResponseWriter(stream, suppressBody = head.method == HttpMethod.HEAD)
                w.header.set("Connection", if requestWantsKeepAlive then "keep-alive" else "close")
                def dispatch(): Unit =
                  try server.handler.serveHTTP(w, request)
                  catch case e: Exception => Handler.of(_ => HttpResponse.internalError(e)).serveHTTP(w, request)
                  w.finish()
                server.writeTimeout.fold(dispatch())(t => withTimeout(t)(dispatch()))
                requestWantsKeepAlive && !w.forcedClose
              catch case _: TimeoutException => false

private def wantsKeepAlive(version: String, headers: Headers): Boolean =
  headers.get("Connection") match
    case Some(v) if v.equalsIgnoreCase("close")      => false
    case Some(v) if v.equalsIgnoreCase("keep-alive") => true
    case _                                             => version == "HTTP/1.1"

/** Streams a response directly to `stream` - see [[ResponseWriter]]'s own
  * doc for the framing rules (`Content-Length` if set before the first
  * write, otherwise `Connection: close`). `suppressBody` implements `HEAD`:
  * the handler still runs as if this were `GET` (so headers, including
  * whatever `Content-Length` it would have sent, come out identically),
  * but the body bytes themselves are never written to the wire.
  */
private final class StreamResponseWriter(stream: TcpStream, suppressBody: Boolean) extends ResponseWriter:
  val header: MutableHeaders = MutableHeaders()
  private var status: HttpStatus = HttpStatus.Ok
  private var headersFlushed = false
  private var _forcedClose = false

  def forcedClose: Boolean = _forcedClose

  def writeHeader(s: HttpStatus): Unit =
    if !headersFlushed then status = s

  private def flushHeaders()(using Async): Unit =
    if !headersFlushed then
      headersFlushed = true
      if header.get("Content-Length").isEmpty && header.get("Transfer-Encoding").isEmpty then
        _forcedClose = true
        header.set("Connection", "close")
      val statusLine = s"HTTP/1.1 ${status.code} ${status.reason}\r\n"
      val headerText = header.toHeaders.iterator.map((k, v) => s"$k: $v\r\n").mkString
      val headBytes = (statusLine + headerText + "\r\n").getBytes(StandardCharsets.ISO_8859_1)
      stream.writeBuf(ByteBuffer.wrap(headBytes))

  def write(bytes: Array[Byte])(using Async): Unit =
    flushHeaders()
    if !suppressBody && bytes.nonEmpty then stream.writeBuf(ByteBuffer.wrap(bytes))

  /** Flushes headers even if the handler never called [[write]] at all
    * (an empty body, or a handler that only calls [[writeHeader]]).
    */
  def finish()(using Async): Unit = flushHeaders()

/** Writes a complete, already-known [[HttpResponse]] in one shot - used
  * only for the two error paths in [[handleOneRequest]] that happen
  * *before* a [[StreamResponseWriter]] would otherwise exist (a malformed
  * request line/headers, or an unsupported `Transfer-Encoding`), where
  * there's no request to dispatch to a handler at all.
  */
private def writeSimpleResponse(stream: TcpStream, response: HttpResponse)(using Async): Unit =
  val withLength = response.withHeader("Content-Length", response.body.length.toString)
  val statusLine = s"HTTP/1.1 ${withLength.status.code} ${withLength.status.reason}\r\n"
  val headerText = withLength.headers.iterator.map((k, v) => s"$k: $v\r\n").mkString
  val headBytes = (statusLine + headerText + "\r\n").getBytes(StandardCharsets.ISO_8859_1)
  stream.writeBuf(ByteBuffer.wrap(headBytes ++ withLength.body))

private final case class ParsedHead(method: HttpMethod, path: String, query: String, version: String, headers: Headers)

private def parseHead(headBytes: Array[Byte]): Either[HttpResponse, ParsedHead] =
  val text = new String(headBytes, StandardCharsets.ISO_8859_1)
  text.split("\r\n", -1).toList match
    case requestLine :: headerLines =>
      requestLine.split(" ") match
        case Array(methodStr, target, version) =>
          if !isWellFormedVersion(version) then Left(HttpResponse.badRequest("malformed HTTP version"))
          else if version != "HTTP/1.0" && version != "HTTP/1.1" then Left(HttpResponse.httpVersionNotSupported())
          else
            val (path, query) = target.indexOf('?') match
              case -1 => (target, "")
              case i  => (target.substring(0, i), target.substring(i + 1))
            parseHeaders(headerLines).flatMap { headers =>
              if version == "HTTP/1.1" && !headers.contains("Host") then
                Left(HttpResponse.badRequest("missing Host header"))
              else Right(ParsedHead(HttpMethod.parse(methodStr), path, query, version, headers))
            }
        case _ => Left(HttpResponse.badRequest("malformed request line"))
    case Nil => Left(HttpResponse.badRequest("empty request"))

private def isWellFormedVersion(v: String): Boolean =
  v.length == 8 && v.startsWith("HTTP/") && v.charAt(6) == '.' && v.charAt(5).isDigit && v.charAt(7).isDigit

private def parseHeaders(lines: List[String]): Either[HttpResponse, Headers] =
  lines.foldLeft[Either[HttpResponse, Headers]](Right(Headers.empty)) { (acc, line) =>
    acc.flatMap { headers =>
      line.indexOf(':') match
        case -1 => Left(HttpResponse.badRequest(s"malformed header line: $line"))
        case i =>
          Right(headers.add(line.substring(0, i).trim, line.substring(i + 1).trim))
    }
  }

private def determineBodyLength(headers: Headers): Either[HttpResponse, Int] =
  headers.get("Transfer-Encoding") match
    case Some(te) if te.equalsIgnoreCase("chunked") =>
      Left(HttpResponse.notImplemented("chunked request bodies are not supported"))
    case _ =>
      headers.get("Content-Length") match
        case None => Right(0)
        case Some(s) =>
          s.toIntOption match
            case Some(n) if n >= 0 => Right(n)
            case _                  => Left(HttpResponse.badRequest("invalid Content-Length"))

private[http] val MaxHeadSize = 16 * 1024

private[http] final class HeadTooLargeException extends Exception("request head exceeds the maximum allowed size")

/** Buffers bytes read off `stream` beyond what one logical piece (a
  * request's head, or its body) consumed, so they carry over to the next
  * read - needed for keep-alive, where the next request's bytes may already
  * have arrived in the same underlying `readBuf` call that finished this
  * one's body.
  */
private[http] final class RequestReader(stream: TcpStream):
  private val pending = new ArrayBuffer[Byte]()
  private val tmp = ByteBuffer.allocate(8192)
  private var eof = false

  private def fill()(using Async): Boolean =
    if eof then false
    else
      tmp.clear()
      stream.readBuf(tmp) match
        case Right(()) =>
          tmp.flip()
          val arr = new Array[Byte](tmp.remaining())
          tmp.get(arr)
          pending ++= arr
          true
        case Left(Error.EOF) =>
          eof = true
          false

  private def indexOfHeadEnd(): Int =
    val (cr, lf) = ('\r'.toByte, '\n'.toByte)
    val n = pending.length
    var i = 0
    var found = -1
    while found < 0 && i + 3 < n do
      if pending(i) == cr && pending(i + 1) == lf && pending(i + 2) == cr && pending(i + 3) == lf then found = i
      i += 1
    found

  /** Reads and consumes bytes up to (not including) the blank-line
    * terminator. `None` means the connection closed cleanly with no partial
    * request pending - the normal way a keep-alive loop ends. A connection
    * that closes mid-head, or a head that never terminates within
    * [[MaxHeadSize]], is instead a real error.
    */
  def readHead()(using Async): Option[Array[Byte]] =
    var idx = indexOfHeadEnd()
    while idx < 0 && eof == false do
      if pending.length > MaxHeadSize then throw HeadTooLargeException()
      fill()
      idx = indexOfHeadEnd()
    if idx >= 0 then
      val head = pending.slice(0, idx).toArray
      pending.remove(0, idx + 4)
      Some(head)
    else if pending.isEmpty then None
    else throw new java.io.EOFException("connection closed mid-request")

  def readBody(n: Int)(using Async): Array[Byte] =
    while pending.length < n && fill() do ()
    if pending.length < n then throw new java.io.EOFException("connection closed mid-body")
    val body = pending.slice(0, n).toArray
    pending.remove(0, n)
    body

  /** Reads and consumes everything up to the connection's close - the
    * "no `Content-Length`, no `Transfer-Encoding`" body framing
    * [[Client]] falls back to on a response.
    */
  def readUntilEof()(using Async): Array[Byte] =
    while fill() do ()
    val body = pending.toArray
    pending.clear()
    body
