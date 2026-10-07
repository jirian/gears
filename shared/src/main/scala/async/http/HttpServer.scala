package gears.async.http

import gears.async._
import gears.async.asyncio.Error
import gears.async.net.TcpListener
import gears.async.net.TcpStream
import gears.async.net.{TlsContext, TlsSupport}

import java.nio.ByteBuffer
import java.nio.channels.ClosedChannelException
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeoutException
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.FiniteDuration

/** `tls`, if given, wraps every accepted connection in a TLS server handshake before treating it as HTTP - build it
  * with `(support, support.serverContext(certFile, keyFile))`.
  *
  * `maxBodySize` bounds a request body, which is read into memory whole: a larger declared `Content-Length` gets
  * `413 Payload Too Large`. `errorLog` receives connection-level errors that have no response to go to, such as a
  * failed TLS handshake or a peer that disconnected.
  */
final case class Server(
    handler: Handler,
    readTimeout: Option[FiniteDuration] = None,
    writeTimeout: Option[FiniteDuration] = None,
    idleTimeout: Option[FiniteDuration] = None,
    tls: Option[(TlsSupport, TlsContext)] = None,
    maxBodySize: Int = 10 * 1024 * 1024,
    errorLog: String => Unit = message => System.err.println(message)
):
  /** Equivalent to `serve(listener, this)` - Go's `Server.Serve`. */
  def serve(listener: TcpListener)(using Async, AsyncOperations): Unit = gears.async.http.serve(listener, this)

def serve(listener: TcpListener, handler: Handler)(using Async, AsyncOperations): Unit =
  serve(listener, Server(handler))

/** Accepts and serves connections until `listener` is closed, then returns once every connection being served has
  * finished. A transient accept failure, such as running out of descriptors, is logged and retried after a short pause
  * rather than ending the server.
  */
def serve(listener: TcpListener, server: Server)(using Async, AsyncOperations): Unit =
  Async.group:
    var serving = true
    while serving do
      try
        listener.accept() match
          case Right(raw)      => Future(serveConnection(raw, server))
          case Left(Error.EOF) => serving = false
      catch
        case _: ClosedChannelException => serving = false // includes AsynchronousCloseException
        case e: java.io.IOException    =>
          server.errorLog(s"accept failed, retrying: $e")
          AsyncOperations.sleep(AcceptRetryDelayMillis)

private val AcceptRetryDelayMillis = 50L

private def serveConnection(raw: TcpStream, server: Server)(using Async, AsyncOperations): Unit =
  val stream =
    try
      server.tls match
        case Some((tls, ctx)) => tls.wrapServer(raw, ctx)
        case None             => raw
    catch
      case e: Exception =>
        server.errorLog(s"TLS handshake failed: $e")
        raw.close()
        null
  if stream != null then handleConnection(stream, server)

private[http] def handleConnection(stream: TcpStream, server: Server)(using Async, AsyncOperations): Unit =
  val reader = RequestReader(stream)
  try
    var keepAlive = true
    while keepAlive do keepAlive = handleOneRequest(stream, reader, server)
  catch case e: Exception => server.errorLog(s"connection error: $e")
  finally closeGracefully(stream)

/** Closes a connection after shutting down its output, which over TLS first sends `close_notify`, so the peer can tell
  * a complete close from a truncated one. A peer that is already gone makes the shutdown fail; the close happens
  * anyway.
  */
private[http] def closeGracefully(stream: TcpStream)(using Async): Unit =
  try stream.shutdownOutput()
  catch case _: Exception => ()
  finally stream.close()

private def handleOneRequest(stream: TcpStream, reader: RequestReader, server: Server)(using
    Async,
    AsyncOperations
): Boolean =
  val headBytesOpt =
    try server.idleTimeout.fold(reader.readHead())(t => withTimeoutOption(t)(reader.readHead()).flatten)
    catch case _: TimeoutException => None
  headBytesOpt match
    case None            => false // client closed the connection cleanly between requests, or the idle timeout elapsed
    case Some(headBytes) =>
      parseHead(headBytes) match
        case Left(errorResponse) =>
          writeSimpleResponse(stream, errorResponse.withHeader("Connection", "close"))
          false
        case Right(head) =>
          requestFraming(head.headers, server.maxBodySize) match
            case Left(errorResponse) =>
              writeSimpleResponse(stream, errorResponse.withHeader("Connection", "close"))
              false
            case Right(framing) =>
              head.headers.get("Expect") match
                case Some(expectation) if !expectation.equalsIgnoreCase("100-continue") =>
                  val refused = HttpResponse(HttpStatus.ExpectationFailed, s"417 Expectation Failed: $expectation\n")
                  writeSimpleResponse(stream, refused.withHeader("Connection", "close"))
                  false
                case expectation =>
                  // The client holds the body back until told to go on; HTTP/1.0 clients do not use 100-continue.
                  if expectation.isDefined && head.version == "HTTP/1.1" && framing != BodyFraming.Fixed(0) then
                    stream.writeBuf(
                      ByteBuffer.wrap("HTTP/1.1 100 Continue\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1))
                    )
                  respond(stream, reader, server, head, framing)

/** Reads the body of a parsed request, dispatches it to the handler, and writes the response. Returns whether the
  * connection stays open for another request.
  */
private def respond(stream: TcpStream, reader: RequestReader, server: Server, head: ParsedHead, framing: BodyFraming)(
    using
    Async,
    AsyncOperations
): Boolean =
  try
    def readBody() = framing match
      case BodyFraming.Fixed(length) => reader.readBody(length)
      case BodyFraming.Chunked       => reader.readChunked(server.maxBodySize)
      case BodyFraming.UntilClose    => reader.readUntilEof(server.maxBodySize) // not used for requests
    val body = server.readTimeout.fold(readBody())(t => withTimeout(t)(readBody()))
    val request = HttpRequest(head.method, head.path, head.query, head.version, head.headers, body)
    val requestWantsKeepAlive = wantsKeepAlive(head.version, head.headers)
    val w = StreamResponseWriter(
      stream,
      suppressBody = head.method == HttpMethod.HEAD,
      chunkingAllowed = head.version == "HTTP/1.1"
    )
    w.header.set("Connection", if requestWantsKeepAlive then "keep-alive" else "close")
    def dispatch(): Unit =
      try server.handler.serveHTTP(w, request)
      catch case e: Exception => Handler.of(_ => HttpResponse.internalError(e)).serveHTTP(w, request)
      w.finish()
    server.writeTimeout.fold(dispatch())(t => withTimeout(t)(dispatch()))
    requestWantsKeepAlive && !w.forcedClose
  catch
    case _: TimeoutException      => false
    case e: BodyTooLargeException =>
      // Only a chunked body's size is unknown up front; it is refused once it grows past the limit.
      writeSimpleResponse(stream, payloadTooLarge(e.limit).withHeader("Connection", "close"))
      false

private def wantsKeepAlive(version: String, headers: Headers): Boolean =
  headers.get("Connection") match
    case Some(v) if v.equalsIgnoreCase("close")      => false
    case Some(v) if v.equalsIgnoreCase("keep-alive") => true
    case _                                           => version == "HTTP/1.1"

/** Writes one response. A body whose length the handler did not declare is sent chunked to an HTTP/1.1 client, so the
  * connection can stay open; an HTTP/1.0 client gets it delimited by closing the connection instead.
  */
private final class StreamResponseWriter(stream: TcpStream, suppressBody: Boolean, chunkingAllowed: Boolean)
    extends ResponseWriter:
  val header: MutableHeaders = MutableHeaders()
  private var status: HttpStatus = HttpStatus.Ok
  private var headersFlushed = false
  private var chunked = false
  private var _forcedClose = false

  def forcedClose: Boolean = _forcedClose

  def writeHeader(s: HttpStatus): Unit =
    if !headersFlushed then status = s

  private def flushHeaders()(using Async): Unit =
    if !headersFlushed then
      headersFlushed = true
      header.get("Transfer-Encoding") match
        case Some(te) => chunked = te.equalsIgnoreCase("chunked")
        case None     =>
          if header.get("Content-Length").isEmpty then
            if chunkingAllowed && !suppressBody then
              header.set("Transfer-Encoding", "chunked")
              chunked = true
            else
              _forcedClose = true
              header.set("Connection", "close")
      if header.get("Transfer-Encoding").isDefined && !chunked then
        // A coding we do not frame ourselves: only closing the connection can end the body.
        _forcedClose = true
        header.set("Connection", "close")
      val statusLine = s"HTTP/1.1 ${status.code} ${status.reason}\r\n"
      val headerText = header.toHeaders.iterator.map((k, v) => s"$k: $v\r\n").mkString
      val headBytes = (statusLine + headerText + "\r\n").getBytes(StandardCharsets.ISO_8859_1)
      stream.writeBuf(ByteBuffer.wrap(headBytes))

  def write(bytes: Array[Byte])(using Async): Unit =
    flushHeaders()
    if !suppressBody && bytes.nonEmpty then
      if chunked then
        val size = (Integer.toHexString(bytes.length) + "\r\n").getBytes(StandardCharsets.ISO_8859_1)
        stream.writeBuf(ByteBuffer.wrap(size ++ bytes ++ Crlf))
      else stream.writeBuf(ByteBuffer.wrap(bytes))

  def finish()(using Async): Unit =
    flushHeaders()
    if chunked && !suppressBody then stream.writeBuf(ByteBuffer.wrap("0\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1)))

private val Crlf = "\r\n".getBytes(StandardCharsets.ISO_8859_1)

private def payloadTooLarge(limit: Int): HttpResponse =
  HttpResponse(HttpStatus.PayloadTooLarge, s"413 Payload Too Large: the limit is $limit bytes\n")

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
        case -1 => Left(HttpResponse.badRequest("malformed header line"))
        case i  =>
          val (name, value) = (line.substring(0, i).trim, line.substring(i + 1).trim)
          if HeaderSyntax.isValidName(name) && HeaderSyntax.isValidValue(value) then Right(headers.add(name, value))
          else Left(HttpResponse.badRequest("malformed header line"))
    }
  }

/** How a message body is delimited. */
private[http] enum BodyFraming:
  case Fixed(length: Int)
  case Chunked
  case UntilClose // responses only: the body runs until the connection closes

/** How the request body is delimited. Ambiguous framing, such as conflicting lengths or both `Transfer-Encoding` and
  * `Content-Length`, is refused rather than guessed at, since a proxy in front might read it differently (request
  * smuggling). A declared length over `maxBodySize` gets 413 before any of the body is read.
  */
private def requestFraming(headers: Headers, maxBodySize: Int): Either[HttpResponse, BodyFraming] =
  val codings = headers.getAll("Transfer-Encoding")
  val lengths = headers.getAll("Content-Length").distinct
  if codings.nonEmpty then
    if lengths.nonEmpty then Left(HttpResponse.badRequest("both Transfer-Encoding and Content-Length"))
    else if codings.size == 1 && codings.head.equalsIgnoreCase("chunked") then Right(BodyFraming.Chunked)
    else Left(HttpResponse.notImplemented("unsupported Transfer-Encoding"))
  else
    lengths match
      case Seq()  => Right(BodyFraming.Fixed(0))
      case Seq(s) =>
        s.toIntOption match
          case Some(n) if n > maxBodySize => Left(payloadTooLarge(maxBodySize))
          case Some(n) if n >= 0          => Right(BodyFraming.Fixed(n))
          case _                          => Left(HttpResponse.badRequest("invalid Content-Length"))
      case _ => Left(HttpResponse.badRequest("conflicting Content-Length headers"))

private[http] val MaxHeadSize = 16 * 1024

/** Longest chunk-size line accepted, chunk extensions included. */
private val MaxChunkLine = 1024

private[http] final class HeadTooLargeException extends Exception("request head exceeds the maximum allowed size")

/** A body grew past the size limit while it was being read. */
private[http] final class BodyTooLargeException(val limit: Int)
    extends java.io.IOException(s"message body exceeds the limit of $limit bytes")

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

  /** Reads until the connection closes; more than `maxSize` bytes throws [[BodyTooLargeException]]. */
  def readUntilEof(maxSize: Int)(using Async): Array[Byte] =
    while pending.length <= maxSize && fill() do ()
    if pending.length > maxSize then throw BodyTooLargeException(maxSize)
    val body = pending.toArray
    pending.clear()
    body

  /** Reads a chunked body (RFC 9112 section 7.1), dropping chunk extensions and trailer fields. More than `maxSize`
    * bytes of data throws [[BodyTooLargeException]] before the excess is read.
    */
  def readChunked(maxSize: Int)(using Async): Array[Byte] =
    val body = java.io.ByteArrayOutputStream()
    var done = false
    while !done do
      val sizeField = readLine(MaxChunkLine).takeWhile(_ != ';').trim
      val size =
        try java.lang.Long.parseLong(sizeField, 16)
        catch case _: NumberFormatException => throw new java.io.IOException(s"malformed chunk size: '$sizeField'")
      if size < 0 then throw new java.io.IOException(s"malformed chunk size: '$sizeField'")
      if size == 0 then
        while readLine(MaxHeadSize).nonEmpty do () // trailer fields, ended by an empty line
        done = true
      else
        if body.size() + size > maxSize then throw BodyTooLargeException(maxSize)
        body.write(readBody(size.toInt))
        if readLine(2).nonEmpty then throw new java.io.IOException("missing CRLF after chunk data")
    body.toByteArray

  /** A line ended by CRLF, without it. One longer than `maxLength` throws, so a peer cannot make us buffer forever. */
  private def readLine(maxLength: Int)(using Async): String =
    var idx = indexOfCrlf()
    while idx < 0 do
      if pending.length > maxLength then throw new java.io.IOException("line too long in message body")
      if !fill() then throw new java.io.EOFException("connection closed mid-body")
      idx = indexOfCrlf()
    if idx > maxLength then throw new java.io.IOException("line too long in message body")
    val line = new String(pending.slice(0, idx).toArray, StandardCharsets.ISO_8859_1)
    pending.remove(0, idx + 2)
    line

  private def indexOfCrlf(): Int =
    val n = pending.length
    var i = 0
    var found = -1
    while found < 0 && i + 1 < n do
      if pending(i) == '\r'.toByte && pending(i + 1) == '\n'.toByte then found = i
      i += 1
    found
