package gears.io.http

import gears.async._
import gears.io.Error
import gears.io.net.TcpListener
import gears.io.net.TcpStream
import gears.io.net.{TlsContext, TlsSupport}

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeoutException
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.{DurationInt, FiniteDuration}

final case class Server(
    handler: Handler,
    readTimeout: Option[FiniteDuration] = None,
    writeTimeout: Option[FiniteDuration] = None,
    idleTimeout: Option[FiniteDuration] = Some(60.seconds),
    tls: Option[(TlsSupport, TlsContext)] = None,
    maxBodySize: Int = 10 * 1024 * 1024,
    errorLog: String => Unit = message => System.err.println(message),
    maxConnections: Int = 1024
):
  require(maxConnections > 0 && maxBodySize >= 0)
  private[http] val lifecycle = ServerLifecycle(maxConnections)

  def serve(listener: TcpListener)(using Async, AsyncOperations): Unit = gears.io.http.serve(listener, this)

  def shutdown(grace: FiniteDuration)(using Async): Unit =
    require(grace.toNanos >= 0, "shutdown grace must be nonnegative")
    lifecycle.stopAccepting()
    try Bodies.timed(Some(grace))(lifecycle.awaitDrained())
    finally lifecycle.stopNow()

  def close(): Unit = lifecycle.stopNow()

def serve(listener: TcpListener, handler: Handler)(using Async, AsyncOperations): Unit =
  serve(listener, Server(handler))

def serve(listener: TcpListener, server: Server)(using Async, AsyncOperations): Unit =
  server.lifecycle.start(listener)
  Async.group:
    var serving = true
    while serving do
      try
        listener.accept() match
          case Right(raw) =>
            if server.lifecycle.add(raw) then
              val connection = Future:
                try serveConnection(raw, server)
                finally server.lifecycle.finished(raw)
              server.lifecycle.attach(raw, connection)
            else raw.close()
          case Left(Error.EOF) => serving = false
      catch
        case e: java.io.IOException if HttpErrors.isClosedChannel(e) => serving = false
        case e: java.io.IOException                                  =>
          server.errorLog(s"accept failed, retrying: $e")
          AsyncOperations.sleep(AcceptRetryDelayMillis)
    server.lifecycle.stopAccepting()
    server.lifecycle.awaitDrained()

private val AcceptRetryDelayMillis = 50L

private def serveConnection(raw: TcpStream, server: Server)(using Async, AsyncOperations): Unit =
  val stream =
    try
      server.tls match
        case Some((tls, ctx)) => Bodies.timed(server.idleTimeout)(tls.wrapServer(raw, ctx))
        case None             => raw
    catch
      case e: Exception =>
        server.errorLog(s"TLS handshake failed: $e")
        raw.close()
        null
  if stream != null then handleConnectionOwned(stream, server, raw)

private def handleConnectionOwned(stream: TcpStream, server: Server, owner: TcpStream)(using
    Async,
    AsyncOperations
): Unit =
  val reader = RequestReader(stream)
  try
    var keepAlive = true
    while keepAlive do keepAlive = handleOneRequest(stream, reader, server, owner)
  catch
    case _: (java.nio.channels.ClosedChannelException | java.util.concurrent.CancellationException)
        if server.lifecycle.isForced =>
      ()
    case e: Exception => server.errorLog(s"connection error: $e")
  finally closeGracefully(stream)

private[http] def closeGracefully(stream: TcpStream)(using Async): Unit =
  try stream.shutdownOutput()
  catch case _: Exception => ()
  finally stream.close()

private def handleOneRequest(stream: TcpStream, reader: RequestReader, server: Server, owner: TcpStream)(using
    Async,
    AsyncOperations
): Boolean =
  val headBytesOpt =
    try
      if !server.lifecycle.working(owner, false) then None
      else server.idleTimeout.fold(reader.readHead())(t => withTimeoutOption(t)(reader.readHead()).flatten)
    catch case _: TimeoutException => None
  headBytesOpt match
    case None            => false
    case Some(headBytes) =>
      if !server.lifecycle.working(owner, true) then return false
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
                  if expectation.isDefined && head.version == "HTTP/1.1" && framing != BodyFraming.Fixed(0) then
                    stream.writeBuf(
                      ByteBuffer.wrap("HTTP/1.1 100 Continue\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1))
                    )
                  respond(stream, reader, server, head, framing)

private def respond(stream: TcpStream, reader: RequestReader, server: Server, head: ParsedHead, framing: BodyFraming)(
    using Async
): Boolean =
  try
    val body = MessageBody(reader, framing, server.maxBodySize.toLong)
    val input = new gears.io.Reader:
      def readBuf(buf: ByteBuffer)(using Async): gears.io.Result[Unit] =
        Bodies.timed(server.readTimeout)(body.readBuf(buf))
    val request = HttpRequest(head.method, head.path, head.query, head.version, head.headers, Array.emptyByteArray)
    val requestWantsKeepAlive = wantsKeepAlive(head.version, head.headers)
    val w = StreamResponseWriter(
      stream,
      reader,
      suppressBody = head.method == HttpMethod.HEAD,
      chunkingAllowed = head.version == "HTTP/1.1"
    )
    w.header.set("Connection", if requestWantsKeepAlive then "keep-alive" else "close")
    def dispatch()(using Async): Unit =
      var abandoned = false
      try server.handler.serveStream(w, request, input)
      catch
        case e: Exception
            if !w.upgraded && !e.isInstanceOf[BodyTooLargeException] && !e.isInstanceOf[TimeoutException] && !e
              .isInstanceOf[java.util.concurrent.CancellationException] =>
          server.errorLog(s"handler failed for ${head.method.rendered} ${head.path}: $e")
          if w.committed then abandoned = true
          else
            w.reset()
            Handler.of(_ => HttpResponse.internalError()).serveHTTP(w, request)
      if abandoned then w.abandon()
      else
        if !body.complete && !w.upgraded then w.header.set("Connection", "close")
        w.finish()
    Bodies.timed(server.writeTimeout, timer => w.cancelDeadline = () => timer.cancel())(dispatch())
    requestWantsKeepAlive && body.complete && !w.forcedClose && !w.upgraded
  catch
    case _: TimeoutException      => false
    case e: BodyTooLargeException =>
      writeSimpleResponse(stream, payloadTooLarge(e.limit).withHeader("Connection", "close"))
      false

private def wantsKeepAlive(version: String, headers: Headers): Boolean =
  headers.get("Connection") match
    case Some(v) if v.equalsIgnoreCase("close")      => false
    case Some(v) if v.equalsIgnoreCase("keep-alive") => true
    case _                                           => version == "HTTP/1.1"

private final class StreamResponseWriter(
    stream: TcpStream,
    reader: RequestReader,
    suppressBody: Boolean,
    chunkingAllowed: Boolean
) extends ResponseWriter:
  private var headers: MutableHeaders = MutableHeaders()
  def header: MutableHeaders = headers
  private var status: HttpStatus = HttpStatus.Ok
  private var headersFlushed = false
  private var declaredLength = -1L
  private var bodyWritten = 0L

  private[http] def committed: Boolean = headersFlushed

  private[http] def reset(): Unit =
    val connection = headers.get("Connection")
    headers = MutableHeaders()
    connection.foreach(headers.set("Connection", _))
    status = HttpStatus.Ok

  private[http] def abandon(): Unit = _forcedClose = true
  private var unsentHead: Array[Byte] | Null = null
  private var chunked = false
  private var _forcedClose = false
  private var _upgraded = false
  private[http] var cancelDeadline: () => Unit = () => ()

  override private[http] def upgraded: Boolean = _upgraded

  override private[http] def upgrade()(using Async): (TcpStream, gears.io.Reader) =
    if headersFlushed then throw new IllegalStateException("HTTP response already committed")
    headersFlushed = true
    _upgraded = true
    header.remove("Content-Length")
    header.remove("Transfer-Encoding")
    val fields = header.toHeaders.iterator.map((k, v) => s"$k: $v\r\n").mkString
    stream.writeBuf(
      ByteBuffer.wrap(
        (s"HTTP/1.1 101 Switching Protocols\r\n${fields}\r\n")
          .getBytes(StandardCharsets.ISO_8859_1)
      )
    )
    cancelDeadline()
    (stream, reader)

  def forcedClose: Boolean = _forcedClose

  def writeHeader(s: HttpStatus): Unit =
    if !headersFlushed then status = s

  private def flushHeaders(): Unit =
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
        _forcedClose = true
        header.set("Connection", "close")
      val statusLine = s"HTTP/1.1 ${status.code} ${status.reason}\r\n"
      val headerText = header.toHeaders.iterator.map((k, v) => s"$k: $v\r\n").mkString
      unsentHead = (statusLine + headerText + "\r\n").getBytes(StandardCharsets.ISO_8859_1)
      if !chunked then declaredLength = header.get("Content-Length").flatMap(_.toLongOption).getOrElse(-1L)

  private def send(bytes: Array[Byte])(using Async): Unit =
    val head = unsentHead
    unsentHead = null
    val out = if head == null then bytes else if bytes.isEmpty then head else head ++ bytes
    if out.nonEmpty then stream.writeBuf(ByteBuffer.wrap(out))

  def write(bytes: Array[Byte])(using Async): Unit =
    if upgraded then throw new IllegalStateException("The connection has been upgraded")
    flushHeaders()
    if !suppressBody && bytes.nonEmpty then
      if chunked then
        val size = (Integer.toHexString(bytes.length) + "\r\n").getBytes(StandardCharsets.ISO_8859_1)
        send(size ++ bytes ++ Crlf)
      else
        if declaredLength >= 0 && bodyWritten + bytes.length > declaredLength then
          throw IllegalStateException(s"response body exceeds its Content-Length of $declaredLength")
        bodyWritten += bytes.length
        send(bytes)
    else send(Array.emptyByteArray)

  def finish()(using Async): Unit =
    if !upgraded then
      flushHeaders()
      if !suppressBody && declaredLength >= 0 && bodyWritten < declaredLength then _forcedClose = true
      send(if chunked && !suppressBody then LastChunk else Array.emptyByteArray)

private val Crlf = "\r\n".getBytes(StandardCharsets.ISO_8859_1)
private val LastChunk = "0\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1)

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
          val (name, value) = (line.substring(0, i), line.substring(i + 1).trim)
          if HeaderSyntax.isValidName(name) && HeaderSyntax.isValidValue(value) then Right(headers.add(name, value))
          else Left(HttpResponse.badRequest("malformed header line"))
    }
  }

private[http] enum BodyFraming:
  case Fixed(length: Int)
  case Chunked
  case UntilClose

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
        (if s.nonEmpty && s.forall(c => c >= '0' && c <= '9') then s.toIntOption else None) match
          case Some(n) if n > maxBodySize => Left(payloadTooLarge(maxBodySize))
          case Some(n) if n >= 0          => Right(BodyFraming.Fixed(n))
          case _                          => Left(HttpResponse.badRequest("invalid Content-Length"))
      case _ => Left(HttpResponse.badRequest("conflicting Content-Length headers"))

private[http] val MaxHeadSize = 16 * 1024

private[http] final class HeadTooLargeException extends Exception("request head exceeds the maximum allowed size")

private[http] final class BodyTooLargeException(val limit: Int)
    extends java.io.IOException(s"message body exceeds the limit of $limit bytes")

private[http] final class RequestReader(stream: TcpStream) extends gears.io.Reader:
  private val pending = new ArrayBuffer[Byte]()
  private val tmp = ByteBuffer.allocate(8192)
  private var eof = false

  def readBuf(buf: ByteBuffer)(using Async): gears.io.Result[Unit] =
    if !buf.hasRemaining then Right(())
    else if pending.nonEmpty then
      val n = math.min(buf.remaining(), pending.length)
      var i = 0
      while i < n do
        buf.put(pending(i))
        i += 1
      pending.remove(0, n)
      Right(())
    else if eof then Left(Error.EOF)
    else stream.readBuf(buf)

  private def fill()(using Async): Boolean =
    if eof then false
    else
      tmp.clear()
      val result =
        try stream.readBuf(tmp)
        finally
          tmp.flip()
          if tmp.hasRemaining then
            val arr = new Array[Byte](tmp.remaining())
            tmp.get(arr)
            pending ++= arr
      result match
        case Right(())       => true
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
      if idx > MaxHeadSize then throw HeadTooLargeException()
      val head = pending.slice(0, idx).toArray
      pending.remove(0, idx + 4)
      Some(head)
    else if pending.isEmpty then None
    else throw new java.io.EOFException("connection closed mid-request")

  private[http] def readLine(maxLength: Int)(using Async): String =
    var idx = indexOfCrlf()
    while idx < 0 do
      if pending.length > maxLength + 1 then throw new java.io.IOException("line too long in message body")
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
