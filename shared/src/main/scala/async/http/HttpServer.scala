package gears.async.http

import gears.async._
import gears.async.net.TcpListener
import gears.async.net.TcpStream
import gears.async.asyncio.Error

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.collection.mutable.ArrayBuffer

/** A backend-agnostic HTTP/1.0 and HTTP/1.1 server, written entirely
  * against `net.TcpListener`/`TcpStream` - the same code runs regardless of
  * which `net.TcpSupport` (uring/epoll/kqueue/...) is plugged in. Each
  * backend only needs a few-line entry point that constructs its own
  * `Scheduler`/`TcpSupport`, builds a [[Router]], and calls [[serve]].
  *
  * Supported: request-line/header parsing, `Content-Length` request and
  * response bodies, `HEAD` (served from the matching `GET` handler, body
  * stripped), and persistent (keep-alive) connections - HTTP/1.1 defaults
  * to keep-alive and HTTP/1.0 to close, either overridable by a
  * `Connection` request header. Not supported: chunked request or response
  * bodies (`Transfer-Encoding: chunked` on a request is answered with 501)
  * and pipelined responses to unread requests beyond the one in flight.
  */
def serve(listener: TcpListener, router: Router)(using Async): Unit =
  Async.group:
    while true do
      listener.accept() match
        case Right(stream) => Future(handleConnection(stream, router))
        case Left(e)        => System.err.println(s"accept failed: $e")

private def handleConnection(stream: TcpStream, router: Router)(using Async): Unit =
  val reader = RequestReader(stream)
  try
    var keepAlive = true
    while keepAlive do keepAlive = handleOneRequest(stream, reader, router)
  catch case e: Exception => System.err.println(s"connection error: $e")
  finally stream.close()

/** Handles one request/response cycle on `stream`, returning whether the
  * connection should stay open for another request.
  */
private def handleOneRequest(stream: TcpStream, reader: RequestReader, router: Router)(using Async): Boolean =
  reader.readHead() match
    case None => false // client closed the connection cleanly between requests
    case Some(headBytes) =>
      parseHead(headBytes) match
        case Left(errorResponse) =>
          writeResponse(stream, errorResponse.withHeader("Connection", "close"), sendBody = true)
          false
        case Right(head) =>
          determineBodyLength(head.headers) match
            case Left(errorResponse) =>
              writeResponse(stream, errorResponse.withHeader("Connection", "close"), sendBody = true)
              false
            case Right(bodyLength) =>
              val body = reader.readBody(bodyLength)
              val request = HttpRequest(head.method, head.path, head.query, head.version, head.headers, body)
              val response = router.dispatch(request)
              val keepAlive = wantsKeepAlive(head.version, head.headers)
              val finalResponse = response.withHeader("Connection", if keepAlive then "keep-alive" else "close")
              writeResponse(stream, finalResponse, sendBody = head.method != HttpMethod.HEAD)
              keepAlive

private def wantsKeepAlive(version: String, headers: Headers): Boolean =
  headers.get("Connection") match
    case Some(v) if v.equalsIgnoreCase("close")      => false
    case Some(v) if v.equalsIgnoreCase("keep-alive") => true
    case _                                             => version == "HTTP/1.1"

private def writeResponse(stream: TcpStream, response: HttpResponse, sendBody: Boolean)(using Async): Unit =
  val withLength = response.withHeader("Content-Length", response.body.length.toString)
  val statusLine = s"HTTP/1.1 ${withLength.status.code} ${withLength.status.reason}\r\n"
  val headerText = withLength.headers.iterator.map((k, v) => s"$k: $v\r\n").mkString
  val headBytes = (statusLine + headerText + "\r\n").getBytes(StandardCharsets.ISO_8859_1)
  val payload = if sendBody then headBytes ++ withLength.body else headBytes
  stream.writeBuf(ByteBuffer.wrap(payload))

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

private val MaxHeadSize = 16 * 1024

private final class HeadTooLargeException extends Exception("request head exceeds the maximum allowed size")

/** Buffers bytes read off `stream` beyond what one logical piece (a
  * request's head, or its body) consumed, so they carry over to the next
  * read - needed for keep-alive, where the next request's bytes may already
  * have arrived in the same underlying `readBuf` call that finished this
  * one's body.
  */
private final class RequestReader(stream: TcpStream):
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
