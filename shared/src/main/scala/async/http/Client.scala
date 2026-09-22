package gears.async.http

import gears.async.Async
import gears.async.net.{TcpStream, TcpSupport}

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

/** A parsed `scheme://host[:port]/path[?query]` target - just enough of a
  * URL to drive [[Client]]. Only `http` is supported: this backend has no
  * TLS stack to speak `https` with (unlike Go's `net/http`, which delegates
  * to `crypto/tls`).
  */
final case class Url(scheme: String, host: String, port: Int, path: String, query: String):
  def target: String = if query.isEmpty then path else s"$path?$query"

  /** The `Host` request-header value - omits the port when it's the
    * scheme's default, matching what browsers and `net/http` send.
    */
  def hostHeader: String = if port == Url.defaultPort(scheme) then host else s"$host:$port"

object Url:
  private def defaultPort(scheme: String): Int = if scheme == "https" then 443 else 80

  def parse(raw: String): Url =
    val schemeSep = raw.indexOf("://")
    if schemeSep < 0 then throw new IllegalArgumentException(s"missing scheme in URL: $raw")
    val scheme = raw.substring(0, schemeSep)
    if scheme != "http" then
      throw new UnsupportedOperationException(s"unsupported URL scheme '$scheme' (no TLS backend - only http:// is supported)")
    val rest = raw.substring(schemeSep + 3)
    val pathStart = rest.indexWhere(c => c == '/' || c == '?')
    val authority = if pathStart < 0 then rest else rest.substring(0, pathStart)
    val pathAndQuery = if pathStart < 0 then "/" else rest.substring(pathStart)
    val (host, port) = authority.split(":", 2) match
      case Array(h, p) => (h, p.toInt)
      case _             => (authority, defaultPort(scheme))
    val (path, query) = pathAndQuery.indexOf('?') match
      case -1 => (pathAndQuery, "")
      case i  => (pathAndQuery.substring(0, i), pathAndQuery.substring(i + 1))
    Url(scheme, host, port, if path.isEmpty then "/" else path, query)

  /** Resolves a `Location` header value against the URL it came from - an
    * absolute URL, an absolute path (`/foo`), or (a simplified, common-case
    * approximation of RFC 3986 relative resolution) a path relative to
    * `base`'s directory.
    */
  def resolve(base: Url, location: String): Url =
    if location.contains("://") then parse(location)
    else
      val (path, query) = location.indexOf('?') match
        case -1 => (location, "")
        case i  => (location.substring(0, i), location.substring(i + 1))
      val absolutePath =
        if path.startsWith("/") then path
        else base.path.substring(0, base.path.lastIndexOf('/') + 1) + path
      base.copy(path = absolutePath, query = query)

/** An outgoing HTTP request - Go's `http.Request` on the client side.
  * `Content-Length`, `Host`, and `Connection` are filled in by [[Transport]]
  * when the request is actually sent, so callers don't set them themselves.
  */
final case class ClientRequest(method: HttpMethod, url: Url, headers: Headers, body: Array[Byte]):
  def withHeader(name: String, value: String): ClientRequest = copy(headers = headers.set(name, value))
  def addCookie(cookie: Cookie): ClientRequest = copy(headers = headers.add("Cookie", cookie.rendered))

object ClientRequest:
  def get(url: String): ClientRequest = ClientRequest(HttpMethod.GET, Url.parse(url), Headers.empty, Array.emptyByteArray)
  def post(url: String, body: Array[Byte], contentType: String = "application/octet-stream"): ClientRequest =
    ClientRequest(HttpMethod.POST, Url.parse(url), Headers("Content-Type" -> contentType), body)

/** Low-level connection handling for [[Client]] - Go's `http.Transport`.
  * Keeps a pool of idle, still-open keep-alive connections per `(host,
  * port)`, reusing one for [[roundTrip]] when available instead of dialing
  * fresh every time. A pooled connection the peer has since closed (idle
  * timeout, e.g.) is detected by the write/read failing, in which case the
  * round trip is retried exactly once against a newly dialed connection -
  * the caller never sees the stale-connection failure.
  */
final class Transport(using tcp: TcpSupport):
  private val idle = new ConcurrentHashMap[(String, Int), ConcurrentLinkedQueue[TcpStream]]()

  private def takeIdle(host: String, port: Int): Option[TcpStream] =
    Option(idle.get((host, port))).flatMap(q => Option(q.poll()))

  private def release(host: String, port: Int, stream: TcpStream): Unit =
    idle.computeIfAbsent((host, port), _ => new ConcurrentLinkedQueue()).offer(stream)

  private def dial(host: String, port: Int)(using Async): TcpStream =
    tcp.connect(new InetSocketAddress(host, port), Seq.empty) match
      case Right(stream) => stream
      case Left(e)         => throw new java.io.IOException(s"connect to $host:$port failed: $e")

  /** One HTTP round trip: writes `request` and reads back the response,
    * over either a freshly dialed connection or (when one is idle) a reused
    * one from the pool.
    */
  def roundTrip(request: ClientRequest)(using Async): HttpResponse =
    val host = request.url.host
    val port = request.url.port
    takeIdle(host, port) match
      case Some(reused) =>
        try attempt(reused, request)
        catch
          case _: Exception =>
            reused.close()
            attempt(dial(host, port), request)
      case None => attempt(dial(host, port), request)

  private def attempt(stream: TcpStream, request: ClientRequest)(using Async): HttpResponse =
    writeRequest(stream, request)
    val (response, keepAlive) = readResponse(stream, isHead = request.method == HttpMethod.HEAD)
    if keepAlive then release(request.url.host, request.url.port, stream) else stream.close()
    response

  private def writeRequest(stream: TcpStream, request: ClientRequest)(using Async): Unit =
    val headers = request.headers
      .set("Host", request.url.hostHeader)
      .set("Content-Length", request.body.length.toString)
      .set("Connection", "keep-alive")
    val requestLine = s"${request.method.rendered} ${request.url.target} HTTP/1.1\r\n"
    val headerText = headers.iterator.map((k, v) => s"$k: $v\r\n").mkString
    val headBytes = (requestLine + headerText + "\r\n").getBytes(StandardCharsets.ISO_8859_1)
    stream.writeBuf(ByteBuffer.wrap(headBytes))
    if request.body.nonEmpty then stream.writeBuf(ByteBuffer.wrap(request.body))

  /** Parses the status line and headers off `stream` (via the same
    * [[RequestReader]] buffering the server side uses - it doesn't care
    * which direction the protocol runs) and, unless `isHead` or the status
    * is 204/304 (which never carry a body regardless of `Content-Length`,
    * per RFC 7230 3.3.3), reads the body: exactly `Content-Length` bytes
    * when present, otherwise everything up to the connection's close
    * (chunked responses aren't supported, matching [[HttpServer]]'s
    * symmetric choice not to send or parse chunked bodies). The returned
    * `Boolean` says whether the connection is safe to pool: never after
    * reading until EOF (the peer already closed it), and never on
    * HTTP/1.0 or an explicit `Connection: close`.
    */
  private def readResponse(stream: TcpStream, isHead: Boolean)(using Async): (HttpResponse, Boolean) =
    val reader = RequestReader(stream)
    val headBytes = reader.readHead().getOrElse(throw new java.io.EOFException("connection closed before any response"))
    val text = new String(headBytes, StandardCharsets.ISO_8859_1)
    val lines = text.split("\r\n", -1).toList
    val (version, code, reason) = lines.headOption.map(_.split(" ", 3)) match
      case Some(Array(v, c, r)) => (v, c.toInt, r)
      case Some(Array(v, c))    => (v, c.toInt, "")
      case _                     => throw new java.io.IOException(s"malformed status line: ${lines.headOption.getOrElse("")}")
    val headers = lines.tail.foldLeft(Headers.empty) { (hs, line) =>
      line.indexOf(':') match
        case -1 => hs
        case i  => hs.add(line.substring(0, i).trim, line.substring(i + 1).trim)
    }
    val noBody = isHead || code == 204 || code == 304
    val (body, readUntilClose) =
      if noBody then (Array.emptyByteArray, false)
      else
        headers.get("Content-Length") match
          case Some(s) => (reader.readBody(s.toIntOption.getOrElse(0)), false)
          case None if headers.get("Transfer-Encoding").exists(_.equalsIgnoreCase("chunked")) =>
            throw new java.io.IOException("chunked responses are not supported")
          case None => (reader.readUntilEof(), true)
    val keepAlive =
      !readUntilClose && version == "HTTP/1.1" &&
        !headers.get("Connection").exists(_.equalsIgnoreCase("close")) &&
        (noBody || headers.get("Content-Length").isDefined)
    (HttpResponse(HttpStatus(code, reason), headers, body), keepAlive)

/** An HTTP client - Go's `http.Client`. Follows redirects (301/302/303/307/
  * 308) up to [[maxRedirects]] hops, per Go's own per-status rules: 301/302/
  * 303 downgrade a non-GET/HEAD method to `GET` with an empty body, while
  * 307/308 resend the original method and body unchanged. No persistent
  * cookie jar (matching bare `net/http` without `net/http/cookiejar`) -
  * `Set-Cookie`s on a response are available via [[HttpResponse.cookies]]
  * for a caller to inspect or re-send explicitly via
  * [[ClientRequest.addCookie]].
  */
final class Client(transport: Transport, maxRedirects: Int = 10):
  def send(request: ClientRequest)(using Async): HttpResponse = follow(request, maxRedirects)

  def get(url: String)(using Async): HttpResponse = send(ClientRequest.get(url))
  def post(url: String, body: Array[Byte], contentType: String = "application/octet-stream")(using Async): HttpResponse =
    send(ClientRequest.post(url, body, contentType))

  private def follow(request: ClientRequest, remaining: Int)(using Async): HttpResponse =
    val response = transport.roundTrip(request)
    if HttpStatus.isRedirect(response.status.code) && remaining > 0 then
      response.headers.get("Location") match
        case None => response
        case Some(location) =>
          val nextUrl = Url.resolve(request.url, location)
          val nextMethod = Client.redirectMethod(response.status.code, request.method)
          val crossHost = nextUrl.host != request.url.host || nextUrl.port != request.url.port
          val nextHeaders =
            val h = request.headers.remove("Content-Length")
            if crossHost then h.remove("Authorization").remove("Cookie") else h
          val nextBody = if nextMethod != request.method then Array.emptyByteArray else request.body
          follow(ClientRequest(nextMethod, nextUrl, nextHeaders, nextBody), remaining - 1)
    else response

object Client:
  def apply()(using TcpSupport): Client = new Client(Transport())
  def apply(transport: Transport): Client = new Client(transport)

  private[http] def redirectMethod(status: Int, method: HttpMethod): HttpMethod =
    if (status == 301 || status == 302 || status == 303) && method != HttpMethod.GET && method != HttpMethod.HEAD then
      HttpMethod.GET
    else method
