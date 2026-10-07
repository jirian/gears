package gears.async.http

import gears.async.Async
import gears.async.net.{DnsSupport, TcpStream, TcpSupport, TlsContext, TlsSupport}

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

/** `host` is a name or a numeric address, without brackets even for IPv6. */
final case class Url(scheme: String, host: String, port: Int, path: String, query: String):
  def target: String = if query.isEmpty then path else s"$path?$query"

  def hostHeader: String =
    val bracketed = if host.contains(':') then s"[$host]" else host // an IPv6 literal
    if port == Url.defaultPort(scheme) then bracketed else s"$bracketed:$port"

object Url:
  private def defaultPort(scheme: String): Int = if scheme == "https" then 443 else 80

  def parse(raw: String): Url =
    val schemeSep = raw.indexOf("://")
    if schemeSep < 0 then throw new IllegalArgumentException(s"missing scheme in URL: $raw")
    val scheme = raw.substring(0, schemeSep)
    if scheme != "http" && scheme != "https" then
      throw new IllegalArgumentException(s"unsupported URL scheme '$scheme' (only http/https are supported)")
    val rest = raw.substring(schemeSep + 3)
    val pathStart = rest.indexWhere(c => c == '/' || c == '?')
    val authority = if pathStart < 0 then rest else rest.substring(0, pathStart)
    val pathAndQuery = if pathStart < 0 then "/" else rest.substring(pathStart)
    val (host, port) =
      if authority.startsWith("[") then // an IPv6 literal, such as [::1]:8080
        val close = authority.indexOf(']')
        if close < 0 then throw new IllegalArgumentException(s"unterminated IPv6 address in URL: $raw")
        val rest = authority.substring(close + 1)
        val port =
          if rest.isEmpty then defaultPort(scheme)
          else if rest.startsWith(":") then rest.substring(1).toInt
          else throw new IllegalArgumentException(s"malformed authority in URL: $raw")
        (authority.substring(1, close), port)
      else
        authority.split(":", 2) match
          case Array(h, p) => (h, p.toInt)
          case _           => (authority, defaultPort(scheme))
    val (path, query) = pathAndQuery.indexOf('?') match
      case -1 => (pathAndQuery, "")
      case i  => (pathAndQuery.substring(0, i), pathAndQuery.substring(i + 1))
    Url(scheme, host, port, if path.isEmpty then "/" else path, query)

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

final case class ClientRequest(method: HttpMethod, url: Url, headers: Headers, body: Array[Byte]):
  def withHeader(name: String, value: String): ClientRequest = copy(headers = headers.set(name, value))
  def addCookie(cookie: Cookie): ClientRequest = copy(headers = headers.add("Cookie", cookie.rendered))

object ClientRequest:
  def get(url: String): ClientRequest =
    ClientRequest(HttpMethod.GET, Url.parse(url), Headers.empty, Array.emptyByteArray)
  def post(url: String, body: Array[Byte], contentType: String = "application/octet-stream"): ClientRequest =
    ClientRequest(HttpMethod.POST, Url.parse(url), Headers("Content-Type" -> contentType), body)

/** `tls`, if given, is used to wrap the raw TCP connection whenever a request's URL scheme is `https`; an `https`
  * request without one fails fast with a clear error rather than silently talking plaintext to a TLS port. `tlsContext`
  * builds the (reusable) client context to hand to `tls.wrapClient` - defaults to `tls.clientContext()` (full
  * certificate verification against the system trust store), overridable to add a CA or disable verification for
  * testing. `maxResponseBodySize` bounds a response body, which is read into memory whole; a larger one fails the
  * request with an `IOException`.
  */
final class Transport(
    tls: Option[TlsSupport] = None,
    tlsContext: TlsSupport => TlsContext = _.clientContext(),
    maxResponseBodySize: Int = 64 * 1024 * 1024
)(using tcp: TcpSupport, dns: DnsSupport):
  private val idle = new ConcurrentHashMap[(String, String, Int), ConcurrentLinkedQueue[TcpStream]]()
  private lazy val tlsCtx: TlsContext = tlsContext(tls.get)

  private def takeIdle(scheme: String, host: String, port: Int): Option[TcpStream] =
    Option(idle.get((scheme, host, port))).flatMap(q => Option(q.poll()))

  private def release(scheme: String, host: String, port: Int, stream: TcpStream): Unit =
    idle.computeIfAbsent((scheme, host, port), _ => new ConcurrentLinkedQueue()).offer(stream)

  private def dial(scheme: String, host: String, port: Int)(using Async): TcpStream =
    // Resolved asynchronously (never blocking this fiber's shard thread)
    // rather than via `new InetSocketAddress(host, port)`, whose constructor
    // resolves `host` *synchronously* - see DnsSupport's own doc for why that
    // matters. The addresses are tried in order until one connects; there is
    // no Happy Eyeballs-style racing.
    val addrs = dns.resolve(host)
    if addrs.isEmpty then throw new java.net.UnknownHostException(host)
    val failures = scala.collection.mutable.ArrayBuffer.empty[String]
    val raw = addrs.iterator
      .map { addr =>
        try
          tcp.connect(new InetSocketAddress(addr, port), Seq.empty) match
            case Right(stream) => Some(stream)
            case Left(e)       =>
              failures += s"$addr: $e"
              None
        catch
          case e: java.io.IOException =>
            failures += s"$addr: ${e.getMessage}"
            None
      }
      .collectFirst { case Some(stream) => stream }
      .getOrElse(throw new java.io.IOException(s"connect to $host:$port failed: ${failures.mkString("; ")}"))
    if scheme != "https" then raw
    else
      val ts = tls.getOrElse(
        throw new UnsupportedOperationException("https requested but no TlsSupport configured on this Transport")
      )
      try ts.wrapClient(raw, tlsCtx, host)
      catch
        case e: Throwable =>
          raw.close()
          throw e

  /** Sends `request` and returns its response.
    *
    * An idempotent request reuses an idle connection to its host if there is one, and if that fails, because the server
    * closed the connection while it sat idle, it is sent again on a new connection. A non-idempotent request, such as a
    * POST, always gets a new connection: with no way to tell whether a server that went away had already processed it,
    * retrying could apply it twice, and not retrying would fail it whenever a pooled connection had gone stale. Either
    * way the connection is pooled afterwards when the response allows.
    */
  def roundTrip(request: ClientRequest)(using Async): HttpResponse =
    val scheme = request.url.scheme
    val host = request.url.host
    val port = request.url.port
    val reusable = if Transport.isIdempotent(request.method) then takeIdle(scheme, host, port) else None
    reusable match
      case Some(reused) =>
        try attempt(reused, request)
        catch case _: Exception => attempt(dial(scheme, host, port), request)
      case None => attempt(dial(scheme, host, port), request)

  /** One exchange on `stream`, which is released for reuse or closed afterwards, and closed on failure. */
  private def attempt(stream: TcpStream, request: ClientRequest)(using Async): HttpResponse =
    try
      writeRequest(stream, request)
      val (response, keepAlive) = readResponse(stream, isHead = request.method == HttpMethod.HEAD)
      if keepAlive then release(request.url.scheme, request.url.host, request.url.port, stream)
      else closeGracefully(stream)
      response
    catch
      case e: Throwable =>
        stream.close()
        throw e

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

  /** Reads the final response, skipping interim 1xx ones such as `100 Continue` or `103 Early Hints`. Returns it with
    * whether the connection can be reused.
    */
  private def readResponse(stream: TcpStream, isHead: Boolean)(using Async): (HttpResponse, Boolean) =
    val reader = RequestReader(stream)
    var head = readResponseHead(reader)
    while head.code >= 100 && head.code < 200 do
      if head.code == 101 then
        throw new java.io.IOException("protocol upgrades (101 Switching Protocols) are not supported")
      head = readResponseHead(reader)
    val ResponseHead(version, code, reason, headers) = head
    val framing =
      if isHead || code == 204 || code == 304 then BodyFraming.Fixed(0)
      else responseFraming(headers)
    val body =
      try
        framing match
          case BodyFraming.Fixed(length) =>
            if length > maxResponseBodySize then throw BodyTooLargeException(maxResponseBodySize)
            reader.readBody(length)
          case BodyFraming.Chunked    => reader.readChunked(maxResponseBodySize)
          case BodyFraming.UntilClose => reader.readUntilEof(maxResponseBodySize)
      catch case e: BodyTooLargeException => throw new java.io.IOException(s"response ${e.getMessage}", e)
    val keepAlive =
      framing != BodyFraming.UntilClose && version == "HTTP/1.1" &&
        !headers.get("Connection").exists(_.equalsIgnoreCase("close"))
    (HttpResponse(HttpStatus(code, reason), headers, body), keepAlive)

  private def readResponseHead(reader: RequestReader)(using Async): ResponseHead =
    val headBytes = reader.readHead().getOrElse(throw new java.io.EOFException("connection closed before any response"))
    val lines = new String(headBytes, StandardCharsets.ISO_8859_1).split("\r\n", -1).toList
    def malformed = new java.io.IOException(s"malformed status line: ${lines.headOption.getOrElse("")}")
    val (version, code, reason) = lines.headOption.map(_.split(" ", 3)) match
      case Some(Array(v, c, r)) => (v, c.toIntOption.getOrElse(throw malformed), r)
      case Some(Array(v, c))    => (v, c.toIntOption.getOrElse(throw malformed), "")
      case _                    => throw malformed
    val headers = lines.tail.foldLeft(Headers.empty) { (hs, line) =>
      line.indexOf(':') match
        case -1 => hs
        case i  =>
          val (name, value) = (line.substring(0, i).trim, line.substring(i + 1).trim)
          if !HeaderSyntax.isValidName(name) || !HeaderSyntax.isValidValue(value) then
            throw new java.io.IOException(s"malformed response header line: $line")
          hs.add(name, value)
    }
    ResponseHead(version, code, reason, headers)

  /** How a response body is delimited (RFC 9112 section 6.3): `Transfer-Encoding` takes precedence over
    * `Content-Length`, and with neither the body runs until the connection closes. A length the client cannot trust
    * leaves no way to tell where this response ends and the next begins, so it fails the request.
    */
  private def responseFraming(headers: Headers): BodyFraming =
    val codings = headers.getAll("Transfer-Encoding")
    if codings.nonEmpty then
      if codings.size == 1 && codings.head.equalsIgnoreCase("chunked") then BodyFraming.Chunked
      else throw new java.io.IOException(s"unsupported Transfer-Encoding in response: ${codings.mkString(", ")}")
    else
      headers.getAll("Content-Length").distinct match
        case Seq()  => BodyFraming.UntilClose
        case Seq(s) =>
          BodyFraming.Fixed(
            s.toIntOption.filter(_ >= 0).getOrElse(throw new java.io.IOException(s"invalid Content-Length: $s"))
          )
        case lengths =>
          throw new java.io.IOException(s"conflicting Content-Length headers in response: ${lengths.mkString(", ")}")

private final case class ResponseHead(version: String, code: Int, reason: String, headers: Headers)

object Transport:
  /** Methods RFC 9110 defines as idempotent, which a client may safely send again. */
  private[http] def isIdempotent(method: HttpMethod): Boolean = method match
    case HttpMethod.GET | HttpMethod.HEAD | HttpMethod.PUT | HttpMethod.DELETE | HttpMethod.OPTIONS |
        HttpMethod.TRACE =>
      true
    case _ => false

final class Client(transport: Transport, maxRedirects: Int = 10):
  def send(request: ClientRequest)(using Async): HttpResponse = follow(request, maxRedirects)

  def get(url: String)(using Async): HttpResponse = send(ClientRequest.get(url))
  def post(url: String, body: Array[Byte], contentType: String = "application/octet-stream")(using
      Async
  ): HttpResponse =
    send(ClientRequest.post(url, body, contentType))

  private def follow(request: ClientRequest, remaining: Int)(using Async): HttpResponse =
    val response = transport.roundTrip(request)
    if HttpStatus.isRedirect(response.status.code) && remaining > 0 then
      response.headers.get("Location") match
        case None           => response
        case Some(location) =>
          val nextUrl = Url.resolve(request.url, location)
          val nextMethod = Client.redirectMethod(response.status.code, request.method)
          // A different origin, including https to http on the same host, must not receive the credentials.
          val crossOrigin =
            nextUrl.scheme != request.url.scheme || nextUrl.host != request.url.host || nextUrl.port != request.url.port
          val nextHeaders =
            val h = request.headers.remove("Content-Length")
            if crossOrigin then h.remove("Authorization").remove("Cookie") else h
          val nextBody = if nextMethod != request.method then Array.emptyByteArray else request.body
          follow(ClientRequest(nextMethod, nextUrl, nextHeaders, nextBody), remaining - 1)
    else response

object Client:
  def apply()(using TcpSupport, DnsSupport): Client = new Client(Transport())
  def apply(transport: Transport): Client = new Client(transport)
  def apply(tls: TlsSupport)(using TcpSupport, DnsSupport): Client = new Client(Transport(tls = Some(tls)))

  private[http] def redirectMethod(status: Int, method: HttpMethod): HttpMethod =
    if (status == 301 || status == 302 || status == 303) && method != HttpMethod.GET && method != HttpMethod.HEAD then
      HttpMethod.GET
    else method
