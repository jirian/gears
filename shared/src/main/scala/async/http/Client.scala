package gears.async.http

import gears.async.Async
import gears.async.net.{TcpStream, TcpSupport, TlsContext, TlsSupport}

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

final case class Url(scheme: String, host: String, port: Int, path: String, query: String):
  def target: String = if query.isEmpty then path else s"$path?$query"

  def hostHeader: String = if port == Url.defaultPort(scheme) then host else s"$host:$port"

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
    val (host, port) = authority.split(":", 2) match
      case Array(h, p) => (h, p.toInt)
      case _             => (authority, defaultPort(scheme))
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
  def get(url: String): ClientRequest = ClientRequest(HttpMethod.GET, Url.parse(url), Headers.empty, Array.emptyByteArray)
  def post(url: String, body: Array[Byte], contentType: String = "application/octet-stream"): ClientRequest =
    ClientRequest(HttpMethod.POST, Url.parse(url), Headers("Content-Type" -> contentType), body)

/** `tls`, if given, is used to wrap the raw TCP connection whenever a
  * request's URL scheme is `https`; an `https` request without one fails
  * fast with a clear error rather than silently talking plaintext to a TLS
  * port. `tlsContext` builds the (reusable) client context to hand to
  * `tls.wrapClient` - defaults to `tls.clientContext()` (full certificate
  * verification against the system trust store), overridable to add a CA
  * or disable verification for testing.
  */
final class Transport(
    tls: Option[TlsSupport] = None,
    tlsContext: TlsSupport => TlsContext = _.clientContext()
)(using tcp: TcpSupport):
  private val idle = new ConcurrentHashMap[(String, String, Int), ConcurrentLinkedQueue[TcpStream]]()
  private lazy val tlsCtx: TlsContext = tlsContext(tls.get)

  private def takeIdle(scheme: String, host: String, port: Int): Option[TcpStream] =
    Option(idle.get((scheme, host, port))).flatMap(q => Option(q.poll()))

  private def release(scheme: String, host: String, port: Int, stream: TcpStream): Unit =
    idle.computeIfAbsent((scheme, host, port), _ => new ConcurrentLinkedQueue()).offer(stream)

  private def dial(scheme: String, host: String, port: Int)(using Async): TcpStream =
    val raw = tcp.connect(new InetSocketAddress(host, port), Seq.empty) match
      case Right(stream) => stream
      case Left(e)         => throw new java.io.IOException(s"connect to $host:$port failed: $e")
    if scheme != "https" then raw
    else
      val ts = tls.getOrElse(throw new UnsupportedOperationException("https requested but no TlsSupport configured on this Transport"))
      ts.wrapClient(raw, tlsCtx, host)

  def roundTrip(request: ClientRequest)(using Async): HttpResponse =
    val scheme = request.url.scheme
    val host = request.url.host
    val port = request.url.port
    takeIdle(scheme, host, port) match
      case Some(reused) =>
        try attempt(reused, request)
        catch
          case _: Exception =>
            reused.close()
            attempt(dial(scheme, host, port), request)
      case None => attempt(dial(scheme, host, port), request)

  private def attempt(stream: TcpStream, request: ClientRequest)(using Async): HttpResponse =
    writeRequest(stream, request)
    val (response, keepAlive) = readResponse(stream, isHead = request.method == HttpMethod.HEAD)
    if keepAlive then release(request.url.scheme, request.url.host, request.url.port, stream) else stream.close()
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
  def apply(tls: TlsSupport)(using TcpSupport): Client = new Client(Transport(tls = Some(tls)))

  private[http] def redirectMethod(status: Int, method: HttpMethod): HttpMethod =
    if (status == 301 || status == 302 || status == 303) && method != HttpMethod.GET && method != HttpMethod.HEAD then
      HttpMethod.GET
    else method
