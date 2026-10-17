package gears.async.http

import gears.async.net.{DnsSupport, SocketOption, TcpStream, TcpSupport, TlsContext, TlsSupport}
import gears.async.{Async, CompletionGroup, Future, Resource, Semaphore}

import java.io.{Closeable, IOException}
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.collection.mutable
import scala.concurrent.duration.*

final case class Url(scheme: String, host: String, port: Int, path: String, query: String):
  def target: String = if query.isEmpty then path else s"$path?$query"

  def hostHeader: String =
    val bracketed = if host.contains(':') then s"[$host]" else host
    if port == Url.defaultPort(scheme) then bracketed else s"$bracketed:$port"

object Url:
  private def defaultPort(scheme: String): Int = if scheme == "https" then 443 else 80

  def parse(raw: String): Url =
    if raw.exists(c => c <= ' ' || c == '\u007f') then
      throw new IllegalArgumentException("URL contains whitespace or control characters")
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
      if authority.startsWith("[") then
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

final class Transport(
    tls: Option[TlsSupport] = None,
    tlsContext: TlsSupport => TlsContext = _.clientContext(),
    maxResponseBodySize: Int = 64 * 1024 * 1024,
    maxIdleConnections: Int = 64,
    maxIdleConnectionsPerHost: Int = 8,
    idleTimeout: FiniteDuration = 60.seconds,
    maxConnections: Int = 128,
    connectTimeout: Option[FiniteDuration] = None,
    responseHeaderTimeout: Option[FiniteDuration] = None,
    readTimeout: Option[FiniteDuration] = None
)(using tcp: TcpSupport, dns: DnsSupport)
    extends Closeable:
  require(maxResponseBodySize >= 0 && maxIdleConnections >= 0 && maxIdleConnectionsPerHost >= 0)
  require(maxConnections > 0 && idleTimeout >= Duration.Zero)
  private type Key = (String, String, Int)
  private case class Idle(stream: TcpStream, since: Long)
  private val idle = mutable.Map.empty[Key, mutable.ArrayBuffer[Idle]]
  private val active = mutable.Map.empty[TcpStream, () => Unit]
  private val capacity = Semaphore(maxConnections)
  private var closed = false
  private var idleCount = 0
  private var ownedTlsContext: Option[TlsContext] = None
  private def tlsCtx: TlsContext = synchronized:
    if closed then throw IllegalStateException("The HTTP transport is closed")
    ownedTlsContext match
      case Some(context) => context
      case None          =>
        val context = tlsContext(tls.get)
        ownedTlsContext = Some(context)
        context

  private def takeIdle(scheme: String, host: String, port: Int): Option[TcpStream] = synchronized:
    if closed then throw IllegalStateException("The HTTP transport is closed")
    val queue = idle.getOrElse((scheme, host, port), mutable.ArrayBuffer.empty)
    var found: Option[TcpStream] = None
    while queue.nonEmpty && found.isEmpty do
      val entry = queue.remove(queue.length - 1)
      idleCount -= 1
      if System.nanoTime() - entry.since >= idleTimeout.toNanos then entry.stream.close()
      else found = Some(entry.stream)
    if queue.isEmpty then idle.remove((scheme, host, port))
    found

  private def release(key: Key, stream: TcpStream): Unit = synchronized:
    val queue = idle.getOrElseUpdate(key, mutable.ArrayBuffer.empty)
    if closed || idleTimeout == Duration.Zero || idleCount >= maxIdleConnections || queue.size >= maxIdleConnectionsPerHost
    then
      stream.close()
      if queue.isEmpty then idle.remove(key)
    else
      queue += Idle(stream, System.nanoTime())
      idleCount += 1

  def closeIdleConnections(): Unit =
    val streams = synchronized:
      val all = idle.valuesIterator.flatMap(_.iterator.map(_.stream)).toList
      idle.clear()
      idleCount = 0
      all
    streams.foreach(_.close())

  override def close(): Unit =
    val abort = synchronized:
      closed = true
      active.values.toList
    closeIdleConnections()
    try abort.foreach(_())
    finally
      val context = synchronized:
        val value = ownedTlsContext
        ownedTlsContext = None
        value
      context.foreach(_.close())

  private[http] def dial(scheme: String, host: String, port: Int)(using Async): TcpStream =
    synchronized { if closed then throw IllegalStateException("The HTTP transport is closed") }
    Bodies.timedOwned(connectTimeout)(dialNow(scheme, host, port))

  private def dialNow(scheme: String, host: String, port: Int)(using Async): TcpStream =
    if scheme == "https" && tls.isEmpty then
      throw new UnsupportedOperationException("https requested but no TlsSupport configured on this Transport")
    val addrs = dns.resolve(host)
    if addrs.isEmpty then throw new java.net.UnknownHostException(host)
    val failures = scala.collection.mutable.ArrayBuffer.empty[String]
    val raw = addrs.iterator
      .map { addr =>
        try
          tcp.connect(new InetSocketAddress(addr, port), Seq(SocketOption.noDelay(true))) match
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

  def roundTrip(request: ClientRequest)(using Async): HttpResponse =
    val response = execute(
      () =>
        StreamingRequest(
          request.method,
          request.url,
          request.headers,
          Bodies.bytes(request.body),
          Some(request.body.length.toLong)
        ),
      replayable = Transport.isIdempotent(request.method),
      owner = Async.current.group
    )
    try HttpResponse(response.status, response.headers, response.body.readAll())
    finally response.finish()

  def roundTripStreaming(request: StreamingRequest)(using Async): StreamingResponse =
    roundTripStreaming(request, Async.current.group)

  private[http] def roundTripStreaming(request: StreamingRequest, owner: CompletionGroup)(using
      Async
  ): StreamingResponse =
    execute(() => request, replayable = false, owner = owner)

  def response(request: StreamingRequest): Resource[StreamingResponse] =
    Resource(roundTripStreaming(request), _.finish())

  private def execute(
      request: () => StreamingRequest,
      replayable: Boolean,
      owner: CompletionGroup
  )(using Async): StreamingResponse =
    val permit = capacity.acquire()
    val permitReleased = new java.util.concurrent.atomic.AtomicBoolean(false)
    def releasePermit(): Unit = if permitReleased.compareAndSet(false, true) then permit.release()
    var transferred = false
    try
      val first = request()
      val key = (first.url.scheme, first.url.host, first.url.port)
      val reused = if replayable then takeIdle(key._1, key._2, key._3) else None
      def attempt(stream: TcpStream, outgoing: StreamingRequest): StreamingResponse =
        val uploadAllocation = Async.spawning.allocated
        val uploadScope = uploadAllocation._1
        val uploading = new java.util.concurrent.atomic.AtomicReference[Future[Unit] | Null](null)
        def awaitUpload()(using async: Async): Unit =
          val worker = uploading.get()
          if worker != null then
            worker.cancel()
            worker.awaitResult(using async.withGroup(CompletionGroup.Unlinked))
          uploadAllocation._2
        val finished = new java.util.concurrent.atomic.AtomicBoolean(false)
        val uploaded = new java.util.concurrent.atomic.AtomicBoolean(false)
        def finish(reuse: Boolean, returnPermit: Boolean = true): Unit = if finished.compareAndSet(false, true) then
          uploadScope.group.cancel()
          val worker = uploading.get()
          if worker != null then worker.cancel()
          synchronized { active.remove(stream) }
          try
            if reuse && uploaded.get() then release(key, stream) else stream.close()
          finally if returnPermit then releasePermit()
        val admitted = synchronized:
          if closed then false
          else
            active.update(stream, () => finish(false))
            true
        if !admitted then
          stream.close()
          awaitUpload()
          throw IllegalStateException("The HTTP transport is closed")
        try
          val upload = Future {
            writeRequest(stream, outgoing)
            uploaded.set(true)
          }(using uploadScope, uploadScope)
          uploading.set(upload)
          upload.link(owner)
          if finished.get() then upload.cancel()
          val reader = RequestReader(stream)
          val head = Bodies.timed(responseHeaderTimeout):
            Async.group:
              val incoming = Future {
                var h = readResponseHead(reader)
                while h.code >= 100 && h.code < 200 do
                  if h.code == 101 then throw IOException("Use WebSocket.connect for protocol upgrades")
                  h = readResponseHead(reader)
                h
              }
              Async.select(
                incoming.handle(_.get),
                upload.handle { result =>
                  result match
                    case scala.util.Failure(_: UploadWriteFailure) => ()
                    case other                                     => other.get
                  incoming.await
                }
              )
          val framing = if outgoing.method == HttpMethod.HEAD || head.code == 204 || head.code == 304 then
            BodyFraming.Fixed(0)
          else responseFraming(head.headers)
          framing match
            case BodyFraming.Fixed(n) if n > maxResponseBodySize => throw BodyTooLargeException(maxResponseBodySize)
            case _                                               => ()
          val reuse = framing != BodyFraming.UntilClose && head.version == "HTTP/1.1" &&
            !WebSocketProtocol.hasToken(head.headers, "Connection", "close")
          val decoder = MessageBody(reader, framing, maxResponseBodySize.toLong)
          val body = ResponseBody(decoder, readTimeout, ok => finish(ok && reuse), awaitUpload())
          transferred = true
          StreamingResponse(HttpStatus(head.code, head.reason), head.headers, body)
        catch
          case t: Throwable =>
            finish(false, returnPermit = false)
            awaitUpload()
            throw t
      reused match
        case Some(stream) =>
          try attempt(stream, first)
          catch
            case _: IOException if !Async.current.group.isCancelled => attempt(dial(key._1, key._2, key._3), request())
        case None => attempt(dial(key._1, key._2, key._3), first)
    finally if !transferred then releasePermit()

  private def writeRequest(stream: TcpStream, request: StreamingRequest)(using Async): Unit =
    def write(buffer: ByteBuffer): Unit =
      try stream.writeBuf(buffer)
      catch case e: IOException => throw UploadWriteFailure(e)
    request.contentLength.foreach(n => require(n >= 0, "contentLength must be nonnegative"))
    var headers = request.headers
      .remove("Content-Length")
      .remove("Transfer-Encoding")
      .set("Host", request.url.hostHeader)
      .set("Connection", "keep-alive")
    headers = request.contentLength match
      case Some(n) => headers.set("Content-Length", n.toString)
      case None    => headers.set("Transfer-Encoding", "chunked")
    val fields = headers.iterator.map((k, v) => s"$k: $v\r\n").mkString
    write(
      ByteBuffer.wrap(
        (s"${request.method.rendered} ${request.url.target} HTTP/1.1\r\n${fields}\r\n")
          .getBytes(StandardCharsets.ISO_8859_1)
      )
    )
    var remaining = request.contentLength.getOrElse(Long.MaxValue)
    val buf = ByteBuffer.allocate(8192)
    var done = false
    while !done && remaining > 0 do
      buf.clear()
      buf.limit(math.min(buf.capacity().toLong, remaining).toInt)
      request.body.readBuf(buf) match
        case Left(gears.async.asyncio.Error.EOF) =>
          if request.contentLength.isDefined then throw java.io.EOFException("Request body shorter than Content-Length")
          done = true
        case Right(()) =>
          val n = buf.position()
          if n == 0 then throw IOException("Request body made no progress")
          buf.flip()
          if request.contentLength.isEmpty then
            write(ByteBuffer.wrap((Integer.toHexString(n) + "\r\n").getBytes(StandardCharsets.ISO_8859_1)))
          write(buf)
          if request.contentLength.isEmpty then write(ByteBuffer.wrap(Array[Byte](13, 10)))
          remaining -= n
    if request.contentLength.isEmpty then write(ByteBuffer.wrap("0\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1)))

  private def readResponseHead(reader: RequestReader)(using Async): ResponseHead =
    val headBytes = reader.readHead().getOrElse(throw new java.io.EOFException("connection closed before any response"))
    val lines = new String(headBytes, StandardCharsets.ISO_8859_1).split("\r\n", -1).toList
    def malformed = new java.io.IOException(s"malformed status line: ${lines.headOption.getOrElse("")}")
    val (version, code, reason) = lines.headOption.map(_.split(" ", 3)) match
      case Some(Array(v, c, r)) => (v, c.toIntOption.getOrElse(throw malformed), r)
      case Some(Array(v, c))    => (v, c.toIntOption.getOrElse(throw malformed), "")
      case _                    => throw malformed
    if (version != "HTTP/1.0" && version != "HTTP/1.1") || code < 100 || code > 599 then throw malformed
    val headers = lines.tail.foldLeft(Headers.empty) { (hs, line) =>
      line.indexOf(':') match
        case -1 => throw IOException(s"malformed response header line: $line")
        case i  =>
          val (name, value) = (line.substring(0, i), line.substring(i + 1).trim)
          if !HeaderSyntax.isValidName(name) || !HeaderSyntax.isValidValue(value) then
            throw new java.io.IOException(s"malformed response header line: $line")
          hs.add(name, value)
    }
    ResponseHead(version, code, reason, headers)

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

private final class UploadWriteFailure(cause: IOException) extends IOException("HTTP request write failed", cause)

private final case class ResponseHead(version: String, code: Int, reason: String, headers: Headers)

object Transport:
  private[http] def isIdempotent(method: HttpMethod): Boolean = method match
    case HttpMethod.GET | HttpMethod.HEAD | HttpMethod.PUT | HttpMethod.DELETE | HttpMethod.OPTIONS |
        HttpMethod.TRACE =>
      true
    case _ => false

final class Client(transport: Transport, maxRedirects: Int = 10, timeout: Option[FiniteDuration] = None)
    extends Closeable:
  def close(): Unit = transport.close()
  def closeIdleConnections(): Unit = transport.closeIdleConnections()
  def sendStreaming(request: StreamingRequest)(using Async): StreamingResponse =
    val owner = Async.current.group
    Bodies.timedOwned(timeout, (response: StreamingResponse) => response.finish())(
      transport.roundTripStreaming(request, owner)
    )
  def response(request: StreamingRequest): Resource[StreamingResponse] = Resource(sendStreaming(request), _.finish())

  def send(request: ClientRequest)(using Async): HttpResponse = Bodies.timed(timeout)(follow(request, maxRedirects))

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
