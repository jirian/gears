package gears.io.http

import gears.async.Async
import gears.async.io.Io
import gears.async.io.IpStreams
import gears.async.io.Resolving
import gears.io.net.BufferedInput
import gears.io.net.Stream
import gears.io.net.Tcp
import gears.io.tls.Tls
import gears.io.tls.TlsClientConfig

import java.io.IOException
import java.util.concurrent.CancellationException
import java.util.concurrent.TimeoutException
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

/** How long a client waits: `connect`, for a connection and its TLS handshake; `responseHead`, from sending a request
  * to its response's head; `read`, for each read of a response's body; `write`, for each write of a request, and of a
  * WebSocket's frame; `total`, for all of a request, unless the request sets its own (`Request.withTimeout`). Each but
  * `total` is finite by default, so that a host that stops answering cannot hang its caller; `total` is not, so that a
  * long download is not cut off.
  */
final case class ClientTimeouts(
    connect: Duration = 10.seconds,
    responseHead: Duration = 30.seconds,
    read: Duration = 30.seconds,
    total: Duration = Duration.Inf,
    write: Duration = 30.seconds
)

/** Whether a client follows redirects: `Follow` up to `max` of them, never from `https` to `http` unless
  * `allowDowngrade`, and taking a request's `Authorization` and `Cookie` headers only to the origin it was made for.
  */
enum Redirects {
  case None
  case Follow(max: Int = 10, allowDowngrade: Boolean = false)
}

/** A client's kept-alive connections: at most `maxIdlePerHost` kept per origin, each for at most `idle`. */
final case class PoolConfig(maxIdlePerHost: Int = 16, idle: FiniteDuration = 60.seconds)

/** A client's configuration. `maxResponseBody` bounds the body `send` reads whole; `stream` reads it as it comes. */
final case class ClientConfig(
    timeouts: ClientTimeouts = ClientTimeouts(),
    redirects: Redirects = Redirects.Follow(),
    pool: PoolConfig = PoolConfig(),
    tls: TlsClientConfig = TlsClientConfig(),
    maxResponseBody: Long = 64L * 1024 * 1024,
    userAgent: Option[String] = Some("gears-http"),
    headers: Headers = Headers.empty
)

/** An HTTP/1.1 client over `io`'s backend: cheap to share, from any future, and kept for many requests, since it keeps
  * their connections alive for the next to the same origin. `close` closes the idle ones.
  *
  * A request reuses a kept connection whatever its method. If the connection turns out to have been closed by the
  * server, which a connection kept idle may be, before any of the response has arrived, the request is sent again on a
  * fresh one if that is safe: its method is idempotent, or it has an `Idempotency-Key`, and its body is in memory.
  */
final class HttpClient(val config: ClientConfig = ClientConfig())(using io: Io[IpStreams & Resolving]) {
  private val pool = new HttpClient.Pool(config.pool)

  /** Sends `request` and returns its response with the body read whole, at most `config.maxResponseBody` bytes. A
    * status that is not a success is a response like any other: `ensureSuccess` turns it into an exception.
    */
  def send(request: Request)(using Async): Response = stream(request) { response =>
    val body = response.body.bytes()
    response.copy(body = Body(body, response.body.contentType.getOrElse("application/octet-stream")))
  }

  /** Sends `request` and runs `use` with its response, whose body arrives as `use` reads it: for a large download, or
    * one read as it comes. The connection goes back to the pool once `use` returns having read the body, and is closed
    * otherwise, however `use` ends, so that nothing is left holding it.
    */
  def stream[A](request: Request)(use: Response => Async ?=> A)(using Async): A = {
    // The configured headers go on the request itself, so that a redirect to another origin drops their credentials
    // as it drops the request's own.
    val withDefaults = request.copy(headers = config.headers ++ request.headers)
    val limit = request.timeout.orElse(Stream.limit(config.timeouts.total) match {
      case null              => scala.None
      case d: FiniteDuration => Some(d)
    })
    limit match {
      case Some(d) => gears.async.withTimeout(d)(follow(withDefaults, use, 0))(using timer, summon[Async])
      case _       => follow(withDefaults, use, 0)
    }
  }

  private def timer: gears.async.AsyncOperations = io

  private def follow[A](request: Request, use: Response => Async ?=> A, hops: Int)(using Async): A = {
    val exchange = open(request)
    val response = exchange.response
    val next = config.redirects match {
      case Redirects.Follow(max, downgrade) if response.status.isRedirect && response.header("Location").isDefined =>
        if hops >= max then
          throw new IOException(s"More than $max redirects, the last to ${response.header("Location").get}")
        val target =
          try request.url.resolve(response.header("Location").get)
          catch case e: IllegalArgumentException => throw new IOException(s"A malformed redirect: ${e.getMessage}", e)
        if request.url.secure && !target.secure && !downgrade then
          throw new IOException(s"A redirect from https to http, refused: $target")
        val code = response.status.code
        val keepsBody = code == 307 || code == 308
        if keepsBody && !request.body.replayable then null // its body is gone: the caller gets the redirect
        else {
          val sameOrigin = target.origin == request.url.origin
          var headers = request.headers
          if !sameOrigin then headers = headers.remove("Authorization").remove("Cookie").remove("Proxy-Authorization")
          if keepsBody then request.copy(url = target, headers = headers)
          else {
            val method = if request.method == Method.Head then Method.Head else Method.Get
            headers = headers.remove("Content-Type").remove("Content-Length")
            request.copy(method = method, url = target, headers = headers, body = Body.empty)
          }
        }
      case _ => null
    }
    if next == null then exchange.finish(use)
    else {
      exchange.discard()
      follow(next.nn, use, hops + 1)
    }
  }

  /** Sends `request` on a kept connection to its origin, or a new one, and reads its response's head. */
  private def open(request: Request)(using Async): HttpClient.Exchange = {
    val origin = request.url.origin
    val kept = pool.take(origin)
    if kept != null then
      try return exchange(kept.nn, request, reused = true)
      catch {
        case _: HttpClient.Stale if safeToRetry(request) => () // sent again below, on a fresh connection
        case e: HttpClient.Stale                         => throw e.getCause
      }
    val fresh = connect(request.url)
    try exchange(fresh, request, reused = false)
    catch case e: HttpClient.Stale => throw e.getCause
  }

  private def safeToRetry(request: Request): Boolean =
    request.body.replayable && (request.method.idempotent || request.headers.contains("Idempotency-Key"))

  private def connect(url: Url)(using Async): HttpClient.Connection = {
    val t = config.timeouts
    val raw = Tcp.connect(url.host, url.port, t.connect)
    val stream =
      if url.secure then Tls.client(raw, url.host, config.tls, t.connect)
      else raw
    new HttpClient.Connection(stream, new BufferedInput(stream, 8192, 64 * 1024), url.origin)
  }

  /** Writes `request` on `connection` and reads the response's head. A failure before any of it arrived on a reused
    * connection is `Stale`, which `open` may retry.
    */
  private def exchange(connection: HttpClient.Connection, request: Request, reused: Boolean)(using
      Async
  ): HttpClient.Exchange = {
    val t = config.timeouts
    var headed = false
    try {
      // A server may answer before taking the whole body, as with a 413, and close: its answer is read all the same.
      val written =
        try {
          writeRequest(connection.stream, request)
          null
        } catch case e: IOException => e
      val head =
        try Wire.readResponseHead(connection.input, 64 * 1024, t.responseHead)
        catch case _: IOException if written != null => throw written.nn
      headed = true
      val noBody = request.method == Method.Head || head.status.forbidsBody
      val framing =
        try Wire.framing(head.headers, if noBody then Wire.Framing.NoBody else Wire.Framing.UntilClose)
        catch case e: Wire.Malformed => throw new IOException(s"A malformed response: ${e.getMessage}")
      val framed = if noBody then Wire.Framing.NoBody else framing
      val source = new Wire.Source(
        connection.input,
        framed,
        config.maxResponseBody,
        t.read,
        () => (),
        limit => new IOException(s"A response body larger than $limit bytes")
      )
      val body =
        if framed == Wire.Framing.NoBody then Body.empty
        else
          new Body.Incoming(
            head.headers.get("Content-Type"),
            framed match {
              case Wire.Framing.Length(n) => Some(n)
              case _                      => scala.None
            },
            source
          )
      val reusable =
        written == null && framed != Wire.Framing.UntilClose && !head.headers.hasToken("Connection", "close") &&
          !request.headers.hasToken("Connection", "close") && head.version == "HTTP/1.1"
      new HttpClient.Exchange(connection, Response(head.status, head.headers, body), source, reusable, pool)
    } catch {
      case e: CancellationException            => connection.close(); throw e
      case e: (IOException | TimeoutException) =>
        connection.close()
        e match {
          case io: IOException if reused && !headed => throw new HttpClient.Stale(io)
          case other                                => throw other
        }
      case e: Throwable =>
        connection.close()
        throw e
    }
  }

  private def writeRequest(stream: Stream, request: Request)(using Async): Unit = {
    val url = request.url
    var headers = request.headers.setIfAbsent("Host", url.authority)
    config.userAgent.foreach(ua => headers = headers.setIfAbsent("User-Agent", ua))
    val body = request.body
    val hasBody = body.length.forall(_ > 0) && !(body eq Body.empty)
    val chunked = hasBody && body.length.isEmpty
    headers = headers.remove("Transfer-Encoding")
    if chunked then headers = headers.remove("Content-Length").add("Transfer-Encoding", "chunked")
    else if hasBody || request.method == Method.Post || request.method == Method.Put || request.method == Method.Patch
    then headers = headers.set("Content-Length", body.length.getOrElse(0L).toString)
    body.contentType.foreach(t => headers = headers.setIfAbsent("Content-Type", t))
    if !Headers.isToken(request.method.name) then
      throw new IllegalArgumentException(s"Not a method token: '${request.method.name}'")
    val head = Wire.head(s"${request.method.name} ${url.target} HTTP/1.1", headers)
    val out = new Wire.Out(stream, head, chunked, body.length.getOrElse(-1L), false, config.timeouts.write)
    if hasBody then body.writeTo(out)
    out.finish()
  }

  /** Closes the kept connections. Requests under way go on, and a later one opens a new connection. */
  def close()(using Async): Unit = pool.closeAll()

  /** Runs `body` with this client, and closes it afterwards. */
  def use[A](body: HttpClient => A)(using Async): A =
    try body(this)
    finally close()

  def get(url: String | Url)(using Async): Response = send(Request.get(url))
  def post(url: String | Url, body: Body)(using Async): Response = send(Request.post(url, body))
  def put(url: String | Url, body: Body)(using Async): Response = send(Request.put(url, body))
  def delete(url: String | Url)(using Async): Response = send(Request.delete(url))
}

object HttpClient {

  /** A connection to `origin`, with its reader. */
  private[http] final class Connection(
      val stream: Stream,
      val input: BufferedInput,
      val origin: (String, String, Int)
  ) {
    var keptSince = 0L
    def close()(using Async): Unit =
      try stream.close()
      catch case _: IOException => ()
  }

  /** A failure of a request on a reused connection before any of its response came. */
  private final class Stale(cause: IOException) extends IOException(cause.getMessage, cause) {
    override def getCause: IOException = cause
  }

  /** A response's head read, its body still on the connection. */
  private final class Exchange(
      connection: Connection,
      val response: Response,
      source: Wire.Source,
      reusable: Boolean,
      pool: Pool
  ) {

    /** Runs `use`, then gives the connection back if the body was read and it may carry another request, or closes it.
      */
    def finish[A](use: Response => Async ?=> A)(using Async): A = {
      var kept = false
      try {
        val result = use(response)
        if reusable && source.complete then {
          pool.give(connection)
          kept = true
        }
        result
      } finally if !kept then connection.close()
    }

    /** Drops the response: its body, if small, is drained so that the connection is kept. */
    def discard()(using Async): Unit = {
      val drained =
        try reusable && source.drain(64 * 1024)
        catch case _: Exception => false
      if drained then pool.give(connection) else connection.close()
    }
  }

  /** Idle connections by origin, the most recently used first. */
  private final class Pool(config: PoolConfig) {
    private val idle = scala.collection.mutable.HashMap.empty[(String, String, Int), List[Connection]]

    def take(origin: (String, String, Int))(using Async): Connection | Null = {
      val now = System.nanoTime()
      var found: Connection | Null = null
      val expired = scala.collection.mutable.ListBuffer.empty[Connection]
      synchronized {
        var list = idle.getOrElse(origin, Nil)
        while found == null && list.nonEmpty do {
          val c = list.head
          list = list.tail
          if now - c.keptSince < config.idle.toNanos then found = c else expired += c
        }
        if list.isEmpty then idle.remove(origin) else idle(origin) = list
      }
      expired.foreach(_.close())
      found
    }

    def give(connection: Connection)(using Async): Unit = {
      connection.keptSince = System.nanoTime()
      val dropped = synchronized {
        val list = connection :: idle.getOrElse(connection.origin, Nil)
        val (keep, drop) = list.splitAt(config.maxIdlePerHost)
        idle(connection.origin) = keep
        drop
      }
      dropped.foreach(_.close())
    }

    def closeAll()(using Async): Unit = {
      val all = synchronized {
        val a = idle.values.flatten.toList
        idle.clear()
        a
      }
      all.foreach(_.close())
    }
  }
}
