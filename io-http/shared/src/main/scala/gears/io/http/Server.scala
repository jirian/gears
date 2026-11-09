package gears.io.http

import gears.async.Async
import gears.async.Future
import gears.async.io.Io
import gears.async.io.IpAddress
import gears.async.io.IpSocketAddress
import gears.async.io.Protocol
import gears.io.net.BufferedInput
import gears.io.net.Listener
import gears.io.net.Stream
import gears.io.net.Tcp
import gears.io.tls.Tls
import gears.io.tls.TlsServerConfig

import java.io.IOException
import java.nio.channels.ClosedChannelException
import java.util.concurrent.CancellationException
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import scala.util.Success

/** How long a server waits for each part of a request, each finite by default, so that a client that stalls cannot hold
  * a connection: `header`, from a request's first byte to the end of its head, all of it, so that a client sending a
  * byte at a time cannot keep it open (slowloris); `read`, for each read of a body; `write`, for each write of a
  * response, so that a client that does not read cannot hold it; `idle`, between requests on a kept-alive connection;
  * `handshake`, for TLS's. `Duration.Inf` turns one off.
  */
final case class ServerTimeouts(
    header: Duration = 10.seconds,
    read: Duration = 30.seconds,
    write: Duration = 30.seconds,
    idle: Duration = 60.seconds,
    handshake: Duration = 10.seconds
)

/** How much a server takes: a request's head, in bytes, and its header fields; a body, in bytes, beyond which reading
  * it is answered 413; and connections at once, beyond which new ones are accepted and closed at once.
  */
final case class Limits(
    head: Int = 16 * 1024,
    headers: Int = 100,
    body: Long = 10L * 1024 * 1024,
    connections: Int = 10_000
) {
  require(head >= 1024 && headers > 0 && body >= 0 && connections > 0, s"Limits out of range: $this")
}

/** A server's configuration: where it listens, `host`, numeric, and `port`, 0 for one the system picks; its time limits
  * and size limits; TLS, if it serves HTTPS; and where it reports a handler's failure and a connection's, which never
  * reach a client. A plain value: starting it is `Http.server`'s.
  *
  * `cancelAbandoned` is how long a handler runs before the server watches its connection for the client going away, and
  * cancels the handler if it has, as Go's server cancels a request's context: the handler sees a
  * `CancellationException` at its next suspension, and the futures it started are cancelled with it. A connection is
  * watched only once the handler has read the request's body, and not while the client has sent the next request
  * already; a handler that finishes sooner costs nothing. A client that shuts its output down once it has sent its
  * request, as `StreamSocketOps.shutdownOutput` suggests, looks gone too, and its handler is cancelled: as with Go's
  * server, such a client is served with `Duration.Inf`. `Duration.Inf` never cancels: for a handler whose work must not
  * stop halfway; a handler can instead protect just that part, running it with
  * `Async.current.withGroup(CompletionGroup.Unlinked)`.
  */
final case class ServerConfig(
    host: String = "0.0.0.0",
    port: Int = 8080,
    timeouts: ServerTimeouts = ServerTimeouts(),
    limits: Limits = Limits(),
    tls: Option[TlsServerConfig] = None,
    cancelAbandoned: Duration = 100.millis,
    log: String => Unit = message => System.err.println(s"[gears-http] $message")
)

/** A running server, which `Http.server` starts in the caller's scope: it stops, abruptly, when the scope ends, as any
  * future of it does; `shutdown` stops it gracefully first.
  */
final class Server private[http] (config: ServerConfig, listener: Listener, done: Future[Unit], state: Server.State) {

  /** The port it listens on, the one the system chose if `config.port` was 0. */
  val port: Int = Tcp.port(listener.localAddress)

  /** How many connections are open. */
  def connections: Int = state.connections.get()

  /** Its URL on this host, `http` or `https`, at the address it listens on, or the loopback address if it listens on
    * every interface: `http://127.0.0.1:<port>`, or `http://[::1]:<port>` for `::`. An IPv6 address's scope is left
    * out, since a URL's host has none.
    */
  val url: Url = {
    val host = listener.localAddress match {
      case IpSocketAddress(a, _) if a == IpAddress.IPv4Any      => "127.0.0.1"
      case IpSocketAddress(a, _) if a == IpAddress.IPv6Any      => "[::1]"
      case IpSocketAddress(a, _) if a.protocol == Protocol.IPv6 =>
        s"[${IpAddress(Protocol.IPv6, a.bytes)}]"
      case IpSocketAddress(a, _) => a.toString
      case other                 => throw new IllegalStateException(s"A server on $other has no URL")
    }
    Url.parse(s"${if config.tls.isDefined then "https" else "http"}://$host:$port")
  }

  /** Stops accepting connections and closes the idle ones, lets the requests under way finish for up to `grace`, and
    * then stops the rest; returns once every connection is closed.
    */
  def shutdown(grace: FiniteDuration = 10.seconds)(using Async): Unit = {
    state.stopping.set(true)
    listener.close()
    state.acceptEnded()
    state.closeIdle()
    gears.async.withTimeoutOption(grace)(state.drained.await)(using state.async, summon[Async]): Unit
    done.cancel()
    done.awaitResult: Unit
  }

  /** Waits until the server stops: for ever, unless something calls `shutdown` or cancels the caller's scope. */
  def await()(using Async): Unit = done.awaitResult: Unit
}

private[http] object Server {

  /** What a server's connections share: whether it is stopping, how many there are, and each, so that stopping closes
    * the idle ones.
    */
  final class State(val async: gears.async.AsyncOperations) {
    val stopping = new AtomicBoolean(false)
    val connections = new AtomicInteger(0)
    val open = java.util.concurrent.ConcurrentHashMap.newKeySet[Connection]()

    private val accepting = new AtomicBoolean(true)
    private val noneLeft = Future.Promise[Unit]()

    /** Completes once no connection will be accepted (`acceptEnded`) and none is open. */
    def drained: Future[Unit] = noneLeft.asFuture

    /** A connection taken, counted: false, uncounted, if there are as many as `limit` already, or the server stops. */
    def admit(limit: Int): Boolean =
      if connections.incrementAndGet() > limit || stopping.get() then {
        ended()
        false
      } else true

    /** A connection admitted has closed. */
    def ended(): Unit = if connections.decrementAndGet() == 0 && !accepting.get() then noneLeft.complete(Success(()))

    /** No more connections will be admitted: from the accept loop, as it stops, or from `shutdown`. */
    def acceptEnded(): Unit = {
      accepting.set(false)
      if connections.get() == 0 then noneLeft.complete(Success(()))
    }

    /** Closes the connections waiting for a request: a client that sends one just then sees the connection close, and
      * may send it again, as one does when a kept-alive connection closes under it.
      */
    def closeIdle()(using Async): Unit = open.forEach(c => if c.idle then c.abort())
  }

  /** Listens as `config` says and serves `routes` until the scope ends or `shutdown`. */
  def start(routes: Routes, config: ServerConfig)(using
      io: Io[gears.io.net.DefaultCapabilities],
      spawn: Async.Spawn
  ): Server = {
    val tlsContext = config.tls.map(Tls.serverContext)
    val listener = Tcp.listen(config.host, config.port)
    val state = new State(io)
    val done = Future {
      try
        Async.group {
          val connectionsGroup = summon[Async.Spawn]
          var accepting = true
          while accepting do {
            val stream =
              try listener.accept()
              catch {
                case _: ClosedChannelException => null
                case e: CancellationException  => throw e
                case e: IOException            =>
                  config.log(s"Accepting a connection failed: ${e.getMessage}")
                  gears.async.AsyncOperations.sleep(50)
                  null
              }
            if stream == null then accepting = !state.stopping.get() && !listener.closed
            else if !state.admit(config.limits.connections) then stream.close()
            else {
              Future {
                try new Connection(stream, routes, config, tlsContext, state, io, connectionsGroup).run()
                finally state.ended()
              }: Unit
            }
          }
          // Leaving the group would cancel the connections: they finish first, or `shutdown`'s grace ends them.
          state.acceptEnded()
          state.drained.await
        }
      finally listener.close()
    }
    new Server(config, listener, done, state)
  }

  /** A request being answered, for the watch of its connection: the group its handler runs in, when it began, and
    * whether the watch is reading the connection for it (`Reading`), or no longer may (`Settled`).
    */
  final class Answering(val group: gears.async.CompletionGroup, val since: Long, body: Body) {
    val state = new AtomicInteger(Answering.Running)

    /** Whether the handler has read all of the request's body, so that what arrives next is the next request's. */
    def bodyRead: Boolean = body match {
      case in: Body.Incoming => in.complete
      case _                 => true
    }
  }

  object Answering {
    inline val Running = 0
    inline val Reading = 1
    inline val Settled = 2
  }

  /** One connection's requests, one after another: each head read, the body framed, the routes asked, and the response
    * written, until either side ends the connection or the server stops.
    */
  final class Connection(
      raw: Stream,
      routes: Routes,
      config: ServerConfig,
      tls: Option[gears.io.tls.TlsServerContext],
      state: State,
      io: Io[gears.io.net.DefaultCapabilities],
      spawn: Async.Spawn
  ) {
    private val t = config.timeouts
    private given Io[gears.io.net.DefaultCapabilities] = io

    // The watch for the client going away (`ServerConfig.cancelAbandoned`): one future per connection at most, started
    // by the first request that may need it, in the server's group rather than the connection's, so that it ends on its
    // own, never cancelled, when there is nothing left to watch. A request being answered is `answering`.
    private val watching = new AtomicBoolean(false)
    @volatile private var answering: Answering | Null = null
    private var watcher: Future[Unit] | Null = null

    /** Whether it waits for a request, with none begun. */
    @volatile var idle = false
    @volatile private var current: Stream = raw

    /** Closes it under the request it waits for, for a server that is stopping. */
    def abort()(using Async): Unit = current.close()

    def run()(using Async): Unit = {
      state.open.add(this)
      try {
        current = tls.fold(raw)(Tls.server(raw, _, t.handshake))
        val input = new BufferedInput(current, 4096, math.max(config.limits.head, 4096))
        var open = !state.stopping.get()
        while open do open = serveOne(current, input)
      } catch {
        case _: CancellationException | _: ClosedChannelException | _: TimeoutException => ()
        case e: IOException                                                             =>
          if !state.stopping.get() then config.log(s"A connection failed: ${e.getMessage}")
      } finally {
        state.open.remove(this)
        current.close()
      }
    }

    /** Serves one request: whether the connection carries on. */
    private def serveOne(stream: Stream, input: BufferedInput)(using Async): Boolean = {
      idle = input.buffered == 0
      if idle && state.stopping.get() then return false
      val head =
        try Wire.readRequestHead(input, config.limits, t.idle, t.header, () => idle = false)
        catch {
          case e: Wire.Malformed =>
            idle = false
            respondAndClose(stream, Response.text(s"${e.getMessage}\n", e.status))
            return false
          case _: TimeoutException =>
            idle = false
            respondAndClose(stream, Response.text("Request Timeout\n", Status.RequestTimeout))
            return false
        }
      idle = false
      if head == null then return false
      val h = head.nn
      val http11 = h.version == "HTTP/1.1"
      val url =
        try requestUrl(h, stream.secure)
        catch {
          case e: Wire.Malformed =>
            respondAndClose(stream, Response.text(s"${e.getMessage}\n", e.status))
            return false
        }
      val framing =
        try Wire.framing(h.headers, Wire.Framing.NoBody)
        catch {
          case e: Wire.Malformed =>
            respondAndClose(stream, Response.text(s"${e.getMessage}\n", e.status))
            return false
        }
      if !http11 && framing == Wire.Framing.Chunked then {
        respondAndClose(stream, Response.text("Chunked encoding is HTTP/1.1's\n", Status.BadRequest))
        return false
      }
      framing match {
        case Wire.Framing.Length(n) if n > config.limits.body =>
          respondAndClose(stream, tooLarge(config.limits.body))
          return false
        case _ => ()
      }
      val expectation = h.headers.get("Expect")
      if expectation.exists(!_.equalsIgnoreCase("100-continue")) then {
        respondAndClose(
          stream,
          Response.text(s"Unsupported expectation: ${expectation.get}\n", Status.ExpectationFailed)
        )
        return false
      }
      // 100 Continue goes out only once the handler reads the body, so one that refuses it first saves the upload.
      var continued = expectation.isEmpty || !http11
      val source = new Wire.Source(
        input,
        framing,
        config.limits.body,
        t.read,
        () =>
          if !continued then {
            continued = true
            stream.write(java.nio.ByteBuffer.wrap(Wire.Continue), t.write)
          },
        limit => HttpError(Status.ContentTooLarge, s"The body is larger than $limit bytes")
      )
      val body =
        if framing == Wire.Framing.NoBody then Body.empty
        else
          new Body.Incoming(
            h.headers.get("Content-Type"),
            framing match {
              case Wire.Framing.Length(n) => Some(n)
              case _                      => None
            },
            source
          )
      val request = Request(h.method, url, h.headers, body, None, h.version)
      val keepAlive =
        if h.headers.hasToken("Connection", "close") then false
        else if http11 then true
        else h.headers.hasToken("Connection", "keep-alive")

      val response =
        try answer(request, body, input)
        catch {
          case e: CancellationException => throw e
          case e: HttpError             => e.response
          case e: Exception             =>
            config.log(s"The handler of ${h.method} ${url.rawPath} failed: $e")
            Response.text("Internal Server Error\n", Status.InternalServerError)
        }
      response.body match {
        case upgrade: Body.Upgrade =>
          stream.write(responseHead(response, h, keepAlive = true, chunked = false), t.write)
          upgrade.run(stream, input)
          false
        case _ =>
          val bodyDone = body match {
            case in: Body.Incoming => in.complete
            case _                 => true
          }
          // A body the handler left unread is drained, if small, so the connection can carry the next request.
          val reusable =
            keepAlive && !state.stopping.get() && (bodyDone || (body match {
              case in: Body.Incoming =>
                try in.drain(64 * 1024)
                catch case Stream.Failed() => false
              case _ => true
            }))
          val carriesOn = writeResponse(stream, response, h, reusable)
          if !carriesOn && !bodyDone && !body.isInstanceOf[Body.Bytes] then linger(stream)
          carriesOn
      }
    }

    /** Answers `request` with the routes, in a group of its own that the watch cancels if the client goes away. */
    private def answer(request: Request, body: Body, input: BufferedInput)(using outer: Async): Response =
      config.cancelAbandoned match {
        case after: FiniteDuration =>
          Async.group {
            val a = new Answering(Async.current.group, System.nanoTime(), body)
            answering = a
            if watching.compareAndSet(false, true) then
              watcher = Future(watch(after.toNanos, input))(using spawn, spawn)
            try routes(request)
            finally settle(a)(using outer)
          }
        case _ => routes(request)
      }

    /** Ends the watch of `a`: once this returns the watch reads nothing more of the connection, so that the response
      * and the next request have it to themselves.
      */
    private def settle(a: Answering)(using Async): Unit = {
      answering = null
      if !a.state.compareAndSet(Answering.Running, Answering.Settled) && a.state.get() == Answering.Reading then {
        val w = watcher.nn
        w.cancel() // a read cancelled took nothing (`Io.perform`)
        w.awaitResult: Unit
      }
    }

    /** Watches the requests being answered, each from `after` nanoseconds after it began: reads the connection once its
      * body is read, and cancels the handler if the stream ended or failed. Bytes that arrive are the next request,
      * which stay buffered for it. Ends when no request is being answered.
      */
    private def watch(after: Long, input: BufferedInput)(using Async): Unit =
      try {
        var going = true
        while going do {
          val a = answering
          if a == null then {
            watching.set(false)
            going = answering != null && watching.compareAndSet(false, true) // a request that came as it stopped
          } else {
            val left = a.since + after - System.nanoTime()
            if left > 0 then gears.async.AsyncOperations.sleep((left + 999_999) / 1_000_000)
            else if a.state.get() == Answering.Running && a.bodyRead && input.buffered == 0 &&
              a.state.compareAndSet(Answering.Running, Answering.Reading)
            then {
              val gone =
                try !input.fill() && input.exhausted
                catch case _: IOException => true
              if gone then a.group.cancel()
              a.state.set(Answering.Settled)
            } else
              gears.async.AsyncOperations.sleep(
                math.max(after / 1_000_000, 1L)
              ) // its body is not read yet, or it was watched
          }
        }
      } catch {
        case e: CancellationException =>
          watching.set(false) // `settle` waits for this, or the server is stopping
          throw e
      }

    private def tooLarge(limit: Long): Response =
      Response.text(s"The body is larger than $limit bytes\n", Status.ContentTooLarge)

    /** The URL a request names: its target's path and query, on the host its `Host` header names; a `Wire.Malformed` if
      * either is malformed.
      */
    private def requestUrl(h: Wire.RequestHead, secure: Boolean): Url = {
      def malformed(what: String) = new Wire.Malformed(Status.BadRequest, what)
      val hosts = h.headers.getAll("Host")
      if hosts.length > 1 then throw malformed("More than one Host header")
      if hosts.isEmpty && h.version == "HTTP/1.1" then throw malformed("No Host header")
      val host = hosts.headOption.filter(_.nonEmpty).getOrElse("localhost")
      if host.exists(c => c == '/' || c == '?' || c == '#' || c == '@' || c <= ' ') then
        throw malformed(s"A malformed Host header: $host")
      val scheme = if secure then "https" else "http"
      if h.target.startsWith("/") then Url.ofRequest(scheme, host, h.target)
      else if h.target == "*" && h.method == Method.Options then Url.ofRequest(scheme, host, "/")
      else if h.target.startsWith("http://") || h.target.startsWith("https://") then
        try Url.parse(h.target)
        catch case e: IllegalArgumentException => throw malformed(e.getMessage)
      else throw malformed(s"A malformed request target: ${h.target}")
    }

    /** Answers with `response` and ends the connection, lingering (`linger`). */
    private def respondAndClose(stream: Stream, response: Response)(using Async): Unit =
      try {
        val h = Wire.RequestHead(Method.Get, "/", "HTTP/1.1", Headers.empty)
        writeResponse(stream, response, h, reusable = false)
        linger(stream)
      } catch case _: IOException | _: TimeoutException => ()

    /** Ends the connection gracefully, with request bytes perhaps unread: shuts it down for writing, then reads and
      * drops what the client still sends, for at most a second and a megabyte. Closing at once with bytes unread would
      * reset the connection, and a reset can discard the response the client has not read yet, as an upload refused
      * with a 413 would be.
      */
    private def linger(stream: Stream)(using Async): Unit =
      try {
        stream.shutdownOutput()
        val scratch = java.nio.ByteBuffer.allocate(16384)
        val deadline = System.nanoTime() + 1_000_000_000L
        var dropped = 0L
        var open = true
        while open && dropped < (1 << 20) do {
          val left = deadline - System.nanoTime()
          if left <= 0 then open = false
          else {
            scratch.clear()
            val n = stream.read(scratch, Duration.fromNanos(left))
            if n < 0 then open = false else dropped += n
          }
        }
      } catch case _: IOException | _: TimeoutException => ()

    /** Writes `response`: framed by its length if known, else chunked for HTTP/1.1, else up to the close. Whether the
      * connection carries on: false if a failure cut the body short, or it was framed by the close.
      */
    private def writeResponse(stream: Stream, handed: Response, h: Wire.RequestHead, reusable: Boolean)(using
        Async
    ): Boolean = {
      var response = handed
      val noBody = response.status.forbidsBody
      val head = h.method == Method.Head
      // A handler's own Content-Length on a body of unknown length is held to as the body's is, so that a body longer
      // or shorter than it says cannot leave a kept-alive connection out of step.
      val length = response.body.length.orElse(response.headers.getAll("Content-Length") match {
        case Seq(text) if text.trim.nonEmpty && text.trim.length <= 18 && text.trim.forall(c => c >= '0' && c <= '9') =>
          Some(text.trim.toLong)
        case Seq() => None
        case other =>
          config.log(s"The response to ${h.method} ${h.target} had a malformed Content-Length, dropped: $other")
          response = response.copy(headers = response.headers.remove("Content-Length"))
          None
      })
      val chunked = !noBody && length.isEmpty && h.version == "HTTP/1.1"
      val untilClose = !noBody && length.isEmpty && !chunked
      val keep = reusable && !untilClose
      val out = responseHead(response, h, keep, chunked)
      val writer = new Wire.Out(stream, out, chunked, length.getOrElse(-1L), noBody || head, t.write)
      try {
        if !noBody && !head then response.body.writeTo(writer)
        writer.finish()
        if length.exists(_ != writer.bytesWritten) && !noBody && !head then false // cut short: the client must know
        else keep
      } catch {
        case e: CancellationException => throw e
        case e: IOException           => throw e
        case e: TimeoutException      => throw e
        case e: Exception             =>
          // The head may be out: the only honest signal left is closing the connection mid-body.
          config.log(s"The body of the response to ${h.method} ${h.target} failed: $e")
          if !writer.committed then {
            writeResponse(stream, Response.text("Internal Server Error\n", Status.InternalServerError), h, false)
          }
          false
      }
    }

    /** The head of `response`, its framing headers set, to go out with the body's first write. */
    private def responseHead(
        response: Response,
        h: Wire.RequestHead,
        keepAlive: Boolean,
        chunked: Boolean
    ): java.nio.ByteBuffer = {
      var headers = response.headers.remove("Transfer-Encoding")
      if !headers.contains("Date") then headers = headers.add("Date", Wire.date())
      response.body match {
        case _: Body.Upgrade => ()
        case body            =>
          if response.status.forbidsBody then headers = headers.remove("Content-Length")
          else if chunked then headers = headers.remove("Content-Length").add("Transfer-Encoding", "chunked")
          else body.length.foreach(n => headers = headers.set("Content-Length", n.toString))
          if !keepAlive then headers = headers.set("Connection", "close")
          else if h.version == "HTTP/1.0" then headers = headers.set("Connection", "keep-alive")
      }
      Wire.head(s"HTTP/1.1 ${response.status.code} ${response.status.reason}", headers)
    }
  }
}
