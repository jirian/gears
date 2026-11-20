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

final case class ServerTimeouts(
    header: Duration = 10.seconds,
    read: Duration = 30.seconds,
    write: Duration = 30.seconds,
    idle: Duration = 60.seconds,
    handshake: Duration = 10.seconds
)

final case class Limits(
    head: Int = 16 * 1024,
    headers: Int = 100,
    body: Long = 10L * 1024 * 1024,
    connections: Int = 10_000
) {
  require(head >= 1024 && headers > 0 && body >= 0 && connections > 0, s"Limits out of range: $this")
}

final case class ServerConfig(
    host: String = "0.0.0.0",
    port: Int = 8080,
    timeouts: ServerTimeouts = ServerTimeouts(),
    limits: Limits = Limits(),
    tls: Option[TlsServerConfig] = None,
    cancelAbandoned: Duration = 100.millis,
    log: String => Unit = message => System.err.println(s"[gears-http] $message")
)

final class Server private[http] (config: ServerConfig, listener: Listener, done: Future[Unit], state: Server.State) {

  val port: Int = Tcp.port(listener.localAddress)

  def connections: Int = state.connections.get()

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

  def shutdown(grace: FiniteDuration = 10.seconds)(using Async): Unit = {
    state.stopping.set(true)
    listener.close()
    state.acceptEnded()
    state.closeIdle()
    gears.async.withTimeoutOption(grace)(state.drained.await)(using state.async, summon[Async]): Unit
    done.cancel()
    done.awaitResult: Unit
  }

  def await()(using Async): Unit = done.awaitResult: Unit
}

private[http] object Server {

  inline val PoolBufferSize = 4096
  inline val PoolBuffers = 256

  final class State(val async: gears.async.AsyncOperations, val pool: gears.async.io.BufferPool) {
    val stopping = new AtomicBoolean(false)
    val connections = new AtomicInteger(0)
    val open = java.util.concurrent.ConcurrentHashMap.newKeySet[Connection]()

    private val accepting = new AtomicBoolean(true)
    private val noneLeft = Future.Promise[Unit]()

    @volatile private var tick = Future.Promise[Unit]()

    def nextTick: Future[Unit] = tick.asFuture

    def ticked(): Unit = {
      val t = tick
      tick = Future.Promise[Unit]()
      t.complete(Success(()))
    }

    def drained: Future[Unit] = noneLeft.asFuture

    def admit(limit: Int): Boolean =
      if connections.incrementAndGet() > limit || stopping.get() then {
        ended()
        false
      } else true

    def ended(): Unit = if connections.decrementAndGet() == 0 && !accepting.get() then noneLeft.complete(Success(()))

    def acceptEnded(): Unit = {
      accepting.set(false)
      if connections.get() == 0 then noneLeft.complete(Success(()))
    }

    def closeIdle()(using Async): Unit = open.forEach(c => if c.idle then c.abort())
  }

  def start(routes: Routes, config: ServerConfig)(using
      io: Io[gears.io.net.DefaultCapabilities],
      spawn: Async.Spawn
  ): Server = {
    val tlsContext = config.tls.map(Tls.serverContext)
    val listener = Tcp.listen(config.host, config.port)
    val state = new State(io, io.backend.pool(PoolBufferSize, PoolBuffers))
    val done = Future {
      try
        Async.group {
          val connectionsGroup = summon[Async.Spawn]
          config.cancelAbandoned match {
            case after: FiniteDuration =>
              val period = math.max(after.toMillis / 2, 1L)
              Future {
                while true do {
                  gears.async.AsyncOperations.sleep(period)
                  state.ticked()
                  val now = System.nanoTime()
                  state.open.forEach(_.watchIfDue(after.toNanos, now))
                }
              }: Unit
            case _ => ()
          }
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
          state.acceptEnded()
          state.drained.await
        }
      finally {
        listener.close()
        try state.pool.close()
        catch case e: IllegalArgumentException => config.log(s"The read pool did not close: ${e.getMessage}")
      }
    }
    new Server(config, listener, done, state)
  }

  final class Answering(val group: gears.async.CompletionGroup, val since: Long, body: Body) {
    val state = new AtomicInteger(Answering.Running)

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

    private val out = new Wire.OutBuffer(16 * 1024)

    private val watching = new AtomicBoolean(false)
    @volatile private var answering: Answering | Null = null
    @volatile private var watcher: Future[Unit] | Null = null
    @volatile private var input: BufferedInput | Null = null

    @volatile var idle = false
    @volatile private var current: Stream = raw

    def abort()(using Async): Unit = current.close()

    def run()(using Async): Unit = {
      state.open.add(this)
      try {
        current = tls.fold(raw)(Tls.server(raw, _, t.handshake))
        val input = new BufferedInput(current, 4096, math.max(config.limits.head, 4096))
        this.input = input
        var open = !state.stopping.get()
        Async.group {
          while open do open = serveOne(current, input)
        }
      } catch {
        case _: CancellationException | _: ClosedChannelException | _: TimeoutException => ()
        case e: IOException                                                             =>
          if !state.stopping.get() then config.log(s"A connection failed: ${e.getMessage}")
      } finally {
        state.open.remove(this)
        current.close()
      }
    }

    private def serveOne(stream: Stream, input: BufferedInput)(using Async): Boolean = {
      idle = input.buffered == 0
      if idle && state.stopping.get() then return false
      val head =
        try Wire.readRequestHead(input, config.limits, t.idle, t.header, () => idle = false, state.pool, out.release)
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
      val http11 = h.version == HttpVersion.Http11
      val (url, framing) =
        try (requestUrl(h, stream.secure), checkedFraming(h))
        catch {
          case e: Wire.Malformed =>
            respondAndClose(stream, Response.text(s"${e.getMessage}\n", e.status))
            return false
        }
      var continued = !h.headers.contains("Expect") || !http11
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
      val body = Body.incoming(h.headers, framing, source)
      val request = Request(h.method, url, h.headers, body, None, h.version)
      val keepAlive =
        if h.headers.hasToken("Connection", "close") then false
        else if http11 then true
        else h.headers.hasToken("Connection", "keep-alive")

      val response =
        try answer(request, body)
        catch {
          case e: CancellationException               => throw e
          case e: HttpError                           => e.response
          case _: Exception if source.failure != null => bodyFailed(source.failure.nn)
          case e: Exception                           =>
            config.log(s"The handler of ${h.method} ${url.rawPath} failed: $e")
            Response.text("Internal Server Error\n", Status.InternalServerError)
        }
      response.body match {
        case upgrade: Body.Upgrade =>
          responseHead(response, h, keepAlive = true, chunked = false)
          out.writeTo(stream, t.write)
          upgrade.run(stream, input)
          false
        case _ =>
          val bodyDone = body match {
            case in: Body.Incoming => in.complete
            case _                 => true
          }
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

    private def bodyFailed(failure: Throwable): Response = failure match {
      case e: HttpError        => e.response
      case e: Wire.Malformed   => Response.text(s"${e.getMessage}\n", e.status)
      case _: TimeoutException => Response.text("Request Timeout\n", Status.RequestTimeout)
      case _                   => Response.text("Bad Request\n", Status.BadRequest)
    }

    private def answer(request: Request, body: Body)(using outer: Async): Response =
      config.cancelAbandoned match {
        case after: FiniteDuration =>
          val a = new Answering(outer.group, System.nanoTime(), body)
          answering = a
          try routes(request)
          finally settle(a)
        case _ => routes(request)
      }

    private def settle(a: Answering)(using Async): Unit = {
      answering = null
      if !a.state.compareAndSet(Answering.Running, Answering.Settled) && a.state.get() == Answering.Reading then {
        val w = watcher
        if w != null then {
          w.cancel()
          w.awaitResult: Unit
        }
      }
    }

    def watchIfDue(after: Long, now: Long): Unit = {
      val a = answering
      val in = input
      if a != null && in != null && now - a.since >= after && watching.compareAndSet(false, true) then
        watcher = Future(watch(after, in.nn))(using spawn, spawn)
    }

    private def watch(after: Long, input: BufferedInput)(using Async): Unit =
      try {
        var going = true
        while going do {
          val a = answering
          if a == null then {
            watcher = null
            watching.set(false)
            going = false
          } else {
            val left = a.since + after - System.nanoTime()
            if left > 0 then state.nextTick.await
            else if watcher != null && a.state.get() == Answering.Running && a.bodyRead && input.buffered == 0 &&
              a.state.compareAndSet(Answering.Running, Answering.Reading)
            then {
              val gone =
                try !input.fill() && input.exhausted
                catch case _: IOException => true
              if gone then a.group.cancel()
              a.state.set(Answering.Settled)
            } else state.nextTick.await
          }
        }
      } catch {
        case e: CancellationException =>
          watcher = null
          watching.set(false)
          throw e
      }

    private def checkedFraming(h: Wire.RequestHead): Wire.Framing = {
      val framing = Wire.framing(h.headers, Wire.Framing.NoBody)
      if h.version != HttpVersion.Http11 && framing == Wire.Framing.Chunked then
        throw new Wire.Malformed(Status.BadRequest, "Chunked encoding is HTTP/1.1's")
      framing match {
        case Wire.Framing.Length(n) if n > config.limits.body =>
          throw new Wire.Malformed(Status.ContentTooLarge, s"The body is larger than ${config.limits.body} bytes")
        case _ => ()
      }
      val expectation = h.headers.get("Expect")
      if expectation.exists(!_.equalsIgnoreCase("100-continue")) then
        throw new Wire.Malformed(Status.ExpectationFailed, s"Unsupported expectation: ${expectation.get}")
      framing
    }

    private def requestUrl(h: Wire.RequestHead, secure: Boolean): Url = {
      def malformed(what: String) = new Wire.Malformed(Status.BadRequest, what)
      val hosts = h.headers.getAll("Host")
      if hosts.length > 1 then throw malformed("More than one Host header")
      if hosts.isEmpty && h.version == HttpVersion.Http11 then throw malformed("No Host header")
      val host = hosts.headOption.filter(_.nonEmpty).getOrElse("localhost")
      var i = 0
      while i < host.length && { val c = host.charAt(i); c != '/' && c != '?' && c != '#' && c != '@' && c > ' ' } do
        i += 1
      if i < host.length then throw malformed(s"A malformed Host header: $host")
      val scheme = if secure then "https" else "http"
      if h.target.startsWith("/") then Url.ofRequest(scheme, host, h.target)
      else if h.target == "*" && h.method == Method.Options then Url.ofRequest(scheme, host, "/")
      else if h.target.startsWith("http://") || h.target.startsWith("https://") then
        try Url.parse(h.target)
        catch case e: IllegalArgumentException => throw malformed(e.getMessage)
      else throw malformed(s"A malformed request target: ${h.target}")
    }

    private def respondAndClose(stream: Stream, response: Response)(using Async): Unit =
      try {
        val h = Wire.RequestHead(Method.Get, "/", HttpVersion.Http11, Headers.empty)
        writeResponse(stream, response, h, reusable = false)
        linger(stream)
      } catch case _: IOException | _: TimeoutException => ()

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

    private def writeResponse(stream: Stream, handed: Response, h: Wire.RequestHead, reusable: Boolean)(using
        Async
    ): Boolean = {
      var response = handed
      val noBody = response.status.forbidsBody
      val head = h.method == Method.Head
      val length = response.body.length.orElse(response.headers.getAll("Content-Length") match {
        case Seq(text) if text.trim.nonEmpty && text.trim.length <= 18 && text.trim.forall(c => c >= '0' && c <= '9') =>
          Some(text.trim.toLong)
        case Seq() => None
        case other =>
          config.log(s"The response to ${h.method} ${h.target} had a malformed Content-Length, dropped: $other")
          response = response.copy(headers = response.headers.remove("Content-Length"))
          None
      })
      val chunked = !noBody && length.isEmpty && h.version == HttpVersion.Http11
      val untilClose = !noBody && length.isEmpty && !chunked
      val keep = reusable && !untilClose
      responseHead(response, h, keep, chunked)
      val writer = new Wire.Out(stream, out, chunked, length.getOrElse(-1L), noBody || head, t.write)
      try {
        if !noBody && !head then response.body.writeTo(writer)
        writer.finish()
        if length.exists(_ != writer.bytesWritten) && !noBody && !head then false
        else keep
      } catch {
        case e: CancellationException => throw e
        case e: IOException           => throw e
        case e: TimeoutException      => throw e
        case e: Exception             =>
          config.log(s"The body of the response to ${h.method} ${h.target} failed: $e")
          if !writer.committed then {
            writeResponse(stream, Response.text("Internal Server Error\n", Status.InternalServerError), h, false)
          }
          false
      }
    }

    private def responseHead(response: Response, h: Wire.RequestHead, keepAlive: Boolean, chunked: Boolean): Unit = {
      out.reset()
      val upgrade = response.body.isInstanceOf[Body.Upgrade]
      val forbids = response.status.forbidsBody
      val length = response.body.length
      val ownLength = !upgrade && (forbids || chunked || length.isDefined)
      val ownConnection = !upgrade && (!keepAlive || h.version == HttpVersion.Http10)
      out.putLatin1("HTTP/1.1 ")
      out.putDecimal(response.status.code)
      out.putLatin1(" ")
      out.putLatin1(response.status.reason)
      out.putCrlf()
      var dated = false
      val fields = response.headers
      var i = 0
      while i < fields.size do {
        val name = fields.name(i)
        val dropped = Text.sameName(name, "Transfer-Encoding") ||
          ownLength && Text.sameName(name, "Content-Length") ||
          ownConnection && Text.sameName(name, "Connection")
        if !dropped then {
          if !dated && Text.sameName(name, "Date") then dated = true
          out.putField(name, fields.value(i))
        }
        i += 1
      }
      if !dated then out.putField("Date", Wire.date())
      if !upgrade then {
        if !forbids then
          if chunked then out.putField("Transfer-Encoding", "chunked")
          else if length.isDefined then {
            out.putLatin1("Content-Length: ")
            out.putDecimal(length.get)
            out.putCrlf()
          }
        if !keepAlive then out.putField("Connection", "close")
        else if h.version == HttpVersion.Http10 then out.putField("Connection", "keep-alive")
      }
      out.putCrlf()
    }
  }
}
