package gears.io.http

import gears.async.Async
import gears.async.AsyncOperations
import gears.async.Future
import gears.async.io.Io
import gears.async.io.IpStreams
import gears.async.io.Resolving
import gears.io.net.DefaultCapabilities
import gears.io.net.Defaults
import gears.io.net.Platform

import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import scala.util.Success

/** The shortest ways in, with the platform's default backend, NIO on the JVM and io_uring on Scala Native:
  * {{{
  * import gears.io.http.*
  *
  * @main def hello(): Unit =
  *   Http.serve(8080)(Routes(GET("/") { _ => "Hello from gears!" }))
  *
  * @main def fetch(): Unit =
  *   Http.run { println(Http.get("https://example.com").ensureSuccess().text()) }
  * }}}
  * A program that already runs with an `Io` uses `Http.server`, which serves in the caller's scope, and an
  * `HttpClient`, or the module's functions, which share one client per `Io`.
  */
object Http {

  /** Serves `routes` on `port`, every interface, until the process is asked to end (see the other `serve`). */
  def serve(port: Int)(routes: Routes): Unit = serve(ServerConfig(port = port))(routes)

  /** Serves `routes` as `config` says until the process is sent SIGTERM, as a container's runtime stops it, or SIGINT,
    * as Ctrl-C does: then it shuts the server down gracefully (`Server.shutdown`), the requests under way given up to
    * `grace` to finish, and returns. A second signal ends the process at once.
    */
  def serve(config: ServerConfig, grace: FiniteDuration = 10.seconds)(routes: Routes): Unit =
    Defaults.run {
      Async.group {
        val running = server(routes, config)
        val asked = Future.Promise[Unit]()
        val finished = Platform.onTermination(() => asked.complete(Success(())))
        try {
          Future {
            asked.asFuture.await
            running.shutdown(grace)
          }: Unit
          running.await()
        } finally finished()
      }
    }

  /** Starts serving `routes` as `config` says, in the caller's scope, and returns the running server: it stops when the
    * scope ends, abruptly, as any future of the scope does, or gracefully with `shutdown`.
    */
  def server(routes: Routes, config: ServerConfig = ServerConfig())(using
      io: Io[DefaultCapabilities],
      spawn: Async.Spawn
  ): Server = Server.start(routes, config)

  /** Runs `body` with `routes` served on a port of their own, on this host only, and returns what it returns, the
    * server stopped: for tests, as Go's `httptest.NewServer`. `config` is the server's but for its host and port;
    * `body` reaches the server at `server.url`, with the module's functions or a client of its own. It starts a run of
    * its own (`run`), so it is not for use inside one.
    */
  def testServer[A](routes: Routes, config: ServerConfig = ServerConfig())(
      body: Server => (Io[DefaultCapabilities], Async.Spawn, AsyncOperations) ?=> A
  ): A =
    run {
      Async.group {
        val running = server(routes, config.copy(host = "127.0.0.1", port = 0))
        body(running)
      }
    }

  /** Runs `program` with an `Io` over the platform's default backend, and returns what it returns: for a script, or a
    * `main`. The client the module's functions share is closed as it returns.
    */
  def run[T](program: Io[DefaultCapabilities] ?=> (Async.Spawn, AsyncOperations) ?=> T): T =
    Defaults.run {
      val io = summon[Io[DefaultCapabilities]]
      try program
      finally
        clients.synchronized(Option(clients.remove(io))).foreach(_.close())
    }

  private val clients = new java.util.WeakHashMap[Io[?], HttpClient]()

  /** The client the module's functions share on `io`: made the first time, with the default configuration. */
  def client(using io: Io[IpStreams & Resolving]): HttpClient =
    clients.synchronized {
      var c = clients.get(io)
      if c == null then {
        c = new HttpClient()
        clients.put(io, c)
      }
      c
    }

  def send(request: Request)(using Io[IpStreams & Resolving], Async): Response = client.send(request)
  def get(url: String | Url, headers: (String, String)*)(using Io[IpStreams & Resolving], Async): Response =
    client.send(Request.get(url).withHeaders(headers*))
  def post(url: String | Url, body: Body)(using Io[IpStreams & Resolving], Async): Response =
    client.send(Request.post(url, body))
  def put(url: String | Url, body: Body)(using Io[IpStreams & Resolving], Async): Response =
    client.send(Request.put(url, body))
  def delete(url: String | Url)(using Io[IpStreams & Resolving], Async): Response = client.send(Request.delete(url))

  /** Sends `request` and runs `use` with its response as it arrives (see `HttpClient.stream`). */
  def stream[A](request: Request)(use: Response => Async ?=> A)(using Io[IpStreams & Resolving], Async): A =
    client.stream(request)(use)
}
