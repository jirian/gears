package gears.io.http

import gears.async.Async
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

/** The shortest ways in, with the platform's default backend, the reference backend on the JVM and io_uring on Scala
  * Native:
  * {{{
  * import gears.io.http.*
  *
  * @main def hello(): Unit =
  *   Http.serve(8080)(Routes(GET("/") { _ => "Hello from gears!" }))
  *
  * @main def fetch(): Unit =
  *   Http.run { println(Http.get("https://example.com").ensureSuccess().text()) }
  * }}}
  * Each run of these gives its program, or its routes, an `HttpClient` as a context value, closed as the run returns,
  * which the module's functions (`get`, `send`, ...) send with: where there is none, as in a run of `Io.run` or
  * `Defaults.run`, each request has a client of its own, which keeps no connection. A program that already runs with an
  * `Io` uses `Http.server`, which serves in the caller's scope, and makes an `HttpClient` to send many requests with.
  */
object Http {

  /** Serves `routes` on `port`, every interface, until the process is asked to end (see the other `serve`). */
  def serve(port: Int)(routes: HttpClient ?=> Routes): Unit = serve(ServerConfig(port = port))(routes)

  /** Serves `routes` as `config` says until the process is sent SIGTERM, as a container's runtime stops it, or SIGINT,
    * as Ctrl-C does: then it shuts the server down gracefully (`Server.shutdown`), the requests under way given up to
    * `grace` to finish, and returns. A second signal ends the process at once. The routes are made in the run, with its
    * client, which their handlers send with.
    */
  def serve(config: ServerConfig, grace: FiniteDuration = 10.seconds)(routes: HttpClient ?=> Routes): Unit =
    run {
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
  def testServer[A](routes: HttpClient ?=> Routes, config: ServerConfig = ServerConfig())(
      body: Server => (Io[DefaultCapabilities], Async.Spawn, HttpClient) ?=> A
  ): A =
    run {
      Async.group {
        val running = server(routes, config.copy(host = "127.0.0.1", port = 0))
        body(running)
      }
    }

  /** Runs `program` with an `Io` over the platform's default backend, and an `HttpClient` for the module's functions,
    * and returns what it returns: for a script, or a `main`. The client is closed as it returns.
    */
  def run[T](program: Io[DefaultCapabilities] ?=> Async.Spawn ?=> HttpClient ?=> T): T =
    Defaults.run { io ?=> spawn ?=>
      val client = new HttpClient()
      try program(using io)(using spawn)(using client)
      finally client.close()
    }

  /** Runs `body` with the context's client, or with one of its own, closed after, where there is none. */
  private def withClient[A](body: HttpClient => A)(using io: Io[IpStreams & Resolving], async: Async)(using
      client: HttpClient | Null
  ): A =
    if client != null then body(client) else new HttpClient().use(body)

  def send(request: Request)(using Io[IpStreams & Resolving], Async)(using client: HttpClient | Null = null): Response =
    withClient(_.send(request))
  def get(url: String | Url, headers: (String, String)*)(using Io[IpStreams & Resolving], Async)(using
      client: HttpClient | Null = null
  ): Response = send(Request.get(url).withHeaders(headers*))
  def post(url: String | Url, body: Body)(using Io[IpStreams & Resolving], Async)(using
      client: HttpClient | Null = null
  ): Response = send(Request.post(url, body))
  def put(url: String | Url, body: Body)(using Io[IpStreams & Resolving], Async)(using
      client: HttpClient | Null = null
  ): Response = send(Request.put(url, body))
  def delete(url: String | Url)(using Io[IpStreams & Resolving], Async)(using
      client: HttpClient | Null = null
  ): Response = send(Request.delete(url))

  /** Sends `request` and runs `use` with its response as it arrives (see `HttpClient.stream`). */
  def stream[A](request: Request)(use: Response => Async ?=> A)(using Io[IpStreams & Resolving], Async)(using
      client: HttpClient | Null = null
  ): A = withClient(_.stream(request)(use))
}
