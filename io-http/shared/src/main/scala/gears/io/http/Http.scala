package gears.io.http

import gears.async.Async
import gears.async.Future
import gears.async.io.Io
import gears.async.io.IpStreams
import gears.async.io.Resolving
import gears.io.net.DefaultCapabilities
import gears.io.net.DefaultPlatform
import gears.io.net.Defaults

import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import scala.util.Success

object Http {

  def serve(port: Int)(routes: HttpClient ?=> Routes): Unit = serve(ServerConfig(port = port))(routes)

  def serve(config: ServerConfig, grace: FiniteDuration = 10.seconds)(routes: HttpClient ?=> Routes): Unit =
    run {
      Async.group {
        val running = server(routes, config)
        val asked = Future.Promise[Unit]()
        val finished = DefaultPlatform.onTermination(() => asked.complete(Success(())))
        try {
          Future {
            asked.asFuture.await
            running.shutdown(grace)
          }: Unit
          running.await()
        } finally finished()
      }
    }

  def server(routes: Routes, config: ServerConfig = ServerConfig())(using
      io: Io[DefaultCapabilities],
      spawn: Async.Spawn
  ): Server = Server.start(routes, config)

  def testServer[A](routes: HttpClient ?=> Routes, config: ServerConfig = ServerConfig())(
      body: Server => (Io[DefaultCapabilities], Async.Spawn, HttpClient) ?=> A
  ): A =
    run {
      Async.group {
        val running = server(routes, config.copy(host = "127.0.0.1", port = 0))
        body(running)
      }
    }

  def run[T](program: Io[DefaultCapabilities] ?=> Async.Spawn ?=> HttpClient ?=> T): T =
    Defaults.run { io ?=> spawn ?=>
      val client = new HttpClient()
      try program(using io)(using spawn)(using client)
      finally client.close()
    }

  private def withClient[A](body: HttpClient => A)(using
      io: Io[IpStreams & Resolving],
      async: Async
  )(using
      client: HttpClient | Null
  ): A =
    if client != null then body(client) else new HttpClient().use(body)

  def send(request: Request)(using Io[IpStreams & Resolving], Async)(using client: HttpClient | Null = null): Response =
    withClient(_.send(request))
  def get(url: String | Url, headers: (String, String)*)(using
      Io[IpStreams & Resolving],
      Async
  )(using
      client: HttpClient | Null = null
  ): Response = send(Request.get(url).withHeaders(headers*))
  def post(url: String | Url, body: Body)(using
      Io[IpStreams & Resolving],
      Async
  )(using
      client: HttpClient | Null = null
  ): Response = send(Request.post(url, body))
  def put(url: String | Url, body: Body)(using
      Io[IpStreams & Resolving],
      Async
  )(using
      client: HttpClient | Null = null
  ): Response = send(Request.put(url, body))
  def delete(url: String | Url)(using
      Io[IpStreams & Resolving],
      Async
  )(using
      client: HttpClient | Null = null
  ): Response = send(Request.delete(url))

  def stream[A](request: Request)(use: Response => Async ?=> A)(using
      Io[IpStreams & Resolving],
      Async
  )(using
      client: HttpClient | Null = null
  ): A = withClient(_.stream(request)(use))
}
