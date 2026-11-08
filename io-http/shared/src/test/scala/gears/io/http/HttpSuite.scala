package gears.io.http

import gears.async.Async
import gears.async.AsyncOperations
import gears.async.Future
import gears.async.io.Io
import gears.io.net.DefaultCapabilities
import gears.io.net.Tcp
import gears.io.tls.TlsClientConfig
import gears.io.tls.TlsServerConfig

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeoutException
import scala.concurrent.duration.DurationInt

import munit.FunSuite

/** The server and the client, over loopback, on the platform's default backend. */
class HttpSuite extends FunSuite {
  override val munitTimeout = scala.concurrent.duration.Duration(60, "s")

  private val quiet = ServerConfig(host = "127.0.0.1", port = 0, log = _ => ())

  /** Runs `body` with a server of `routes` on a port of its own, and the client the module shares. */
  private def withServer[A](routes: Routes, config: ServerConfig = quiet)(
      body: Server => (Io[DefaultCapabilities], Async.Spawn) ?=> A
  ): A = Http.testServer(routes, config)(body)

  /** Sends `raw` on a new connection and returns all the server answers until it closes, or `wait` passes. */
  private def rawExchange(server: Server, raw: String, wait: scala.concurrent.duration.FiniteDuration = 5.seconds)(using
      Io[DefaultCapabilities],
      Async
  ): String = {
    val stream = Tcp.connect("127.0.0.1", server.port)
    try {
      stream.write(ByteBuffer.wrap(raw.getBytes(StandardCharsets.ISO_8859_1)))
      val out = new java.io.ByteArrayOutputStream()
      val buf = ByteBuffer.allocate(4096)
      var open = true
      while open do {
        buf.clear()
        val n =
          try stream.read(buf, wait)
          catch case _: TimeoutException | _: IOException => -1
        if n < 0 then open = false else out.write(buf.array(), 0, n)
      }
      new String(out.toByteArray, StandardCharsets.ISO_8859_1)
    } finally stream.close()
  }

  test("hello world, and the response's framing headers") {
    withServer(Routes(GET("/") { _ => "Hello from gears!" })) { server =>
      val res = Http.get(server.url.resolve("/"))
      assertEquals(res.status, Status.Ok)
      assertEquals(res.text(), "Hello from gears!")
      assertEquals(res.header("Content-Length"), Some("17"))
      assert(res.header("Date").isDefined)
    }
  }

  test("a kept-alive connection carries request after request, POST too") {
    withServer(Routes(POST("/echo") { r => Response.ok(r.bytes()) }, GET("/n") { _ => "n" })) { server =>
      val client = new HttpClient()
      try {
        for i <- 1 to 5 do {
          assertEquals(client.post(server.url.resolve("/echo"), Body.text(s"body $i")).text(), s"body $i")
          assertEquals(client.get(server.url.resolve("/n")).text(), "n")
        }
        assertEquals(server.connections, 1, "one connection for all of them")
      } finally client.close()
    }
  }

  test("a streamed response is chunked, and a client streams it as it comes") {
    val routes = Routes(GET("/count") { _ =>
      Response.stream("text/plain") { out =>
        for i <- 1 to 100 do out.write(s"$i\n")
      }
    })
    withServer(routes) { server =>
      val raw = rawExchange(server, "GET /count HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
      assert(raw.contains("Transfer-Encoding: chunked"), raw)
      val lines = Http.stream(Request.get(server.url.resolve("/count"))) { res =>
        new String(res.bytes(), StandardCharsets.UTF_8).linesIterator.toList
      }
      assertEquals(lines, (1 to 100).map(_.toString).toList)
    }
  }

  test("a large upload and download pass whole, and a body over the limit is a 413") {
    val big = Array.tabulate[Byte](3 * 1024 * 1024)(i => (i * 31).toByte)
    val routes = Routes(
      POST("/sum") { r =>
        val bytes = r.bytes()
        s"${bytes.length} ${java.util.Arrays.hashCode(bytes)}"
      },
      GET("/big") { _ => Response.ok(big) }
    )
    withServer(routes, quiet.copy(limits = Limits(body = 4L * 1024 * 1024))) { server =>
      assertEquals(
        Http.post(server.url.resolve("/sum"), Body(big)).text(),
        s"${big.length} ${java.util.Arrays.hashCode(big)}"
      )
      assertEquals(
        java.util.Arrays.hashCode(Http.get(server.url.resolve("/big")).bytes()),
        java.util.Arrays.hashCode(big)
      )
      val tooBig = Array.fill[Byte](5 * 1024 * 1024)(1)
      assertEquals(Http.post(server.url.resolve("/sum"), Body(tooBig)).status, Status.ContentTooLarge)
    }
  }

  test("a head too large is a 431, a malformed one a 400, and both close the connection") {
    withServer(Routes(GET("/") { _ => "ok" }), quiet.copy(limits = Limits(head = 2048))) { server =>
      val huge = rawExchange(server, s"GET / HTTP/1.1\r\nHost: x\r\nX-Big: ${"a" * 4096}\r\n\r\n")
      assert(huge.startsWith("HTTP/1.1 431 "), huge)
      assert(huge.contains("Connection: close"), huge)
      for bad <- Seq(
          "GET / HTTP/1.1\r\nHost: x\r\nContent-Length: 1\r\nTransfer-Encoding: chunked\r\n\r\n",
          "GET / HTTP/1.1\r\nHost : x\r\n\r\n",
          "GET / HTTP/1.1\r\nHost: x\r\n folded\r\n\r\n",
          "GET / HTTP/1.1\r\nHost: x\r\nContent-Length: +5\r\n\r\n",
          "GET / HTTP/1.1\r\n\r\n"
        )
      do {
        val answer = rawExchange(server, bad)
        assert(answer.startsWith("HTTP/1.1 400 "), s"$bad -> $answer")
      }
    }
  }

  test("a client that trickles its head is cut off by the header timeout (slowloris)") {
    val config = quiet.copy(timeouts = ServerTimeouts(header = 300.millis))
    withServer(Routes(GET("/") { _ => "ok" }), config) { server =>
      val stream = Tcp.connect("127.0.0.1", server.port)
      try {
        val started = System.nanoTime()
        var closed = false
        while !closed && System.nanoTime() - started < 5_000_000_000L do {
          try {
            stream.write(ByteBuffer.wrap("G".getBytes))
            AsyncOperations.sleep(50)
            val buf = ByteBuffer.allocate(1024)
            val n = stream.read(buf, 1.millis)
            if n < 0 || new String(buf.array(), 0, n).contains("408") then closed = true
          } catch {
            case _: TimeoutException => ()
            case _: IOException      => closed = true
          }
        }
        assert(closed, "the server kept a trickling client")
        assert(System.nanoTime() - started < 3_000_000_000L)
      } finally stream.close()
    }
  }

  test("the client's time limits: a server that never answers fails the request, not the caller's patience") {
    withServer(Routes(GET("/slow") { _ =>
      AsyncOperations.sleep(10_000)
      "late"
    })) { server =>
      val client = new HttpClient(ClientConfig(timeouts = ClientTimeouts(responseHead = 200.millis)))
      val started = System.nanoTime()
      intercept[TimeoutException](client.get(server.url.resolve("/slow")))
      assert(System.nanoTime() - started < 3_000_000_000L)
      val started2 = System.nanoTime()
      intercept[TimeoutException](Http.send(Request.get(server.url.resolve("/slow")).withTimeout(200.millis)))
      assert(System.nanoTime() - started2 < 3_000_000_000L)
    }
  }

  test("redirects: followed, a 303 becoming a GET, and credentials kept from another origin") {
    Http.run {
      Async.group {
        val other = Http.server(Routes(GET("/seen") { r => r.header("Authorization").getOrElse("none") }), quiet)
        val routes = Routes(
          POST("/form") { _ => Response.redirect("/done", Status.SeeOther) },
          GET("/done") { r => s"${r.method} done" },
          GET("/away") { _ => Response.redirect(other.url.resolve("/seen").toString) },
          GET("/seen") { r => r.header("Authorization").getOrElse("none") },
          GET("/here") { _ => Response.redirect("/seen") }
        )
        val server = Http.server(routes, quiet)
        assertEquals(Http.post(server.url.resolve("/form"), Body.text("x")).text(), "GET done")
        val auth = Request.get(server.url.resolve("/away")).withHeader("Authorization", "Bearer secret")
        assertEquals(Http.send(auth).text(), "none", "dropped for another origin")
        val same = Request.get(server.url.resolve("/here")).withHeader("Authorization", "Bearer secret")
        assertEquals(Http.send(same).text(), "Bearer secret", "kept for the same one")
        val configured = HttpClient(ClientConfig(headers = Headers("Authorization" -> "Bearer configured")))
        try {
          assertEquals(configured.send(Request.get(server.url.resolve("/away"))).text(), "none", "a client's own too")
          assertEquals(configured.send(Request.get(server.url.resolve("/here"))).text(), "Bearer configured")
        } finally configured.close()
      }
    }
  }

  test("a method that is not a token is refused before anything is sent") {
    Http.run {
      Async.group {
        val server = Http.server(Routes(GET("/") { _ => "ok" }), quiet)
        for name <- Seq("GET / HTTP/1.1\r\nHost: evil\r\n\r\nGET", "A B", "") do
          intercept[IllegalArgumentException](Http.send(Request(Method.Other(name), server.url.resolve("/").toString)))
        assertEquals(Http.get(server.url.resolve("/")).text(), "ok", "and the client goes on")
      }
    }
  }

  test("a handler's failure is a 500 and the server goes on; ensureSuccess turns it into an exception") {
    withServer(Routes(GET("/boom") { _ => throw new RuntimeException("boom") }, GET("/ok") { _ => "ok" })) { server =>
      val res = Http.get(server.url.resolve("/boom"))
      assertEquals(res.status, Status.InternalServerError)
      val e = intercept[HttpStatusException](res.ensureSuccess())
      assertEquals(e.status, Status.InternalServerError)
      assertEquals(Http.get(server.url.resolve("/ok")).text(), "ok")
    }
  }

  test("HTTPS: verified against a trusted certificate, refused without it") {
    val tls = TlsServerConfig(TestCertificate.certificate, TestCertificate.key)
    withServer(Routes(GET("/") { r => s"secure ${r.url.scheme}" }), quiet.copy(tls = Some(tls))) { server =>
      val trusting = new HttpClient(ClientConfig(tls = TlsClientConfig(trusted = Some(TestCertificate.certificate))))
      try {
        assertEquals(trusting.get(s"https://localhost:${server.port}/").text(), "secure https")
        assertEquals(trusting.get(s"https://127.0.0.1:${server.port}/").text(), "secure https")
      } finally trusting.close()
      intercept[IOException](new HttpClient().get(s"https://localhost:${server.port}/"))
    }
  }

  test("WebSockets: an echo, text and binary, and the close handshake") {
    val routes = Routes(GET("/echo") { r =>
      WebSocket.accept(r) { ws =>
        for m <- ws.messages do ws.send(m)
      }
    })
    withServer(routes) { server =>
      val url = s"ws://127.0.0.1:${server.port}/echo"
      val got = WebSocket.connect(url) { ws =>
        ws.send("hello")
        ws.send(Array[Byte](1, 2, 3))
        ws.send("x" * 70000)
        val a = ws.receive()
        val b = ws.receive()
        val c = ws.receive()
        (
          a,
          b.map { case WebSocket.Message.Binary(bytes) => bytes.toList; case other => other },
          c.map {
            case WebSocket.Message.Text(t) => t.length
            case other                     => other
          }
        )
      }
      assertEquals(got, (Some(WebSocket.Message.Text("hello")), Some(List[Byte](1, 2, 3)), Some(70000)))
    }
  }

  test("a WebSocket from another site's page is refused") {
    val routes = Routes(GET("/ws") { r => WebSocket.accept(r)(_ => ()) })
    withServer(routes) { server =>
      val e = intercept[HttpStatusException](
        WebSocket.connect(s"ws://127.0.0.1:${server.port}/ws", headers = Headers("Origin" -> "https://evil.example"))(
          _ => ()
        )
      )
      assertEquals(e.status, Status.Forbidden)
    }
  }

  test("a graceful shutdown lets the request under way finish, and closes the idle connections") {
    val started = new java.util.concurrent.CountDownLatch(1)
    val routes = Routes(
      GET("/slow") { _ =>
        started.countDown()
        AsyncOperations.sleep(300)
        "finished"
      },
      GET("/fast") { _ => "fast" }
    )
    withServer(routes) { server =>
      val idle = new HttpClient()
      assertEquals(idle.get(server.url.resolve("/fast")).text(), "fast") // leaves a kept connection
      val slow = Future(Http.get(server.url.resolve("/slow")).text())
      while started.getCount > 0 do AsyncOperations.sleep(5)
      server.shutdown(5.seconds)
      assertEquals(slow.await, "finished")
      assertEquals(server.connections, 0)
      idle.close()
    }
  }

  test("the module's client lives as long as its run; under another Io each request has a client of its own") {
    val routes = Routes(GET("/") { _ => "ok" })
    withServer(routes) { server =>
      assertEquals(Http.get(server.url.resolve("/")).text(), "ok")
      assert(Http.client eq Http.client, "one client for the run")
    }
    Io.run(using gears.io.net.Platform.backend) {
      Async.group {
        val server = Http.server(routes, quiet)
        intercept[IllegalStateException](Http.client)
        assertEquals(Http.get(server.url.resolve("/")).text(), "ok")
        assertEquals(Http.get(server.url.resolve("/")).text(), "ok")
      }
    }
  }

  test("a server's URL is the address it listens on, loopback for every interface") {
    val routes = Routes(GET("/") { _ => "ok" })
    Http.run {
      Async.group {
        val any = Http.server(routes, quiet.copy(host = "0.0.0.0"))
        assertEquals(any.url.toString, s"http://127.0.0.1:${any.port}/")
        val v6 =
          try Some(Http.server(routes, quiet.copy(host = "::1")))
          catch case _: IOException => None // no IPv6 here
        for s <- v6 do {
          assertEquals(s.url.toString, s"http://[::1]:${s.port}/")
          assertEquals(Http.get(s.url.resolve("/")).text(), "ok")
        }
      }
    }
  }

  test("a WebSocket session busy with a message is not taken for a peer gone away by its pings") {
    val routes = Routes(GET("/ws") { r =>
      WebSocket.accept(r, pingEvery = Some(100.millis)) { ws =>
        for m <- ws.messages do {
          AsyncOperations.sleep(500) // while the next waits for it, and the reader reads nothing
          ws.send(m)
        }
      }
    })
    withServer(routes) { server =>
      val got = WebSocket.connect(s"ws://127.0.0.1:${server.port}/ws") { ws =>
        for i <- 1 to 3 do ws.send(s"m$i")
        (1 to 3).map(_ => ws.receive())
      }
      assertEquals(got, (1 to 3).map(i => Some(WebSocket.Message.Text(s"m$i"))))
    }
  }

  test("a WebSocket frame its peer does not read fails within the write timeout, and closes the socket") {
    val failed = Future.Promise[String]()
    val routes = Routes(GET("/ws") { r =>
      WebSocket.accept(r, pingEvery = None, writeTimeout = 300.millis) { ws =>
        val big = new Array[Byte](256 * 1024)
        try while true do ws.send(big)
        catch case e: IOException => failed.complete(scala.util.Success(e.getMessage))
      }
    })
    withServer(routes) { server =>
      val stream = Tcp.connect("127.0.0.1", server.port)
      try {
        val handshake = s"GET /ws HTTP/1.1\r\nHost: 127.0.0.1:${server.port}\r\nConnection: Upgrade\r\n" +
          "Upgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
        stream.write(ByteBuffer.wrap(handshake.getBytes(StandardCharsets.ISO_8859_1)))
        // Nothing is read from here on.
        val message = gears.async.withTimeout(10.seconds)(failed.asFuture.await)
        assert(message.contains("not written within"), message)
      } finally stream.close()
    }
  }
}
