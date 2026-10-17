import gears.async.*
import gears.async.asyncio.Error
import gears.async.asyncio.tls.NativeTlsSupport
import gears.async.http.*
import gears.async.net.{DnsSupport, TcpStream, TcpSupport, TlsContext, TlsSupport}

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.concurrent.duration.*
import scala.scalanative.libc.stdlib
import scala.scalanative.unsafe.{Zone, toCString}

abstract class HttpBehavior extends munit.FunSuite:
  protected given tcp: TcpSupport
  protected given dns: DnsSupport
  protected given operations: AsyncOperations

  protected def run[A](body: Async.Spawn ?=> A): A

  private val router = Router()
    .get("/")(_ => HttpResponse.text("hello"))
    .post("/echo")(req => HttpResponse.text(new String(req.body, StandardCharsets.UTF_8)))
    .handle("/ws")(
      WebSocket.handler(
        socket =>
          var message = socket.receive()
          while message.nonEmpty do
            socket.send(message.get)
            message = socket.receive()
        ,
        subprotocols = Seq("echo")
      )
    )

  test("WebSockets: text, binary, ping, subprotocol selection, and graceful close over a router endpoint"):
    withServer(Server(router)): port =>
      WebSocket
        .resource(s"ws://127.0.0.1:$port/ws", subprotocols = Seq("other", "echo"))
        .use: socket =>
          assertEquals(socket.subprotocol, Some("echo"))
          socket.ping(Array[Byte](1, 2))
          socket.sendText("hello €")
          assertEquals(socket.receive(), Some(WebSocket.Message.Text("hello €")))
          val binary = Array.tabulate[Byte](70000)(_.toByte)
          socket.sendBinary(binary)
          socket.receive() match
            case Some(WebSocket.Message.Binary(bytes)) => assertEquals(bytes.toSeq, binary.toSeq)
            case other                                 => fail(s"Expected binary echo, got $other")
          socket.sendClose(1000, "done")
          assertEquals(socket.receive(), None)
          assertEquals(socket.peerClose, Some(WebSocket.Close(Some(1000), "done")))

  test("WebSockets: an upgrade preserves a masked frame sent alongside the HTTP request"):
    withServer(Server(router)): port =>
      val stream = TcpSupport.connect(InetSocketAddress("127.0.0.1", port)).toOption.get
      try
        val request = s"GET /ws HTTP/1.1\r\nHost: 127.0.0.1:$port\r\nConnection: keep-alive, Upgrade\r\n" +
          "Upgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
        val frame =
          Array[Byte](0x81.toByte, 0x85.toByte, 0x37, 0xfa.toByte, 0x21, 0x3d, 0x7f, 0x9f.toByte, 0x4d, 0x51, 0x58)
        stream.writeBuf(ByteBuffer.wrap(request.getBytes(StandardCharsets.ISO_8859_1) ++ frame))
        val received = java.io.ByteArrayOutputStream()
        val buffer = ByteBuffer.allocate(4096)
        var headEnd = -1
        while headEnd < 0 || received.size() < headEnd + 4 + 7 do
          buffer.clear()
          assertEquals(stream.readBuf(buffer), Right(()))
          received.write(buffer.array(), 0, buffer.position())
          headEnd = received.toString(StandardCharsets.ISO_8859_1).indexOf("\r\n\r\n")
        val bytes = received.toByteArray
        val head = new String(bytes.take(headEnd), StandardCharsets.ISO_8859_1)
        assert(head.startsWith("HTTP/1.1 101 "))
        assert(head.contains("Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo="))
        assert(!head.contains("Transfer-Encoding"))
        assertEquals(
          bytes.drop(headEnd + 4).take(7).toSeq,
          (Array[Byte](0x81.toByte, 5) ++ "Hello".getBytes(StandardCharsets.UTF_8)).toSeq
        )
        stream.writeBuf(ByteBuffer.wrap(Array[Byte](0x88.toByte, 0x80.toByte, 1, 2, 3, 4)))
        buffer.clear()
        while stream.readBuf(buffer) != Left(Error.EOF) do ()
      finally stream.close()

  test("WebSockets: browser origins are checked, and a failed upgrade leaves ordinary HTTP usable"):
    withServer(Server(router)): port =>
      intercept[java.io.IOException]:
        WebSocket.connect(s"ws://127.0.0.1:$port/ws", headers = Headers("Origin" -> "https://elsewhere.invalid"))
      WebSocket
        .resource(s"ws://127.0.0.1:$port/ws", headers = Headers("Origin" -> s"http://127.0.0.1:$port"))
        .use: socket =>
          socket.sendClose()
          assertEquals(socket.receive(), None)
      assertEquals(Client().get(s"http://127.0.0.1:$port/").status, HttpStatus.Ok)

  test("WebSockets: the client rejects invalid handshake responses"):
    for response <- Seq(
        "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: wrong\r\n\r\n"
      )
    do
      withRawServer(response): port =>
        intercept[java.io.IOException](WebSocket.connect(s"ws://127.0.0.1:$port/ws"))

  test("the HTTP client rejects malformed status lines, header names, and response framing"):
    for response <- Seq(
        "HTTP/9.9 200 OK\r\nContent-Length: 0\r\n\r\n",
        "HTTP/1.1 99 Invalid\r\nContent-Length: 0\r\n\r\n",
        "HTTP/1.1 200 OK\r\nBad-Name : value\r\nContent-Length: 0\r\n\r\n",
        "HTTP/1.1 200 OK\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\nx",
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip\r\n\r\nx"
      )
    do
      withRawServer(response): port =>
        val client = Client()
        try intercept[java.io.IOException](client.get(s"http://127.0.0.1:$port/"))
        finally client.close()

  private def withServer[A](server: Server, host: String = "127.0.0.1")(body: Int => Async ?=> A): A =
    run:
      val listener = TcpSupport.listen(InetSocketAddress(host, 0)).toOption.get
      val serving = Future(server.serve(listener))
      try body(listener.localAddress.asInstanceOf[InetSocketAddress].getPort)
      finally
        listener.close()
        serving.await

  private def rawExchange(port: Int, request: String)(using Async): String =
    val stream = TcpSupport.connect(InetSocketAddress("127.0.0.1", port)).toOption.get
    try
      stream.writeBuf(ByteBuffer.wrap(request.getBytes(StandardCharsets.ISO_8859_1)))
      val all = ByteBuffer.allocate(4096)
      while stream.readBuf(all) != Left(Error.EOF) do ()
      all.flip()
      StandardCharsets.ISO_8859_1.decode(all).toString
    finally stream.close()

  test("GET and POST round trips, with the connection reused"):
    withServer(Server(router)): port =>
      val client = Client()
      assertEquals(new String(client.get(s"http://127.0.0.1:$port/").body, StandardCharsets.UTF_8), "hello")
      val echoed = client.post(s"http://127.0.0.1:$port/echo", "ping".getBytes(StandardCharsets.UTF_8))
      assertEquals(new String(echoed.body, StandardCharsets.UTF_8), "ping")
      assertEquals(client.get(s"http://127.0.0.1:$port/").status, HttpStatus.Ok)

  test("requests survive the server closing idle connections"):
    withServer(Server(router, idleTimeout = Some(100.millis))): port =>
      val client = Client()
      assertEquals(client.get(s"http://127.0.0.1:$port/").status, HttpStatus.Ok)
      AsyncOperations.sleep(300)
      assertEquals(client.get(s"http://127.0.0.1:$port/").status, HttpStatus.Ok, "an idempotent request is retried")
      AsyncOperations.sleep(300)
      val echoed = client.post(s"http://127.0.0.1:$port/echo", "once".getBytes(StandardCharsets.UTF_8))
      assertEquals(new String(echoed.body, StandardCharsets.UTF_8), "once", "a POST never uses a stale connection")

  test("a client reaches a server over IPv6 with a bracketed URL"):
    withServer(Server(router), host = "::1"): port =>
      assertEquals(Client().get(s"http://[::1]:$port/").status, HttpStatus.Ok)

  test("URLs with IPv6 literals parse, and render back with brackets"):
    val url = Url.parse("http://[::1]:8080/a?b=c")
    assertEquals((url.host, url.port, url.path, url.query), ("::1", 8080, "/a", "b=c"))
    assertEquals(url.hostHeader, "[::1]:8080")
    assertEquals(Url.parse("https://[2001:db8::1]/").port, 443)

  test("a body over the limit is refused with 413 before it is read"):
    withServer(Server(router, maxBodySize = 16)): port =>
      val response = rawExchange(port, "POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 1000000\r\n\r\n")
      assert(response.startsWith("HTTP/1.1 413 "), response)

  test("conflicting Content-Length headers are refused"):
    withServer(Server(router)): port =>
      val response =
        rawExchange(port, "POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\nab")
      assert(response.startsWith("HTTP/1.1 400 "), response)

  test("header values cannot inject header lines"):
    intercept[IllegalArgumentException](HttpResponse.text("x").withHeader("X-Note", "a\r\nSet-Cookie: evil=1"))
    intercept[IllegalArgumentException](MutableHeaders().set("X-Note", "a\nb"))
    intercept[IllegalArgumentException](Headers("Bad Name" -> "v"))
    withServer(Server(Handler.of(_ => HttpResponse.text("x").withHeader("X-Note", "a\r\nInjected: 1")))): port =>
      val response = rawExchange(port, "GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
      assert(response.startsWith("HTTP/1.1 500 "), response)
      assert(!response.contains("Injected"), response)

  private final class HeadReader(stream: TcpStream):
    private val pending = java.io.ByteArrayOutputStream()

    def next()(using Async): String =
      var text = pending.toString(StandardCharsets.ISO_8859_1)
      while !text.contains("\r\n\r\n") do
        val buf = ByteBuffer.allocate(1024)
        if stream.readBuf(buf) == Left(Error.EOF) then throw java.io.EOFException(s"no complete head in: $text")
        pending.write(buf.array(), 0, buf.position())
        text = pending.toString(StandardCharsets.ISO_8859_1)
      val end = text.indexOf("\r\n\r\n")
      pending.reset()
      pending.write(text.substring(end + 4).getBytes(StandardCharsets.ISO_8859_1))
      text.substring(0, end)

  private def withRawServer[A](response: String)(body: Int => Async ?=> A): A =
    run:
      val listener = TcpSupport.listen(InetSocketAddress("127.0.0.1", 0)).toOption.get
      val serving = Future:
        val conn = listener.accept().toOption.get
        try
          HeadReader(conn).next()
          conn.writeBuf(ByteBuffer.wrap(response.getBytes(StandardCharsets.ISO_8859_1)))
          conn.shutdownOutput()
          AsyncOperations.sleep(50)
        finally conn.close()
      try body(listener.localAddress.asInstanceOf[InetSocketAddress].getPort)
      finally
        serving.await
        listener.close()

  test("a chunked request body is decoded, extensions and trailers included"):
    withServer(Server(router)): port =>
      val response = rawExchange(
        port,
        "POST /echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n" +
          "4;note=1\r\nWiki\r\n5\r\npedia\r\n0\r\nX-Trailer: t\r\n\r\n"
      )
      assert(response.startsWith("HTTP/1.1 200 "), response)
      assert(response.endsWith("Wikipedia"), response)

  test("a chunked request body over the limit is refused with 413"):
    withServer(Server(router, maxBodySize = 8)): port =>
      val response = rawExchange(
        port,
        "POST /echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nabcde\r\n5\r\nfghij\r\n0\r\n\r\n"
      )
      assert(response.startsWith("HTTP/1.1 413 "), response)

  test("Transfer-Encoding together with Content-Length is refused"):
    withServer(Server(router)): port =>
      val response = rawExchange(
        port,
        "POST /echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\nContent-Length: 3\r\n\r\n0\r\n\r\n"
      )
      assert(response.startsWith("HTTP/1.1 400 "), response)

  test("header syntax that proxies read differently is refused"):
    withServer(Server(router)): port =>
      for request <- Seq(
          "POST /echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding : chunked\r\n\r\n0\r\n\r\n",
          "GET / HTTP/1.1\r\nHost: x\r\n X-Folded: y\r\n\r\n",
          "POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: +3\r\n\r\nabc"
        )
      do
        val response = rawExchange(port, request)
        assert(response.startsWith("HTTP/1.1 400 "), s"$request -> $response")

  test("a failing handler is logged, and fails the connection once its response has started"):
    val logged = java.util.concurrent.ConcurrentLinkedQueue[String]()
    val handler = Router()
      .get("/early")(_ => throw IllegalStateException("before the response"))
      .handle("/late")(
        Handler.streaming: (w, _, _) =>
          w.header.set("Content-Length", "100")
          w.write("partial")
          throw IllegalStateException("after the response started")
      )
    withServer(Server(handler, errorLog = logged.add(_))): port =>
      val early = rawExchange(port, "GET /early HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
      assert(early.startsWith("HTTP/1.1 500 "), early)
      val late = rawExchange(port, "GET /late HTTP/1.1\r\nHost: x\r\n\r\n")
      assert(late.startsWith("HTTP/1.1 200 ") && late.endsWith("partial"), late)
      assert(!late.contains("500"), late)
      assertEquals(logged.toArray.count(_.toString.contains("handler failed")), 2)

  test("routes may suspend, and literal routes win over wildcards"):
    val handler = Router()
      .get("/items/{id}")(request =>
        AsyncOperations.sleep(1)
        HttpResponse.text(s"item ${request.pathValue("id").getOrElse("?")}")
      )
      .get("/items/new")(_ => HttpResponse.text("new item form"))
    withServer(Server(handler)): port =>
      val client = Client()
      try
        assertEquals(
          new String(client.get(s"http://127.0.0.1:$port/items/new").body, StandardCharsets.UTF_8),
          "new item form"
        )
        assertEquals(new String(client.get(s"http://127.0.0.1:$port/items/7").body, StandardCharsets.UTF_8), "item 7")
      finally client.close()

  test("a URL with whitespace or control characters is refused before anything is sent"):
    intercept[IllegalArgumentException](Url.parse("http://127.0.0.1/a\r\nX-Injected: 1"))
    intercept[IllegalArgumentException](Url.parse("http://127.0.0.1/a b"))

  test("a response of undeclared length is streamed chunked, and the connection stays open"):
    val streaming = new Handler:
      def serveHTTP(w: ResponseWriter, r: HttpRequest)(using Async): Unit =
        w.write("stream")
        w.write("ed")
    withServer(Server(streaming)): port =>
      val client = Client()
      val first = client.get(s"http://127.0.0.1:$port/")
      assertEquals(first.headers.get("Transfer-Encoding"), Some("chunked"))
      assertEquals(new String(first.body, StandardCharsets.UTF_8), "streamed")
      assertEquals(new String(client.get(s"http://127.0.0.1:$port/").body, StandardCharsets.UTF_8), "streamed")
      val legacy = rawExchange(port, "GET / HTTP/1.0\r\n\r\n")
      assert(!legacy.contains("Transfer-Encoding") && legacy.endsWith("streamed"), legacy)

  test("Expect: 100-continue gets an interim 100 before the body is sent"):
    withServer(Server(router)): port =>
      val stream = TcpSupport.connect(InetSocketAddress("127.0.0.1", port)).toOption.get
      try
        val head =
          "POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 4\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n"
        stream.writeBuf(ByteBuffer.wrap(head.getBytes(StandardCharsets.ISO_8859_1)))
        val reader = HeadReader(stream)
        val interim = reader.next()
        assertEquals(interim, "HTTP/1.1 100 Continue")
        stream.writeBuf(ByteBuffer.wrap("body".getBytes(StandardCharsets.ISO_8859_1)))
        val finalHead = reader.next()
        assert(finalHead.startsWith("HTTP/1.1 200 "), finalHead)
      finally stream.close()

  test("an unknown expectation is refused with 417"):
    withServer(Server(router)): port =>
      val response = rawExchange(port, "GET / HTTP/1.1\r\nHost: x\r\nExpect: something-else\r\n\r\n")
      assert(response.startsWith("HTTP/1.1 417 "), response)

  test("the client skips interim 1xx responses and decodes a chunked final one"):
    val raw = "HTTP/1.1 103 Early Hints\r\nLink: </style.css>\r\n\r\n" +
      "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n"
    withRawServer(raw): port =>
      val response = Client().get(s"http://127.0.0.1:$port/")
      assertEquals(response.status.code, 200)
      assertEquals(new String(response.body, StandardCharsets.UTF_8), "abcde")

  test("the client bounds response bodies, whether sized, chunked, or ended by the close"):
    for raw <- Seq(
        "HTTP/1.1 200 OK\r\nContent-Length: 20\r\n\r\n01234567890123456789",
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n14\r\n01234567890123456789\r\n0\r\n\r\n",
        "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n01234567890123456789"
      )
    do
      withRawServer(raw): port =>
        val client = Client(Transport(maxResponseBodySize = 10))
        val failure = intercept[java.io.IOException](client.get(s"http://127.0.0.1:$port/"))
        assert(failure.getMessage.contains("exceeds the limit"), failure.getMessage)

  if scala.scalanative.meta.LinktimeInfo.isLinux then
    test("HTTPS: a trusting client gets through, a verifying one rejects a self-signed certificate"):
      val stem = s"/tmp/gears-http-suite-${scala.scalanative.posix.unistd.getpid()}"
      val (cert, key) = (s"$stem-cert.pem", s"$stem-key.pem")
      val command =
        s"openssl req -x509 -newkey rsa:2048 -nodes -keyout $key -out $cert -days 1 -subj /CN=localhost >/dev/null 2>&1"
      assertEquals(Zone.acquire(z => stdlib.system(toCString(command)(using z))), 0, "openssl must be on PATH")
      try
        val context = NativeTlsSupport.serverContext(cert, key)
        try
          withServer(Server(router, tls = Some((NativeTlsSupport, context)))): port =>
            val trusting = Client(Transport(tls = Some(NativeTlsSupport), tlsContext = _.clientContext(verify = false)))
            val verifying = Client(NativeTlsSupport)
            val transport = Transport(tls = Some(NativeTlsSupport), tlsContext = _.clientContext(verify = false))
            try
              assertEquals(new String(trusting.get(s"https://127.0.0.1:$port/").body, StandardCharsets.UTF_8), "hello")
              intercept[java.io.IOException](verifying.get(s"https://127.0.0.1:$port/"))
              val socket = WebSocket.connectWith(transport, s"wss://127.0.0.1:$port/ws")
              try
                socket.sendText("encrypted websocket")
                assertEquals(socket.receive(), Some(WebSocket.Message.Text("encrypted websocket")))
                socket.sendClose()
                assertEquals(socket.receive(), None)
              finally socket.close()
            finally
              trusting.close()
              verifying.close()
              transport.close()
        finally context.close()
      finally
        java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(cert))
        java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(key))

  if scala.scalanative.meta.LinktimeInfo.isLinux then
    test("TLS: a verifying client checks the certificate's host name and IP address entries"):
      val stem = s"/tmp/gears-tls-verify-${scala.scalanative.posix.unistd.getpid()}"
      def makeCert(name: String, san: String): (String, String) =
        val (cert, key) = (s"$stem-$name-cert.pem", s"$stem-$name-key.pem")
        val command = s"openssl req -x509 -newkey rsa:2048 -nodes -keyout $key -out $cert -days 1 " +
          s"-subj /CN=localhost -addext subjectAltName=$san >/dev/null 2>&1"
        assertEquals(Zone.acquire(z => stdlib.system(toCString(command)(using z))), 0, "openssl must be on PATH")
        (cert, key)
      val dnsCert = makeCert("dns", "DNS:localhost")
      val ipCert = makeCert("ip", "IP:127.0.0.1")
      try
        run:
          def handshakes(certAndKey: (String, String), serverName: String): Boolean =
            val (cert, key) = certAndKey
            val serverContext = NativeTlsSupport.serverContext(cert, key)
            val clientContext = NativeTlsSupport.clientContext(verify = true, caFile = Some(cert))
            val listener = TcpSupport.listen(InetSocketAddress("127.0.0.1", 0)).toOption.get
            try
              val server = Future:
                val conn = NativeTlsSupport.wrapServer(listener.accept().toOption.get, serverContext)
                closeQuietly(conn)
              val raw = TcpSupport.connect(listener.localAddress).toOption.get
              val outcome = scala.util.Try(NativeTlsSupport.wrapClient(raw, clientContext, serverName))
              outcome.foreach(closeQuietly)
              server.awaitResult
              outcome.isSuccess
            finally
              listener.close()
              clientContext.close()
              serverContext.close()
          assert(handshakes(dnsCert, "localhost"), "the certificate's own host name is accepted")
          assert(!handshakes(dnsCert, "example.com"), "a trusted certificate for another host is rejected")
          assert(!handshakes(dnsCert, "127.0.0.1"), "an IP address is not matched against host names")
          assert(handshakes(ipCert, "127.0.0.1"), "an IP address is matched against the IP entries")
          assert(!handshakes(ipCert, "127.0.0.2"), "and only the listed one")
      finally
        for path <- Seq(dnsCert._1, dnsCert._2, ipCert._1, ipCert._2) do
          java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(path))

    test("TLS: one stream can be read and written by two fibers at once, and closed while a read is pending"):
      val stem = s"/tmp/gears-tls-duplex-${scala.scalanative.posix.unistd.getpid()}"
      val (cert, key) = (s"$stem-cert.pem", s"$stem-key.pem")
      val command =
        s"openssl req -x509 -newkey rsa:2048 -nodes -keyout $key -out $cert -days 1 -subj /CN=localhost >/dev/null 2>&1"
      assertEquals(Zone.acquire(z => stdlib.system(toCString(command)(using z))), 0, "openssl must be on PATH")
      try
        run:
          val serverContext = NativeTlsSupport.serverContext(cert, key)
          val listener = TcpSupport.listen(InetSocketAddress("127.0.0.1", 0)).toOption.get
          val rounds = 200
          val echo = Future:
            val conn = NativeTlsSupport.wrapServer(listener.accept().toOption.get, serverContext)
            try
              val buf = ByteBuffer.allocate(256)
              while conn.readBuf(buf) != Left(Error.EOF) do
                buf.flip()
                conn.writeBuf(buf)
                buf.clear()
            finally closeQuietly(conn)
          val raw = TcpSupport.connect(listener.localAddress).toOption.get
          val clientContext = NativeTlsSupport.clientContext(verify = false)
          val client = NativeTlsSupport.wrapClient(raw, clientContext, "localhost")
          clientContext.close()
          clientContext.close()
          serverContext.close()
          intercept[java.nio.channels.ClosedChannelException](
            NativeTlsSupport.wrapClient(client, clientContext, "localhost")
          )
          try
            val writer = Future:
              for i <- 1 to rounds do client.writeBuf(ByteBuffer.wrap(f"msg$i%04d".getBytes(StandardCharsets.UTF_8)))
            val expected = (1 to rounds).map(i => f"msg$i%04d").mkString
            val received = java.io.ByteArrayOutputStream()
            val buf = ByteBuffer.allocate(256)
            while received.size() < expected.length do
              client.readBuf(buf)
              buf.flip()
              received.write(buf.array(), 0, buf.remaining())
              buf.clear()
            writer.await
            assertEquals(received.toString(StandardCharsets.UTF_8), expected)
            val pending = Future(client.readBuf(ByteBuffer.allocate(16)))
            AsyncOperations.sleep(50)
            client.close()
            assert(pending.awaitResult.isFailure, "a read pending across close must fail, not crash or hang")
          finally
            client.close()
            echo.awaitResult
            listener.close()
            clientContext.close()
            serverContext.close()
      finally
        java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(cert))
        java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(key))

  if scala.scalanative.meta.LinktimeInfo.isLinux then
    test("WebSocket.connect closes its owned TLS context on success and failed upgrade"):
      val stem = s"/tmp/gears-ws-context-${scala.scalanative.posix.unistd.getpid()}"
      val (cert, key) = (s"$stem-cert.pem", s"$stem-key.pem")
      val command =
        s"openssl req -x509 -newkey rsa:2048 -nodes -keyout $key -out $cert -days 1 -subj /CN=localhost >/dev/null 2>&1"
      assertEquals(Zone.acquire(z => stdlib.system(toCString(command)(using z))), 0, "openssl must be on PATH")
      val closedContexts = java.util.concurrent.atomic.AtomicInteger()
      final class CountedContext(val delegate: TlsContext) extends TlsContext:
        override def close(): Unit =
          closedContexts.incrementAndGet()
          delegate.close()
      val tls = new TlsSupport:
        def clientContext(verify: Boolean, caFile: Option[String]): TlsContext =
          CountedContext(NativeTlsSupport.clientContext(verify = false))
        def serverContext(certFile: String, keyFile: String): TlsContext =
          NativeTlsSupport.serverContext(certFile, keyFile)
        def wrapClient(stream: TcpStream, context: TlsContext, serverName: String)(using Async): TcpStream =
          NativeTlsSupport.wrapClient(stream, context.asInstanceOf[CountedContext].delegate, serverName)
        def wrapServer(stream: TcpStream, context: TlsContext)(using Async): TcpStream =
          NativeTlsSupport.wrapServer(stream, context)
      try
        val serverContext = NativeTlsSupport.serverContext(cert, key)
        try
          withServer(Server(router, tls = Some((NativeTlsSupport, serverContext)))): port =>
            val socket = WebSocket.connect(s"wss://127.0.0.1:$port/ws", tls = Some(tls))
            assertEquals(closedContexts.get(), 1, "connect releases its internally created context")
            socket.sendText("context retained by session")
            assertEquals(socket.receive(), Some(WebSocket.Message.Text("context retained by session")))
            socket.close()

            intercept[java.io.IOException](WebSocket.connect(s"wss://127.0.0.1:$port/", tls = Some(tls)))
            assertEquals(closedContexts.get(), 2, "failed upgrade releases its internally created context")

            val transport = Transport(tls = Some(tls))
            val borrowed = WebSocket.connectWith(transport, s"wss://127.0.0.1:$port/ws")
            assertEquals(closedContexts.get(), 2, "connectWith leaves the caller's transport open")
            borrowed.sendText("caller owns transport")
            assertEquals(borrowed.receive(), Some(WebSocket.Message.Text("caller owns transport")))
            borrowed.close()
            transport.close()
            assertEquals(closedContexts.get(), 3, "the caller closes its transport-owned context")
        finally serverContext.close()
      finally
        java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(cert))
        java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(key))

  private def closeQuietly(stream: java.io.Closeable): Unit =
    try stream.close()
    catch case _: Exception => ()

  test("streaming uploads and downloads preserve framing and release connections on EOF"):
    val streaming = Handler.streaming: (w, _, input) =>
      var count = 0
      val buf = ByteBuffer.allocate(1024)
      var done = false
      while !done do
        buf.clear()
        input.readBuf(buf) match
          case Left(Error.EOF) => done = true
          case Right(())       => count += buf.position()
      w.header.set("X-Received", count.toString)
      for _ <- 0 until 32 do w.write(Array.fill[Byte](1024)(42))
    withServer(Server(streaming)): port =>
      val client = Client(Transport(maxConnections = 1))
      try
        for length <- Seq(Some(100000L), None) do
          val source = new gears.async.asyncio.Reader:
            private var remaining = 100000
            def readBuf(buf: ByteBuffer)(using Async): gears.async.asyncio.Result[Unit] =
              if remaining == 0 then Left(Error.EOF)
              else
                val n = math.min(remaining, buf.remaining())
                for _ <- 0 until n do buf.put(7.toByte)
                remaining -= n
                Right(())
          val request =
            StreamingRequest(HttpMethod.POST, Url.parse(s"http://127.0.0.1:$port/"), Headers.empty, source, length)
          client
            .response(request)
            .use: response =>
              assertEquals(response.headers.get("X-Received"), Some("100000"))
              assertEquals(response.body.readAll().toSeq, Array.fill[Byte](32768)(42).toSeq)
      finally client.close()

  test("an early rejection survives the peer closing its input during a large upload"):
    withServer(Server(router, maxBodySize = 8, errorLog = _ => ())): port =>
      val client = Client(Transport(responseHeaderTimeout = Some(2.seconds)))
      try
        assertEquals(client.post(s"http://127.0.0.1:$port/echo", Array.fill[Byte](8 * 1024 * 1024)(1)).status.code, 413)
      finally client.close()

  test("early final responses cancel a blocked upload and release its reader before scope exit"):
    withServer(Server(router, maxBodySize = 8, errorLog = _ => ())): port =>
      val stopped = new java.util.concurrent.atomic.AtomicBoolean(false)
      val source = new gears.async.asyncio.Reader:
        def readBuf(buf: ByteBuffer)(using Async): gears.async.asyncio.Result[Unit] =
          try
            Future.Promise[Unit]().await
            Left(Error.EOF)
          finally stopped.set(true)
      val client = Client(Transport(responseHeaderTimeout = Some(2.seconds)))
      try
        val request = StreamingRequest(
          HttpMethod.POST,
          Url.parse(s"http://127.0.0.1:$port/echo"),
          Headers.empty,
          source,
          Some(1000000)
        )
        client
          .response(request)
          .use: response =>
            assertEquals(response.status.code, 413)
            response.body.readAll()
        assert(stopped.get(), "the upload must release the caller's reader before resource cleanup returns")
      finally client.close()

  test("streaming responses can unblock an upload before the request reaches EOF"):
    val continueUpload = Future.Promise[Unit]()
    val handler = Handler.streaming: (writer, _, input) =>
      writer.write("ready")
      val buffer = ByteBuffer.allocate(128)
      while input.readBuf(buffer) != Left(Error.EOF) do buffer.clear()
      writer.write("done")
    withServer(Server(handler, errorLog = _ => ())): port =>
      val source = new gears.async.asyncio.Reader:
        private var sent = false
        def readBuf(buf: ByteBuffer)(using Async): gears.async.asyncio.Result[Unit] =
          continueUpload.await
          if sent then Left(Error.EOF)
          else
            buf.put(1.toByte)
            sent = true
            Right(())
      val client = new Client(Transport(responseHeaderTimeout = Some(2.seconds)), timeout = Some(3.seconds))
      try
        val request = StreamingRequest(HttpMethod.POST, Url.parse(s"http://127.0.0.1:$port/"), Headers.empty, source)
        client
          .response(request)
          .use: response =>
            val first = ByteBuffer.allocate(5)
            while first.hasRemaining do assertEquals(response.body.readBuf(first), Right(()))
            assertEquals(new String(first.array(), StandardCharsets.UTF_8), "ready")
            continueUpload.complete(scala.util.Success(()))
            assertEquals(new String(response.body.readAll(), StandardCharsets.UTF_8), "done")
      finally client.close()

  test("closing a partial response releases a bounded transport's capacity"):
    withServer(Server(Handler.of(_ => HttpResponse.text("a" * 100000)), errorLog = _ => ())): port =>
      val client = Client(Transport(maxConnections = 1))
      try
        val empty = new gears.async.asyncio.Reader:
          def readBuf(buf: ByteBuffer)(using Async): gears.async.asyncio.Result[Unit] = Left(Error.EOF)
        val request =
          StreamingRequest(HttpMethod.GET, Url.parse(s"http://127.0.0.1:$port/"), Headers.empty, empty, Some(0))
        val response = client.sendStreaming(request)
        response.body.readBuf(ByteBuffer.allocate(1))
        response.close()
        assertEquals(client.get(s"http://127.0.0.1:$port/").body.length, 100000)
        client.closeIdleConnections()
      finally client.close()
      intercept[IllegalStateException](client.get(s"http://127.0.0.1:$port/"))

  test("response header and body read timeouts cancel pending socket operations"):
    val slowHead = Handler.streaming: (w, _, _) =>
      AsyncOperations.sleep(150)
      w.write("late")
    withServer(Server(slowHead, errorLog = _ => ())): port =>
      val client = Client(Transport(responseHeaderTimeout = Some(20.millis)))
      try intercept[java.util.concurrent.TimeoutException](client.get(s"http://127.0.0.1:$port/"))
      finally client.close()
    val slowBody = Handler.streaming: (w, _, _) =>
      w.write("first")
      AsyncOperations.sleep(150)
      w.write("last")
    withServer(Server(slowBody, errorLog = _ => ())): port =>
      val client = Client(Transport(readTimeout = Some(20.millis)))
      try intercept[java.util.concurrent.TimeoutException](client.get(s"http://127.0.0.1:$port/"))
      finally client.close()

  test("HTTP write deadlines stop at a WebSocket upgrade"):
    withServer(Server(router, writeTimeout = Some(50.millis))): port =>
      WebSocket
        .resource(s"ws://127.0.0.1:$port/ws")
        .use: socket =>
          AsyncOperations.sleep(100)
          socket.sendText("still open")
          assertEquals(socket.receive(), Some(WebSocket.Message.Text("still open")))
          socket.sendClose()
          assertEquals(socket.receive(), None)

  test("server shutdown lets active requests finish and closes idle connections"):
    run:
      val entered = Future.Promise[Unit]()
      val handler = Handler.streaming: (w, _, _) =>
        entered.complete(scala.util.Success(()))
        AsyncOperations.sleep(80)
        w.write("finished")
      val server = Server(handler, errorLog = _ => ())
      val listener = TcpSupport.listen(InetSocketAddress("127.0.0.1", 0)).toOption.get
      val serving = Future(server.serve(listener))
      val client = Client()
      try
        val request =
          Future(client.get(s"http://127.0.0.1:${listener.localAddress.asInstanceOf[InetSocketAddress].getPort}/"))
        entered.await
        server.shutdown(1.second)
        assertEquals(new String(request.await.body, StandardCharsets.UTF_8), "finished")
        serving.await
      finally
        server.close()
        client.close()

  test("closing idle connections during shutdown is not reported as a connection error"):
    run:
      val logged = java.util.concurrent.ConcurrentLinkedQueue[String]()
      val server = Server(Handler.of(_ => HttpResponse.text("ok")), errorLog = logged.add(_))
      val listener = TcpSupport.listen(InetSocketAddress("127.0.0.1", 0)).toOption.get
      val serving = Future(server.serve(listener))
      val client = Client()
      try
        val port = listener.localAddress.asInstanceOf[InetSocketAddress].getPort
        assertEquals(new String(client.get(s"http://127.0.0.1:$port/").body, StandardCharsets.UTF_8), "ok")
        server.shutdown(1.second)
        serving.await
        assertEquals(logged.toArray.toList, Nil)
      finally
        server.close()
        client.close()

  test("idle pool bounds, expiry, and explicit cleanup control reuse"):
    withServer(Server(router)): port =>
      val underlying = tcp
      val connects = new java.util.concurrent.atomic.AtomicInteger()
      val counted = new TcpSupport:
        type Stream = underlying.Stream
        type Listener = underlying.Listener
        def connect(address: java.net.SocketAddress, options: Seq[gears.async.net.SocketOption])(using
            Async
        ): gears.async.asyncio.Result[Stream] =
          connects.incrementAndGet()
          underlying.connect(address, options)
        def listen(address: java.net.SocketAddress, options: Seq[gears.async.net.SocketOption])(using
            Async
        ): gears.async.asyncio.Result[Listener] =
          underlying.listen(address, options)
      val client = Client(new Transport(idleTimeout = 40.millis)(using counted, dns))
      val url = s"http://127.0.0.1:$port/"
      try
        client.get(url)
        client.get(url)
        assertEquals(connects.get(), 1)
        AsyncOperations.sleep(60)
        client.get(url)
        assertEquals(connects.get(), 2)
        client.closeIdleConnections()
        client.get(url)
        assertEquals(connects.get(), 3)
      finally client.close()
      val unpooled = Client(new Transport(maxIdleConnections = 0)(using counted, dns))
      try
        unpooled.get(url)
        unpooled.get(url)
        assertEquals(connects.get(), 5)
      finally unpooled.close()

  test("transport close releases capacity held by an unread response"):
    withServer(Server(router)): port =>
      Async.group:
        val client = Client(Transport(maxConnections = 1))
        val url = s"http://127.0.0.1:$port/"
        val empty = new gears.async.asyncio.Reader:
          def readBuf(buf: ByteBuffer)(using Async): gears.async.asyncio.Result[Unit] = Left(Error.EOF)
        val response =
          client.sendStreaming(StreamingRequest(HttpMethod.GET, Url.parse(url), Headers.empty, empty, Some(0)))
        val waiting = Future(client.get(url))
        AsyncOperations.sleep(30)
        client.close()
        assert(waiting.awaitResult.isFailure)
        response.close()

  test("server connection limits refuse excess connections without stopping the listener"):
    withServer(Server(router, maxConnections = 1, errorLog = _ => ())): port =>
      val first = TcpSupport.connect(InetSocketAddress("127.0.0.1", port)).toOption.get
      try
        first.writeBuf(ByteBuffer.wrap("GET / HTTP/1.1\r\nHost: x\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1)))
        assert(HeadReader(first).next().startsWith("HTTP/1.1 200 "))
        val client = Client()
        try intercept[java.io.IOException](client.get(s"http://127.0.0.1:$port/"))
        finally client.close()
      finally first.close()

  test("a shutdown deadline cancels active handlers and releases their connections"):
    run:
      val entered = Future.Promise[Unit]()
      val finished = new java.util.concurrent.atomic.AtomicBoolean(false)
      val handler = Handler.streaming: (_, _, _) =>
        try
          entered.complete(scala.util.Success(()))
          AsyncOperations.sleep(100000)
        finally finished.set(true)
      val server = Server(handler, errorLog = _ => ())
      val listener = TcpSupport.listen(InetSocketAddress("127.0.0.1", 0)).toOption.get
      val serving = Future(server.serve(listener))
      val client = Client()
      try
        val request =
          Future(client.get(s"http://127.0.0.1:${listener.localAddress.asInstanceOf[InetSocketAddress].getPort}/"))
        entered.await
        intercept[java.util.concurrent.TimeoutException](server.shutdown(20.millis))
        assert(request.awaitResult.isFailure)
        serving.await
        assert(finished.get())
      finally
        server.close()
        client.close()
