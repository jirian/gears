import gears.async.*
import gears.async.asyncio.Error
import gears.async.asyncio.tls.NativeTlsSupport
import gears.async.asyncio.uring.UringPerThreadSupport
import gears.async.http.*
import gears.async.net.{DnsSupport, TcpListener, TcpStream, TcpSupport}

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.concurrent.duration.*
import scala.scalanative.libc.stdlib
import scala.scalanative.unsafe.{Zone, toCString}

/** The HTTP server and client, and TLS over them, on the uring backend. */
class HttpSuite extends munit.FunSuite:
  private lazy val support = UringPerThreadSupport(parallelism = 2)
  private given UringPerThreadSupport = support
  private given TcpSupport = support.tcpSupport
  private given DnsSupport = support.dnsSupport

  override def afterAll(): Unit = support.close()

  private val router = Router()
    .get("/")(_ => HttpResponse.text("hello"))
    .post("/echo")(req => HttpResponse.text(new String(req.body, StandardCharsets.UTF_8)))

  /** Serves `server` on a fresh listener while `body` runs, then closes the listener, which ends `serve`. */
  private def withServer[A](server: Server, host: String = "127.0.0.1")(body: Int => Async ?=> A): A =
    Async.blocking:
      val listener = TcpSupport.listen(InetSocketAddress(host, 0)).toOption.get
      val serving = Future(server.serve(listener))
      try body(listener.localAddress.asInstanceOf[InetSocketAddress].getPort)
      finally
        listener.close()
        serving.await // returns once the listener is closed: serve ends instead of failing or hanging

  /** Sends raw bytes and reads the whole response, for requests a well-behaved client would not send. */
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
      AsyncOperations.sleep(300) // the server drops the pooled connection
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
      assert(response.startsWith("HTTP/1.1 500 "), response) // the handler failed; nothing was injected
      assert(!response.contains("Injected"), response)

  /** Reads message heads off a raw stream, one at a time, keeping any bytes past the current one. */
  private final class HeadReader(stream: TcpStream):
    private val pending = java.io.ByteArrayOutputStream()

    /** The next head, without its terminating empty line. */
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

  /** A one-connection server that answers the first request with `response`, raw, for client tests. */
  private def withRawServer[A](response: String)(body: Int => Async ?=> A): A =
    Async.blocking:
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
      // An HTTP/1.0 client cannot read chunked bodies, so it gets the body delimited by the close instead.
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
        assertEquals(interim, "HTTP/1.1 100 Continue") // sent before the body exists
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

  test("HTTPS: a trusting client gets through, a verifying one rejects a self-signed certificate"):
    val stem = s"/tmp/gears-http-suite-${scala.scalanative.posix.unistd.getpid()}"
    val (cert, key) = (s"$stem-cert.pem", s"$stem-key.pem")
    val command =
      s"openssl req -x509 -newkey rsa:2048 -nodes -keyout $key -out $cert -days 1 -subj /CN=localhost >/dev/null 2>&1"
    assertEquals(Zone.acquire(z => stdlib.system(toCString(command)(using z))), 0, "openssl must be on PATH")
    try
      val tls = Some((NativeTlsSupport, NativeTlsSupport.serverContext(cert, key)))
      withServer(Server(router, tls = tls)): port =>
        val trusting = Client(Transport(tls = Some(NativeTlsSupport), tlsContext = _.clientContext(verify = false)))
        assertEquals(new String(trusting.get(s"https://127.0.0.1:$port/").body, StandardCharsets.UTF_8), "hello")
        val verifying = Client(NativeTlsSupport)
        intercept[java.io.IOException](verifying.get(s"https://127.0.0.1:$port/"))
    finally
      java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(cert))
      java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(key))

  test("TLS: one stream can be read and written by two fibers at once, and closed while a read is pending"):
    val stem = s"/tmp/gears-tls-duplex-${scala.scalanative.posix.unistd.getpid()}"
    val (cert, key) = (s"$stem-cert.pem", s"$stem-key.pem")
    val command =
      s"openssl req -x509 -newkey rsa:2048 -nodes -keyout $key -out $cert -days 1 -subj /CN=localhost >/dev/null 2>&1"
    assertEquals(Zone.acquire(z => stdlib.system(toCString(command)(using z))), 0, "openssl must be on PATH")
    try
      Async.blocking:
        val serverContext = NativeTlsSupport.serverContext(cert, key)
        val listener = TcpSupport.listen(InetSocketAddress("127.0.0.1", 0)).toOption.get
        val rounds = 200
        // Echoes everything back over TLS until the client half-closes.
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
        val client = NativeTlsSupport.wrapClient(raw, NativeTlsSupport.clientContext(verify = false), "localhost")
        try
          // The writer and the reader run concurrently on the same TLS stream.
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
          // Nothing more is coming, so this read stays pending until the close fails it.
          val pending = Future(client.readBuf(ByteBuffer.allocate(16)))
          AsyncOperations.sleep(50)
          client.close()
          assert(pending.awaitResult.isFailure, "a read pending across close must fail, not crash or hang")
        finally
          client.close()
          echo.awaitResult
          listener.close()
    finally
      java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(cert))
      java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(key))

  private def closeQuietly(stream: java.io.Closeable): Unit =
    try stream.close()
    catch case _: Exception => ()
