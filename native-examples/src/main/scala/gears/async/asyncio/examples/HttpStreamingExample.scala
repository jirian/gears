package gears.async.asyncio.examples

import gears.async.*
import gears.async.asyncio.uring.UringPerThreadSupport
import gears.async.asyncio.{Error, Reader, Result}
import gears.async.http.*
import gears.async.net.{DnsSupport, TcpSupport}

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.concurrent.duration.*

def httpStreamingExample(): Unit = withUring:
  val support = summon[UringPerThreadSupport]
  given TcpSupport = support.tcpSupport
  given DnsSupport = support.dnsSupport
  val router = Router()
    .get("/greet/{name}")(request => HttpResponse.text(s"hello, ${request.pathValue("name").getOrElse("?")}\n"))
    .handle("/upload")(
      Handler.streaming: (w, _, body) =>
        val buf = ByteBuffer.allocate(4096)
        var total = 0L
        var chunks = 0
        while body.readBuf(buf) != Left(Error.EOF) do
          total += buf.position()
          chunks += 1
          buf.clear()
        w.header.set("Content-Type", "text/plain")
        w.write(s"received $total bytes in $chunks reads\n")
    )
    .handle("/countdown")(
      Handler.streaming: (w, _, _) =>
        for i <- 3 to 1 by -1 do
          w.write(s"$i...\n")
          AsyncOperations.sleep(100)
        w.write("liftoff\n")
    )
  Async.blocking:
    val listener = orFail("listen")(TcpSupport.listen(InetSocketAddress("127.0.0.1", 0)))
    val port = listener.localAddress.asInstanceOf[InetSocketAddress].getPort
    val server = Server(router)
    val serving = Future(server.serve(listener))
    val client = Client()
    try
      val greeting = client.get(s"http://127.0.0.1:$port/greet/uring")
      println(s"GET /greet/uring -> ${greeting.status.code}: ${new String(greeting.body, StandardCharsets.UTF_8).trim}")

      val chunk = Array.fill[Byte](16 * 1024)('x')
      val upload = new Reader:
        private var remaining = 64
        def readBuf(buf: ByteBuffer)(using Async): Result[Unit] =
          if remaining == 0 then Left(Error.EOF)
          else
            remaining -= 1
            buf.put(chunk, 0, math.min(chunk.length, buf.remaining()))
            Right(())
      val request =
        StreamingRequest(HttpMethod.POST, Url.parse(s"http://127.0.0.1:$port/upload"), Headers.empty, upload)
      client
        .response(request)
        .use: response =>
          println(
            s"POST /upload, streamed without a Content-Length -> ${response.status.code}: ${readToEnd(response.body).trim}"
          )

      val countdown = StreamingRequest(
        HttpMethod.GET,
        Url.parse(s"http://127.0.0.1:$port/countdown"),
        Headers.empty,
        new Reader:
          def readBuf(buf: ByteBuffer)(using Async): Result[Unit] = Left(Error.EOF)
        ,
        Some(0L)
      )
      client
        .response(countdown)
        .use: response =>
          println(
            s"GET /countdown -> ${response.status.code}, Transfer-Encoding ${response.headers.get("Transfer-Encoding").getOrElse("none")}"
          )
          val started = System.nanoTime()
          val buf = ByteBuffer.allocate(256)
          while response.body.readBuf(buf) != Left(Error.EOF) do
            val elapsed = (System.nanoTime() - started) / 1_000_000
            new String(buf.array(), 0, buf.position(), StandardCharsets.UTF_8).linesIterator
              .foreach(line => println(f"  +$elapsed%4d ms  $line"))
            buf.clear()
    finally
      client.close()
      server.shutdown(1.second)
      serving.awaitResult
