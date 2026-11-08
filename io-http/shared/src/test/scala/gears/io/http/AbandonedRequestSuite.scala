package gears.io.http

import gears.async.Async
import gears.async.AsyncOperations
import gears.async.Future
import gears.async.Future.Promise
import gears.async.io.Io
import gears.io.net.DefaultCapabilities
import gears.io.net.Tcp

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.concurrent.CancellationException
import java.util.concurrent.TimeoutException
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt
import scala.util.Success

import munit.FunSuite

/** A handler whose client went away is cancelled (`ServerConfig.cancelAbandoned`), and one whose client is still there
  * is not.
  */
class AbandonedRequestSuite extends FunSuite {
  override val munitTimeout = scala.concurrent.duration.Duration(60, "s")

  private val quiet = ServerConfig(host = "127.0.0.1", port = 0, log = _ => ())

  private def withServer[A](routes: Routes, config: ServerConfig = quiet)(
      body: Server => (Io[DefaultCapabilities], Async.Spawn) ?=> A
  ): A = Http.testServer(routes, config)(body)

  /** Sends `raw` on a new connection and closes it `after` later, without reading. */
  private def sendAndLeave(server: Server, raw: String, after: Int = 50)(using Io[DefaultCapabilities], Async): Unit = {
    val stream = Tcp.connect("127.0.0.1", server.port)
    try {
      stream.write(ByteBuffer.wrap(raw.getBytes(StandardCharsets.ISO_8859_1)))
      AsyncOperations.sleep(after)
    } finally stream.close()
  }

  /** What `promise` completes with, within `limit`. */
  private def within[A](limit: scala.concurrent.duration.FiniteDuration)(
      promise: Promise[A]
  )(using AsyncOperations, Async): A =
    gears.async.withTimeout(limit)(promise.asFuture.await)

  /** A handler that waits `millis`, telling `outcome` whether it was cancelled first, and starts a future that tells
    * `inner` the same.
    */
  private def slow(
      millis: Int,
      outcome: Promise[String],
      inner: Promise[String]
  ): Request => (Async, Io[DefaultCapabilities]) ?=> String = _ =>
    try {
      Async.group {
        val child = Future {
          try { AsyncOperations.sleep(millis * 2L); inner.complete(Success("finished")) }
          catch case e: CancellationException => { inner.complete(Success("cancelled")); throw e }
        }
        AsyncOperations.sleep(millis)
        child.cancel()
      }
      outcome.complete(Success("finished"))
      "done"
    } catch {
      case e: CancellationException =>
        outcome.complete(Success("cancelled"))
        throw e
    }

  test("a handler whose client closes is cancelled, with the futures it started") {
    val outcome = Promise[String]()
    val inner = Promise[String]()
    withServer(Routes(GET("/slow")(slow(10_000, outcome, inner)))) { server =>
      val began = System.nanoTime()
      sendAndLeave(server, "GET /slow HTTP/1.1\r\nHost: x\r\n\r\n")
      assertEquals(within(5.seconds)(outcome), "cancelled")
      assertEquals(within(5.seconds)(inner), "cancelled")
      assert((System.nanoTime() - began) < 5_000_000_000L, "well before the handler's own 10 s")
    }
  }

  test("a handler is cancelled once it has read the body, if the client is gone") {
    val outcome = Promise[String]()
    val inner = Promise[String]()
    val routes = Routes(POST("/upload") { req =>
      req.bytes(): Unit
      slow(10_000, outcome, inner)(req)
    })
    withServer(routes) { server =>
      sendAndLeave(server, "POST /upload HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n\r\nhello")
      assertEquals(within(5.seconds)(outcome), "cancelled")
    }
  }

  test("with cancelAbandoned off, a handler whose client closes runs to its end") {
    val outcome = Promise[String]()
    val inner = Promise[String]()
    withServer(Routes(GET("/slow")(slow(400, outcome, inner))), quiet.copy(cancelAbandoned = Duration.Inf)) { server =>
      sendAndLeave(server, "GET /slow HTTP/1.1\r\nHost: x\r\n\r\n")
      assertEquals(within(5.seconds)(outcome), "finished")
    }
  }

  test("a slow handler whose client has sent the next request is not cancelled, and both are answered") {
    val outcome = Promise[String]()
    val inner = Promise[String]()
    val routes = Routes(GET("/slow")(slow(300, outcome, inner)), GET("/n") { _ => "n" })
    withServer(routes) { server =>
      val stream = Tcp.connect("127.0.0.1", server.port)
      try {
        stream.write(ByteBuffer.wrap("GET /slow HTTP/1.1\r\nHost: x\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1)))
        AsyncOperations.sleep(150) // the watch has begun, and reads the next request as it arrives
        stream.write(
          ByteBuffer.wrap(
            "GET /n HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1)
          )
        )
        val out = new java.io.ByteArrayOutputStream()
        val buf = ByteBuffer.allocate(4096)
        var open = true
        while open do {
          buf.clear()
          val n =
            try stream.read(buf, 5.seconds)
            catch case _: TimeoutException | _: IOException => -1
          if n < 0 then open = false else out.write(buf.array(), 0, n)
        }
        val text = new String(out.toByteArray, StandardCharsets.ISO_8859_1)
        assertEquals(text.split("HTTP/1.1 200 OK", -1).length - 1, 2, text)
        assert(text.indexOf("done") < text.lastIndexOf("\r\n\r\nn"), text)
      } finally stream.close()
      assertEquals(within(5.seconds)(outcome), "finished")
    }
  }

  test("requests that finish before the watch begins are answered as before, many on one connection") {
    withServer(Routes(GET("/n") { _ => "n" })) { server =>
      val client = new HttpClient()
      try {
        for _ <- 1 to 200 do assertEquals(client.get(server.url.resolve("/n")).text(), "n")
        assertEquals(server.connections, 1)
      } finally client.close()
    }
  }
}
