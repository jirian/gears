package gears.async.asyncio.examples

import gears.async._
import gears.async.net.TcpSupport
import gears.async.http.{HttpResponse, Router, serve}
import gears.async.asyncio.uring.UringPerThreadSupport

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import scala.scalanative.meta.LinktimeInfo

private def demoRouter(): Router =
  Router()
    .get("/")(_ => HttpResponse.text("Hello from gears!\n"))
    .get("/health")(_ => HttpResponse.text("ok\n"))
    .post("/echo")(req => HttpResponse.text(new String(req.body, StandardCharsets.UTF_8)))

private def runServer(port: Int, backendName: String)(using Async, TcpSupport): Unit =
  val listener = TcpSupport.listen(InetSocketAddress("0.0.0.0", port)) match
    case Right(l) => l
    case Left(e)  => throw new RuntimeException(s"listen failed: $e")
  println(s"listening on http://127.0.0.1:$port ($backendName)")
  serve(listener, demoRouter())

@main def httpServer(): Unit =
  val port = 8080
  if LinktimeInfo.isLinux then
    given support: UringPerThreadSupport = UringPerThreadSupport()
    given tcp: TcpSupport = support.tcpSupport
    Async.blocking(runServer(port, "uring"))
  else
    // The kqueue backend was removed as unverified/unmaintained (no macOS
    // toolchain in this repo to build or test it against); uring is
    // Linux-only. Until a macOS-capable backend exists, every non-Linux
    // platform is equally unsupported - no separate isMac branch to keep
    // silently doing nothing on macOS specifically.
    throw new UnsupportedOperationException("no TCP backend wired up for this platform")
