package gears.async.asyncio.examples

import gears.async._
import gears.async.net.TcpSupport
// import gears.async.asyncio.kqueue.ForkJoinKqueueSupport ?
import gears.async.asyncio.uring.UringPerThreadSupport

import java.net.InetSocketAddress
import scala.scalanative.meta.LinktimeInfo

private def runServer(port: Int, backendName: String)(using Async, TcpSupport): Unit =
  val listener = TcpSupport.listen(InetSocketAddress("0.0.0.0", port)) match
    case Right(l) => l
    case Left(e)  => throw new RuntimeException(s"listen failed: $e")
  println(s"listening on http://127.0.0.1:$port ($backendName)")
  serve(listener)

@main def httpServer(): Unit =
  val port = 8080
  if LinktimeInfo.isLinux then
    given support: UringPerThreadSupport = UringPerThreadSupport()
    given tcp: TcpSupport = support.tcpSupport
    Async.blocking(runServer(port, "uring"))
  else if LinktimeInfo.isMac then
    throw "kqueue not implemented"
    // given support: ForkJoinKqueueSupport = ForkJoinKqueueSupport()
    // given tcp: TcpSupport = support.tcpSupport
    // Async.blocking(runServer(port, "kqueue"))
  else
    throw new UnsupportedOperationException("no TCP backend wired up for this platform")
