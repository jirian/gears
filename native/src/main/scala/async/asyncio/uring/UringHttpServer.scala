package gears.async.asyncio.uring.examples

import gears.async._
import gears.async.net.TcpSupport
import gears.async.asyncio.examples.serve
import gears.async.asyncio.uring.ForkJoinUringSupport

import java.net.InetSocketAddress

/** Thin, backend-specific entry point: wires up the uring `Scheduler`/
  * `TcpSupport` and hands off to the shared, backend-agnostic server logic
  * in `gears.async.asyncio.examples`.
  */
@main def uringHttpServer(): Unit =
  given support: ForkJoinUringSupport = ForkJoinUringSupport()
  given tcp: TcpSupport = support.tcpSupport

  val port = 8080
  Async.blocking:
    val listener = TcpSupport.listen(InetSocketAddress("0.0.0.0", port)) match
      case Right(l) => l
      case Left(e)  => throw new RuntimeException(s"listen failed: $e")
    println(s"listening on http://127.0.0.1:$port (uring)")
    serve(listener)
