package gears.async.asyncio.kqueue.examples

import gears.async._
import gears.async.net.TcpSupport
import gears.async.asyncio.examples.serve
import gears.async.asyncio.kqueue.ForkJoinKqueueSupport

import java.net.InetSocketAddress

/** Thin, backend-specific entry point - see
  * `gears.async.asyncio.uring.examples.uringHttpServer` for the Linux/uring
  * counterpart. Both hand off to the exact same `serve` logic; only the
  * `Scheduler`/`TcpSupport` wiring differs. UNVERIFIED, see
  * [[gears.async.asyncio.kqueue.KqueuePoller]] - this repo has no macOS
  * toolchain to compile/run it against.
  */
@main def kqueueHttpServer(): Unit =
  given support: ForkJoinKqueueSupport = ForkJoinKqueueSupport()
  given tcp: TcpSupport = support.tcpSupport

  val port = 8080
  Async.blocking:
    val listener = TcpSupport.listen(InetSocketAddress("0.0.0.0", port)) match
      case Right(l) => l
      case Left(e)  => throw new RuntimeException(s"listen failed: $e")
    println(s"listening on http://127.0.0.1:$port (kqueue)")
    serve(listener)
