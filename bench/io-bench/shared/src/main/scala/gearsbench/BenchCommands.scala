package gearsbench

import gears.async.io.Backend
import gears.async.io.IpAddress
import gears.async.io.IpSocketAddress
import gears.async.io.IpStreams
import gears.async.io.StreamSockets
import gears.async.io.UnixAddress
import gears.async.io.UnixStreams
import gears.io.demos.{BackendChoice, DemoBackends, DemoRunner, Endpoint}

import mainargs.{arg, main}

/** The gears bench's command line, the same for every backend: `bench` and `bench-serve` over the backends a demo
  * application offers (`DemoBackends`), whose runners start the bench's runs, so that the scenarios, like the demos,
  * only take an `Io`. Each platform's `Main` extends it with its own commands, and runs it as `<executable> gears
  * <command>`, which is what bench/compare.sc runs.
  */
class BenchCommands(backends: DemoBackends) {

  @main(doc = "Stress tests to measure reactor changes against; see bench/compare.sc")
  def bench(
      @arg(doc = "comma-separated scenarios to run, or all of them when empty") scenarios: String = "",
      @arg(doc = "multiplies every scenario's operation count") scale: Double = 1.0,
      @arg(doc = "connections in the concurrent scenario") connections: Int = 64,
      @arg(doc = "use TCP on --host at --port instead of a Unix socket") tcp: mainargs.Flag,
      @arg(doc = "the TCP address: 127.0.0.1, or this machine's, or with --no-server another machine's") host: String =
        "127.0.0.1",
      @arg port: Int = 19876,
      @arg(doc =
        "connect to an echo server already listening (`bench-serve`), instead of starting one"
      ) noServer: mainargs.Flag,
      @arg(doc = "worker threads for the clients, and as many again for the server") workers: Int = 1,
      @arg(doc =
        "run every scenario once at a fifth of its count first, so the JIT has compiled it"
      ) warmup: mainargs.Flag,
      choice: BackendChoice
  ): Unit = {
    val names = scenarios.split(',').toSeq.map(_.trim).filter(_.nonEmpty)
    def run[B <: StreamSockets](endpoint: Endpoint[B])(using BenchRunner[B]): Unit =
      GearsBench.run(
        names,
        scale,
        connections,
        endpoint,
        workers,
        warmup = warmup.value,
        server = !noServer.value
      )
    if tcp.value then
      run(Endpoint.Ip(address(host, port)))(using
        bench(backends.needing(choice.backend, "IP stream sockets") { case b: IpStreams => b })
      )
    else
      run(Endpoint.Unix(UnixAddress(s"/tmp/gears-bench-${System.nanoTime() % 1_000_000}.sock")))(using
        bench(backends.needing(choice.backend, "Unix stream sockets") { case b: UnixStreams => b })
      )
  }

  @main(name = "bench-serve", doc = "The bench's echo server alone, for a bench on another machine with --no-server")
  def benchServe(
      @arg(doc = "the address to listen on: this machine's, or 0.0.0.0") host: String = "0.0.0.0",
      @arg port: Int = 19876,
      @arg(doc = "worker threads for the server") workers: Int = 1,
      choice: BackendChoice
  ): Unit =
    GearsBench.serveForever(Endpoint.Ip(address(host, port)), workers)(using
      bench(backends.needing(choice.backend, "IP stream sockets") { case b: IpStreams => b })
    )

  /** A numeric IPv4 address and a port. */
  private def address(host: String, port: Int): IpSocketAddress = {
    val parts = host.split('.').map(_.toInt)
    require(parts.length == 4, s"Not a numeric IPv4 address: $host")
    IpSocketAddress(IpAddress.ipv4(parts(0), parts(1), parts(2), parts(3)), port)
  }

  /** The runner, as the bench's: every backend a bench application offers has one. */
  private def bench[C <: Backend](runner: DemoRunner[C]): BenchRunner[C] = runner match {
    case b: BenchRunner[C] @unchecked => b
    case other                        => throw new IllegalArgumentException(s"$other cannot run the bench on a pool")
  }
}
