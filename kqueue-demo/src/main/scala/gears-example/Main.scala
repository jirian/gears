package gearsexample

import gears.asyncio.Address
import gears.asyncio.Reactor
import gears.asyncio.kqueue.KqueueReactor

import mainargs.{ParserForMethods, arg, main}

/** The gears mirrors, run as `kqueuedemo gears <command>` with the same commands and flags as the plain demos.
  *
  * This is the edge: the demos only know the `asyncio` interface, and this is the one place that chooses kqueue.
  */
object Main {
  given Reactor.Factory[Reactor] = () => KqueueReactor.open()

  @main
  def fileRead(@arg fifo: String): Unit = {
    GearsFileRead.run(fifo)
  }
  @main
  def fileWrite(@arg fifo: String, @arg(short = 'm') message: String): Unit = {
    GearsFileWrite.run(fifo, message)
  }
  @main
  def timer(): Unit = {
    GearsTimer.run()
  }
  @main
  def sockServe(@arg sock: String): Unit = {
    GearsServerSocket.run(sock)
  }
  @main
  def sock(@arg sock: String): Unit = {
    GearsSocket.run(sock)
  }
  @main(name = "sock4-serve")
  def sock4Serve(@arg host: String = "127.0.0.1", @arg port: Int = 9999): Unit = {
    GearsServerSocket.run(Address.IPv4(host, port))
  }
  @main(name = "sock4")
  def sock4(@arg host: String = "127.0.0.1", @arg port: Int = 9999): Unit = {
    GearsSocket.run(Address.IPv4(host, port))
  }
  @main(name = "sock6-serve")
  def sock6Serve(@arg host: String = "::1", @arg port: Int = 9999): Unit = {
    GearsServerSocket.run(Address.IPv6(host, port))
  }
  @main(name = "sock6")
  def sock6(@arg host: String = "::1", @arg port: Int = 9999): Unit = {
    GearsSocket.run(Address.IPv6(host, port))
  }
  @main
  def kvServe(@arg sock: String): Unit = {
    GearsKeyValue.serve(Address.Unix(sock))
  }
  @main
  def kv(@arg sock: String, @arg count: Int = 1000): Unit = {
    GearsKeyValue.run(Address.Unix(sock), count)
  }
  @main(name = "kv4-serve")
  def kv4Serve(@arg host: String = "127.0.0.1", @arg port: Int = 9999): Unit = {
    GearsKeyValue.serve(Address.IPv4(host, port))
  }
  @main(name = "kv4")
  def kv4(@arg host: String = "127.0.0.1", @arg port: Int = 9999, @arg count: Int = 1000): Unit = {
    GearsKeyValue.run(Address.IPv4(host, port), count)
  }
  @main(name = "kv6-serve")
  def kv6Serve(@arg host: String = "::1", @arg port: Int = 9999): Unit = {
    GearsKeyValue.serve(Address.IPv6(host, port))
  }
  @main(name = "kv6")
  def kv6(@arg host: String = "::1", @arg port: Int = 9999, @arg count: Int = 1000): Unit = {
    GearsKeyValue.run(Address.IPv6(host, port), count)
  }
  @main
  def datagramServe(@arg sock: String): Unit = {
    GearsDatagram.serve(Address.Unix(sock))
  }
  @main
  def datagram(
      @arg sock: String,
      @arg localSock: String,
      @arg(short = 'm') message: String = "Hello from Scala Native KQueue Datagram Example!"
  ): Unit = {
    GearsDatagram.run(sock, localSock, message)
  }
  @main(name = "datagram4-serve")
  def datagram4Serve(@arg host: String = "127.0.0.1", @arg port: Int = 9999): Unit = {
    GearsDatagram.serve(Address.IPv4(host, port))
  }
  @main(name = "datagram4")
  def datagram4(
      @arg host: String = "127.0.0.1",
      @arg port: Int = 9999,
      @arg(short = 'm') message: String = "Hello from Scala Native KQueue Datagram Example!"
  ): Unit = {
    GearsDatagram.run(Address.IPv4(host, port), message)
  }
  @main(name = "datagram6-serve")
  def datagram6Serve(@arg host: String = "::1", @arg port: Int = 9999): Unit = {
    GearsDatagram.serve(Address.IPv6(host, port))
  }
  @main(name = "datagram6")
  def datagram6(
      @arg host: String = "::1",
      @arg port: Int = 9999,
      @arg(short = 'm') message: String = "Hello from Scala Native KQueue Datagram Example!"
  ): Unit = {
    GearsDatagram.run(Address.IPv6(host, port), message)
  }
  @main
  def whois(
      @arg host: String = "whois.iana.org",
      @arg port: Int = 43,
      @arg query: String = "example.com"
  ): Unit = {
    GearsWhois.run(host, port, query)
  }
  @main
  def cancel(): Unit = {
    GearsCancel.run()
  }
  @main
  def resolve(@arg host: String = "localhost"): Unit = {
    GearsWhois.resolve(host)
  }
  @main(doc = "Stress tests to measure reactor changes against; see kqueue-demo/bench/compare.py")
  def bench(
      @arg(doc = "comma-separated scenarios to run, or all of them when empty") scenarios: String = "",
      @arg(doc = "multiplies every scenario's operation count") scale: Double = 1.0,
      @arg(doc = "connections in the concurrent scenario") connections: Int = 64,
      @arg(doc = "use TCP on 127.0.0.1 at --port instead of a Unix socket") tcp: mainargs.Flag,
      @arg port: Int = 19876
  ): Unit = {
    val address =
      if tcp.value then Address.IPv4("127.0.0.1", port)
      else Address.Unix(s"/tmp/gears-bench-${System.nanoTime() % 1_000_000}.sock")
    GearsBench.run(scenarios.split(',').toSeq.map(_.trim).filter(_.nonEmpty), scale, connections, address)
  }
  def main(args: Array[String]): Unit = {
    ParserForMethods(this).runOrExit(args.toIndexedSeq)
  }
}
