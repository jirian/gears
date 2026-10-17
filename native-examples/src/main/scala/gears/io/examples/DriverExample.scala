package gears.io.examples

import gears.async.*
import gears.async.default.given
import gears.asyncio.uring.UringReactor
import gears.io.net.{DnsSupport, TcpSupport}
import gears.io.reactor.{SingleReactorDriver, dnsSupport, tcpSupport}

import java.net.InetSocketAddress

def driverExample(): Unit =
  val driver = SingleReactorDriver(() => UringReactor.open(), threadName = "example-reactor")
  try
    given TcpSupport = driver.tcpSupport
    given DnsSupport = driver.dnsSupport
    Async.blocking:
      println(s"fibers run on the default gears scheduler, here ${Thread.currentThread().getName}")
      println(s"localhost resolves to ${DnsSupport.resolve("localhost").mkString(", ")}")
      val listener = orFail("listen")(TcpSupport.listen(InetSocketAddress("127.0.0.1", 0)))
      try
        val server = Future:
          val connection = orFail("accept")(listener.accept())
          try
            orFail("write")(connection.writeBuf(bytes("served through SingleReactorDriver")))
            connection.shutdownOutput()
          finally connection.close()
        val client = orFail("connect")(TcpSupport.connect(listener.localAddress))
        try println(s"client read '${readToEnd(client)}', with every op run by the driver's reactor thread")
        finally client.close()
        server.await
      finally listener.close()
  finally driver.close()
