package gears.async.asyncio.examples

import gears.async.*
import gears.async.asyncio.Error
import gears.async.net.{SocketOption, TcpSupport}

import java.net.InetSocketAddress
import java.nio.ByteBuffer

def tcpEchoExample(): Unit = withUring:
  given TcpSupport = summon[gears.async.asyncio.uring.UringPerThreadSupport].tcpSupport
  Async.blocking:
    val listener = orFail("listen")(TcpSupport.listen(InetSocketAddress("127.0.0.1", 0), SocketOption.reuseAddr(true)))
    println(s"echo server listening on ${listener.localAddress}")
    try
      val server = Future:
        val connection = orFail("accept")(listener.accept())
        println(s"server accepted a connection from ${connection.remoteAddress}")
        try
          val buf = ByteBuffer.allocate(1024)
          var echoed = 0
          while connection.readBuf(buf) != Left(Error.EOF) do
            buf.flip()
            echoed += buf.remaining()
            orFail("echo")(connection.writeBuf(buf))
            buf.clear()
          println(s"server echoed $echoed bytes, then saw the client half-close its side")
        finally connection.close()

      val client = orFail("connect")(TcpSupport.connect(listener.localAddress, SocketOption.noDelay(true)))
      try
        val messages = Seq("hello ", "from ", "gears")
        messages.foreach(message => orFail("write")(client.writeBuf(bytes(message))))
        client.shutdownOutput()
        val reply = readToEnd(client)
        println(s"client sent ${messages.size} writes and read back '$reply'")
        check(reply == messages.mkString, "echo matches")
      finally client.close()
      server.await
    finally listener.close()
