package gears.io.examples

import gears.async.*
import gears.io.net.{TcpSupport, UdpSupport}
import gears.io.reactor.UnixSocketAddress
import gears.io.uring.UringPerThreadSupport

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

def unixSocketExample(): Unit = withUring:
  val support = summon[UringPerThreadSupport]
  given TcpSupport = support.tcpSupport
  given UdpSupport = support.udpSupport
  val stem = s"/tmp/gears-unix-example-${scala.scalanative.posix.unistd.getpid()}"
  Async.blocking:
    val streamPath = s"$stem-stream.sock"
    val listener = orFail("listen")(TcpSupport.listen(UnixSocketAddress(streamPath)))
    println(s"stream listener bound at $streamPath, socket file exists: ${Files.exists(Paths.get(streamPath))}")
    try
      val server = Future:
        val connection = orFail("accept")(listener.accept())
        try
          orFail("write")(connection.writeBuf(bytes("over a Unix stream socket")))
          connection.shutdownOutput()
        finally connection.close()
      val client = orFail("connect")(TcpSupport.connect(UnixSocketAddress(streamPath)))
      try println(s"client read '${readToEnd(client)}'")
      finally client.close()
      server.await
    finally listener.close()
    println(s"after close, socket file exists: ${Files.exists(Paths.get(streamPath))}")

    val first = orFail("bind")(UdpSupport.bind(UnixSocketAddress(s"$stem-a.sock")))
    val second = orFail("bind")(UdpSupport.bind(UnixSocketAddress(s"$stem-b.sock")))
    try
      orFail("send")(first.sendTo(bytes("a Unix datagram"), second.localAddress))
      val received = ByteBuffer.allocate(256)
      val sender = orFail("receive")(second.receiveFrom(received))
      received.flip()
      println(s"datagram socket received '${StandardCharsets.UTF_8.decode(received)}' from $sender")
      check(sender == first.localAddress, "sender matches the first socket's path")
    finally
      first.close()
      second.close()
