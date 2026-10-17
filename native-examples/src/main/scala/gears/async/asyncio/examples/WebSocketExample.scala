package gears.async.asyncio.examples

import gears.async.*
import gears.async.asyncio.uring.UringPerThreadSupport
import gears.async.http.*
import gears.async.net.{DnsSupport, TcpSupport}

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import scala.concurrent.duration.*

def webSocketExample(): Unit = withUring:
  val support = summon[UringPerThreadSupport]
  given TcpSupport = support.tcpSupport
  given DnsSupport = support.dnsSupport
  val router = Router().handle("/echo")(
    WebSocket.handler(
      socket =>
        var message = socket.receive()
        while message.nonEmpty do
          socket.send(message.get)
          message = socket.receive()
        println(s"server: the client closed with ${socket.peerClose}")
      ,
      subprotocols = Seq("echo")
    )
  )
  Async.blocking:
    val listener = orFail("listen")(TcpSupport.listen(InetSocketAddress("127.0.0.1", 0)))
    val port = listener.localAddress.asInstanceOf[InetSocketAddress].getPort
    val server = Server(router)
    val serving = Future(server.serve(listener))
    try
      WebSocket
        .resource(s"ws://127.0.0.1:$port/echo", subprotocols = Seq("echo"))
        .use: socket =>
          println(s"client: connected to ws://127.0.0.1:$port/echo, subprotocol ${socket.subprotocol}")
          socket.sendText("hello over a WebSocket")
          println(s"client: echoed ${socket.receive()}")
          socket.sendBinary("binary frame".getBytes(StandardCharsets.UTF_8))
          socket.receive() match
            case Some(WebSocket.Message.Binary(bytes)) =>
              println(s"client: echoed ${bytes.length} binary bytes: '${new String(bytes, StandardCharsets.UTF_8)}'")
            case other => fail(s"client: expected a binary echo, got $other")
          socket.ping("are you there".getBytes(StandardCharsets.UTF_8))
          socket.sendClose(1000, "done")
          println(
            s"client: after the close handshake, receive gives ${socket.receive()}, peer closed with ${socket.peerClose}"
          )
    finally
      server.shutdown(1.second)
      serving.awaitResult
