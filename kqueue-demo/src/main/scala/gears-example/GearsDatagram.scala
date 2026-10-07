package gearsexample

import gears.async.Async
import gears.async.Future

import java.io.IOException
import java.nio.charset.StandardCharsets
import scala.scalanative.unsafe.Zone

import asyncio.Address
import asyncio.Reactor
import asyncio.Slot
import asyncio.unsafe.NativeBuffer
import example.KQueueExampleIO.text
import ReactorFutures.*

/** Mirrors of `example.KQueueExampleDatagramSocket` and `example.KQueueExampleDatagramServerSocket`, using the
  * reactor's `receive` and `send` ops.
  */
object GearsDatagram {

  // Client

  def run(sock: String, localSock: String, message: String)(using Reactor.Factory[Reactor]): Unit = {
    require(sock != localSock, "The client and server socket paths must be different")
    // Unix datagram clients need their own bound address for the server's reply.
    exchange(Address.Unix(localSock), Address.Unix(sock), message)
  }

  def run(address: Address, message: String)(using Reactor.Factory[Reactor]): Unit = {
    require(!address.isInstanceOf[Address.Unix], "Unix datagrams need a local socket path")
    // Connecting an IP datagram socket also assigns it an ephemeral local port.
    exchange(null, address, message)
  }

  private def exchange(
      local: Address | Null,
      remote: Address,
      message: String
  )(using Reactor.Factory[Reactor]): Unit = {
    Zone.acquire { implicit z =>
      val request = NativeBuffer.of(message.getBytes(StandardCharsets.UTF_8))
      val response = NativeBuffer.allocate(65536)
      ReactorFutures.run { r =>
        val socket = r.handles.datagram(local, remote)
        try {
          println(s"Connected datagram socket to $remote")
          // Scoped, so the exchange is cancelled and settled before the socket is closed.
          Async.group {
            val ping = Future {
              val send = r.ops.send(socket, request, null) // resubmitted until the packet has gone
              perform(r, send) // completes once the whole packet has gone
              println(s"Sent datagram of ${request.limit()} bytes.")
              val receive = r.ops.receive(socket, response, Slot[Address]()) // resubmitted until a packet arrives
              perform(r, receive) // completes once a packet has arrived
              println(s"Received datagram of ${response.remaining()} bytes: `${text(response)}`")
            }
            // UDP has no delivery guarantee; make a missing reply visible in the demo.
            val deadline = Future(perform(r, r.ops.timer(5000)))
            Async.select(
              ping.handle(_.get),
              deadline.handle(_ => throw new IOException("Timed out waiting for datagram socket readiness or a reply"))
            )
          }
        } finally r.handles.close(socket)
      }
    }
  }

  // Server

  /** Echoes each packet to its sender, keeping the packet until the whole echo has gone. */
  def serve(address: Address)(using Reactor.Factory[Reactor]): Unit = {
    Zone.acquire { implicit z =>
      val packet = NativeBuffer.allocate(65536)
      ReactorFutures.run { r =>
        val socket = r.handles.datagram(address, null)
        try {
          println("Datagram socket is now waiting for messages.")
          val sender = Slot[Address]() // each packet's sender, written alongside the packet itself
          val receive = r.ops.receive(socket, packet, sender) // one repeatable op for every packet
          while true do {
            perform(r, receive)
            val size = packet.remaining()
            println(s"Received datagram of $size bytes: `${text(packet)}`")
            val reply = r.ops.send(socket, packet, sender.clear()) // a new op per sender
            perform(r, reply)
            println(s"Replied with datagram of $size bytes.")
          }
        } finally r.handles.close(socket)
      }
    }
  }
}
