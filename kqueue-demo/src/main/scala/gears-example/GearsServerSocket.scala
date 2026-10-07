package gearsexample

import gears.async.Async
import gears.async.Future

import java.io.ByteArrayOutputStream
import java.io.IOException
import java.nio.charset.StandardCharsets
import scala.scalanative.unsafe.Zone

import asyncio.Address
import asyncio.HandleSlot
import asyncio.Reactor
import asyncio.unsafe.NativeBuffer
import example.KQueueExampleIO.{StreamProtocol, drainTo}
import ReactorFutures.*

/** Mirror of `example.KQueueExampleServerSocket`. The acceptor is a loop, and each connection is its own future whose
  * state machine is now plain sequential code.
  */
object GearsServerSocket {
  private val Response = "Just pinging back!\n".getBytes(StandardCharsets.UTF_8)

  /** Reads a length-framed request through one 8 KiB buffer, replies, and closes the connection. */
  private def serve(r: Reactor, client: Int)(using Async): Unit = {
    val zone = Zone.open()
    try {
      val buf = NativeBuffer.allocate(8192)(using zone)
      // One repeatable op per direction for the whole connection; the buffer's limit says how much each read takes.
      val read = r.ops.read(client, buf)
      val write = r.ops.write(client, buf)

      /** Reads until the buffer's limit is reached; false at an unexpected EOF. */
      def fill(): Boolean = {
        var open = true
        while open && buf.hasRemaining() do {
          val before = buf.position()
          perform(r, read)
          open = buf.position() > before // a read that adds nothing is the end of the stream
        }
        if !open then println(s"Unexpected EOF from client socket $client.")
        open
      }

      buf.clear().limit(StreamProtocol.HeaderLength)
      if fill() then {
        val size = StreamProtocol.messageLength(buf)
        if size < 0 || size > StreamProtocol.MaxMessageLength then
          println(s"Invalid message size from client socket $client: $size")
        else {
          // Gather the body one buffer's worth at a time.
          val body = new ByteArrayOutputStream()
          var remaining = size
          var open = true
          while open && remaining > 0 do {
            buf.clear().limit(math.min(remaining, buf.capacity()))
            open = fill()
            buf.flip()
            remaining -= buf.remaining()
            drainTo(buf, body)
          }
          if open then {
            println(s"message contents: `${body.toString(StandardCharsets.UTF_8)}`")
            buf.clear()
            buf.put(Response)
            buf.flip()
            while buf.hasRemaining() do perform(r, write)
          }
        }
      }
    } catch {
      case e: IOException =>
        // One misbehaving client must not take the server down.
        println(s"Client socket $client failed: ${e.getMessage}")
    } finally {
      zone.close()
      r.handles.close(client)
      println(s"Closed client socket: $client")
    }
  }

  def run(sock: String)(using Reactor.Factory[Reactor]): Unit = run(Address.Unix(sock))

  def run(address: Address)(using Reactor.Factory[Reactor]): Unit = {
    ReactorFutures.run { r =>
      val server = r.handles.listen(address)
      try {
        println("Socket is now listening for connections.")
        val accepted = HandleSlot() // each accept writes its connection here, like a read fills a buffer
        val accept = r.ops.accept(server, accepted) // one repeatable op, resubmitted for every connection
        while true do {
          perform(r, accept) // completes once a connection has been accepted
          val client = accepted.clear()
          println(s"Accepted new client connection: $client")
          Future(serve(r, client))
        }
      } finally r.handles.close(server)
    }
  }
}
