package gearsexample

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import scala.scalanative.unsafe.Zone

import asyncio.Address
import asyncio.Reactor
import asyncio.unsafe.NativeBuffer
import example.KQueueExampleIO.{StreamProtocol, drainTo}
import ReactorFutures.*

/** Mirror of `example.KQueueExampleSocket`: connect, send one framed request, collect the response until EOF. */
object GearsSocket {
  def run(sock: String)(using Reactor.Factory[Reactor]): Unit = run(Address.Unix(sock))

  def run(address: Address)(using Reactor.Factory[Reactor]): Unit = {
    Zone.acquire { implicit z =>
      val buf = NativeBuffer.allocate(8192)
      val response = new ByteArrayOutputStream()
      ReactorFutures.run { r =>
        val socket = r.handles.connect(address)
        try {
          println(s"connection in progress to: $address")
          perform(r, r.ops.connect(socket))
          // One repeatable op per direction, resubmitted until the request is out and the response is in.
          val write = r.ops.write(socket, buf)
          val read = r.ops.read(socket, buf)
          StreamProtocol.frame("Hello from Scala Native KQueue Example!\n", buf)
          while buf.hasRemaining() do {
            val before = buf.position()
            perform(r, write)
            println(s"Wrote ${buf.position() - before} bytes to socket.")
          }
          buf.clear() // The request is out; the buffer now collects the response.
          var eof = false
          while !eof do {
            perform(r, read)
            if buf.position() == 0 then eof = true // Nothing added: the server closed after its response.
            else {
              println(s"Read ${buf.position()} bytes from socket.")
              buf.flip()
              drainTo(buf, response)
              buf.clear()
            }
          }
          println(s"Received message: `${new String(response.toByteArray, StandardCharsets.UTF_8)}`")
        } finally r.handles.close(socket)
      }
    }
  }
}
