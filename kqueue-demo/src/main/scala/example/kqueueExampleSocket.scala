package example

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import scala.scalanative.unsafe.Zone

import asyncio.Completion
import asyncio.kqueue.Command
import asyncio.kqueue.Connect
import asyncio.kqueue.KqueueReactor
import asyncio.kqueue.ReadIntoBuffer
import asyncio.kqueue.WriteFromBuffer
import asyncio.unsafe.NativeBuffer
import asyncio.unsafe.Sockets.Transport
import KQueueExampleIO.*

object KQueueExampleSocket {

  /** Connects, sends one framed request, then reuses the same buffer to collect the response until the server closes
    * the connection. Built entirely from the library's commands.
    */
  final class Exchange(r: KqueueReactor, fd: Int, message: String)(using Zone) extends Completion[Command] {
    private val buf = NativeBuffer.allocate(8192)
    private val response = new ByteArrayOutputStream()

    private var written = 0 // how much of the request the buffer's position had reached before the latest write

    def responseMessage: String = new String(response.toByteArray, StandardCharsets.UTF_8)

    def start(): Unit = r.submit(Connect(fd), this)

    def onComplete(command: Command): Unit = command match {
      case Connect(_) =>
        StreamProtocol.frame(message, buf)
        r.submit(WriteFromBuffer(fd, buf), this)
      case write: WriteFromBuffer =>
        println(s"Wrote ${buf.position() - written} bytes to socket.")
        written = buf.position()
        if buf.hasRemaining() then r.submit(write, this)
        else {
          buf.clear() // The request is out; the buffer now collects the response.
          r.submit(ReadIntoBuffer(fd, buf), this)
        }
      case read: ReadIntoBuffer =>
        if buf.position() == 0 then {
          // Nothing added: end of stream. The server closes the connection after its response.
          println(s"Received message: `$responseMessage`")
          r.stop()
        } else {
          println(s"Read ${buf.position()} bytes from socket.")
          buf.flip()
          drainTo(buf, response)
          buf.clear() // Each chunk is copied out, so the buffer starts over.
          r.submit(read, this)
        }
      case _ => ()
    }
  }

  def run(sock: String): Unit = run(KQueueExampleAddress.Unix(sock))

  def run(address: KQueueExampleAddress): Unit = {
    KqueueReactor.scoped() { r =>
      address.withSocket(Transport.Stream) { clientFd =>
        println(s"opened file descriptor: $clientFd (socket-type)")
        address.connect(clientFd)
        println(s"connection in progress to: $address")
        Zone.acquire { implicit z =>
          val exchange = Exchange(r, clientFd, "Hello from Scala Native KQueue Example!\n")
          exchange.start()
          r.run()
        }
      }
    }
  }
}
