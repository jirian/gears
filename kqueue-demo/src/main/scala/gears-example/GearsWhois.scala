package gearsexample

import java.nio.charset.StandardCharsets
import scala.scalanative.unsafe.Zone

import asyncio.Address
import asyncio.AddressFamily
import asyncio.Reactor
import asyncio.ResolvedAddress
import asyncio.Slot
import asyncio.unsafe.NativeBuffer
import example.KQueueExampleIO.text
import ReactorFutures.*

/** Mirror of `example.KQueueExampleWhois`: resolve, connect to port 43, send one query line, print the raw reply. */
object GearsWhois {
  def run(host: String, port: Int, query: String)(using Reactor.Factory[Reactor]): Unit = {
    Zone.acquire { implicit z =>
      val buf = NativeBuffer.allocate(8192)
      ReactorFutures.run { r =>
        val addresses = Slot[List[ResolvedAddress]]()
        perform(r, r.ops.resolve(host, addresses)) // a failed lookup throws
        val resolvedAll = addresses.clear()
        val resolved = resolvedAll.head
        val address = resolved.family match {
          case AddressFamily.IPv6 => Address.IPv6(resolved.host, port)
          case AddressFamily.IPv4 => Address.IPv4(resolved.host, port)
        }
        println(s"resolved $host to $address (${resolvedAll.length} addresses)")
        val fd = r.handles.connect(address)
        try {
          perform(r, r.ops.connect(fd))
          val write = r.ops.write(fd, buf)
          val read = r.ops.read(fd, buf)
          println(s"connected; sending query `$query`")
          buf.clear()
          buf.put(s"$query\r\n".getBytes(StandardCharsets.UTF_8))
          buf.flip()
          while buf.hasRemaining() do perform(r, write)
          var received = 0L
          var eof = false
          while !eof do {
            buf.clear()
            perform(r, read)
            if buf.position() == 0 then eof = true // nothing added: the server closed the connection
            else {
              received += buf.position()
              buf.flip()
              print(text(buf)) // Raw reply text, exactly as received.
            }
          }
          println(s"--- server closed the connection after $received bytes")
        } finally r.handles.close(fd)
      }
    }
  }

  /** Resolves a host through the reactor's blocker pool and prints every address. */
  def resolve(host: String)(using Reactor.Factory[Reactor]): Unit = {
    ReactorFutures.run { r =>
      val addresses = Slot[List[ResolvedAddress]]()
      perform(r, r.ops.resolve(host, addresses)) // a failed lookup throws
      val resolvedAll = addresses.clear()
      resolvedAll.foreach { a =>
        val family = if a.family == AddressFamily.IPv4 then "IPv4" else "IPv6"
        println(s"$family ${a.host}")
      }
      println(s"resolved $host to ${resolvedAll.length} addresses")
    }
  }
}
