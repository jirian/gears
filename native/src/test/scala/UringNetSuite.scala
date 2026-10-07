import gears.async.*
import gears.async.asyncio.Error
import gears.async.asyncio.uring.{UnixSocketAddress, UringPerThreadSupport}
import gears.async.net.{TcpStream, TcpSupport, UdpSupport}

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

/** The gears network layer on the uring backend, end to end: sockets created, bound and connected by
  * `UringTcpSupport`/`UringUdpSupport`, and I/O through the shard reactors.
  */
class UringNetSuite extends munit.FunSuite:
  private lazy val support = UringPerThreadSupport(parallelism = 2)
  private given UringPerThreadSupport = support
  private given TcpSupport = support.tcpSupport
  private given UdpSupport = support.udpSupport

  private def bytes(s: String) = ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8))
  private def text(buf: ByteBuffer) =
    buf.flip()
    val out = new Array[Byte](buf.remaining())
    buf.get(out)
    new String(out, StandardCharsets.UTF_8)

  /** Reads until end of stream. */
  private def readAll(stream: TcpStream)(using Async): String =
    val all = ByteBuffer.allocate(1024)
    while stream.readBuf(all) != Left(Error.EOF) do ()
    text(all)

  /** A server that echoes one connection's bytes back until the client half-closes, then a client that checks it. */
  private def echo(address: java.net.SocketAddress)(using Async.Spawn): Unit =
    val listener = TcpSupport.listen(address).toOption.get
    try
      val server = Future:
        val conn = listener.accept().toOption.get
        try
          val buf = ByteBuffer.allocate(64)
          while conn.readBuf(buf) != Left(Error.EOF) do
            buf.flip()
            conn.writeBuf(buf)
            buf.clear()
        finally conn.close()
      val client = TcpSupport.connect(listener.localAddress).toOption.get
      try
        client.writeBuf(bytes("ping over uring"))
        client.shutdownOutput()
        assertEquals(readAll(client), "ping over uring")
      finally client.close()
      server.await
    finally listener.close()

  test("TCP: listen, connect, accept, and echo over IPv4 loopback"):
    Async.blocking:
      echo(InetSocketAddress("127.0.0.1", 0))

  test("TCP: the same over a Unix domain socket"):
    Async.blocking:
      echo(UnixSocketAddress(s"/tmp/gears-uring-net-${scala.scalanative.posix.unistd.getpid()}.sock"))

  test("UDP: a datagram and its reply carry the right senders"):
    Async.blocking:
      val a = UdpSupport.bind(InetSocketAddress("127.0.0.1", 0)).toOption.get
      val b = UdpSupport.bind(InetSocketAddress("127.0.0.1", 0)).toOption.get
      try
        a.sendTo(bytes("hello"), b.localAddress)
        val got = ByteBuffer.allocate(64)
        val from = b.receiveFrom(got).toOption.get
        assertEquals(text(got), "hello")
        assertEquals(from, a.localAddress)
      finally
        a.close()
        b.close()
