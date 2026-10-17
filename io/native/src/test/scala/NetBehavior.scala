import gears.async.*
import gears.io.Error
import gears.io.net.{DnsSupport, TcpStream, TcpSupport, UdpSupport}
import gears.io.reactor.UnixSocketAddress

import java.net.{InetSocketAddress, UnknownHostException}
import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousCloseException, ClosedChannelException}
import java.nio.charset.StandardCharsets
import scala.concurrent.duration.*

abstract class NetBehavior(driverName: String) extends munit.FunSuite:
  protected given tcp: TcpSupport
  protected given udp: UdpSupport
  protected def dns: DnsSupport
  protected given operations: AsyncOperations

  protected def run[A](body: Async.Spawn ?=> A): A

  protected def bytes(s: String) = ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8))
  protected def text(buf: ByteBuffer) =
    buf.flip()
    val out = new Array[Byte](buf.remaining())
    buf.get(out)
    new String(out, StandardCharsets.UTF_8)

  protected def readAll(stream: TcpStream)(using Async): String =
    val all = ByteBuffer.allocate(1024)
    while stream.readBuf(all) != Left(Error.EOF) do ()
    text(all)

  protected def echo(address: java.net.SocketAddress)(using Async.Spawn): Unit =
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
        client.writeBuf(bytes("ping over the reactor"))
        client.shutdownOutput()
        assertEquals(readAll(client), "ping over the reactor")
      finally client.close()
      server.await
    finally listener.close()

  test(s"$driverName: a client that resets before being accepted does not fail accept"):
    run:
      val listener = TcpSupport.listen(InetSocketAddress("127.0.0.1", 0)).toOption.get
      try
        val reset = TcpSupport.connect(listener.localAddress, gears.io.net.SocketOption.linger(0)).toOption.get
        reset.close()
        AsyncOperations.sleep(50)
        val client = TcpSupport.connect(listener.localAddress).toOption.get
        try
          val accepted = listener.accept().toOption.get
          try
            accepted.writeBuf(bytes("served"))
            accepted.shutdownOutput()
            assertEquals(readAll(client), "served")
          finally accepted.close()
        finally client.close()
      finally listener.close()

  test("TCP: listen, connect, accept, and echo over IPv4 loopback"):
    run:
      echo(InetSocketAddress("127.0.0.1", 0))

  test("TCP: the same over a Unix domain socket"):
    run:
      echo(UnixSocketAddress(s"/tmp/gears-net-$driverName-${scala.scalanative.posix.unistd.getpid()}.sock"))

  protected def udpRoundTrip(host: String)(using Async): Unit =
    val a = UdpSupport.bind(InetSocketAddress(host, 0)).toOption.get
    val b = UdpSupport.bind(InetSocketAddress(host, 0)).toOption.get
    try
      a.sendTo(bytes("hello"), b.localAddress)
      val got = ByteBuffer.allocate(64)
      val from = b.receiveFrom(got).toOption.get
      assertEquals(text(got), "hello")
      assertEquals(from, a.localAddress)
      b.sendTo(bytes("hello back"), from)
      val reply = ByteBuffer.allocate(64)
      assertEquals(a.receiveFrom(reply).toOption.get, b.localAddress)
      assertEquals(text(reply), "hello back")
    finally
      a.close()
      b.close()

  test("TCP: the same over IPv6 loopback"):
    run:
      echo(InetSocketAddress("::1", 0))

  test("UDP: a datagram and its reply carry the right senders, over IPv4"):
    run:
      udpRoundTrip("127.0.0.1")

  test("UDP: the same over IPv6"):
    run:
      udpRoundTrip("::1")

  test("closing a listener fails a pending accept instead of leaving it hanging"):
    run:
      val listener = TcpSupport.listen(InetSocketAddress("127.0.0.1", 0)).toOption.get
      val accepting = Future(listener.accept())
      AsyncOperations.sleep(50)
      listener.close()
      val outcome = accepting.awaitResult
      assert(outcome.failed.toOption.exists(_.isInstanceOf[AsynchronousCloseException]), outcome)

  test("closing a stream fails a pending read, and later I/O on it is refused"):
    run:
      val listener = TcpSupport.listen(InetSocketAddress("127.0.0.1", 0)).toOption.get
      try
        val client = TcpSupport.connect(listener.localAddress).toOption.get
        val server = listener.accept().toOption.get
        try
          val reading = Future(server.readBuf(ByteBuffer.allocate(16)))
          AsyncOperations.sleep(50)
          server.close()
          val outcome = reading.awaitResult
          assert(outcome.failed.toOption.exists(_.isInstanceOf[AsynchronousCloseException]), outcome)
          intercept[ClosedChannelException](server.writeBuf(bytes("too late")))
          intercept[ClosedChannelException](server.shutdownOutput())
        finally client.close()
      finally listener.close()

  test("host names resolve through the reactor, and an unknown one fails cleanly"):
    run:
      assert(dns.resolve("localhost").nonEmpty)
      intercept[UnknownHostException](dns.resolve("this-host-does-not-exist.invalid"))

  test("a stream rejects overlapping reads while still allowing a simultaneous write"):
    run:
      val listener = TcpSupport.listen(InetSocketAddress("127.0.0.1", 0)).toOption.get
      val client = TcpSupport.connect(listener.localAddress).toOption.get
      val server = listener.accept().toOption.get
      try
        val first = Future(server.readBuf(ByteBuffer.allocate(8)))
        AsyncOperations.sleep(30)
        intercept[gears.io.ConcurrentReadException](server.readBuf(ByteBuffer.allocate(1)))
        server.writeBuf(bytes("duplex"))
        val got = ByteBuffer.allocate(16)
        client.readBuf(got)
        assertEquals(text(got), "duplex")
        client.writeBuf(bytes("reply"))
        assertEquals(first.await, Right(()))
      finally
        client.close()
        server.close()
        listener.close()

  test("a timeout cancels a pending read submitted from outside the reactor thread"):
    run:
      val listener = TcpSupport.listen(InetSocketAddress("127.0.0.1", 0)).toOption.get
      val client = TcpSupport.connect(listener.localAddress).toOption.get
      val server = listener.accept().toOption.get
      try
        val outcome = withTimeoutOption(100.millis)(server.readBuf(ByteBuffer.allocate(16)))
        assertEquals(outcome, None)
        client.writeBuf(bytes("still usable"))
        val buf = ByteBuffer.allocate(32)
        server.readBuf(buf)
        assertEquals(text(buf), "still usable")
      finally
        client.close()
        server.close()
        listener.close()
