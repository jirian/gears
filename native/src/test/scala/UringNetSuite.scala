import gears.async.*
import gears.async.asyncio.Error
import gears.async.asyncio.uring.{UnixSocketAddress, UringPerThreadSupport}
import gears.async.net.{TcpStream, TcpSupport, UdpSupport}

import java.net.{InetSocketAddress, UnknownHostException}
import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousCloseException, ClosedChannelException}
import java.nio.charset.StandardCharsets

/** The gears network layer on the uring backend, end to end: sockets created, bound and connected by
  * `UringTcpSupport`/`UringUdpSupport`, and I/O through the shard reactors.
  */
class UringNetSuite extends munit.FunSuite:
  private lazy val support = UringPerThreadSupport(parallelism = 2)
  private given UringPerThreadSupport = support
  private given TcpSupport = support.tcpSupport
  private given UdpSupport = support.udpSupport

  override def afterAll(): Unit = support.close()

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

  private def udpRoundTrip(host: String)(using Async): Unit =
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
    Async.blocking:
      echo(InetSocketAddress("::1", 0))

  test("UDP: a datagram and its reply carry the right senders, over IPv4"):
    Async.blocking:
      udpRoundTrip("127.0.0.1")

  test("UDP: the same over IPv6"):
    Async.blocking:
      udpRoundTrip("::1")

  test("closing a listener fails a pending accept instead of leaving it hanging"):
    Async.blocking:
      val listener = TcpSupport.listen(InetSocketAddress("127.0.0.1", 0)).toOption.get
      val accepting = Future(listener.accept())
      AsyncOperations.sleep(50) // let the accept reach the kernel
      listener.close()
      val outcome = accepting.awaitResult
      assert(outcome.failed.toOption.exists(_.isInstanceOf[AsynchronousCloseException]), outcome)

  test("closing a stream fails a pending read, and later I/O on it is refused"):
    Async.blocking:
      val listener = TcpSupport.listen(InetSocketAddress("127.0.0.1", 0)).toOption.get
      try
        val client = TcpSupport.connect(listener.localAddress).toOption.get
        val server = listener.accept().toOption.get
        try
          val reading = Future(server.readBuf(ByteBuffer.allocate(16)))
          AsyncOperations.sleep(50) // nothing is sent, so the read stays pending
          server.close()
          val outcome = reading.awaitResult
          assert(outcome.failed.toOption.exists(_.isInstanceOf[AsynchronousCloseException]), outcome)
          intercept[ClosedChannelException](server.writeBuf(bytes("too late")))
          intercept[ClosedChannelException](server.shutdownOutput())
        finally client.close()
      finally listener.close()

  test("host names resolve through the reactors, and an unknown one fails cleanly"):
    Async.blocking:
      val dns = support.dnsSupport
      assert(dns.resolve("localhost").nonEmpty)
      intercept[UnknownHostException](dns.resolve("this-host-does-not-exist.invalid"))

  test("a scheduler can be closed, stopping its shards"):
    val scheduler = UringPerThreadSupport(parallelism = 2)
    given UringPerThreadSupport = scheduler
    Async.blocking:
      AsyncOperations.sleep(10) // a timer through a shard reactor
    scheduler.close() // returns once both shard threads have finished
    scheduler.close() // closing twice does nothing
    // Opening a file is a reactor op, which a closed scheduler refuses.
    intercept[IllegalStateException](Async.blocking(scheduler.fileSupport.openRead("build.sbt")))
