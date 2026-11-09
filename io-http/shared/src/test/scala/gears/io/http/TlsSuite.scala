package gears.io.http

import gears.async.Async
import gears.async.AsyncOperations
import gears.async.Future
import gears.io.net.Stream
import gears.io.net.Tcp
import gears.io.tls.Tls
import gears.io.tls.TlsClientConfig
import gears.io.tls.TlsServerConfig

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeoutException
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt
import scala.util.Try

import munit.FunSuite

/** TLS streams against a peer that misbehaves, one that trickles its handshake and one that stops reading, and how many
  * writes a TLS stream makes.
  */
class TlsSuite extends FunSuite {
  override val munitTimeout = scala.concurrent.duration.Duration(30, "s")

  private val serverTls = TlsServerConfig(TestCertificate.certificate, TestCertificate.key)
  private val trusting = TlsClientConfig(trusted = Some(TestCertificate.certificate))

  test("a handshake trickled a byte at a time is cut off by its time limit, not given it for every byte") {
    val config = ServerConfig(
      host = "127.0.0.1",
      port = 0,
      tls = Some(serverTls),
      timeouts = ServerTimeouts(handshake = 1.second),
      log = _ => ()
    )
    Http.testServer(Routes(GET("/") { _ => "ok" }), config) { server =>
      val raw = Tcp.connect("127.0.0.1", server.port)
      try {
        val began = System.nanoTime()
        // A record of 512 bytes holding a ClientHello of 508, of which only the headers come at once, then a byte every
        // 200 ms: nothing to refuse until the message is whole.
        raw.write(ByteBuffer.wrap(Array[Byte](0x16, 0x03, 0x01, 0x02, 0x00, 0x01, 0x00, 0x01, 0xfc.toByte, 0x03, 0x03)))
        val trickle = Future {
          while true do {
            AsyncOperations.sleep(200)
            raw.write(ByteBuffer.wrap(Array[Byte](0)))
          }
        }
        val ended =
          try raw.read(ByteBuffer.allocate(64), 8.seconds) < 0
          catch case _: IOException => true
        trickle.cancel()
        val took = (System.nanoTime() - began) / 1_000_000
        assert(ended, "the server closed the connection")
        assert(took < 4000, s"it took $took ms, past a one-second handshake limit")
      } finally raw.close()
    }
  }

  test("a TLS stream whose peer stops reading still reads, and its close does not wait for the stuck write") {
    Http.run {
      Async.group {
        val listener = Tcp.listen("127.0.0.1", 0)
        val context = Tls.serverContext(serverTls)
        val peer = Future {
          val tls = Tls.server(listener.accept(), context, 5.seconds)
          tls.write(ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8)), 5.seconds)
          AsyncOperations.sleep(60_000) // reads nothing more
        }
        val raw = Tcp.connect("127.0.0.1", Tcp.port(listener.localAddress))
        val tls = Tls.client(raw, "localhost", trusting, 5.seconds)
        val chunk = new Array[Byte](1 << 20)
        val writer = Future(Try {
          while true do tls.write(ByteBuffer.wrap(chunk), Duration.Inf)
        })
        AsyncOperations.sleep(500) // the socket's buffers fill, and the write waits for a peer that does not read
        assert(writer.poll().isEmpty, s"the write is stuck, not ${writer.poll()}")
        val got = ByteBuffer.allocate(16)
        val n = gears.async.withTimeout(3.seconds)(tls.read(got, 3.seconds))
        assertEquals(new String(got.array(), 0, n, StandardCharsets.UTF_8), "hello", "the read was not held up")
        val began = System.nanoTime()
        gears.async.withTimeout(3.seconds)(tls.close())
        val result = gears.async.withTimeout(3.seconds)(writer.await)
        val took = (System.nanoTime() - began) / 1_000_000
        assert(result.isFailure, "the stuck write failed")
        assert(took < 3000, s"close and the write's failure took $took ms")
        peer.cancel()
        listener.close()
      }
    }
  }

  test("small buffers written together go out as one record, in one write of the connection") {
    Http.run {
      Async.group {
        val listener = Tcp.listen("127.0.0.1", 0)
        val context = Tls.serverContext(serverTls)
        val peer = Future {
          val tls = Tls.server(listener.accept(), context, 5.seconds)
          val got = ByteBuffer.allocate(64)
          while got.position() < 11 && tls.read(got, 5.seconds) > 0 do ()
          tls.close()
          new String(got.array(), 0, got.position(), StandardCharsets.UTF_8)
        }
        val raw = Tcp.connect("127.0.0.1", Tcp.port(listener.localAddress))
        val writes = new java.util.concurrent.atomic.AtomicInteger()
        val counted = new Stream {
          def read(buf: ByteBuffer, timeout: Duration)(using Async) = raw.read(buf, timeout)
          def write(buf: ByteBuffer, timeout: Duration)(using Async) = {
            writes.incrementAndGet()
            raw.write(buf, timeout)
          }
          def shutdownOutput()(using Async) = raw.shutdownOutput()
          def close()(using Async) = raw.close()
          def localAddress = raw.localAddress
          def peerAddress = raw.peerAddress
        }
        val tls = Tls.client(counted, "localhost", trusting, 5.seconds)
        try {
          val before = writes.get()
          val parts = Array("hello", " ", "world").map(p => ByteBuffer.wrap(p.getBytes(StandardCharsets.UTF_8)))
          tls.write(parts, 5.seconds)
          assertEquals(writes.get() - before, 1, "one write for the three buffers")
          assert(parts.forall(!_.hasRemaining), "each buffer is taken")
          assertEquals(peer.await, "hello world")
        } finally {
          tls.close()
          listener.close()
        }
      }
    }
  }
}
