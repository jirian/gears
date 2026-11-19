package gears.io.tls

import gears.async.Async
import gears.async.AsyncOperations
import gears.async.Future
import gears.async.io.IpAddress
import gears.async.io.IpSocketAddress
import gears.io.http.Http
import gears.io.net.Stream

import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

import munit.FunSuite

class TlsInputSuite extends FunSuite {
  // - awaits go outside munit's assert: it holds the suite's lock, which a suspension would take to another thread

  // - reads take the next fed chunk, else time out
  private final class Fed(using AsyncOperations) extends Stream {
    val chunks = new ConcurrentLinkedQueue[Array[Byte]]()
    val reads = new AtomicInteger()
    def read(buf: ByteBuffer, timeout: Duration)(using Async): Int = {
      reads.incrementAndGet()
      val end = System.nanoTime() + timeout.asInstanceOf[FiniteDuration].toNanos
      var chunk = chunks.poll()
      while chunk == null do {
        if System.nanoTime() > end then throw new TimeoutException("nothing fed")
        AsyncOperations.sleep(1)
        chunk = chunks.poll()
      }
      buf.put(chunk)
      chunk.length
    }
    def write(buf: ByteBuffer, timeout: Duration)(using Async): Unit = buf.position(buf.limit()): Unit
    def shutdownOutput()(using Async): Unit = ()
    def close()(using Async): Unit = ()
    def localAddress = IpSocketAddress(IpAddress.IPv4Loopback, 0)
    def peerAddress = IpSocketAddress(IpAddress.IPv4Loopback, 0)
  }

  test("a fill that waited while another caller filled the input returns at once, without reading again") {
    Http.run {
      val fed = Fed()
      val context = TlsPlatform.clientContext(TlsClientConfig(verify = false))
      val tls = OpenSslStream.open(fed, context.ctx, "localhost", verify = false)
      try {
        val seen = tls.fills
        val first = Future(tls.fillNetworkIn(5.seconds, seen))
        AsyncOperations.sleep(50)
        val second = Future(tls.fillNetworkIn(2.seconds, seen))
        AsyncOperations.sleep(50)
        fed.chunks.add(Array[Byte](1, 2, 3))
        val firstResult = first.await
        assert(firstResult)
        val secondResult = second.await
        assert(secondResult)
        assertEquals(fed.reads.get(), 1)
      } finally tls.close()
    }
  }
}
