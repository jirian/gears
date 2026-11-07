package gears.async.io
package uring

import gears.async.Async
import gears.async.AsyncOperations
import gears.async.AsyncSupport
import gears.async.Future

import java.nio.ByteBuffer
import java.nio.channels.ClosedChannelException
import java.util.concurrent.CancellationException
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.*
import scala.scalanative.posix.fcntl
import scala.scalanative.posix.unistd
import scala.scalanative.runtime.ByteArray
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/** What the io_uring backend promises beyond the contracts: its descriptors, and how its rings meet cancels and closes,
  * on a pool of a chosen size.
  */
class UringSuite extends munit.FunSuite {
  override def munitTimeout: Duration = 60.seconds

  private def onPool[T](workers: Int)(program: Io[UringBackend.type] ?=> Async.Spawn ?=> T): T = {
    val pool = UringScheduler(workers)
    given poolSupport: (AsyncSupport { type Scheduler = pool.Scheduler }) = pool
    given poolScheduler: pool.Scheduler = pool
    try Async.blocking(Future(program(using pool)).await)
    finally pool.shutdown()
  }

  private def ops = UringBackend.ops

  private def pipe()(using io: Io[UringBackend.type], async: Async): (UringReadable, UringWritable) = {
    val read = Slot[UringReadable]()
    val write = Slot[UringWritable]()
    io.perform(ops.pipe(read, write))
    (read.clear(), write.clear())
  }

  private def close(handle: UringHandle)(using io: Io[UringBackend.type], async: Async): Unit =
    io.perform(ops.close(handle))

  /** Writes `text` with a plain system call, not through the backend. */
  private def rawWrite(handle: UringHandle, text: String): Unit = {
    val bytes = text.getBytes("UTF-8")
    assertEquals(unistd.write(handle.fd, bytes.asInstanceOf[ByteArray].at(0), bytes.length.toUSize).toInt, bytes.length)
  }

  /** Reads what is waiting with a plain non-blocking system call: "" if nothing is. */
  private def rawRead(handle: UringHandle): String = {
    val bytes = new Array[Byte](64)
    val n = unistd.read(handle.fd, bytes.asInstanceOf[ByteArray].at(0), 64.toUSize).toInt
    if n <= 0 then "" else new String(bytes, 0, n, "UTF-8")
  }

  private def text(buf: ByteBuffer): String = {
    val copy = buf.duplicate()
    copy.flip()
    val bytes = new Array[Byte](copy.remaining())
    copy.get(bytes)
    new String(bytes, "UTF-8")
  }

  /** A future performing `op`, with what `perform` itself did: a cancelled future fails whatever its body did. */
  private def performing(op: UringOp)(using io: Io[UringBackend.type], spawn: Async.Spawn) = {
    val performed = new AtomicReference[Try[Unit] | Null](null)
    val future = Future {
      performed.set(Try(io.perform(op)))
      performed.get().nn.get
    }
    (future, performed)
  }

  /** Runs `body` on a thread of its own, failing if it has not returned within `limit`: for a check that, broken,
    * hangs.
    */
  private def returnsWithin(limit: FiniteDuration)(body: => Unit): Unit = {
    val failure = new AtomicReference[Throwable | Null](null)
    val thread = new Thread(() =>
      try body
      catch case e: Throwable => failure.set(e)
    )
    thread.setDaemon(true)
    thread.start()
    thread.join(limit.toMillis)
    assert(!thread.isAlive, s"it had not returned after $limit")
    val e = failure.get()
    if e != null then throw e
  }

  test("an op whose time runs out while its failing completion waits in the ring fails, and does not hang") {
    returnsWithin(20.seconds) {
      onPool(1) { io ?=>
        for _ <- 1 to 10 do {
          val (r, w) = pipe()
          val filler = new Array[Byte](65536)
          while unistd.write(w.fd, filler.asInstanceOf[ByteArray].at(0), filler.length.toUSize).toInt > 0 do ()
          val writer = Future(Try(io.perform(ops.write(w, ByteBuffer.wrap(Array[Byte](1))), 20.millis)))
          AsyncOperations.sleep(5) // so that it waits in the kernel, for the pipe to have room
          close(r) // its write then fails with EPIPE
          // One worker, which this future holds past the write's time limit: the failing completion waits in the
          // ring, and the limit is reached first.
          val until = System.nanoTime() + 40_000_000L
          while System.nanoTime() < until do ()
          writer.await match {
            case Failure(_: java.util.concurrent.TimeoutException | _: java.io.IOException) => ()
            case other => fail(s"expected the write to fail, not $other")
          }
          close(w)
        }
      }
    }
  }

  test("a blocking op that throws an Error fails with it, and does not hang") {
    returnsWithin(20.seconds) {
      onPool(2) { io ?=>
        val e = intercept[NotImplementedError](io.perform(ops.blocking(() => throw new NotImplementedError("boom"))))
        assertEquals(e.getMessage, "boom")
      }
    }
  }

  test("every handle is close-on-exec: pipes, listeners, connections, accepted ones, and files") {
    onPool(2) { io ?=>
      def cloexec(h: UringHandle) = (fcntl.fcntl(h.fd, fcntl.F_GETFD, 0) & fcntl.FD_CLOEXEC) != 0
      val (r, w) = pipe()
      val listener = Slot[ListenerHandle]()
      io.perform(ops.listen(IpSocketAddress(IpAddress.IPv4Loopback, 0), listener))
      val l = listener.clear()
      val accepted = Slot[ConnectionHandle]()
      val accepting = Future(io.perform(ops.accept(l, accepted)))
      val client = Slot[ConnectionHandle]()
      io.perform(ops.connect(ops.localAddress(l).asInstanceOf[IpSocketAddress], client))
      accepting.await
      val file = Slot[FileHandle]()
      io.perform(ops.open("/proc/self/stat", write = false, file))
      val handles = List(r, w, l, client.clear(), accepted.clear(), file.clear())
      for h <- handles do assert(cloexec(h), s"$h is not close-on-exec")
      handles.foreach(close)
    }
  }

  test("a read cancelled as data arrives either completes with it, or takes nothing and leaves it") {
    onPool(1) { io ?=>
      for _ <- 1 to 50 do {
        val (r, w) = pipe()
        val buf = ByteBuffer.allocate(16)
        val (reader, performed) = performing(ops.read(r, buf))
        AsyncOperations.sleep(5) // so that it waits in the kernel
        // One worker, which this future holds: the read's completion, if any, waits for it in the ring.
        rawWrite(w, "data")
        reader.cancel()
        reader.awaitResult: Unit
        performed.get() match {
          case Success(_)                        => assertEquals(text(buf), "data")
          case Failure(_: CancellationException) =>
            assertEquals(buf.position(), 0, "cancelled, so it took nothing")
            assertEquals(rawRead(r), "data", "and the bytes are still there")
          case other => fail(s"unexpected: $other")
        }
        close(r)
        close(w)
      }
    }
  }

  test("a read cancelled while it waits in the kernel takes nothing written after") {
    onPool(2) { io ?=>
      val (r, w) = pipe()
      val buf = ByteBuffer.allocate(16)
      val (reader, performed) = performing(ops.read(r, buf))
      AsyncOperations.sleep(20)
      reader.cancel()
      reader.awaitResult: Unit
      assert(performed.get().nn.failed.toOption.exists(_.isInstanceOf[CancellationException]), performed.get())
      rawWrite(w, "later")
      AsyncOperations.sleep(20)
      assertEquals(buf.position(), 0)
      assertEquals(rawRead(r), "later")
      close(r)
      close(w)
    }
  }

  test("a close between a read's preparation and its submission fails the read, and completes") {
    onPool(1) { io ?=>
      for _ <- 1 to 50 do {
        val (r, w) = pipe()
        // One worker: the reader prepares its entry and suspends, then the closer runs, before the ring is entered.
        val (reader, performed) = performing(ops.read(r, ByteBuffer.allocate(16)))
        val closer = Future(close(r))
        closer.await
        reader.awaitResult: Unit
        assert(performed.get().nn.failed.toOption.exists(_.isInstanceOf[ClosedChannelException]), performed.get())
        close(w)
      }
    }
  }

  test("a close frees a listener's port as it returns, while another worker has an accept on it") {
    onPool(4) { io ?=>
      for _ <- 1 to 20 do {
        val into = Slot[ListenerHandle]()
        io.perform(ops.listen(IpSocketAddress(IpAddress.IPv4Loopback, 0), into))
        val l = into.clear()
        val address = ops.localAddress(l).asInstanceOf[IpSocketAddress]
        val accepts = List.fill(4)(performing(ops.accept(l, Slot[ConnectionHandle]())))
        AsyncOperations.sleep(5)
        close(l)
        val again = Slot[ListenerHandle]()
        io.perform(ops.listen(address, again)) // fails if the port is still held
        close(again.clear())
        for (f, performed) <- accepts do {
          f.awaitResult: Unit
          assert(performed.get().nn.failed.toOption.exists(_.isInstanceOf[ClosedChannelException]), performed.get())
        }
      }
    }
  }

  test("a close ends the peer's stream as it returns, while another worker has a read on it") {
    onPool(4) { io ?=>
      for _ <- 1 to 20 do {
        val into = Slot[ListenerHandle]()
        io.perform(ops.listen(IpSocketAddress(IpAddress.IPv4Loopback, 0), into))
        val l = into.clear()
        val accepted = Slot[ConnectionHandle]()
        val accepting = Future(io.perform(ops.accept(l, accepted)))
        val client = Slot[ConnectionHandle]()
        io.perform(ops.connect(ops.localAddress(l).asInstanceOf[IpSocketAddress], client))
        accepting.await
        val (a, b) = (accepted.clear(), client.clear())
        val (reader, _) = performing(ops.read(a, ByteBuffer.allocate(16)))
        AsyncOperations.sleep(5)
        close(a)
        val end = ByteBuffer.allocate(16)
        io.perform(ops.read(b, end), 5.seconds) // the end of the stream, at once
        assertEquals(end.position(), 0)
        reader.awaitResult: Unit
        close(b)
        close(l)
      }
    }
  }

  test("idle streams with pooled reads pending hold at most half the pool, however many there are") {
    onPool(2) { io ?=>
      val pool = UringBackend.pool(256, 8)
      val pipes = List.fill(12)(pipe())
      val slots = pipes.map(_ => Slot[Received]())
      val reads = pipes.zip(slots).map { case ((r, _), into) => performing(ops.read(r, pool, into)) }
      AsyncOperations.sleep(20)
      assert(pool.available >= 4, s"${pool.available} of 8 free")
      // One at a time, each buffer given back before the next stream has bytes, so that none finds the pool empty.
      for (((_, w), into), (f, performed)) <- pipes.zip(slots).zip(reads) do {
        rawWrite(w, "x")
        f.awaitResult: Unit
        assert(performed.get().nn.isSuccess, performed.get())
        into.clear().release()
      }
      assertEquals(pool.available, 8)
      pipes.foreach((r, w) => { close(r); close(w) })
    }
  }
}
