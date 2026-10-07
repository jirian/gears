import asyncio.*
import asyncio.uring.{BlockingPool, HostResolver, UringReactor}
import asyncio.unsafe.NativeBuffer

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.collection.mutable.ArrayBuffer
import scala.scalanative.unsafe.Zone

/** The uring reactor, driven through the `asyncio.Reactor` interface wherever it can be: the tests on `Reactor.scoped`
  * would run unchanged on any reactor whose handles are POSIX descriptors, such as the kqueue one. The few that need
  * uring-only ops or wiring open a `UringReactor` directly.
  */
class UringReactorSuite extends munit.FunSuite:
  given Reactor.Factory[Reactor] = () => UringReactor.open()

  private def bytes(s: String): ByteBuffer = ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8))
  private def text(buf: ByteBuffer): String =
    val dup = buf.duplicate()
    dup.flip()
    val out = new Array[Byte](dup.remaining())
    dup.get(out)
    new String(out, StandardCharsets.UTF_8)

  private def tempPath(name: String): String = s"/tmp/gears-uring-${scala.scalanative.posix.unistd.getpid()}-$name"

  test("a timer completes and its completion can stop the reactor"):
    Reactor.scoped: r =>
      val started = System.nanoTime()
      var fired = false
      r.submit(
        r.ops.timer(50),
        _ =>
          fired = true
          r.stop()
      )
      r.run()
      assert(fired)
      assert(System.nanoTime() - started >= 45_000_000L)

  test("reads and writes on a pipe, then end of stream"):
    Reactor.scoped: r =>
      val (in, out) = r.handles.pipe()
      val received = ByteBuffer.allocate(64)
      val message = bytes("hello uring")
      val read = r.ops.read(in, received)
      r.submit(
        r.ops.write(out, message),
        _ =>
          r.handles.close(out)
          r.submit(
            read,
            _ =>
              val got = received.position()
              r.submit(read, _ => // repeatable: the same op reads again, and finds the end of the stream
                assertEquals(received.position(), got, "nothing is added at end of stream")
                r.stop()
              )
          )
      )
      r.run()
      r.handles.close(in)
      assertEquals(text(received), "hello uring")
      assert(!message.hasRemaining)

  test("a heap buffer's position offsets the read"):
    Reactor.scoped: r =>
      val (in, out) = r.handles.pipe()
      val received = ByteBuffer.allocate(16)
      received.put("ab".getBytes(StandardCharsets.UTF_8))
      r.submit(r.ops.write(out, bytes("cd")), _ => r.submit(r.ops.read(in, received), _ => r.stop()))
      r.run()
      r.handles.close(in)
      r.handles.close(out)
      assertEquals(text(received), "abcd")

  test("vectored reads and writes, a uring-only op, preserve buffer order and positions"):
    UringReactor.scoped(): r =>
      val (in, out) = r.handles.pipe()
      val first = bytes("ab")
      val second = bytes("cd")
      val left = ByteBuffer.allocate(2)
      val right = ByteBuffer.allocate(2)
      r.submit(r.ops.writev(out, Array(first, second)), _ => r.handles.close(out))
      r.submit(r.ops.readv(in, Array(left, right)), _ => r.stop())
      r.run()
      r.handles.close(in)
      assertEquals(text(left), "ab")
      assertEquals(text(right), "cd")
      assert(!first.hasRemaining)
      assert(!second.hasRemaining)

  test("a TCP write half-close sends EOF without disabling reads"):
    Reactor.scoped: r =>
      val address = Address.Unix(tempPath("half-close.sock"))
      val server = r.handles.listen(address)
      val client = r.handles.connect(address)
      val accepted = HandleSlot()
      val request = ByteBuffer.allocate(16)
      val response = ByteBuffer.allocate(16)
      var serverHandle = -1
      var sawEof = false
      r.submit(
        r.ops.accept(server, accepted),
        _ =>
          serverHandle = accepted.clear()
          val read = r.ops.read(serverHandle, request)
          r.submit(read, _ =>
            if request.position() == 7 then
              r.submit(
                read,
                _ =>
                  sawEof = request.position() == 7
                  r.submit(r.ops.write(serverHandle, bytes("reply")), _ => ())
              )
          )
      )
      r.submit(
        r.ops.connect(client),
        _ =>
          r.submit(
            r.ops.write(client, bytes("request")),
            _ =>
              // Half-closing is not part of `Handles`, so this calls shutdown(2) directly.
              scala.scalanative.posix.sys.socket.shutdown(client, scala.scalanative.posix.sys.socket.SHUT_WR)
              r.submit(r.ops.read(client, response), _ => r.stop())
          )
      )
      r.run()
      assert(sawEof)
      assertEquals(text(response), "reply")
      r.handles.close(serverHandle)
      r.handles.close(client)
      r.handles.close(server)

  test("a promise completed from another thread wakes the reactor and carries its value"):
    Reactor.scoped: r =>
      val promise = r.ops.promise[String]()
      var got: String | Null = null
      r.submit(
        promise,
        _ =>
          got = promise.result
          r.stop()
      )
      val t = new Thread(() =>
        Thread.sleep(20)
        promise.complete("from afar")
      )
      t.start()
      r.run()
      t.join()
      assertEquals(got, "from afar")
      intercept[IllegalStateException](promise.complete("again")) // only the first completion counts

  test("a promise's result is only there once it has been completed"):
    Reactor.scoped: r =>
      val promise = r.ops.promise[Unit]()
      r.submit(promise, _ => r.stop())
      intercept[IllegalStateException](promise.result)
      promise.complete(())
      r.run()
      assertEquals(promise.result, ())

  test("cancelling a pending read runs onCancel once the kernel lets go, and then the op can be submitted again"):
    Reactor.scoped: r =>
      val (in, out) = r.handles.pipe()
      val received = ByteBuffer.allocate(16)
      val read = r.ops.read(in, received)
      val events = ArrayBuffer.empty[String]
      val completion = new Completion[r.Op]:
        def onComplete(op: r.Op): Unit = events += "complete"
        override def onCancel(op: r.Op): Unit = events += "cancel"
      r.submit(read, completion)
      assert(r.cancel(read))
      assertEquals(events.toList, Nil, "onCancel waits until the kernel has let go of the buffer")
      assert(!r.cancel(read), "nothing is pending any more")
      intercept[IllegalStateException](r.submit(read, _ => ())) // the kernel may still be writing into its buffer
      // The cancelled read's CQE arrives while the reactor runs.
      r.submit(r.ops.timer(20), _ => r.stop())
      r.run()
      assertEquals(events.toList, List("cancel"))
      r.submit(
        read,
        _ =>
          events += "again"
          r.stop()
      )
      r.submit(r.ops.write(out, bytes("x")), _ => ())
      r.run()
      assertEquals(events.toList, List("cancel", "again"))
      assertEquals(text(received), "x")
      r.handles.close(in)
      r.handles.close(out)

  test("a one-shot op cannot be submitted twice"):
    Reactor.scoped: r =>
      val promise = r.ops.promise[Unit]()
      r.submit(promise, _ => ())
      intercept[IllegalStateException](r.submit(promise, _ => ()))

  test("closing cancels pending ops"):
    val r = summon[Reactor.Factory[Reactor]].open()
    val (in, out) = r.handles.pipe()
    val events = ArrayBuffer.empty[String]
    def recorder(name: String) = new Completion[r.Op]:
      def onComplete(op: r.Op): Unit = events += s"$name complete"
      override def onCancel(op: r.Op): Unit = events += s"$name cancel"
    r.submit(r.ops.read(in, ByteBuffer.allocate(8)), recorder("read"))
    r.submit(r.ops.timer(60_000), recorder("timer"))
    r.submit(r.ops.promise[Unit](), recorder("promise"))
    r.close()
    r.close() // closing twice does nothing
    assertEquals(events.toSet, Set("read cancel", "timer cancel", "promise cancel"))
    r.handles.close(in)
    r.handles.close(out)

  test("a stream socket: listen, connect, accept, then talk both ways"):
    Reactor.scoped: r =>
      val address = Address.Unix(tempPath("stream.sock"))
      val server = r.handles.listen(address)
      val client = r.handles.connect(address)
      val accepted = HandleSlot()
      val request = ByteBuffer.allocate(64)
      val response = ByteBuffer.allocate(64)
      var connection = -1
      r.submit(
        r.ops.accept(server, accepted),
        _ =>
          connection = accepted.clear()
          r.submit(
            r.ops.read(connection, request),
            _ => r.submit(r.ops.write(connection, bytes("pong")), _ => ())
          )
      )
      r.submit(
        r.ops.connect(client),
        _ =>
          r.submit(
            r.ops.write(client, bytes("ping")),
            _ => r.submit(r.ops.read(client, response), _ => r.stop())
          )
      )
      r.run()
      assertEquals(text(request), "ping")
      assertEquals(text(response), "pong")
      r.handles.close(connection)
      r.handles.close(client)
      r.handles.close(server)

  test("connecting to nothing fails the connect op"):
    Reactor.scoped: r =>
      // A port nothing should be listening on.
      val address = Address.IPv4("127.0.0.1", 40000 + (scala.scalanative.posix.unistd.getpid() % 20000).toInt)
      val client = r.handles.connect(address)
      var failure: Throwable | Null = null
      r.submit(
        r.ops.connect(client),
        new Completion[r.Op]:
          def onComplete(op: r.Op): Unit = r.stop()
          override def onFailure(op: r.Op, t: Throwable): Unit =
            failure = t
            r.stop()
      )
      r.run()
      r.handles.close(client)
      assert(failure.isInstanceOf[java.io.IOException], failure)

  test("datagrams carry their sender"):
    Reactor.scoped: r =>
      val serverAddress = Address.Unix(tempPath("dgram-server.sock"))
      val clientAddress = Address.Unix(tempPath("dgram-client.sock"))
      val server = r.handles.datagram(serverAddress, null)
      val client = r.handles.datagram(clientAddress, serverAddress)
      val packet = ByteBuffer.allocate(64)
      val from = Slot[Address]()
      var sender: Address | Null = null
      r.submit(
        r.ops.receive(server, packet, from),
        _ =>
          sender = from.clear()
          r.stop()
      )
      r.submit(r.ops.send(client, bytes("datagram"), null), _ => ())
      r.run()
      // The op leaves the buffer flipped for reading.
      val out = new Array[Byte](packet.remaining())
      packet.get(out)
      assertEquals(new String(out, StandardCharsets.UTF_8), "datagram")
      assertEquals(sender, clientAddress)
      r.handles.close(client)
      r.handles.close(server)

  test("a reactor resolves names with its injected resolver, on its injected blocker pool"):
    val pool = new BlockingPool(parallelism = 1, queueCapacity = 4, threadNamePrefix = "uring-test-blocker")
    val resolverThread = new java.util.concurrent.atomic.AtomicReference[String]()
    val resolver = new HostResolver:
      def resolve(host: String): Either[String, List[ResolvedAddress]] =
        resolverThread.set(Thread.currentThread().getName())
        Right(List(ResolvedAddress(AddressFamily.IPv4, "127.0.0.1")))
    val r = UringReactor.open(blockerPool = pool, resolver = resolver)
    try
      val resolved = Slot[List[ResolvedAddress]]()
      r.submit(r.ops.resolve("example.test", resolved), _ => r.stop())
      r.run()
      assertEquals(resolved.clear(), List(ResolvedAddress(AddressFamily.IPv4, "127.0.0.1")))
      assert(resolverThread.get().startsWith("uring-test-blocker-"))
    finally
      r.close()
      pool.close()

  test("whenReady runs its body once the descriptor is ready, and waits again when told to"):
    Reactor.scoped: r =>
      // Pointer-backed, so this would also run on kqueue, whose `readNow`/`writeNow` need them.
      Zone.acquire: zone =>
        given Zone = zone
        val (in, out) = r.handles.pipe()
        var attempts = 0
        val buf = NativeBuffer.allocate(8)
        val drain = r.ops.whenReady(in, Interest.Read): () =>
          attempts += 1
          r.handles.readNow(in, buf) > 0
        r.submit(drain, _ => r.stop())
        r.submit(r.ops.timer(20), _ => r.handles.writeNow(out, NativeBuffer.of("ready".getBytes(StandardCharsets.UTF_8))))
        r.run()
        assertEquals(text(buf), "ready")
        assert(attempts >= 1)
        r.handles.close(in)
        r.handles.close(out)

  test("ring ops work on pointer-backed buffers too"):
    Reactor.scoped: r =>
      Zone.acquire: zone =>
        given Zone = zone
        val (in, out) = r.handles.pipe()
        val received = NativeBuffer.allocate(16)
        val message = NativeBuffer.of("native".getBytes(StandardCharsets.UTF_8))
        r.submit(r.ops.write(out, message), _ => r.submit(r.ops.read(in, received), _ => r.stop()))
        r.run()
        assertEquals(text(received), "native")
        r.handles.close(in)
        r.handles.close(out)

  test("ring ops copy through scratch memory for read-only buffers"):
    Reactor.scoped: r =>
      val (in, out) = r.handles.pipe()
      val received = ByteBuffer.allocate(16)
      val message = bytes("read-only").asReadOnlyBuffer()
      r.submit(r.ops.write(out, message), _ => r.submit(r.ops.read(in, received), _ => r.stop()))
      r.run()
      assertEquals(text(received), "read-only")
      assert(!message.hasRemaining)
      r.handles.close(in)
      r.handles.close(out)

  test("blocking work and name resolution run off the reactor thread"):
    Reactor.scoped: r =>
      val loop = Thread.currentThread()
      val ranOn = Slot[Thread]()
      val addresses = Slot[List[ResolvedAddress]]()
      var done = 0
      def finish(): Unit =
        done += 1
        if done == 2 then r.stop()
      r.submit(r.ops.blocking(() => Thread.currentThread(), ranOn), _ => finish())
      r.submit(r.ops.resolve("localhost", addresses), _ => finish())
      r.run()
      assert(ranOn.clear() ne loop)
      assert(addresses.clear().nonEmpty)

  test("readNow and writeNow take heap buffers too"):
    Reactor.scoped: r =>
      val (in, out) = r.handles.pipe()
      val buf = ByteBuffer.allocate(8)
      assertEquals(r.handles.readNow(in, buf), 0, "nothing to read yet")
      assertEquals(r.handles.writeNow(out, bytes("heap").asReadOnlyBuffer()), 4)
      assertEquals(r.handles.readNow(in, buf), 4)
      assertEquals(text(buf), "heap")
      r.handles.close(out)
      assertEquals(r.handles.readNow(in, buf), -1, "end of stream")
      r.handles.close(in)

  test("cancelling a running blocking op interrupts it, waits for it to return, and drops its result"):
    Reactor.scoped: r =>
      val started = new java.util.concurrent.CountDownLatch(1)
      val result = Slot[String]()
      val events = ArrayBuffer.empty[String]
      val op = r.ops.blocking(
        () =>
          started.countDown()
          try Thread.sleep(10_000)
          catch case _: InterruptedException => events.synchronized(events += "interrupted")
          "too late"
        ,
        result
      )
      r.submit(
        op,
        new Completion[r.Op]:
          def onComplete(op: r.Op): Unit = events.synchronized(events += "complete")
          override def onCancel(op: r.Op): Unit =
            events.synchronized(events += "cancel")
            r.stop()
      )
      started.await()
      assert(r.cancel(op))
      r.run()
      assertEquals(events.synchronized(events.toList), List("interrupted", "cancel"))
      intercept[IllegalStateException](result.clear()) // nothing is written after a cancel
