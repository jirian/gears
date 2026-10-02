import asyncio.*
import asyncio.uring.UringReactor
import asyncio.unsafe.NativeBuffer

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.collection.mutable.ArrayBuffer
import scala.scalanative.unsafe.Zone

/** The uring reactor, driven only through the `asyncio.Reactor` interface: everything below the factory would run
  * unchanged on any `Reactor.Posix`, such as the kqueue one.
  */
class UringReactorSuite extends munit.FunSuite:
  given Reactor.Factory[Reactor.Posix] = () => UringReactor.open()

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

  test("a promise completed from another thread wakes the reactor and carries its value"):
    Reactor.scoped: r =>
      val slot = Slot[String]()
      val promise = r.ops.promise(slot)
      var got: String | Null = null
      r.submit(
        promise,
        _ =>
          got = slot.clear()
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

  test("cancelling a pending read runs onCancel at once, and the op can be submitted again"):
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
      assertEquals(events.toList, List("cancel"))
      assert(!r.cancel(read), "nothing is pending any more")
      // Let the kernel finish the cancelled submission before reusing the buffer.
      r.submit(r.ops.timer(20), _ => r.stop())
      r.run()
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
      val promise = r.ops.promise()
      r.submit(promise, _ => ())
      intercept[IllegalStateException](r.submit(promise, _ => ()))

  test("closing cancels pending ops"):
    val r = summon[Reactor.Factory[Reactor.Posix]].open()
    val (in, out) = r.handles.pipe()
    val events = ArrayBuffer.empty[String]
    def recorder(name: String) = new Completion[r.Op]:
      def onComplete(op: r.Op): Unit = events += s"$name complete"
      override def onCancel(op: r.Op): Unit = events += s"$name cancel"
    r.submit(r.ops.read(in, ByteBuffer.allocate(8)), recorder("read"))
    r.submit(r.ops.timer(60_000), recorder("timer"))
    r.submit(r.ops.promise(), recorder("promise"))
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
      val accepted = Slot[r.BoxedHandle]()
      val request = ByteBuffer.allocate(64)
      val response = ByteBuffer.allocate(64)
      var connection = -1
      r.submit(
        r.ops.accept(server, accepted),
        _ =>
          connection = r.unbox(accepted.clear())
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

  test("whenReady runs its body once the descriptor is ready, and waits again when told to"):
    Reactor.scoped: r =>
      // Kqueue's `readNow`/`writeNow`, which this reactor shares, need pointer-backed buffers.
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
