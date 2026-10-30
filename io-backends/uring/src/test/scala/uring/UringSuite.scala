package gears.async.io
package uring

import gears.async.io.internal.Completion

import java.nio.ByteBuffer
import java.nio.channels.ClosedChannelException
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import scala.scalanative.posix.errno
import scala.scalanative.posix.fcntl
import scala.scalanative.posix.unistd
import scala.scalanative.runtime.ByteArray
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import munit.FunSuite

/** What the io_uring backend does beyond the reactor contract: how it opens descriptors, how a cancel meets a
  * completion queue entry that has already arrived, how a command waits for readiness after `EAGAIN`, and how a close
  * meets an entry prepared and not yet submitted.
  */
class UringSuite extends FunSuite {
  private lazy val arena = gears.async.io.testkit.BufferArena.open()
  private def allocate(capacity: Int): ByteBuffer = arena.allocate(capacity)

  override def afterAll(): Unit = {
    arena.close()
    super.afterAll()
  }

  private def bytes(text: String): ByteBuffer = {
    val encoded = text.getBytes(StandardCharsets.UTF_8)
    val buf = allocate(math.max(encoded.length, 1))
    buf.put(encoded).flip()
    buf
  }

  private def text(buf: ByteBuffer): String = {
    val copy = buf.duplicate()
    copy.flip()
    val out = new Array[Byte](copy.remaining())
    copy.get(out)
    new String(out, StandardCharsets.UTF_8)
  }

  private final class Outcome extends Completion[UringOp] {
    var completed = 0
    var failure: Throwable | Null = null
    var cancelled = 0
    def onComplete(op: UringOp): Unit = completed += 1
    override def onFailure(op: UringOp, t: Throwable): Unit = failure = t
    override def onCancel(op: UringOp): Unit = cancelled += 1
    def finished: Boolean = completed > 0 || failure != null || cancelled > 0
  }

  private def turnUntil(r: UringReactor)(cond: => Boolean)(using loc: munit.Location): Unit = {
    val deadline = System.nanoTime() + 5_000_000_000L
    while !cond do {
      if System.nanoTime() > deadline then fail(s"timed out waiting for the reactor at line ${loc.line}")
      r.runOnce(block = false)
    }
  }

  /** Runs `op` on `r` until it is done, and returns its failure, if any. */
  private def run(r: UringReactor, op: UringOp)(using munit.Location): Throwable | Null = {
    val outcome = Outcome()
    r.submit(op, outcome)
    turnUntil(r)(outcome.finished)
    outcome.failure
  }

  private def fd(handle: UringBackend.Handle): Int = UringHandles.descriptor(handle)

  private def closeOnExec(handle: UringBackend.Handle): Boolean =
    (fcntl.fcntl(fd(handle), fcntl.F_GETFD, 0) & fcntl.FD_CLOEXEC) != 0

  test("every handle is close-on-exec, accepted connections and opened files too") {
    val dir = Files.createTempDirectory("uring")
    UringReactor.scoped() { r =>
      val (in, out) = UringHandles.pipe()
      val listener = UringHandles.listen(IpAddress.IPv4("127.0.0.1", 0))
      val datagram = UringHandles.bindDatagram(IpAddress.IPv4("127.0.0.1", 0))
      val path = dir.resolve("file")
      Files.write(path, Array[Byte](1))
      val opened = Slot[UringKinds.File]()
      assertEquals(run(r, UringOps.open(path.toString, write = false, opened)), null)
      val file = opened.clear()
      val unixPath = dir.resolve("unix.sock").toString
      val unixListener = UringHandles.listen(UnixAddress(unixPath))
      val client = UringHandles.connect(UnixAddress(unixPath))
      val accepted = Slot[UringKinds.Connection]()
      assertEquals(run(r, UringOps.accept(unixListener, accepted)), null)
      val server = accepted.clear()
      val handles = Seq(
        "pipe" -> in,
        "pipe" -> out,
        "listener" -> listener,
        "datagram" -> datagram,
        "file" -> file,
        "client" -> client,
        "accepted" -> server
      )
      try
        for (name, h) <- handles do
          assert(closeOnExec(h), s"the $name descriptor ${fd(h)} is inherited by child processes")
      finally (handles.map(_._2) :+ unixListener).foreach(UringHandles.close)
    }
    Files.list(dir).forEach(Files.deleteIfExists(_): Unit)
    Files.delete(dir)
  }

  test("a path left by a descriptor closed around the handles is not removed under a newer descriptor") {
    val path = s"/tmp/gears-uring-stale-${unistd.getpid()}.sock"
    val stale = UringHandles.listen(UnixAddress(path))
    unistd.close(fd(stale))
    try {
      // POSIX hands out the lowest free number, so the pipe's read end reuses the closed listener's.
      val (in, out) = UringHandles.pipe()
      assertEquals(fd(in), fd(stale))
      UringHandles.close(in)
      UringHandles.close(out)
      assert(java.io.File(path).exists(), "closing an unrelated descriptor removed the path")
    } finally java.io.File(path).delete(): Unit
  }

  test("a cancel after the entry of a read that finished has arrived loses nothing: the read completes") {
    UringReactor.scoped() { r =>
      val (in, out) = UringHandles.pipe()
      try {
        val buf = allocate(16)
        val read = UringOps.read(in, buf)
        val outcome = Outcome()
        var cancelled: Option[Boolean] = None
        // Issued together, in order, so the read finds the write's bytes, and their entries arrive in one batch.
        r.submit(UringOps.write(out, bytes("arrived")), _ => cancelled = Some(r.cancel(read)))
        r.submit(read, outcome)
        turnUntil(r)(outcome.finished)
        assertEquals(cancelled, Some(false), "the read had finished, so the cancel is too late")
        assertEquals(outcome.completed, 1)
        assertEquals(text(buf), "arrived")
      } finally {
        UringHandles.close(in)
        UringHandles.close(out)
      }
    }
  }

  test("a cancel of a read waiting in the kernel runs onCancel before it returns, and the read takes nothing after") {
    UringReactor.scoped() { r =>
      val (in, out) = UringHandles.pipe()
      val (otherIn, otherOut) = UringHandles.pipe()
      try {
        val buf = allocate(16)
        val read = UringOps.read(in, buf)
        val outcome = Outcome()
        var cancelled: Option[Boolean] = None
        var cancelledFirst = -1
        r.submit(
          UringOps.write(otherOut, bytes("x")),
          _ => {
            cancelled = Some(r.cancel(read))
            cancelledFirst = outcome.cancelled
          }
        )
        r.submit(read, outcome)
        turnUntil(r)(cancelled.isDefined)
        assertEquals(cancelled, Some(true))
        assertEquals(cancelledFirst, 1, "onCancel ran before cancel returned")
        assertEquals(run(r, UringOps.write(out, bytes("late"))), null)
        for _ <- 1 to 5 do r.runOnce(block = false)
        assertEquals((outcome.completed, outcome.cancelled), (0, 1), "the read was not submitted again")
        assertEquals(buf.position(), 0)
      } finally Seq(in, out, otherIn, otherOut).foreach(UringHandles.close)
    }
  }

  test("a close while a read's entry waits to be submitted waits for the kernel to take it, then the read fails") {
    UringReactor.scoped() { r =>
      val (in, out) = UringHandles.pipe()
      try {
        val buf = allocate(16)
        val outcome = Outcome()
        r.submit(UringOps.read(in, buf), outcome) // prepared, not yet issued: its entry names the descriptor's number
        val closer = new Thread(() => UringHandles.close(in))
        closer.start()
        closer.join(100)
        assert(closer.isAlive, "the close did not wait for the entry naming the number")
        r.runOnce(block = false) // issues the entry, which ends the use, so the close goes on
        closer.join(5000)
        assert(!closer.isAlive, "the close did not finish once the entry was issued")
        // The close cancelled the entry before it freed the number, so whatever takes the number now is out of reach.
        val (reused, reusedOut) = UringHandles.pipe()
        try {
          assertEquals(run(r, UringOps.write(reusedOut, bytes("not yours"))), null)
          turnUntil(r)(outcome.finished)
          assert(outcome.failure.isInstanceOf[ClosedChannelException], String.valueOf(outcome.failure))
          assertEquals(buf.position(), 0)
        } finally {
          UringHandles.close(reused)
          UringHandles.close(reusedOut)
        }
      } finally UringHandles.close(out)
    }
  }

  /** Runs `body` with a reactor on a thread of its own that issues `op` to the kernel and then stops turning, until
    * `body` returns; then it turns until `op` is done, and its outcome is returned.
    */
  private def withIdleOwner(op: UringOp)(body: => Unit): Outcome = {
    val outcome = Outcome()
    val issued = java.util.concurrent.CountDownLatch(1)
    val resume = java.util.concurrent.CountDownLatch(1)
    var failure: Throwable | Null = null
    val owner = new Thread(() =>
      try
        UringReactor.scoped() { r =>
          r.submit(op, outcome)
          r.runOnce(block = false) // issues the entry
          issued.countDown()
          resume.await()
          turnUntil(r)(outcome.finished)
        }
      catch case t: Throwable => failure = t
      finally issued.countDown()
    )
    owner.start()
    issued.await()
    try body
    finally {
      resume.countDown()
      owner.join(10_000)
    }
    if failure != null then throw failure.nn
    outcome
  }

  test("a close frees a listener's port as it returns, while another reactor, not turning, has an accept on it") {
    for _ <- 1 to 100 do {
      val listener = UringHandles.listen(IpAddress.IPv4("127.0.0.1", 0))
      val address = UringHandles.addressOf(listener, peer = false).asInstanceOf[IpAddress]
      val outcome = withIdleOwner(UringOps.accept(listener, Slot[UringKinds.Connection]())) {
        UringHandles.close(listener)
        UringHandles.close(UringHandles.listen(address)) // the port is free again at once
      }
      assert(outcome.failure.isInstanceOf[ClosedChannelException], String.valueOf(outcome.failure))
    }
  }

  test("a close ends the peer's stream as it returns, while another reactor, not turning, has a read on it") {
    UringReactor.scoped() { r =>
      for _ <- 1 to 100 do {
        val listener = UringHandles.listen(IpAddress.IPv4("127.0.0.1", 0))
        val client = UringHandles.connect(UringHandles.addressOf(listener, peer = false).asInstanceOf[IpAddress])
        val accepted = Slot[UringKinds.Connection]()
        try {
          assertEquals(run(r, UringOps.accept(listener, accepted)), null)
          val server = accepted.clear()
          val outcome = withIdleOwner(UringOps.read(server, allocate(16))) {
            UringHandles.close(server)
            val buf = allocate(16)
            assertEquals(run(r, UringOps.read(client, buf)), null)
            assertEquals(buf.position(), 0, "the peer read bytes, not the end of the stream")
          }
          assert(outcome.failure.isInstanceOf[ClosedChannelException], String.valueOf(outcome.failure))
        } finally {
          UringHandles.close(client)
          UringHandles.close(listener)
        }
      }
    }
  }

  test("a cancel finds a command whose completion waits in the kernel's overflow list, as the ring is full") {
    UringReactor.scoped(entries = 8) { r => // a completion queue of 16
      val count = 100
      val pipes = Seq.fill(count)(UringHandles.pipe())
      try {
        val reads = pipes.map((in, _) => UringOps.read(in, allocate(1)))
        val completed = Array.fill(count)(0)
        var cancelled: Option[Boolean] = None
        var attempts = 0
        for i <- 0 until count do
          r.submit(
            reads(i),
            new Completion[UringOp] {
              def onComplete(op: UringOp): Unit = {
                completed(i) += 1
                // The first to complete cancels one whose completion is still in the kernel, for want of room. A cancel
                // that throws would be reported and the next completion would try again, so only one may try.
                if attempts == 0 then {
                  attempts += 1
                  cancelled = Some(r.cancel(reads(if i == count - 1 then 0 else count - 1)))
                }
              }
              override def onFailure(op: UringOp, t: Throwable): Unit = throw t
              override def onCancel(op: UringOp): Unit = fail(s"read $i was cancelled after it finished")
            }
          )
        r.runOnce(block = false) // issues them all
        val byte = stackalloc[Byte]()
        !byte = 1
        // Each write ends a read, whose completion the kernel posts as the writing thread returns to it: the ring fills.
        for (_, out) <- pipes do assertEquals(unistd.write(fd(out), byte, 1.toCSize).toInt, 1)
        turnUntil(r)(completed.forall(_ > 0))
        assertEquals(cancelled, Some(false), "the read had finished, so the cancel is too late")
        assertEquals(completed.toSeq, Seq.fill(count)(1))
      } finally pipes.foreach((in, out) => { UringHandles.close(in); UringHandles.close(out) })
    }
  }

  test("a cancel of a file op a kernel worker is running does not wait for it, and the op completes") {
    // A regular file, not /tmp, which may be tmpfs, where a sync has nothing to do.
    val dir = Files.createTempDirectory(java.nio.file.Paths.get(System.getProperty("user.dir")), "uring-sync")
    val path = dir.resolve("file")
    try {
      Files.write(path, new Array[Byte](32 << 20))
      UringReactor.scoped() { r =>
        val opened = Slot[UringKinds.File]()
        assertEquals(run(r, UringOps.open(path.toString, write = true, opened)), null)
        val file = opened.clear()
        try {
          val write = allocate(1 << 20)
          var offset = 0L
          while offset < (32L << 20) do { // dirties the page cache, for the sync to write out
            write.clear()
            assertEquals(run(r, UringOps.write(file, write, offset)), null)
            offset += 1 << 20
          }
          val sync = UringOps.sync(file)
          val outcome = Outcome()
          r.submit(sync, outcome)
          r.runOnce(block = false) // issues it to a kernel worker
          val started = System.nanoTime()
          val cancelled = r.cancel(sync)
          val took = (System.nanoTime() - started) / 1_000_000
          assert(took < 20, s"the cancel waited $took ms for the sync")
          turnUntil(r)(outcome.finished)
          if cancelled then assertEquals((outcome.completed, outcome.cancelled), (0, 1))
          else assertEquals((outcome.completed, outcome.cancelled, outcome.failure), (1, 0, null))
        } finally UringHandles.close(file)
      }
    } finally {
      Files.deleteIfExists(path)
      Files.delete(dir)
    }
  }

  test("a datagram from more buffers than one sendmsg takes is refused, and nothing is sent") {
    UringReactor.scoped() { r =>
      val receiver = UringHandles.bindDatagram(IpAddress.IPv4("127.0.0.1", 0))
      val sender = UringHandles.bindDatagram(IpAddress.IPv4("127.0.0.1", 0))
      try {
        val to = UringHandles.addressOf(receiver, peer = false).asInstanceOf[IpAddress]
        val failure = run(r, UringOps.send(sender, Array.fill(1025)(bytes("x")), to))
        assert(failure.isInstanceOf[IllegalArgumentException], String.valueOf(failure))
        val got = Outcome()
        val read = UringOps.receive(receiver, allocate(2048), Slot[IpAddress]())
        r.submit(read, got)
        for _ <- 1 to 20 do r.runOnce(block = false)
        assert(!got.finished, "a datagram arrived")
        r.cancel(read): Unit
      } finally {
        UringHandles.close(receiver)
        UringHandles.close(sender)
      }
    }
  }

  private def preparedOpcode(command: Command): Int = {
    val storage = new Array[Byte](sizeof[uring.io_uring_sqe].toInt)
    command.prepareSubmission(storage.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[uring.io_uring_sqe]])
    storage(0).toInt
  }

  private def isOpen(descriptor: Int): Boolean = fcntl.fcntl(descriptor, fcntl.F_GETFD, 0) >= 0

  test("EAGAIN makes a command wait for readiness, then submit itself again") {
    val (in, out) = UringHandles.pipe()
    try {
      val read = UringOps.read(in, allocate(8))
      assertEquals(preparedOpcode(read), uring.IORING_OP_READ, "the command itself first")
      assert(!read.finishSubmission(-errno.EAGAIN), "EAGAIN is not completion")
      assertEquals(preparedOpcode(read), uring.IORING_OP_POLL_ADD, "then a readiness wait, rather than a spin")
      assert(!read.finishSubmission(Linux.POLLIN), "readiness is not completion either")
      assertEquals(preparedOpcode(read), uring.IORING_OP_READ, "and then the command again")
    } finally {
      UringHandles.close(in)
      UringHandles.close(out)
    }
  }

  test("a withdrawn readiness wait releases nothing, and a withdrawn accept closes its late connection") {
    val (in, out) = UringHandles.pipe()
    try {
      val accept = new Accept(fd(in), Slot())
      assert(!accept.finishSubmission(-errno.EAGAIN))
      accept.discardSubmission(fd(in)) // a poll mask that happens to equal a descriptor's number
      assert(isOpen(fd(in)), "the number in a readiness wait's result is not closed")
      val (lateIn, lateOut) = UringHandles.pipe()
      UringHandles.close(lateIn)
      accept.discardSubmission(fd(lateOut))
      assert(!isOpen(fd(lateOut)), "a withdrawn accept's late descriptor is closed")
    } finally {
      UringHandles.close(in)
      UringHandles.close(out)
    }
  }

  test("pooled reads take buffers up front while more than half the pool is free, and wait for readiness after") {
    UringReactor.scoped() { r =>
      val pipes = (1 to 4).map(_ => UringHandles.pipe())
      val pool = UringBackend.pool(16, 4)
      try {
        val reads = pipes.map((in, _) => (Slot[Received](), Outcome())).toArray
        val ops = pipes.zip(reads).map { case ((in, _), (into, outcome)) => UringOps.read(in, pool, into) }
        for (op, (_, outcome)) <- ops.zip(reads) do r.submit(op, outcome)
        r.runOnce(block = false)
        // Two take a buffer, and two wait for readiness with none; one whose kernel answers a read of an empty pipe
        // with EAGAIN, rather than waiting, gives its buffer back and waits too.
        assert(pool.available >= 2, s"reads hold at most half the pool, not ${4 - pool.available} buffers")
        val before = pool.available
        assert(r.cancel(ops(0)))
        assert(pool.available >= before && pool.available <= before + 1, "a withdrawn read gives back what it held")
        for (((_, out), (into, outcome)), i) <- pipes.zip(reads).zipWithIndex.drop(1) do {
          assertEquals(run(r, UringOps.write(out, bytes(s"p$i"))), null)
          turnUntil(r)(outcome.completed == 1)
          val received = into.clear()
          assertEquals(StandardCharsets.UTF_8.decode(received.buffer).toString, s"p$i")
          received.release()
        }
        assertEquals(pool.available, 4)
      } finally {
        for (in, out) <- pipes do {
          UringHandles.close(in)
          UringHandles.close(out)
        }
        pool.close()
      }
    }
  }

  /** `count` connected pairs of TCP sockets on loopback, as (client, server), closed after `body`. */
  private def withConnections(r: UringReactor, count: Int)(
      body: IndexedSeq[(UringKinds.Connection, UringKinds.Connection)] => Unit
  ): Unit = {
    val listener = UringHandles.listen(IpAddress.IPv4("127.0.0.1", 0))
    val address = UringHandles.addressOf(listener, peer = false).asInstanceOf[IpAddress]
    val pairs = scala.collection.mutable.ArrayBuffer.empty[(UringKinds.Connection, UringKinds.Connection)]
    try {
      for _ <- 1 to count do {
        val client = UringHandles.connect(address)
        val accepted = Slot[UringKinds.Connection]()
        assertEquals(run(r, UringOps.accept(listener, accepted)), null)
        pairs += ((client, accepted.clear()))
      }
      body(pairs.toIndexedSeq)
    } finally {
      for (client, server) <- pairs do {
        UringHandles.close(client)
        UringHandles.close(server)
      }
      UringHandles.close(listener)
    }
  }

  test("idle connections with pooled reads pending hold at most half the pool, however many there are") {
    UringReactor.scoped() { r =>
      withConnections(r, 20) { pairs =>
        val pool = UringBackend.pool(16, 2)
        try {
          val reads = pairs.map(_ => (Slot[Received](), Outcome()))
          for ((_, server), (into, outcome)) <- pairs.zip(reads) do r.submit(UringOps.read(server, pool, into), outcome)
          for _ <- 1 to 3 do r.runOnce(block = false)
          assert(pool.available >= 1, "idle connections hold at most half the pool")
          assert(reads.forall(_._2.failure == null), "no read failed for want of a buffer")
          for (((client, _), (into, outcome)), i) <- pairs.zip(reads).zipWithIndex do {
            assertEquals(run(r, UringOps.write(client, bytes(s"m$i"))), null)
            turnUntil(r)(outcome.finished)
            assertEquals(outcome.failure, null)
            val received = into.clear()
            assertEquals(StandardCharsets.UTF_8.decode(received.buffer).toString, s"m$i")
            received.release()
          }
          assertEquals(pool.available, 2)
        } finally pool.close()
      }
    }
  }

  test("a read and a write of a socket are a receive and a send, which the kernel waits for itself") {
    UringReactor.scoped() { r =>
      withConnections(r, 1) { pairs =>
        val (client, server) = pairs(0)
        val buf = allocate(16)
        val read = UringOps.read(server, buf)
        assertEquals(preparedOpcode(read), uring.IORING_OP_RECV)
        assertEquals(preparedOpcode(UringOps.write(client, bytes("x"))), uring.IORING_OP_SEND)
        assertEquals(preparedOpcode(UringOps.read(server, Array(allocate(4), allocate(4)))), uring.IORING_OP_RECVMSG)
        assertEquals(preparedOpcode(UringOps.write(client, Array(bytes("a"), bytes("b")))), uring.IORING_OP_SENDMSG)
        val outcome = Outcome()
        r.submit(read, outcome)
        for _ <- 1 to 3 do r.runOnce(block = false)
        assert(!outcome.finished)
        assert(!read.waitsForReadiness, "the receive waits in the kernel, not in a poll after EAGAIN")
        assertEquals(run(r, UringOps.write(client, bytes("sent"))), null)
        turnUntil(r)(outcome.finished)
        assertEquals(text(buf), "sent")
      }
    }
  }

  test("heap buffers, at an offset into their array, are read into and written from in place") {
    UringReactor.scoped() { r =>
      withConnections(r, 1) { pairs =>
        val (client, server) = pairs(0)
        val backing = ByteBuffer.allocate(32)
        backing.position(5)
        val out = backing.slice() // array offset 5
        out.put("heap!".getBytes(StandardCharsets.UTF_8)).flip()
        assertEquals(run(r, UringOps.write(client, Array(ByteBuffer.wrap("<".getBytes), out))), null)
        assert(!out.hasRemaining)
        val first = ByteBuffer.allocate(3)
        val second = ByteBuffer.allocate(16)
        second.position(2)
        val in = second.slice()
        val read = UringOps.read(server, Array(first, in))
        var got = 0
        while got < 6 do {
          assertEquals(run(r, read), null)
          got = first.position() + in.position()
        }
        assertEquals(new String(first.array(), StandardCharsets.UTF_8), "<he")
        assertEquals(new String(in.array(), in.arrayOffset(), in.position(), StandardCharsets.UTF_8), "ap!")
        assertEquals(new String(backing.array(), 5, 5, StandardCharsets.UTF_8), "heap!", "a write leaves the bytes")
      }
    }
  }

  test("a close tells, and cancels on, only the reactors that had commands in the kernel on the handle") {
    UringReactor.scoped() { a =>
      UringReactor.scoped() { b =>
        val (in, out) = UringHandles.pipe()
        try {
          val outcome = Outcome()
          a.submit(UringOps.read(in, allocate(8)), outcome)
          a.runOnce(block = false)
          assert(a.bit != b.bit)
          assertEquals(in.reactorBits & (a.bit | b.bit), a.bit)
          UringHandles.close(in)
          turnUntil(a)(outcome.finished)
          assert(outcome.failure.isInstanceOf[ClosedChannelException], String.valueOf(outcome.failure))
        } finally UringHandles.close(out)
      }
    }
  }

  test("a stream read of a regular file is not tried on the loop's thread, and a cancel does not wait for it") {
    UringReactor.scoped() { r =>
      val path = Files.createTempFile(java.nio.file.Paths.get(System.getProperty("user.dir")), "uring", ".bin")
      try {
        Files.write(path, Array.fill[Byte](64)(7))
        val file = UringHandles.openFile(path.toString, write = false)
        try {
          assert(!file.worthTrying(read = true), "a read of a regular file would wait for the disk on the loop")
          val read = UringOps.read(file, allocate(64))
          assert(read.runsInWorker)
          assertEquals(run(r, read), null)
        } finally UringHandles.close(file)
      } finally Files.delete(path)
    }
  }

  test("wakeup from another thread ends a blocked turn, and one before a turn keeps it from blocking") {
    UringReactor.scoped() { r =>
      r.runOnce(block = false) // the eventfd's read is in the kernel
      val waker = new Thread(() => {
        Thread.sleep(50)
        r.wakeup()
      })
      val started = System.nanoTime()
      waker.start()
      r.runOnce(block = true)
      waker.join()
      assert(System.nanoTime() - started >= 40_000_000L, "the turn did not block")
      r.wakeup()
      r.wakeup()
      val woken = System.nanoTime()
      r.runOnce(block = true)
      assert(System.nanoTime() - woken < 1_000_000_000L, "a wake-up before the turn was lost")
    }
  }

  test("trySubmit writes and reads at once, and leaves a read with nothing to read in the kernel") {
    UringReactor.scoped() { r =>
      val (in, out) = UringHandles.pipe()
      try {
        val sent = Outcome()
        assert(r.trySubmit(UringOps.write(out, bytes("now")), sent))
        val buf = allocate(8)
        val read = UringOps.read(in, buf)
        val got = Outcome()
        assert(r.trySubmit(read, got))
        assertEquals(text(buf), "now")
        val waiting = Outcome()
        assert(!r.trySubmit(read, waiting))
        r.runOnce(block = false)
        assertEquals(waiting.completed, 0)
        assertEquals(run(r, UringOps.write(out, bytes("!"))), null)
        turnUntil(r)(waiting.finished)
        assertEquals(text(buf), "now!")
        assertEquals((sent.completed, got.completed), (0, 0), "an op finished by trySubmit never calls its completion")
      } finally {
        UringHandles.close(in)
        UringHandles.close(out)
      }
    }
  }
}
