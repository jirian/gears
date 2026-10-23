package asyncio.uring

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import scala.scalanative.posix.errno
import scala.scalanative.posix.fcntl
import scala.scalanative.posix.unistd
import scala.scalanative.runtime.ByteArray
import scala.scalanative.unsafe.*

import asyncio.Completion
import asyncio.Handle
import asyncio.Interest
import asyncio.IpAddress
import asyncio.Slot
import asyncio.UnixAddress
import munit.FunSuite

/** What the io_uring backend does beyond the reactor contract: how it opens descriptors, how a cancel meets a
  * completion queue entry that has already arrived, and how a command waits for readiness after `EAGAIN`.
  */
class UringSuite extends FunSuite {
  private lazy val arena = asyncio.testkit.BufferArena.open()
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

  private def fd(handle: Handle): Int = UringHandles.descriptor(handle)

  private def closeOnExec(handle: Handle): Boolean =
    (fcntl.fcntl(fd(handle), fcntl.F_GETFD, 0) & fcntl.FD_CLOEXEC) != 0

  test("every handle is close-on-exec, accepted connections and opened files too") {
    val dir = Files.createTempDirectory("uring")
    UringReactor.scoped() { r =>
      val (in, out) = UringHandles.pipe()
      val listener = UringHandles.listener(IpAddress.IPv4("127.0.0.1", 0))
      assertEquals(run(r, UringOps.listening(listener)), null)
      val datagram = UringHandles.datagram(IpAddress.IPv4("127.0.0.1", 0), null)
      val path = dir.resolve("file")
      Files.write(path, Array[Byte](1))
      val opened = Handle.Slot()
      assertEquals(run(r, UringOps.open(path.toString, write = false, opened)), null)
      val file = opened.clear()
      val unixPath = dir.resolve("unix.sock").toString
      val unixListener = UringHandles.listener(UnixAddress(unixPath))
      assertEquals(run(r, UringOps.listening(unixListener)), null)
      val client = UringHandles.connect(UnixAddress(unixPath))
      val accepted = Handle.Slot()
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
    val path = s"/tmp/asyncio-uring-stale-${unistd.getpid()}.sock"
    UringReactor.scoped() { r =>
      val stale = UringHandles.listener(UnixAddress(path))
      assertEquals(run(r, UringOps.listening(stale)), null)
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
  }

  /** Runs `op` on `r` until it is done, and returns its failure, if any. */
  private def run(r: UringReactor, op: UringOp)(using munit.Location): Throwable | Null = {
    val outcome = Outcome()
    r.submit(op, outcome)
    turnUntil(r)(outcome.finished)
    outcome.failure
  }

  test("a cancel after the entry of a read that finished has arrived loses nothing: the read completes") {
    UringReactor.scoped() { r =>
      val (in, out) = UringHandles.pipe()
      try {
        assertEquals(UringHandles.writeNow(out, bytes("arrived")), 7)
        val buf = allocate(16)
        val read = UringOps.read(in, buf)
        val outcome = Outcome()
        var cancelled: Option[Boolean] = None
        // Both finish when they reach the kernel, so their entries arrive in one batch, the readiness first.
        r.submit(UringOps.ready(in, Interest.Read), _ => cancelled = Some(r.cancel(read)))
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

  test("a cancel after the entry of a read that found nothing has arrived cancels it at once") {
    UringReactor.scoped() { r =>
      val (in, out) = UringHandles.pipe()
      try {
        val buf = allocate(16)
        val read = UringOps.read(in, buf)
        val outcome = Outcome()
        var cancelled: Option[Boolean] = None
        var cancelledFirst = -1
        r.submit(
          UringOps.ready(out, Interest.Write),
          _ => {
            cancelled = Some(r.cancel(read))
            cancelledFirst = outcome.cancelled
          }
        )
        r.submit(read, outcome)
        turnUntil(r)(cancelled.isDefined)
        assertEquals(cancelled, Some(true))
        assertEquals(cancelledFirst, 1, "onCancel ran before cancel returned")
        UringHandles.writeNow(out, bytes("late"))
        for _ <- 1 to 5 do r.runOnce(block = false)
        assertEquals((outcome.completed, outcome.cancelled), (0, 1), "the read was not submitted again")
        assertEquals(buf.position(), 0)
      } finally {
        UringHandles.close(in)
        UringHandles.close(out)
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

  test("a cancelled readiness wait releases nothing, and a cancelled accept closes its late connection") {
    val (in, out) = UringHandles.pipe()
    try {
      val accept = UringOps.accept(in, Handle.Slot())
      assert(!accept.finishSubmission(-errno.EAGAIN))
      accept.discardSubmission(fd(in)) // a poll mask that happens to equal a descriptor's number
      assert(isOpen(fd(in)), "the number in a readiness wait's result is not closed")
      val (lateIn, lateOut) = UringHandles.pipe()
      UringHandles.close(lateIn)
      accept.discardSubmission(fd(lateOut))
      assert(!isOpen(fd(lateOut)), "a cancelled accept's late descriptor is closed")
    } finally {
      UringHandles.close(in)
      UringHandles.close(out)
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

  test("a packet larger than the buffer fails the receive, and an empty one is received") {
    UringReactor.scoped() { r =>
      val server = UringHandles.datagram(IpAddress.IPv4("127.0.0.1", 0), null)
      val serverAddress = {
        val storage = new Array[Byte](128)
        val length = stackalloc[scala.scalanative.posix.sys.socket.socklen_t]()
        !length = scala.scalanative.unsigned.UInt.valueOf(128)
        val address =
          storage.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[scala.scalanative.posix.sys.socket.sockaddr]]
        assertEquals(scala.scalanative.posix.sys.socket.getsockname(fd(server), address, length), 0)
        UringHandles.decode(address, !length).asInstanceOf[IpAddress]
      }
      val client = UringHandles.datagram(null, serverAddress)
      try {
        val small = allocate(4)
        val from = Slot[IpAddress]()
        assertEquals(run(r, UringOps.send(client, bytes("too long for it"))), null)
        val tooLong = run(r, UringOps.receive(server, small, from))
        assert(tooLong.isInstanceOf[IOException], String.valueOf(tooLong))
        val empty = allocate(1)
        empty.limit(0)
        assertEquals(run(r, UringOps.send(client, empty)), null)
        assertEquals(run(r, UringOps.receive(server, small, from)), null)
        assertEquals(small.remaining(), 0)
        assert(from.clear().isInstanceOf[IpAddress.IPv4])
      } finally {
        UringHandles.close(client)
        UringHandles.close(server)
      }
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
        UringHandles.writeNow(out, bytes("!"))
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
