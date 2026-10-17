package gears.io.examples

import gears.io.reactor.{Completion, Reactor, ResolvedAddress, Slot}
import gears.io.uring.UringReactor

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

def reactorExample(): Unit =
  given Reactor.Factory[Reactor] = () => UringReactor.open()
  Reactor.scoped: r =>
    var pending = 0
    def submit(op: r.Op)(whenDone: => Unit): Unit =
      pending += 1
      r.submit(
        op,
        new Completion[r.Op]:
          def onComplete(op: r.Op): Unit =
            pending -= 1
            whenDone
          override def onFailure(op: r.Op, failure: Throwable): Unit =
            pending -= 1
            println(s"op failed: $failure")
          override def onCancel(op: r.Op): Unit =
            pending -= 1
            println("onCancel ran: the kernel has released the read's buffer, so it is the caller's again")
      )
    def runUntilIdle(): Unit = while pending > 0 do r.runOnce(block = true)

    val (in, out) = r.handles.pipe()
    val received = ByteBuffer.allocate(64)
    val read = r.ops.read(in, received)
    submit(read)(
      println(s"read '${new String(received.array(), 0, received.position(), StandardCharsets.UTF_8)}' from a pipe")
    )
    submit(r.ops.write(out, ByteBuffer.wrap("through the reactor".getBytes(StandardCharsets.UTF_8))))(())
    val started = System.nanoTime()
    submit(r.ops.timer(100))(println(s"a 100 ms timer fired after ${(System.nanoTime() - started) / 1_000_000} ms"))
    val computed = Slot[String]()
    submit(r.ops.blocking(() => s"computed on ${Thread.currentThread().getName}", computed))(
      println(s"a blocking op ${computed.clear()}")
    )
    val addresses = Slot[List[ResolvedAddress]]()
    submit(r.ops.resolve("localhost", addresses))(println(s"localhost resolves to ${addresses.clear().mkString(", ")}"))
    runUntilIdle()

    val waker = new Thread(() =>
      Thread.sleep(100)
      r.wakeup()
    )
    val blockedAt = System.nanoTime()
    waker.start()
    r.runOnce(block = true)
    waker.join()
    println(
      s"with nothing pending, runOnce blocked until another thread's wakeup, ${(System.nanoTime() - blockedAt) / 1_000_000} ms"
    )

    submit(read)(fail("the cancelled read completed"))
    println(s"cancel on a pending read returns ${r.cancel(read)}")
    runUntilIdle()
    r.handles.close(out)
    r.handles.close(in)
