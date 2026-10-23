package gears.io.uring

import gears.io.reactor.{Completion, Interest}

import java.nio.ByteBuffer

class UringCancelRaceSuite extends munit.FunSuite:
  test("a cancel after a completion that does not finish the op still cancels it"):
    val reactor = UringReactor.open()
    val (in, out) = reactor.handles.pipe()
    try
      reactor.handles.writeNow(out, ByteBuffer.wrap(Array[Byte](1)))
      var checks = 0
      var cancelled = false
      var cancelResult: Option[Boolean] = None
      // The pipe stays readable and the check never passes, so without the cancel this op would wait again forever.
      val waiting = reactor.ops.whenReady(in, Interest.Read): () =>
        checks += 1
        false
      val first = reactor.ops.whenReady(in, Interest.Read)(() => true)
      // Both are ready at once, so both completions arrive in one batch, the first one first.
      reactor.submit(first, _ => cancelResult = Some(reactor.cancel(waiting)))
      reactor.submit(
        waiting,
        new Completion[reactor.Op]:
          def onComplete(op: reactor.Op): Unit = fail("The unsatisfied wait completed")
          override def onCancel(op: reactor.Op): Unit =
            cancelled = true
            reactor.stop()
      )
      reactor.submit(reactor.ops.timer(2000), _ => reactor.stop())
      reactor.run()
      assert(cancelled, "the cancel was lost and the op submitted again")
      assertEquals(checks, 1)
      assertEquals(cancelResult, Some(false), "too late to promise onComplete won't run")
    finally
      reactor.close()
      reactor.handles.close(in)
      reactor.handles.close(out)

