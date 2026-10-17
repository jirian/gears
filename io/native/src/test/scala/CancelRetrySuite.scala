package gears.asyncio.uring

import gears.asyncio.{Completion, Repeatable}

import java.io.IOException
import java.nio.ByteBuffer
import scala.scalanative.unsafe.Ptr

import uring.io_uring_sqe

class CancelRetrySuite extends munit.FunSuite:
  test("failed cancellation preparation is retried without returning the buffer early"):
    val reactor = UringReactor.open()
    val (read, write) = reactor.handles.pipe()
    var cancelled = false
    var attempts = 0
    val op = new RingOp[reactor.type] with Repeatable:
      private val underlying = new Read[reactor.type](read, ByteBuffer.allocate(8))
      private[uring] def prepare(sqe: Ptr[io_uring_sqe]): Unit = underlying.prepare(sqe)
      private[uring] def finish(result: Int): Boolean = underlying.finish(result)
      override private[uring] def prepareCancel(sqe: Ptr[io_uring_sqe], id: Long): Unit =
        attempts += 1
        if attempts == 1 then throw IOException("Injected temporary cancellation failure")
        super.prepareCancel(sqe, id)
    try
      reactor.submit(
        op,
        new Completion[reactor.Op]:
          def onComplete(op: reactor.Op): Unit = fail("Cancelled read completed normally")
          override def onCancel(op: reactor.Op): Unit =
            cancelled = true
            reactor.stop()
      )
      assert(reactor.cancel(op))
      assert(!cancelled)
      reactor.submit(reactor.ops.timer(1000), _ => reactor.stop())
      reactor.run()
      assert(cancelled, "The failed cancellation was not retried")
      assertEquals(attempts, 2)
    finally
      reactor.close()
      reactor.handles.close(read)
      reactor.handles.close(write)
