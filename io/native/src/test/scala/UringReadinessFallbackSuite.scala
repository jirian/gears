package gears.io.uring

import gears.io.reactor.HandleSlot

import scala.scalanative.posix.errno
import scala.scalanative.posix.fcntl
import scala.scalanative.runtime.ByteArray
import scala.scalanative.unsafe.*

class UringReadinessFallbackSuite extends munit.FunSuite:
  private type Owner = this.type

  private def preparedOpcode(op: RingOp[?]): Int =
    val storage = new Array[Byte](sizeof[uring.io_uring_sqe].toInt)
    val sqe = storage.asInstanceOf[ByteArray].at(0)
    op.prepareSubmission(sqe.asInstanceOf[Ptr[uring.io_uring_sqe]])
    storage(0).toInt

  private def isOpen(fd: Int): Boolean = fcntl.fcntl(fd, fcntl.F_GETFD, 0) >= 0

  test("EAGAIN makes an op wait for readiness, then submit itself again"):
    val (in, out) = UringHandles.pipe()
    try
      val read = new Read[Owner](in, java.nio.ByteBuffer.allocate(8))
      assertEquals(preparedOpcode(read), uring.IORING_OP_READ, "the op itself first")
      assert(!read.finishSubmission(-errno.EAGAIN), "EAGAIN is not completion")
      assertEquals(preparedOpcode(read), uring.IORING_OP_POLL_ADD, "then a readiness wait, rather than a spin")
      assert(!read.finishSubmission(LinuxConstants.POLLIN), "readiness is not completion either")
      assertEquals(preparedOpcode(read), uring.IORING_OP_READ, "and then the op again")
    finally
      UringHandles.close(in)
      UringHandles.close(out)

  test("a cancelled readiness wait releases nothing: its result is a poll mask, not a descriptor"):
    val (in, out) = UringHandles.pipe()
    try
      val accept = new Accept[Owner](in, HandleSlot())
      assert(!accept.finishSubmission(-errno.EAGAIN))
      accept.discardSubmission(in)
      assert(isOpen(in), "the number in a readiness wait's result is not closed")
      val (lateIn, lateOut) = UringHandles.pipe()
      UringHandles.close(lateIn)
      accept.discardSubmission(lateOut)
      assert(!isOpen(lateOut), "a cancelled accept's late descriptor is closed")
    finally
      UringHandles.close(in)
      UringHandles.close(out)

  test("ops without a readiness wait treat EAGAIN as a plain retry"):
    val timer = new UringTimer[Owner](1_000_000L)
    intercept[java.io.IOException](timer.finishSubmission(-errno.EAGAIN))
    val (in, out) = UringHandles.pipe()
    try
      val connect = new Connect[Owner](out)
      assert(!connect.finishSubmission(-errno.EAGAIN), "a poll op just polls again")
      assertEquals(preparedOpcode(connect), uring.IORING_OP_POLL_ADD)
    finally
      UringHandles.close(in)
      UringHandles.close(out)
