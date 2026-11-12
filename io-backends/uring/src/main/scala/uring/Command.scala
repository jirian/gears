package gears.async.io
package uring

import gears.async.io.shareable.AbstractMakeDeadlineOp
import gears.async.io.shareable.AbstractMakeTimerOp
import gears.async.io.shareable.RepeatableAbstractTimerHeapOp
import gears.async.io.shareable.SingleUseAbstractTimerHeapOp
import gears.async.io.shareable.TimerHeapOp
import gears.async.io.shareable.WorkpoolScheduler

import java.io.IOException
import java.nio.ByteBuffer
import scala.concurrent.duration.FiniteDuration
import scala.scalanative.libc.string
import scala.scalanative.memory.PointerBufferOps.given
import scala.scalanative.posix.errno
import scala.scalanative.posix.sys.socket
import scala.scalanative.posix.sys.uio
import scala.scalanative.runtime.ByteArray
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.control.NonFatal

import uring.*
import uringStructOps.*

/** An op of the io_uring backend. */
sealed trait UringOp extends TimerHeapOp

private[uring] final class UringTimer(duration: FiniteDuration) extends AbstractMakeTimerOp(duration) with UringOp

private[uring] final class UringDeadline(initial: Long) extends AbstractMakeDeadlineOp(initial) with UringOp

/** An op io_uring runs: each submission of it is one submission queue entry, which `prepare` fills in, and the
  * completion queue entry that answers it is handed to `finish`. It is tried at once first with a non-blocking system
  * call (`tryAtOnce`), and goes to the ring only if that would block. Its worker, the performer's own, prepares and
  * reaps it, and keeps its state (`UringScheduler.UringWorker`); the kernel owns its buffers from the submission until
  * its completion has been reaped, or until a cancel has made the kernel let go of it.
  *
  * A command on a non-blocking descriptor that has nothing to do yet may complete with `EAGAIN`, where the kernel does
  * not wait for it itself: it then waits for readiness with a poll for `readinessEvents`, and is submitted again once
  * the descriptor is ready, rather than spinning.
  *
  * Once its completion has been applied (`finish`), its work is done (`committed`): a performer cancelled meanwhile
  * gets its outcome all the same, as `Io.perform` has it.
  */
private[uring] abstract class Command extends RepeatableAbstractTimerHeapOp with UringOp {

  /** The handle it works on: given, or for one that opens a socket, made by `begin`. */
  def handle: UringHandle

  final def fd: Int = handle.fd

  /** Done as it is performed, after `check` and before `handle` is used: an opener opens here. */
  def begin(): Unit = ()

  /** Whether it is its stream's one read, its one write, or neither. */
  protected def direction: Command.Direction = Command.Direction.Neither

  /** Checks made as it is performed, before anything is done, throwing an `IllegalArgumentException`. */
  def check(): Unit = ()

  /** The turns it takes on its handle, waiting for those before it, or null if it takes none. */
  def turns: Turns | Null = null

  /** Fills in `sqe` for one submission. Throwing fails the op, as it would at once: nothing has reached the kernel. */
  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit

  /** Takes in the result of one submission, a count or a negated `errno`: true once the op is done, false to submit it
    * again. Throwing fails the op.
    */
  protected def finish(res: Int): Boolean

  /** Whether it has nothing to do as it stands, such as a write with no bytes remaining: done with no system call. */
  def nothingToDo: Boolean = false

  /** Performs the op at once with a non-blocking system call: true if that finished it, false, having done nothing, if
    * it would block. Throwing fails the op. By default it is never tried, and always goes to the ring.
    */
  def tryAtOnce(): Boolean = false

  /** Lets go of what a submission took, its result never applied: the stand-ins of its buffers, say. */
  protected def discard(): Unit = ()

  /** The poll events to wait for when a submission completes with `EAGAIN`, or 0 for a command that never does. */
  protected def readinessEvents: Int = 0

  // A performance's state, kept by its worker on the worker's thread; `outcome`, `done`, `applied`, `offHeap` and
  // `wantOut` are read by the firing and by a withdrawal from other threads. `applied`: its outcome is set and nothing
  // of it is in use, so a time limit reached as it fires has nothing to take back.
  private var stage = Command.Stage.Idle
  private var entryId = 0L
  private var entry: Ptr[io_uring_sqe] = null
  private[uring] var owner: UringScheduler#UringWorker | Null = null
  private var awaitingReadiness = false
  @volatile private[uring] var outcome: Try[Unit] = WorkpoolScheduler.SuccessUnit
  @volatile private[uring] var done = false
  @volatile private[uring] var applied = false
  @volatile private[uring] var offHeap = false
  @volatile private[uring] var wantOut = false

  override def onEntry(): Unit = {
    super.onEntry()
    stage = Command.Stage.Idle
    awaitingReadiness = false
    outcome = WorkpoolScheduler.SuccessUnit
    done = false
    applied = false
    offHeap = false
    wantOut = false
  }

  override protected def committed: Boolean = done

  override protected def limitReached(): Boolean = owner.nn.limitReached(this)

  def wakeup(resumedAt: Long): Try[Unit] = outcome

  /** Where its entry is: not in the ring, prepared and not yet submitted, or submitted. */
  private[uring] final def state: Command.Stage = stage

  /** Its entry's user data and slot, while it is prepared or submitted. */
  private[uring] final def id: Long = entryId
  private[uring] final def sqe: Ptr[io_uring_sqe] = entry

  /** Its entry has been prepared, in `sqe` with user data `id`. */
  private[uring] final def prepared(id: Long, sqe: Ptr[io_uring_sqe]): Unit = {
    entryId = id
    entry = sqe
    stage = Command.Stage.Prepared
  }

  /** Its entry has gone to the kernel. */
  private[uring] final def submitted(): Unit = stage = Command.Stage.Submitted

  /** Its entry is out of the ring: completed, withdrawn, or turned into a no-op. */
  private[uring] final def outOfRing(): Unit = stage = Command.Stage.Idle

  /** Whether it may go to the ring now, asked on its worker before each submission: false if it has parked instead
    * (`park`), off the ring, until something of the backend's own starts it again on its worker
    * (`UringWorker.startTurn`).
    */
  private[uring] def readyToSubmit(): Boolean = true

  /** Takes it out of where it parked, from any thread: false if something has started it again already. */
  private[uring] def leaveParking(): Boolean = false

  /** It waits off the ring, where `leaveParking` takes it out. Worker thread. */
  protected final def park(): Unit = stage = Command.Stage.Parked

  /** It has been started again, out of parking, if it was parked. Worker thread. */
  private[uring] final def resume(): Unit =
    if stage == Command.Stage.Parked then {
      stage = Command.Stage.Idle
      if handle.parked eq this then handle.parked = null
    }

  /** Done with no entry: nothing to do, or done at once by a non-blocking call, if its handle allows that. */
  private[uring] final def doneAtOnce(): Boolean = nothingToDo || (handle.triesAtOnce && tryAtOnce())

  /** Takes its stream's one read or one write, if it is one: false if another holds it. */
  private[uring] final def claim(): Boolean = direction match {
    case Command.Direction.Reading => handle.reading.compareAndSet(false, true)
    case Command.Direction.Writing => handle.writing.compareAndSet(false, true)
    case Command.Direction.Neither => true
  }

  /** The ring its entries are on, counted by its handle (`onRing`) until it is done with it (`release`). */
  private var ringOf: Ring | Null = null

  private[uring] final def onRing(ring: Ring): Unit = {
    handle.onRing(ring)
    ringOf = ring
  }

  /** Lets go of its stream's read or write, and of its count on its ring: it is done with its handle for now. */
  private[uring] final def release(): Unit = {
    val ring = ringOf
    if ring != null then {
      ringOf = null
      handle.offRing(ring)
    }
    direction match {
      case Command.Direction.Reading => handle.reading.set(false)
      case Command.Direction.Writing => handle.writing.set(false)
      case Command.Direction.Neither => ()
    }
  }

  private[uring] final def prepareSubmission(sqe: Ptr[io_uring_sqe]): Unit =
    if awaitingReadiness then io_uring_prep_poll_add(sqe, fd, readinessEvents.toUInt)
    else prepare(sqe)

  private[uring] final def finishSubmission(res: Int): Boolean =
    if awaitingReadiness then {
      awaitingReadiness = false // the descriptor is ready: submitting again runs the command itself
      if res < 0 && !Results.transient(res) then {
        discard() // what the submission before it staged
        throw Results.failure(s"Failed to wait for descriptor $fd", res)
      }
      false
    } else if res == -errno.EAGAIN && readinessEvents != 0 then {
      awaitingReadiness = true
      false
    } else finish(res)

  /** Lets go of what the completion `res`, not applied, made: only the command's own entry's, not a readiness poll's.
    */
  private[uring] final def dropCompletion(res: Int): Unit = if !awaitingReadiness then dropResult(res)

  /** What `dropCompletion` lets go of: nothing, but an accepted connection's descriptor. */
  protected def dropResult(res: Int): Unit = ()

  /** Lets go of a submission withdrawn, or failed by a close, before its result was applied. */
  private[uring] final def discardSubmission(): Unit = {
    awaitingReadiness = false
    discard()
  }

  override def toString: String = s"${getClass.getSimpleName} on $handle"
}

private[uring] object Command {
  enum Direction { case Neither, Reading, Writing }

  /** A performance's stage on its worker: not in the ring; an entry prepared and not yet submitted; submitted; parked
    * off the ring, waiting for the backend to start it again.
    */
  enum Stage { case Idle, Prepared, Submitted, Parked }

  def alreadyPending(op: AnyRef): IllegalArgumentException =
    new IllegalArgumentException(s"An op reading, or writing, as $op does, is already pending on its handle")
}

/** An op done at once, on the performer's thread, which never reaches the ring. */
private[uring] abstract class AtOnce extends SingleUseAbstractTimerHeapOp with UringOp {
  def run(): Unit
  def wakeup(resumedAt: Long): Try[Unit] = WorkpoolScheduler.SuccessUnit
}

private[uring] object Results {

  /** The offset that makes a read or write use and advance the file position, as `read(2)` and `write(2)` do. */
  val currentPosition: __u64 = (-1L).toULong

  /** Whether a negative result only means "try again". */
  def transient(res: Int): Boolean = res == -errno.EINTR || res == -errno.EAGAIN

  /** The failure of a system call or a submission whose result is `res`, a negated `errno`. */
  def failure(what: String, res: Int): IOException = new SystemFailure(s"$what: ${fromCString(string.strerror(-res))}")

  /** The failure of a system call that has just failed, by its `errno`. */
  def failed(what: String): IOException = failure(what, -errno.errno)

  /** A system call's failure. With no stack trace: most come from the peer or the system, such as a reset, a refused
    * connection or a descriptor limit, and on Scala Native a trace costs about as much as serving a request.
    */
  final class SystemFailure(message: String) extends IOException(message) {
    override def fillInStackTrace(): Throwable = this
  }
}

private[uring] object Checks {
  def checkFillable(buf: ByteBuffer): Unit =
    if buf.isReadOnly() then throw new IllegalArgumentException(s"A read-only buffer cannot be read into: $buf")

  def checkFillable(bufs: Array[ByteBuffer]): Unit = {
    if bufs.isEmpty then throw new IllegalArgumentException("A vectored op takes at least one buffer")
    bufs.foreach(checkFillable)
  }

  def checkBuffers(bufs: Array[ByteBuffer]): Unit =
    if bufs.isEmpty then throw new IllegalArgumentException("A vectored op takes at least one buffer")

  def anyRemaining(bufs: Array[ByteBuffer]): Boolean = bufs.exists(_.hasRemaining())

  def bufferFull(buf: ByteBuffer) =
    new IllegalArgumentException(s"The buffer is full, so a read into it can read nothing: $buf")

  def buffersFull(bufs: Array[ByteBuffer]) =
    new IllegalArgumentException(s"No buffer has room, so a read into them can read nothing: ${bufs.mkString(", ")}")

  def notEmpty(slots: Slot[?]*): Unit =
    if !slots.forall(_.isEmpty) then
      throw new IllegalArgumentException(
        "The op's slot still holds a value, not yet cleared, which the op would overwrite"
      )
}

// ---- Buffers given to the kernel, from the backend on async-io

private[uring] final class Staging(bufs: Array[ByteBuffer], copyIn: Boolean, copyOut: Boolean) {
  private val direct = NativeBuffer.allAddressable(bufs)
  private val stands = if direct then bufs else new Array[ByteBuffer](bufs.length)
  private var zone: Zone | Null = null

  /** The buffers the kernel is given for this submission, in `bufs`' order. */
  def begin(): Array[ByteBuffer] = {
    if !direct then {
      end()
      var i = 0
      while i < bufs.length do {
        val b = bufs(i)
        stands(i) =
          if NativeBuffer.addressable(b) then b
          else {
            if zone == null then zone = Zone.open()
            NativeBuffer.standIn(b, NativeBuffer.stagingMemory(b.remaining())(using zone.nn), copyIn)
          }
        i += 1
      }
    }
    stands
  }

  /** Advances the stand-ins by the `n` bytes the kernel moved, in order, and makes it the buffers' own. */
  def commit(n: Int): Unit = {
    NonBlocking.advance(stands, n.toLong)
    if !direct then {
      var i = 0
      while i < bufs.length do {
        val b = bufs(i)
        if !(stands(i) eq b) then NativeBuffer.settle(b, stands(i), copyOut)
        i += 1
      }
      end()
    }
  }

  /** Lets the stand-ins go, the buffers left as they were. */
  def end(): Unit = {
    val z = zone
    if z != null then {
      zone = null
      z.close()
    }
  }
}

/** A `msghdr`, its `iovec`s, one per buffer, and room for a socket address if `named`, for `readv`, `writev`, `sendmsg`
  * and `recvmsg`: arrays the command owns, since the kernel reads them after the submitting call returns.
  */
private[uring] final class MessageHeader(buffers: Int, named: Boolean) {
  private val header = new Array[Byte](sizeof[msghdr].toInt)
  private val vectorStorage = new Array[Byte](sizeof[uio.iovec].toInt * math.max(buffers, 1))
  val name = new Array[Byte](if named then math.max(sizeof[socket.sockaddr_storage].toInt, Sockets.StorageSize) else 1)

  def msg: Ptr[msghdr] = header.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[msghdr]]
  def vectors: Ptr[uio.iovec] = vectorStorage.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[uio.iovec]]
  def address: Ptr[socket.sockaddr] = name.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[socket.sockaddr]]
  def capacity: Int = name.length

  /** Points the vectors at `bufs`, those with room if `room`, else with bytes remaining: their count. */
  def fill(bufs: Array[ByteBuffer], room: Boolean): Int = NonBlocking.fill(vectors, bufs, room)

  /** Points the header at the first `count` vectors, and at the first `nameLength` bytes of `address`, or no address.
    */
  def set(count: Int, nameLength: Int): Unit = {
    val m = msg
    m.msg_name = if nameLength > 0 then address.asInstanceOf[Ptr[Byte]] else null
    m.msg_namelen = nameLength.toUInt
    m.msg_iov = vectors.asInstanceOf[Ptr[iovec]]
    m.msg_iovlen = count.toCSize
    m.msg_control = null
    m.msg_controllen = 0.toCSize
    m.msg_flags = 0
  }
}

// ---- Streams

/** A command that moves bytes between `bufs` and its handle: an entry has their stand-ins (`staging`), committed by as
  * many bytes as each completion moved, and let go of, untouched, if its result is never applied.
  */
private[uring] abstract class Transfer(bufs: Array[ByteBuffer], copyIn: Boolean) extends Command {
  protected val staging = new Staging(bufs, copyIn = copyIn, copyOut = !copyIn)

  /** What it failed to do, should it fail: "read from <handle>", say. */
  protected def failing: String

  /** Whether the result `res` only asks for the entry to be submitted again. */
  protected def again(res: Int): Boolean = Results.transient(res)

  /** After a completion moved `n` bytes: a positional one advances its offset. */
  protected def moved(n: Int): Unit = ()

  protected def finish(res: Int): Boolean =
    if again(res) then false
    else if res < 0 then {
      staging.end()
      throw Results.failure(s"Failed to $failing", res)
    } else {
      staging.commit(res)
      moved(res)
      true
    }

  override protected def discard(): Unit = staging.end()
}

/** Fills `buf` between position and limit. Done once at least one byte has been read, or at end of stream; refused if
  * `buf` has no room, since a read of nothing would look like the end of the stream.
  */
private[uring] final class ReadIntoBuffer(val handle: UringHandle, buf: ByteBuffer)
    extends Transfer(Array(buf), copyIn = false) {
  Checks.checkFillable(buf)
  override protected def direction = Command.Direction.Reading
  override def check(): Unit = if !buf.hasRemaining() then throw Checks.bufferFull(buf)
  override protected def readinessEvents: Int = Linux.POLLIN
  protected def failing: String = s"read from $handle" // a count of 0, at end of stream, leaves the buffer as it was

  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit = {
    val b = staging.begin()(0)
    io_uring_prep_read(sqe, fd, NativeBuffer.atPosition(b), b.remaining().toUInt, Results.currentPosition)
  }

  override def tryAtOnce(): Boolean = NonBlocking.read(fd, buf) != 0 // -1 at EOF, which leaves the buffer as it was
}

/** Fills `bufs` in order with one `readv`, as `ReadIntoBuffer` fills one (see `Ops.read`). */
private[uring] final class ReadIntoBuffers(val handle: UringHandle, bufs: Array[ByteBuffer])
    extends Transfer(bufs, copyIn = false) {
  Checks.checkFillable(bufs)
  override protected def direction = Command.Direction.Reading
  override def check(): Unit = if !Checks.anyRemaining(bufs) then throw Checks.buffersFull(bufs)
  override protected def readinessEvents: Int = Linux.POLLIN
  protected def failing: String = s"read from $handle"
  private val header = new MessageHeader(math.min(bufs.length, NonBlocking.MaxBuffers), named = false)

  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit = {
    val count = header.fill(staging.begin(), room = true)
    io_uring_prep_readv(sqe, fd, header.vectors.asInstanceOf[Ptr[iovec]], count.toUInt, Results.currentPosition)
  }

  override def tryAtOnce(): Boolean = NonBlocking.readv(fd, bufs) != 0
}

/** Drains `buf` between position and limit. Done once at least one byte has been written, or, with nothing remaining,
  * at once, with no system call.
  */
private[uring] final class WriteFromBuffer(val handle: UringHandle, buf: ByteBuffer)
    extends Transfer(Array(buf), copyIn = true) {
  override protected def direction = Command.Direction.Writing
  override protected def readinessEvents: Int = Linux.POLLOUT
  override def nothingToDo: Boolean = !buf.hasRemaining()
  protected def failing: String = s"write to $handle"

  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit = {
    val b = staging.begin()(0)
    io_uring_prep_write(sqe, fd, NativeBuffer.atPosition(b), b.remaining().toUInt, Results.currentPosition)
  }

  override def tryAtOnce(): Boolean = NonBlocking.write(fd, buf) >= 0
}

/** Drains `bufs` in order with one `writev`, as `WriteFromBuffer` drains one (see `Ops.write`). */
private[uring] final class WriteFromBuffers(val handle: UringHandle, bufs: Array[ByteBuffer])
    extends Transfer(bufs, copyIn = true) {
  Checks.checkBuffers(bufs)
  override protected def direction = Command.Direction.Writing
  override protected def readinessEvents: Int = Linux.POLLOUT
  override def nothingToDo: Boolean = !Checks.anyRemaining(bufs)
  protected def failing: String = s"write to $handle"
  private val header = new MessageHeader(math.min(bufs.length, NonBlocking.MaxBuffers), named = false)

  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit = {
    val count = header.fill(staging.begin(), room = false)
    io_uring_prep_writev(sqe, fd, header.vectors.asInstanceOf[Ptr[iovec]], count.toUInt, Results.currentPosition)
  }

  override def tryAtOnce(): Boolean = NonBlocking.writev(fd, bufs) >= 0
}

// ---- Done at once, or on a close

/** Opens a pipe, both ends non-blocking and close-on-exec. */
private[uring] final class OpenPipe(read: Slot[PipeSource], write: Slot[PipeSink]) extends AtOnce {
  def run(): Unit = {
    Checks.notEmpty(read, write)
    val fds = stackalloc[CInt](2)
    if LinuxCalls.pipe2(fds, scala.scalanative.posix.fcntl.O_NONBLOCK | Linux.O_CLOEXEC) != 0 then
      throw Results.failed(s"Failed to open a pipe")
    SigPipe.ignored: Unit // a write once its reader has gone fails with EPIPE, not the signal
    read.set(new PipeSource(fds(0)))
    write.set(new PipeSink(fds(1)))
  }
}

/** Closes a handle (see `UringHandle.close`): done at once if no use of its descriptor is in progress, else once the
  * last ends, which a close makes the kernel end soon. It has no time limit: it ends once begun.
  */
private[uring] final class CloseHandle(handle: UringHandle) extends SingleUseAbstractTimerHeapOp with UringOp {
  // 0 while it is being performed, 1 once it waits for the last use, 2 once the descriptor is closed.
  private val phase = new java.util.concurrent.atomic.AtomicInteger()

  /** Begins the close: true if the descriptor is closed already, false if the last use's end fires the op. */
  def start(): Boolean =
    handle.close(() => if phase.getAndSet(2) == 1 then timerWakeup(System.nanoTime())) || !phase.compareAndSet(0, 1)

  override protected def committed: Boolean = phase.get() == 2
  override protected def limitReached(): Boolean = false
  def wakeup(resumedAt: Long): Try[Unit] = WorkpoolScheduler.SuccessUnit
  override def toString: String = s"close of $handle"
}

// ---- Reads into a pool's buffers

/** Fills a buffer of `pool`'s, writing it into `into` (see `Ops.read`). While more than half the pool is free, the
  * kernel reads straight into a buffer taken as the read is submitted, in one round trip; otherwise the kernel only
  * waits for the descriptor to be readable, and the buffer is taken then, and the bytes read without waiting, so that
  * reads waiting on idle streams hold at most half the pool between them and never exhaust it. A buffer taken is given
  * back if the read fails, finds nothing after all, or is withdrawn.
  *
  * While the pool has no buffer free, the read waits for one, parked on the pool, which starts it again as a buffer is
  * released (`NativePool.park`): it takes nothing from the descriptor meanwhile, so the source is held back, as
  * `BufferPool` allows.
  */
private[uring] final class PooledRead(val handle: UringHandle, pool: NativePool, into: Slot[Received]) extends Command {
  override protected def direction = Command.Direction.Reading
  override def check(): Unit = Checks.notEmpty(into)
  private var held: pool.PoolBuffer | Null = null
  private var pollNext = false // a read straight into a buffer found nothing: wait for readiness first

  // A regular file is always readable, and a read of it blocks on the disk: it is read only by a kernel worker, into
  // a buffer taken before it is submitted (`readyToSubmit`), never polled and then read on this thread.
  private val file = handle.isInstanceOf[FileHandle]

  /** Parks it on the pool, which has no buffer free: false if one is free after all, or the handle is closing, and it
    * is not parked.
    */
  private def parkOnPool(): Boolean = {
    handle.parked = this
    if pool.park(this) && !(handle.isClosing && leaveParking()) then {
      park()
      true
    } else {
      handle.parked = null
      false
    }
  }

  override private[uring] def leaveParking(): Boolean =
    pool.leave(this) && {
      handle.parked = null
      true
    }

  // A socket's read needs no buffer to wait for readiness, only to read; a file's needs one to be submitted. A closing
  // handle's goes to the ring, which fails it as closed.
  override private[uring] def readyToSubmit(): Boolean =
    if file then {
      var ready = true
      while held == null && ready && !handle.isClosing do {
        held = pool.take()
        if held == null && parkOnPool() then ready = false
      }
      ready
    } else pool.available > 0 || handle.isClosing || !parkOnPool()

  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit = {
    if held == null && !file && !pollNext && pool.available * 2 > pool.buffers then held = pool.take()
    val slot = held
    if slot == null then io_uring_prep_poll_add(sqe, fd, Linux.POLLIN.toUInt)
    else io_uring_prep_read(sqe, fd, slot.address, slot.buffer.capacity().toUInt, Results.currentPosition)
  }

  protected def finish(res: Int): Boolean = {
    val slot = held
    if slot == null then {
      pollNext = false
      if Results.transient(res) then false
      else if res < 0 then throw Results.failure(s"Failed to wait for $handle", res)
      else tryAtOnce() // readable: false, to wait again, if another reader took the bytes first
    } else if Results.transient(res) then {
      held = null
      slot.release()
      pollNext = !file
      false
    } else {
      held = null
      if res < 0 then {
        slot.release() // a failed read holds nothing
        throw Results.failure(s"Failed to read from $handle", res)
      }
      val buffer = slot.buffer
      buffer.clear()
      buffer.position(res)
      buffer.flip()
      into.set(slot)
      true
    }
  }

  override def tryAtOnce(): Boolean = {
    val slot = pool.take()
    if slot == null then return false // the worker parks it (`readyToSubmit`)
    val read =
      try NonBlocking.read(fd, slot.buffer) // -1 at EOF, which leaves the buffer as it was
      catch {
        case failure: Throwable =>
          slot.release()
          throw failure
      }
    if read == 0 then {
      slot.release()
      false
    } else {
      slot.buffer.flip()
      into.set(slot)
      true
    }
  }

  override protected def discard(): Unit = {
    val slot = held
    held = null
    if slot != null then slot.release()
  }
}

// ---- Stream sockets

/** One-shot: performed again, it throws `IllegalStateException`, as an op that opens or closes does. */
private[uring] trait OneShot extends Command {
  private val used = new java.util.concurrent.atomic.AtomicBoolean()
  override def check(): Unit = {
    if !used.compareAndSet(false, true) then throw new IllegalStateException(s"$this is one-shot, and was performed")
    super.check()
  }
}

/** Connects a new socket to `address`, writing it into `into`: at once if the kernel connects it as it is asked, as a
  * Unix socket's often is, else once the ring says it is writable. The socket is closed if the connect fails or is
  * withdrawn, so nothing is left open.
  */
private[uring] final class Connect(
    address: IpSocketAddress | UnixAddress,
    options: SocketOptions,
    into: Slot[ConnectionHandle]
) extends Command
    with OneShot {
  private val unix = address.isInstanceOf[UnixAddress]
  private var connection: ConnectionHandle | Null = null
  def handle: UringHandle = connection.nn

  override def check(): Unit = {
    super.check()
    Checks.notEmpty(into)
  }

  override def begin(): Unit = {
    val fd = Sockets.open(address, socket.SOCK_STREAM)
    val kind = if unix then SocketOption.Kind.UnixConnection else SocketOption.Kind.Connection
    Sockets.apply(fd, SocketOption.opening(options, kind))
    connection = new ConnectionHandle(fd, unix)
  }

  override def tryAtOnce(): Boolean =
    try
      if Sockets.startConnect(fd, address) then {
        into.set(connection.nn)
        true
      } else false
    catch {
      case e: Throwable =>
        abandon()
        throw e
    }

  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit = io_uring_prep_poll_add(sqe, fd, Linux.POLLOUT.toUInt)

  protected def finish(res: Int): Boolean =
    if Results.transient(res) then false
    else
      try {
        if res < 0 then throw Results.failure(s"Failed to connect to $address", res)
        Sockets.checkConnected(fd, address)
        into.set(connection.nn)
        true
      } catch {
        case e: Throwable =>
          abandon()
          throw e
      }

  override protected def discard(): Unit = abandon()

  /** Closes the socket, which never reached the caller. */
  private def abandon(): Unit = {
    val c = connection
    if c != null then scala.scalanative.posix.unistd.close(c.fd): Unit
  }

  override def toString: String = s"connect to $address"
}

/** Accepts one connection, writing it, with the options its listener gives the connections it accepts, into `into`.
  * Accepts on one listener take turns, first come first served; one aborted before it was accepted is skipped.
  */
private[uring] final class Accept(val handle: ListenerHandle, into: Slot[ConnectionHandle]) extends Command {
  override def check(): Unit = Checks.notEmpty(into)
  override def turns: Turns | Null = handle.accepts
  override protected def readinessEvents: Int = Linux.POLLIN

  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit =
    io_uring_prep_accept(sqe, fd, null, null, Linux.SOCK_NONBLOCK | Linux.SOCK_CLOEXEC)

  protected def finish(res: Int): Boolean =
    if Results.transient(res) || res == -errno.ECONNABORTED then false
    else if res < 0 then throw Results.failure("Failed to accept a connection", res)
    else {
      into.set(Accepts.accepted(handle, res))
      true
    }

  override def tryAtOnce(): Boolean = {
    val client = Accepts.acceptNow(fd)
    if client >= 0 then into.set(Accepts.accepted(handle, client))
    client >= 0
  }

  override protected def dropResult(res: Int): Unit = if res >= 0 then scala.scalanative.posix.unistd.close(res): Unit
}

/** Accepts the connections waiting on a listener, at most `max`, writing them into `into`: it waits in the kernel for
  * the listener to be readable, then accepts until none waits. Done once at least one has been accepted.
  */
private[uring] final class AcceptAll(val handle: ListenerHandle, into: Slot[Array[ConnectionHandle]], max: Int)
    extends Command {
  override def check(): Unit = Checks.notEmpty(into)
  override def turns: Turns | Null = handle.accepts
  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit = io_uring_prep_poll_add(sqe, fd, Linux.POLLIN.toUInt)

  protected def finish(res: Int): Boolean =
    if Results.transient(res) then false
    else if res < 0 then throw Results.failure(s"Failed to wait for $handle", res)
    else tryAtOnce()

  /** Accepts while connections wait, up to `max`. A failure after the first is not thrown, which would drop the
    * connections taken already with their descriptors open: they are delivered, and the next accept meets it again.
    */
  override def tryAtOnce(): Boolean = {
    val accepted = scala.collection.mutable.ArrayBuffer.empty[ConnectionHandle]
    var more = true
    while more && accepted.size < max do {
      try {
        val client = Accepts.acceptNow(fd)
        if client < 0 then more = false else accepted += Accepts.accepted(handle, client)
      } catch {
        case NonFatal(_) if accepted.nonEmpty => more = false
      }
    }
    if accepted.nonEmpty then into.set(accepted.toArray)
    accepted.nonEmpty
  }
}

private[uring] object Accepts {

  /** Accepts a connection on `fd`, non-blocking and close-on-exec: its descriptor, or -1 if none waits. One its client
    * reset while it was queued is skipped, as the JDK's accept skips it.
    */
  def acceptNow(fd: Int): Int = {
    var client = LinuxCalls.accept4(fd, null, null, Linux.SOCK_NONBLOCK | Linux.SOCK_CLOEXEC)
    while client < 0 && errno.errno == errno.ECONNABORTED do
      client = LinuxCalls.accept4(fd, null, null, Linux.SOCK_NONBLOCK | Linux.SOCK_CLOEXEC)
    if client < 0 then {
      val e = errno.errno
      if e != errno.EAGAIN && e != errno.EWOULDBLOCK && e != errno.EINTR then
        throw Results.failed(s"Failed to accept a connection")
    }
    client
  }

  /** The connection `client`, accepted on `listener`, with the options the listener gives each it accepts. */
  def accepted(listener: ListenerHandle, client: Int): ConnectionHandle = {
    Sockets.apply(client, SocketOption.accepted(listener.options, listener.kind)) // which closes it if it fails
    new ConnectionHandle(client, listener.unix)
  }
}

/** Opens a listening socket on `address`. An IP one reuses an address in `TIME_WAIT`; a Unix one replaces a socket file
  * left at its path, and fails on any other file there.
  */
private[uring] final class Listen(
    address: IpSocketAddress | UnixAddress,
    options: SocketOptions,
    into: Slot[ListenerHandle]
) extends AtOnce {
  def run(): Unit = {
    Checks.notEmpty(into)
    val unix = address.isInstanceOf[UnixAddress]
    val kind = if unix then SocketOption.Kind.UnixListener else SocketOption.Kind.Listener
    val fd = Sockets.open(address, socket.SOCK_STREAM)
    try {
      val path = address match {
        case UnixAddress(p) =>
          Sockets.replaceSocketFile(p)
          p
        case _ =>
          Sockets.setInt(fd, socket.SOL_SOCKET, Sockets.SO_REUSEADDR, 1, "SO_REUSEADDR")
          null
      }
      SocketOption.opening(options, kind).foreach((o, v) => Sockets.set(fd, o, v))
      Sockets.bind(fd, address)
      Sockets.listen(fd)
      into.set(new ListenerHandle(fd, unix, options, path))
    } catch {
      case e: Throwable =>
        scala.scalanative.posix.unistd.close(fd)
        throw e
    }
  }
  override def toString: String = s"listen on $address"
}

/** Shuts a connection down for writing: its peer reads to the end, and it may still read. */
private[uring] final class ShutdownOutput(handle: ConnectionHandle) extends AtOnce {
  def run(): Unit = {
    if !handle.enter() then throw handle.closedFailure
    try Sockets.shutdownOutput(handle.fd)
    finally handle.leave()
  }
}

// ---- Datagram sockets

/** Opens an IP datagram socket, bound to `local`, and connected to `remote` if given; refused, with nothing open, if
  * `local` is of another family than `remote`.
  */
private[uring] final class OpenDatagram(
    local: IpSocketAddress | Null,
    remote: IpSocketAddress | Null,
    options: SocketOptions,
    into: Slot[? <: DatagramHandle]
) extends AtOnce {
  def run(): Unit = {
    Checks.notEmpty(into)
    val family: IpSocketAddress = if local != null then local else remote.nn
    val fd = Sockets.open(family, socket.SOCK_DGRAM)
    val kind = if remote == null then SocketOption.Kind.BoundDatagram else SocketOption.Kind.ConnectedDatagram
    try {
      SocketOption.opening(options, kind).foreach((o, v) => Sockets.set(fd, o, v))
      if local != null then Sockets.bind(fd, local)
      val ipv6 = family.address.protocol == Protocol.IPv6
      if remote != null then {
        if !Sockets.startConnect(fd, remote) then throw new IOException(s"Failed to connect to $remote")
        into.asInstanceOf[Slot[ConnectedDatagramHandle]].set(new ConnectedDatagramHandle(fd, ipv6))
      } else into.asInstanceOf[Slot[BoundDatagramHandle]].set(new BoundDatagramHandle(fd, ipv6))
    } catch {
      case e: Throwable =>
        scala.scalanative.posix.unistd.close(fd)
        throw e
    }
  }
}

/** Receives one packet into `buf`, cleared first and left flipped, and writes its sender into `from`; on a connected
  * socket, whose sender is its peer, `from` is null. Done once a packet has arrived; the bytes beyond `buf`'s capacity
  * are dropped (see `IpDatagramOps.receive`). Receives on one socket run side by side, each taking its own packet.
  */
private[uring] final class ReceiveFrom(val handle: DatagramHandle, buf: ByteBuffer, from: Slot[IpSocketAddress] | Null)
    extends Command {
  Checks.checkFillable(buf)
  override def check(): Unit = {
    if from != null then Checks.notEmpty(from.nn)
    if buf.capacity() == 0 then
      throw new IllegalArgumentException(
        s"The buffer has no capacity, so a packet received into it would be dropped: $buf"
      )
  }
  override def turns: Turns | Null = handle.receives
  override protected def readinessEvents: Int = Linux.POLLIN
  private val staging = new Staging(Array(buf), copyIn = false, copyOut = true)
  private val header = new MessageHeader(1, named = true)

  private def setUp(): Unit = {
    buf.clear()
    header.set(header.fill(staging.begin(), room = true), header.capacity)
  }

  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit = {
    setUp()
    io_uring_prep_recvmsg(sqe, fd, header.msg, 0)
  }

  /** A packet waiting already is taken with one `recvmsg`, without a turn of the worker's loop. */
  override def tryAtOnce(): Boolean = {
    setUp()
    val n = Unblocked.recvmsg(fd, header.msg.asInstanceOf[Ptr[socket.msghdr]], Linux.MSG_DONTWAIT).toInt
    Datagrams.atOnce(n, finish, staging)
  }

  protected def finish(res: Int): Boolean =
    if Results.transient(res) then false
    else if res < 0 then {
      staging.end()
      throw Results.failure("Failed to receive a datagram", res)
    } else {
      staging.commit(res)
      buf.flip()
      if from != null then
        Sockets.read(header.name, header.msg.msg_namelen.toInt) match {
          case ip: IpSocketAddress => from.nn.set(ip)
          case other               => throw new IOException(s"Received from $other on an IP socket")
        }
      true
    }

  override protected def discard(): Unit = staging.end()
}

/** Sends one packet, consuming `bufs`: from a bound socket to `to`, or from a connected one, `to` null, to its peer. An
  * empty packet is refused on a connected socket (see `IpDatagramOps.send`). Done once it has gone, whole. Sends on one
  * socket run side by side.
  */
private[uring] final class SendTo(val handle: DatagramHandle, bufs: Array[ByteBuffer], to: IpSocketAddress | Null)
    extends Command {
  Checks.checkBuffers(bufs)
  override def check(): Unit = {
    if to == null && !Checks.anyRemaining(bufs) then
      throw new IllegalArgumentException("A connected datagram socket sends no empty packet")
    if bufs.count(_.hasRemaining) > NonBlocking.MaxBuffers then // one sendmsg takes no more, and a packet goes whole
      throw new IllegalArgumentException(
        s"A datagram is sent from at most ${NonBlocking.MaxBuffers} buffers with bytes remaining"
      )
  }
  override def turns: Turns | Null = handle.sends
  override protected def readinessEvents: Int = Linux.POLLOUT
  private val staging = new Staging(bufs, copyIn = true, copyOut = false)
  private val header = new MessageHeader(math.min(bufs.length, NonBlocking.MaxBuffers), named = to != null)
  private var size = 0L

  private def setUp(): Unit = {
    size = bufs.foldLeft(0L)(_ + _.remaining())
    val nameLength = if to == null then 0 else Sockets.write(Sockets.target(handle.ipv6, to.nn), header.name)
    header.set(header.fill(staging.begin(), room = false), nameLength)
  }

  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit = {
    setUp()
    io_uring_prep_sendmsg(sqe, fd, header.msg, 0)
  }

  /** Sent with one `sendmsg` if the socket has room, without a turn of the worker's loop. */
  override def tryAtOnce(): Boolean = {
    setUp()
    val n =
      Unblocked.sendmsg(fd, header.msg.asInstanceOf[Ptr[socket.msghdr]], Linux.MSG_DONTWAIT | Linux.MSG_NOSIGNAL).toInt
    Datagrams.atOnce(n, finish, staging)
  }

  protected def finish(res: Int): Boolean =
    if Results.transient(res) then false
    else {
      staging.end()
      if res < 0 then throw Results.failure("Failed to send a datagram", res)
      if res.toLong != size then throw new IOException(s"Incomplete datagram: sent $res of $size bytes")
      for b <- bufs do b.position(b.limit()) // a datagram goes whole or not at all
      true
    }

  override protected def discard(): Unit = staging.end()
}

private object Datagrams {

  /** A datagram's system call tried at once, `n` what it returned: done, through `finish`, unless it would block, which
    * leaves the buffers as they were for the ring to try.
    */
  def atOnce(n: Int, finish: Int => Boolean, staging: Staging): Boolean = {
    val res = if n >= 0 then n else -errno.errno
    if res == -errno.EAGAIN || res == -errno.EWOULDBLOCK || res == -errno.EINTR then {
      staging.end()
      false
    } else finish(res)
  }
}

// ---- Run on the blocking pool (see Blocking.scala)

/** An op run on the blocking pool (see `BlockingOps.blocking`): `body` runs on a pool thread, its value written by
  * `deliver`, and the op fired from that thread once it has returned. A cancel or a time limit before it starts skips
  * it; one while it runs interrupts it, if `interruptible`, and the op settles once it has returned, so that `perform`
  * returns only then; one after its value was written is too late, the op completing with it (`committed`). A body that
  * nothing interrupts, a name lookup or an open, which touches nothing of the caller's but its slot, is abandoned
  * instead, so that the cancel or the limit holds at once: its value, once it returns, is not written, and `discard`
  * lets go of it. One-shot.
  */
private[uring] class BlockingTask[A](
    body: () => A,
    deliver: A => Unit,
    slots: List[Slot[?]],
    interruptible: Boolean,
    discard: A => Unit = (_: A) => ()
) extends SingleUseAbstractTimerHeapOp
    with UringOp {
  import BlockingTask.*

  private var state = Queued // guarded by `this`
  private var stopped = false // guarded by `this`
  private var runner: Thread | Null = null // guarded by `this`
  @volatile private var outcome: Try[Unit] = WorkpoolScheduler.SuccessUnit
  @volatile private[uring] var owner: UringScheduler#UringWorker | Null = null
  @volatile private[uring] var offHeap = false

  /** Checks made as it is performed: its slots empty. */
  def check(): Unit = Checks.notEmpty(slots*)

  /** Hands the body to the pool. */
  def start(): Unit = BlockingPool.run(() => runIt())

  private def runIt(): Unit = {
    val begun = synchronized {
      if state == Queued && !stopped then {
        state = Running
        runner = Thread.currentThread()
        true
      } else false
    }
    if begun then {
      val result =
        // Whatever it throws is its outcome, an `Error` too, so that it always fires and its performer never hangs.
        try Success(body())
        catch case e: Throwable => Failure(e)
      val next = synchronized {
        runner = null
        if state == Abandoned then Abandoned
        else {
          state = if result.isSuccess && !stopped then Delivering else Finishing
          state
        }
      }
      Thread.interrupted(): Unit // the flag a late interrupt left goes with the op
      if next == Abandoned then
        result.foreach(v =>
          try discard(v)
          catch case NonFatal(_) => ()
        )
      else {
        // The outcome is written before the state says it is there, which a time limit reads (`committed`).
        outcome =
          if next == Delivering then {
            try {
              deliver(result.get)
              WorkpoolScheduler.SuccessUnit
            } catch case NonFatal(e) => Failure(e)
          } else
            result match {
              case Failure(e) if !isStopped => Failure(e)
              case _                        => Failure(gears.async.cancellation())
            }
        synchronized { state = if next == Delivering then Committed else Finished }
        fire()
      }
      result match {
        case Failure(e: VirtualMachineError) => throw e // the pool's thread is not to carry on as if all were well
        case _                               =>
      }
    }
  }

  private def isStopped: Boolean = synchronized(stopped)

  /** Fires it on its worker, from the pool's thread: there the performer has suspended, its listener registered, before
    * the inbox runs, where a firing that finds no listener takes the performer to have been cancelled.
    */
  private def fire(): Unit = {
    val o = owner.nn
    // Not once settled: a time limit may have failed it already, with nothing in use.
    val run: Runnable = () =>
      if !isSettled && (!hasDeadline || offHeap || o.removeTimer(this)) then timerWakeup(System.nanoTime())
    if !o.submit(run) then run.run() // the worker has stopped: nothing waits on its inbox
  }

  /** Stops it, from any thread: true if it never fires, having never started or been abandoned; false if it runs,
    * interrupted, and fires once it has returned, or has returned already.
    */
  private[uring] def stop(): Boolean = synchronized {
    stopped = true
    state match {
      case Queued =>
        state = Skipped
        true
      case Running if !interruptible =>
        state = Abandoned
        true
      case Running =>
        runner.nn.interrupt()
        false
      case Skipped | Abandoned => true
      case _                   => false
    }
  }

  override protected def committed: Boolean = synchronized(state == Committed)

  /** True, so that it fails as out of time now, if nothing of it is in use: it never started, was abandoned, or has its
    * failure; else it fires itself as it returns.
    */
  override protected def limitReached(): Boolean = synchronized(state == Finished) || {
    val now = stop()
    if !now then offHeap = true
    now
  }

  def wakeup(resumedAt: Long): Try[Unit] = outcome
}

private[uring] object BlockingTask {
  inline val Queued = 0
  inline val Running = 1
  inline val Finished = 2 // returned, its outcome a failure, set
  inline val Committed = 3 // its value written, its outcome set
  inline val Skipped = 4 // never started
  inline val Delivering = 5 // returned, its value being written
  inline val Finishing = 6 // returned, its failure being set
  inline val Abandoned = 7 // stopped while running, its value to be let go of
}

// ---- Regular files

/** A read at an offset (see `FileOps.read`): fills `buf` from the file at the op's offset, which each completion
  * advances by the bytes read, so that performing the op again goes on from there. A kernel worker runs it.
  */
private[uring] final class PositionalRead(val handle: FileHandle, buf: ByteBuffer, private var offset: Long)
    extends Transfer(Array(buf), copyIn = false) {
  Checks.checkFillable(buf)
  override def check(): Unit = if !buf.hasRemaining() then throw Checks.bufferFull(buf)
  protected def failing: String = s"read $handle at $offset"
  override protected def again(res: Int): Boolean = res == -errno.EINTR
  override protected def moved(n: Int): Unit = offset += n

  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit = {
    val b = staging.begin()(0)
    io_uring_prep_read(sqe, fd, NativeBuffer.atPosition(b), b.remaining().toUInt, offset.toULong)
  }
}

/** A write at an offset (see `FileOps.write`): drains `buf` into the file at the op's offset, which each completion
  * advances by the bytes written; with nothing remaining, it completes at once, writing nothing.
  */
private[uring] final class PositionalWrite(val handle: FileHandle, buf: ByteBuffer, private var offset: Long)
    extends Transfer(Array(buf), copyIn = true) {
  override def nothingToDo: Boolean = !buf.hasRemaining()
  protected def failing: String = s"write $handle at $offset"
  override protected def again(res: Int): Boolean = res == -errno.EINTR
  override protected def moved(n: Int): Unit = offset += n

  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit = {
    val b = staging.begin()(0)
    io_uring_prep_write(sqe, fd, NativeBuffer.atPosition(b), b.remaining().toUInt, offset.toULong)
  }
}

/** `fsync` (see `FileOps.sync`). */
private[uring] final class FileSync(val handle: FileHandle) extends Command {
  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit = io_uring_prep_fsync(sqe, fd)

  protected def finish(res: Int): Boolean =
    if res == -errno.EINTR then false
    else if res < 0 then throw Results.failure(s"Failed to sync $handle", res)
    else true
}
