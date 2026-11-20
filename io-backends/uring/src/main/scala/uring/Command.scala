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

sealed trait UringOp extends TimerHeapOp

private[uring] final class UringTimer(duration: FiniteDuration) extends AbstractMakeTimerOp(duration) with UringOp

private[uring] final class UringDeadline(initial: Long) extends AbstractMakeDeadlineOp(initial) with UringOp

// - the kernel owns its buffers from submission until its completion is reaped, or a synchronous cancel returns
// - only its worker touches its entry; anyone else goes through that worker's inbox
private[uring] abstract class Command extends RepeatableAbstractTimerHeapOp with UringOp {

  def handle: UringHandle

  final def fd: Int = handle.fd

  def begin(): Unit = ()

  protected def direction: Command.Direction = Command.Direction.Neither

  def check(): Unit = ()

  def turns: Turns | Null = null

  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit

  protected def finish(res: Int): Boolean

  def nothingToDo: Boolean = false

  def tryAtOnce(): Boolean = false

  protected def discard(): Unit = ()

  protected def readinessEvents: Int = 0

  private var stage = Command.Stage.Idle
  private var entryId = 0L
  private var entry: Ptr[io_uring_sqe] = null
  private[uring] var owner: UringScheduler#UringWorker | Null = null
  private var awaitingReadiness = false
  @volatile private[uring] var outcome: Try[Unit] = WorkpoolScheduler.SuccessUnit
  @volatile private[uring] var done = false
  @volatile private[uring] var applied = false
  @volatile private[uring] var wantOut = false
  @volatile private[uring] var performance = 0L

  override def onEntry(): Unit = {
    super.onEntry()
    performance += 1
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

  private[uring] final def state: Command.Stage = stage

  private[uring] final def id: Long = entryId
  private[uring] final def sqe: Ptr[io_uring_sqe] = entry

  private[uring] final def prepared(id: Long, sqe: Ptr[io_uring_sqe]): Unit = {
    entryId = id
    entry = sqe
    stage = Command.Stage.Prepared
  }

  private[uring] final def submitted(): Unit = stage = Command.Stage.Submitted

  private[uring] final def outOfRing(): Unit = stage = Command.Stage.Idle

  private[uring] def readyToSubmit(): Boolean = true

  private[uring] def leaveParking(): Boolean = false

  protected final def park(): Unit = stage = Command.Stage.Parked

  private[uring] final def resume(): Unit =
    if stage == Command.Stage.Parked then {
      stage = Command.Stage.Idle
      if handle.parked eq this then handle.parked = null
    }

  private[uring] final def doneAtOnce(): Boolean = nothingToDo || (handle.triesAtOnce && tryAtOnce())

  private[uring] final def claim(): Boolean = direction match {
    case Command.Direction.Reading => handle.reading.compareAndSet(false, true)
    case Command.Direction.Writing => handle.writing.compareAndSet(false, true)
    case Command.Direction.Neither => true
  }

  private var ringOf: Ring | Null = null

  private[uring] final def onRing(ring: Ring): Unit =
    if ringOf ne ring then {
      handle.onRing(ring)
      ringOf = ring
    }

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
      awaitingReadiness = false
      if res < 0 && !Results.transient(res) then {
        discard()
        throw Results.failure(s"Failed to wait for descriptor $fd", res)
      }
      false
    } else if res == -errno.EAGAIN && readinessEvents != 0 then {
      awaitingReadiness = true
      false
    } else finish(res)

  private[uring] final def dropCompletion(res: Int): Unit = if !awaitingReadiness then dropResult(res)

  protected def dropResult(res: Int): Unit = ()

  private[uring] final def discardSubmission(): Unit = {
    awaitingReadiness = false
    discard()
  }

  override def toString: String = s"${getClass.getSimpleName} on $handle"
}

private[uring] object Command {
  enum Direction { case Neither, Reading, Writing }

  enum Stage { case Idle, Prepared, Submitted, Parked }

  def alreadyPending(op: AnyRef): IllegalArgumentException =
    new IllegalArgumentException(s"An op reading, or writing, as $op does, is already pending on its handle")
}

private[uring] abstract class AtOnce extends SingleUseAbstractTimerHeapOp with UringOp {
  def run(): Unit
  def wakeup(resumedAt: Long): Try[Unit] = WorkpoolScheduler.SuccessUnit
}

private[uring] object Results {

  val currentPosition: __u64 = (-1L).toULong

  def transient(res: Int): Boolean = res == -errno.EINTR || res == -errno.EAGAIN

  def failure(what: String, res: Int): IOException = new SystemFailure(s"$what: ${fromCString(string.strerror(-res))}")

  def failed(what: String): IOException = failure(what, -errno.errno)

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

private[uring] final class Staging(bufs: Array[ByteBuffer], copyIn: Boolean, copyOut: Boolean) {
  private val direct = NativeBuffer.allAddressable(bufs)
  private val stands = if direct then bufs else new Array[ByteBuffer](bufs.length)
  private var zone: Zone | Null = null

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

  def end(): Unit = {
    val z = zone
    if z != null then {
      zone = null
      z.close()
    }
  }
}

private[uring] final class MessageHeader(buffers: Int, named: Boolean) {
  private val header = new Array[Byte](sizeof[msghdr].toInt)
  private val vectorStorage = new Array[Byte](sizeof[uio.iovec].toInt * math.max(buffers, 1))
  val name = new Array[Byte](if named then math.max(sizeof[socket.sockaddr_storage].toInt, Sockets.StorageSize) else 1)

  def msg: Ptr[msghdr] = header.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[msghdr]]
  def vectors: Ptr[uio.iovec] = vectorStorage.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[uio.iovec]]
  def address: Ptr[socket.sockaddr] = name.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[socket.sockaddr]]
  def capacity: Int = name.length

  def fill(bufs: Array[ByteBuffer], room: Boolean): Int = NonBlocking.fill(vectors, bufs, room)

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

private[uring] abstract class Transfer(bufs: Array[ByteBuffer], copyIn: Boolean) extends Command {
  protected val staging = new Staging(bufs, copyIn = copyIn, copyOut = !copyIn)

  protected def failing: String

  protected def again(res: Int): Boolean = Results.transient(res)

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

private[uring] final class ReadIntoBuffer(val handle: UringHandle, buf: ByteBuffer)
    extends Transfer(Array(buf), copyIn = false) {
  Checks.checkFillable(buf)
  override protected def direction = Command.Direction.Reading
  override def check(): Unit = if !buf.hasRemaining() then throw Checks.bufferFull(buf)
  override protected def readinessEvents: Int = Linux.POLLIN
  protected def failing: String = s"read from $handle"

  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit = {
    val b = staging.begin()(0)
    io_uring_prep_read(sqe, fd, NativeBuffer.atPosition(b), b.remaining().toUInt, Results.currentPosition)
  }

  override def tryAtOnce(): Boolean = NonBlocking.read(fd, buf) != 0
}

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

private[uring] final class OpenPipe(read: Slot[PipeSource], write: Slot[PipeSink]) extends AtOnce {
  def run(): Unit = {
    Checks.notEmpty(read, write)
    val fds = stackalloc[CInt](2)
    if LinuxCalls.pipe2(fds, scala.scalanative.posix.fcntl.O_NONBLOCK | Linux.O_CLOEXEC) != 0 then
      throw Results.failed(s"Failed to open a pipe")
    SigPipe.ignored: Unit
    read.set(new PipeSource(fds(0)))
    write.set(new PipeSink(fds(1)))
  }
}

private[uring] final class CloseHandle(handle: UringHandle) extends SingleUseAbstractTimerHeapOp with UringOp {
  private val phase = new java.util.concurrent.atomic.AtomicInteger()

  def start(): Boolean =
    handle.close(() => if phase.getAndSet(2) == 1 then timerWakeup(System.nanoTime())) || !phase.compareAndSet(0, 1)

  override protected def committed: Boolean = phase.get() == 2
  override protected def limitReached(): Boolean = false
  def wakeup(resumedAt: Long): Try[Unit] = WorkpoolScheduler.SuccessUnit
  override def toString: String = s"close of $handle"
}

private[uring] final class PooledRead(val handle: UringHandle, pool: NativePool, into: Slot[Received]) extends Command {
  override protected def direction = Command.Direction.Reading
  override def check(): Unit = Checks.notEmpty(into)
  private var held: pool.PoolBuffer | Null = null
  private var pollNext = false

  private val file = handle.isInstanceOf[FileHandle]

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
      else tryAtOnce()
    } else if Results.transient(res) then {
      held = null
      slot.release()
      pollNext = !file
      false
    } else {
      held = null
      if res < 0 then {
        slot.release()
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
    if slot == null then return false
    val read =
      try NonBlocking.read(fd, slot.buffer)
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

private[uring] trait OneShot extends Command {
  private val used = new java.util.concurrent.atomic.AtomicBoolean()
  override def check(): Unit = {
    if !used.compareAndSet(false, true) then throw new IllegalStateException(s"$this is one-shot, and was performed")
    super.check()
  }
}

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

  private def abandon(): Unit = {
    val c = connection
    if c != null then scala.scalanative.posix.unistd.close(c.fd): Unit
  }

  override def toString: String = s"connect to $address"
}

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

private[uring] final class AcceptAll(val handle: ListenerHandle, into: Slot[Array[ConnectionHandle]], max: Int)
    extends Command {
  override def check(): Unit = Checks.notEmpty(into)
  override def turns: Turns | Null = handle.accepts
  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit = io_uring_prep_poll_add(sqe, fd, Linux.POLLIN.toUInt)

  protected def finish(res: Int): Boolean =
    if Results.transient(res) then false
    else if res < 0 then throw Results.failure(s"Failed to wait for $handle", res)
    else tryAtOnce()

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

  def accepted(listener: ListenerHandle, client: Int): ConnectionHandle = {
    Sockets.apply(client, SocketOption.accepted(listener.options, listener.kind))
    new ConnectionHandle(client, listener.unix)
  }
}

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

private[uring] final class ShutdownOutput(handle: ConnectionHandle) extends AtOnce {
  def run(): Unit = {
    if !handle.enter() then throw handle.closedFailure
    try Sockets.shutdownOutput(handle.fd)
    finally handle.leave()
  }
}

private[uring] final class OpenDatagram(
    local: IpSocketAddress | UnixAddress | Null,
    remote: IpSocketAddress | UnixAddress | Null,
    options: SocketOptions,
    into: Slot[? <: DatagramHandle]
) extends AtOnce {
  def run(): Unit = {
    Checks.notEmpty(into)
    val family = if local != null then local.nn else remote.nn
    val fd = Sockets.open(family, socket.SOCK_DGRAM)
    val path = local match {
      case UnixAddress(p) => p
      case _              => null
    }
    val kind = (family, remote) match {
      case (_: UnixAddress, null) => SocketOption.Kind.UnixBoundDatagram
      case (_: UnixAddress, _)    => SocketOption.Kind.UnixConnectedDatagram
      case (_, null)              => SocketOption.Kind.BoundDatagram
      case _                      => SocketOption.Kind.ConnectedDatagram
    }
    var bound = false
    try {
      SocketOption.opening(options, kind).foreach((o, v) => Sockets.set(fd, o, v))
      if local != null then {
        Sockets.bind(fd, local.nn)
        bound = true
      }
      val ipv6 = family match {
        case ip: IpSocketAddress => ip.address.protocol == Protocol.IPv6
        case _                   => false
      }
      if remote != null && !Sockets.startConnect(fd, remote.nn) then
        throw new IOException(s"Failed to connect to $remote")
      val handle = kind match {
        case SocketOption.Kind.UnixBoundDatagram     => new UnixBoundDatagramHandle(fd, path.nn)
        case SocketOption.Kind.UnixConnectedDatagram => new UnixConnectedDatagramHandle(fd, path)
        case SocketOption.Kind.BoundDatagram         => new BoundDatagramHandle(fd, ipv6)
        case _                                       => new ConnectedDatagramHandle(fd, ipv6)
      }
      into.asInstanceOf[Slot[DatagramHandle]].set(handle)
    } catch {
      case e: Throwable =>
        scala.scalanative.posix.unistd.close(fd)
        if bound && path != null then Sockets.unlink(path)
        throw e
    }
  }
}

private[uring] final class ReceiveFrom(
    val handle: DatagramHandle,
    buf: ByteBuffer,
    from: Slot[IpSocketAddress] | Slot[UnixAddress] | Null
) extends Command {
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
      if from != null then {
        val length = header.msg.msg_namelen.toInt
        val sender = if handle.unix && length < 2 then UnixAddress("") else Sockets.read(header.name, length)
        sender match {
          case ip: IpSocketAddress if !handle.unix => from.asInstanceOf[Slot[IpSocketAddress]].set(ip)
          case path: UnixAddress if handle.unix    => from.asInstanceOf[Slot[UnixAddress]].set(path)
          case other                               => throw new IOException(s"Received from $other on $handle")
        }
      }
      true
    }

  override protected def discard(): Unit = staging.end()
}

private[uring] final class SendTo(
    val handle: DatagramHandle,
    bufs: Array[ByteBuffer],
    to: IpSocketAddress | UnixAddress | Null
) extends Command {
  Checks.checkBuffers(bufs)
  override def check(): Unit = {
    if to == null && !Checks.anyRemaining(bufs) then
      throw new IllegalArgumentException("A connected datagram socket sends no empty packet")
    if bufs.count(_.hasRemaining) > NonBlocking.MaxBuffers then
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
    val nameLength = to match {
      case null                => 0
      case ip: IpSocketAddress => Sockets.write(Sockets.target(handle.ipv6, ip), header.name)
      case path: UnixAddress   => Sockets.write(path, header.name)
    }
    header.set(header.fill(staging.begin(), room = false), nameLength)
  }

  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit = {
    setUp()
    io_uring_prep_sendmsg(sqe, fd, header.msg, 0)
  }

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
      for b <- bufs do b.position(b.limit())
      true
    }

  override protected def discard(): Unit = staging.end()
}

private object Datagrams {

  def atOnce(n: Int, finish: Int => Boolean, staging: Staging): Boolean = {
    val res = if n >= 0 then n else -errno.errno
    if res == -errno.EAGAIN || res == -errno.EWOULDBLOCK || res == -errno.EINTR then {
      staging.end()
      false
    } else finish(res)
  }
}

private[uring] class BlockingTask[A](
    body: () => A,
    deliver: A => Unit,
    slots: List[Slot[?]],
    interruptible: Boolean,
    discard: A => Unit = (_: A) => (),
    pool: BlockingPool.Pool = BlockingPool.others
) extends SingleUseAbstractTimerHeapOp
    with UringOp {
  import BlockingTask.*

  private var state = Queued
  private var stopped = false
  private var runner: Thread | Null = null
  @volatile private var outcome: Try[Unit] = WorkpoolScheduler.SuccessUnit
  @volatile private[uring] var owner: UringScheduler#UringWorker | Null = null

  def check(): Unit = Checks.notEmpty(slots*)

  def start(): Unit = pool.run(() => runIt())

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
      Thread.interrupted(): Unit
      if next == Abandoned then
        result.foreach(v =>
          try discard(v)
          catch case NonFatal(_) => ()
        )
      else {
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
        case Failure(e: VirtualMachineError) => throw e
        case _                               =>
      }
    }
  }

  private def isStopped: Boolean = synchronized(stopped)

  private def fire(): Unit = {
    val o = owner.nn
    val run: Runnable = () =>
      if !isSettled && (!hasDeadline || o.claimFiring(this)) then
        timerWakeup(System.nanoTime())
    if !o.submit(run) then run.run()
  }

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
  inline val Finished = 2
  inline val Committed = 3
  inline val Skipped = 4
  inline val Delivering = 5
  inline val Finishing = 6
  inline val Abandoned = 7
}

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

private[uring] final class FileSync(val handle: FileHandle) extends Command {
  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit = io_uring_prep_fsync(sqe, fd)

  protected def finish(res: Int): Boolean =
    if res == -errno.EINTR then false
    else if res < 0 then throw Results.failure(s"Failed to sync $handle", res)
    else true
}
