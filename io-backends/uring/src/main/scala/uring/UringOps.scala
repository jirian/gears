package gears.async.io
package uring

import gears.async.io.BlockingOps
import gears.async.io.BufferPool
import gears.async.io.FifoOps
import gears.async.io.FileOps
import gears.async.io.IpAddress
import gears.async.io.IpDatagramOps
import gears.async.io.IpStreamOps
import gears.async.io.PipeOps
import gears.async.io.Received
import gears.async.io.Repeatable
import gears.async.io.ResolveOps
import gears.async.io.ResolvedAddress
import gears.async.io.Slot
import gears.async.io.UnixAddress
import gears.async.io.UnixDatagramOps
import gears.async.io.UnixStreamOps
import gears.async.io.internal.Interest
import gears.async.io.internal.NativePool

import java.nio.ByteBuffer
import scala.scalanative.posix.errno
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import UringBackend.Handle
import uring.*
import uringStructOps.*

/** Everything `UringReactor` can run: its `Op` member type. io_uring-specific code may construct the classes below
  * directly; code that only knows the `Reactor` interface builds them through `reactor.ops`. What each keeps while
  * pending is `LoopOp`'s.
  */
sealed trait UringOp extends gears.async.io.internal.LoopOp[UringOp]

/** Builds the ops `UringReactor` understands. */
object UringOps
    extends IpStreamOps[UringOp, Handle]
    with IpDatagramOps[UringOp, Handle]
    with UnixStreamOps[UringOp, Handle]
    with UnixDatagramOps[UringOp, Handle]
    with PipeOps[UringOp, Handle]
    with FifoOps[UringOp, Handle]
    with BlockingOps[UringOp, Handle]
    with FileOps[UringOp, Handle]
    with ResolveOps[UringOp, Handle] {
  type Readable = UringKinds.Readable
  type Writable = UringKinds.Writable
  type Connection = UringKinds.Connection
  type Listener = UringKinds.Listener
  type BoundDatagram = UringKinds.BoundDatagram
  type ConnectedDatagram = UringKinds.ConnectedDatagram
  type UnixBoundDatagram = UringKinds.UnixBoundDatagram
  type UnixConnectedDatagram = UringKinds.UnixConnectedDatagram
  type File = UringKinds.File

  def read(handle: Readable, buf: ByteBuffer): ReadIntoBuffer = on(handle)(new ReadIntoBuffer(fd(handle), buf))
  def write(handle: Writable, buf: ByteBuffer): WriteFromBuffer = on(handle)(new WriteFromBuffer(fd(handle), buf))
  def read(handle: Readable, bufs: Array[ByteBuffer]): ReadIntoBuffers = {
    gears.async.io.internal.Checks.checkBuffers(bufs)
    on(handle)(new ReadIntoBuffers(fd(handle), bufs))
  }
  def write(handle: Writable, bufs: Array[ByteBuffer]): WriteFromBuffers = {
    gears.async.io.internal.Checks.checkBuffers(bufs)
    on(handle)(new WriteFromBuffers(fd(handle), bufs))
  }
  def accept(handle: Listener, into: Slot[Connection]): Accept = on(handle)(new Accept(fd(handle), into))
  def acceptAll(handle: Listener, into: Slot[Array[Connection]], max: Int): AcceptAll = {
    require(max > 0, s"max must be positive, not $max")
    on(handle)(new AcceptAll(fd(handle), into, max))
  }
  def connect(address: IpAddress, options: SocketOptions, into: Slot[Connection]): ConnectTo = {
    gears.async.io.internal.Checks.checkOptions(options, supports, SocketOption.Kind.Connection)
    new ConnectTo(gears.async.io.internal.Checks.ip(address), into, options)
  }
  def connect(address: UnixAddress, options: SocketOptions, into: Slot[Connection]): ConnectTo = {
    gears.async.io.internal.Checks.checkOptions(options, supports, SocketOption.Kind.UnixConnection)
    new ConnectTo(gears.async.io.internal.Checks.unix(address), into, options)
  }
  def listen(address: IpAddress, options: SocketOptions, into: Slot[Listener]): UringOp = {
    gears.async.io.internal.Checks.checkOptions(options, supports, SocketOption.Kind.Listener)
    new Listen(gears.async.io.internal.Checks.ip(address), into, options)
  }
  def listen(address: UnixAddress, options: SocketOptions, into: Slot[Listener]): UringOp = {
    gears.async.io.internal.Checks.checkOptions(options, supports, SocketOption.Kind.UnixListener)
    new Listen(gears.async.io.internal.Checks.unix(address), into, options)
  }
  def bindDatagram(local: IpAddress, options: SocketOptions, into: Slot[BoundDatagram]): UringOp = {
    gears.async.io.internal.Checks.checkOptions(options, supports, SocketOption.Kind.BoundDatagram)
    new OpenDatagram(gears.async.io.internal.Checks.ip(local), null, into, UringKinds.boundDatagram, options)
  }
  def connectDatagram(
      local: IpAddress | Null,
      remote: IpAddress,
      options: SocketOptions,
      into: Slot[ConnectedDatagram]
  ): UringOp = {
    gears.async.io.internal.Checks.checkOptions(options, supports, SocketOption.Kind.ConnectedDatagram)
    val (l, r) = gears.async.io.internal.Checks.ipPair(local, remote)
    new OpenDatagram(l, r, into, UringKinds.connectedDatagram, options)
  }
  def bindDatagram(local: UnixAddress, options: SocketOptions, into: Slot[UnixBoundDatagram]): UringOp = {
    gears.async.io.internal.Checks.checkOptions(options, supports, SocketOption.Kind.UnixBoundDatagram)
    new OpenDatagram(gears.async.io.internal.Checks.unix(local), null, into, UringKinds.unixBoundDatagram, options)
  }
  def connectDatagram(
      local: UnixAddress | Null,
      remote: UnixAddress,
      options: SocketOptions,
      into: Slot[UnixConnectedDatagram]
  ): UringOp = {
    gears.async.io.internal.Checks.checkOptions(options, supports, SocketOption.Kind.UnixConnectedDatagram)
    new OpenDatagram(
      if local == null then null else gears.async.io.internal.Checks.unix(local),
      gears.async.io.internal.Checks.unix(remote),
      into,
      UringKinds.unixConnectedDatagram,
      options
    )
  }
  def pipe(read: Slot[Readable], write: Slot[Writable]): UringOp = new OpenPipe(read, write)
  def openFifoForReading(path: String, into: Slot[Readable]): UringOp =
    new OpenFifo(path, write = false, into, UringKinds.readable)
  def openFifoForWriting(path: String, into: Slot[Writable]): UringOp =
    new OpenFifo(path, write = true, into, UringKinds.writable)
  def receive(handle: BoundDatagram, buf: ByteBuffer, from: Slot[IpAddress]): ReceiveFrom =
    on(handle)(new ReceiveFrom(fd(handle), buf, from, unix = false))
  def receive(handle: ConnectedDatagram, buf: ByteBuffer): ReceiveFrom =
    on(handle)(new ReceiveFrom(fd(handle), buf, null, unix = false))
  def send(handle: BoundDatagram, buf: ByteBuffer, to: IpAddress): SendTo =
    on(handle)(new SendTo(fd(handle), buf, gears.async.io.internal.Checks.ip(to)))
  def send(handle: ConnectedDatagram, buf: ByteBuffer): SendTo = on(handle)(new SendTo(fd(handle), buf, null))
  def send(handle: BoundDatagram, bufs: Array[ByteBuffer], to: IpAddress): SendTo = {
    gears.async.io.internal.Checks.checkBuffers(bufs)
    on(handle)(new SendTo(fd(handle), bufs, gears.async.io.internal.Checks.ip(to)))
  }
  def send(handle: ConnectedDatagram, bufs: Array[ByteBuffer]): SendTo = {
    gears.async.io.internal.Checks.checkBuffers(bufs)
    on(handle)(new SendTo(fd(handle), bufs, null))
  }
  @annotation.targetName("receiveUnix")
  def receive(handle: UnixBoundDatagram, buf: ByteBuffer, from: Slot[UnixAddress]): ReceiveFrom =
    on(handle)(new ReceiveFrom(fd(handle), buf, from, unix = true))
  @annotation.targetName("receiveUnixConnected")
  def receive(handle: UnixConnectedDatagram, buf: ByteBuffer): ReceiveFrom =
    on(handle)(new ReceiveFrom(fd(handle), buf, null, unix = true))
  def send(handle: UnixBoundDatagram, buf: ByteBuffer, to: UnixAddress): SendTo =
    on(handle)(new SendTo(fd(handle), buf, gears.async.io.internal.Checks.unix(to)))
  @annotation.targetName("sendUnixConnected")
  def send(handle: UnixConnectedDatagram, buf: ByteBuffer): SendTo = on(handle)(new SendTo(fd(handle), buf, null))
  def send(handle: UnixBoundDatagram, bufs: Array[ByteBuffer], to: UnixAddress): SendTo = {
    gears.async.io.internal.Checks.checkBuffers(bufs)
    on(handle)(new SendTo(fd(handle), bufs, gears.async.io.internal.Checks.unix(to)))
  }
  @annotation.targetName("sendUnixConnectedMany")
  def send(handle: UnixConnectedDatagram, bufs: Array[ByteBuffer]): SendTo = {
    gears.async.io.internal.Checks.checkBuffers(bufs)
    on(handle)(new SendTo(fd(handle), bufs, null))
  }
  def timer(duration: scala.concurrent.duration.FiniteDuration): UringTimer = {
    require(duration.length >= 0, s"A timer's time is not negative: $duration")
    new UringTimer(duration.toNanos)
  }
  def deadlineTimer(nanoTime: Long): UringDeadlineTimer = new UringDeadlineTimer(nanoTime)
  def blocking(body: () => Unit): UringBlocking = new BlockingTask(body)
  def blocking[A <: AnyRef](body: () => A, into: Slot[A]): UringBlocking = new BlockingResultTask(body, into)
  def resolve(host: String, into: Slot[List[ResolvedAddress]]): Resolve = new Resolve(host, into)
  def read(handle: Readable, pool: BufferPool, into: Slot[Received]): PooledRead =
    on(handle)(new PooledRead(fd(handle), UringOps.native(pool), into))
  def close(handle: Handle): UringOp = new Close(handle)
  def shutdownOutput(handle: Connection): UringOp = new ShutdownOutput(handle)

  // Socket options: every one that applies, IP or Unix, changed at any time but `ReusePort`, set before binding.
  def supports(option: SocketOption[?], kind: SocketOption.Kind): Boolean = option.appliesTo(kind)
  def canChange(option: SocketOption[?], kind: SocketOption.Kind): Boolean =
    supports(option, kind) && option != SocketOption.ReusePort
  def option[V](handle: Handle, option: SocketOption[V]): V =
    onSocket(handle, option)((fd, _) => UringHandles.optionOf(fd, option).asInstanceOf[V])
  def setOption[V](handle: Handle, option: SocketOption[V], value: V): Unit = {
    option.check(value)
    onSocket(handle, option) { (fd, kind) =>
      if !canChange(option, kind) then
        throw new IllegalArgumentException(s"$option is given as a socket opens, not changed")
      UringHandles.setOption(fd, option, value)
    }
  }

  /** Runs `body` on `handle`'s descriptor, a socket `option` applies to, with its kind; else an
    * `IllegalArgumentException`.
    */
  private def onSocket[A](handle: Handle, option: SocketOption[?])(body: (Int, SocketOption.Kind) => A): A = {
    UringHandles.inUse(handle) {
      val kind = UringHandles.socketKind(handle.fd)
      if kind == null then throw new IllegalArgumentException(s"$option applies to sockets, not $handle")
      if !SocketOption.onSocket(option, kind.nn) then throw gears.async.io.internal.Checks.notFor(option, kind.nn)
      body(handle.fd, kind.nn)
    }
  }
  def localAddress(handle: Listener | Connection): IpAddress | UnixAddress =
    UringHandles.addressOf(handle, peer = false)
  def peerAddress(handle: Connection): IpAddress | UnixAddress = UringHandles.addressOf(handle, peer = true)
  @annotation.targetName("datagramLocalAddress")
  def localAddress(handle: BoundDatagram | ConnectedDatagram): IpAddress = ip(handle, peer = false)
  @annotation.targetName("datagramPeerAddress")
  def peerAddress(handle: ConnectedDatagram): IpAddress = ip(handle, peer = true)
  @annotation.targetName("unixDatagramLocalAddress")
  def localAddress(handle: UnixBoundDatagram | UnixConnectedDatagram): UnixAddress = unix(handle, peer = false)
  @annotation.targetName("unixDatagramPeerAddress")
  def peerAddress(handle: UnixConnectedDatagram): UnixAddress = unix(handle, peer = true)

  private def ip(handle: Handle, peer: Boolean): IpAddress = UringHandles.addressOf(handle, peer) match {
    case ip: IpAddress => ip
    case other         => throw new java.io.IOException(s"$handle is not an IP socket: $other")
  }
  private def unix(handle: Handle, peer: Boolean): UnixAddress = UringHandles.addressOf(handle, peer) match {
    case unix: UnixAddress => unix
    case other             => throw new java.io.IOException(s"$handle is not a Unix socket: $other")
  }
  def open(path: String, write: Boolean, into: Slot[File]): UringOp = new OpenFile(path, write, into)
  def read(handle: File, buf: ByteBuffer, offset: Long): PositionalRead =
    on(handle)(new PositionalRead(fd(handle), buf, offset))
  def write(handle: File, buf: ByteBuffer, offset: Long): PositionalWrite =
    on(handle)(new PositionalWrite(fd(handle), buf, offset))
  def sync(handle: File): FileSync = on(handle)(new FileSync(fd(handle)))

  private def fd(handle: Handle): Int = UringHandles.fd(handle)

  /** `command`, carrying `handle`, which the reactor takes for the command's time in the kernel (see `Command.handle`).
    */
  private def on[C <: Command](handle: Handle)(command: C): C = {
    command.handle = handle
    command
  }

  private def native(pool: BufferPool): NativePool = pool match {
    case native: NativePool => native
    case other              => throw new IllegalArgumentException(s"$other is not a pool of this backend's")
  }
}

/** An op the reactor performs as soon as it is submitted, with nothing for the kernel to do: its completion runs on the
  * next turn, with the failure `perform` threw, if any. One-shot.
  */
sealed abstract class UringAtOnce extends gears.async.io.internal.LoopAtOnce[UringOp] with UringOp

/** An op that opens a handle and writes it into a slot (see `LoopOpens`): what it opened, abandoned, is closed by its
  * descriptor, the handle never having reached the caller.
  */
sealed trait Opens extends UringOp with gears.async.io.internal.LoopOpens[UringOp]

/** An op that opens its handle at once, when submitted (see `UringAtOnce`), as io_uring's listeners, datagram sockets,
  * pipes and FIFOs are: `openDescriptor` opens its descriptor, whose handle, of the kind `kind` makes, is written into
  * `into` by the time the op completes.
  */
sealed abstract class OpensAtOnce[K <: Handle](into: Slot[K], kind: Handle => K)
    extends UringAtOnce
    with gears.async.io.internal.LoopOpensAtOnce[UringOp, K](into)
    with Opens {
  protected def openDescriptor(): Int
  protected final def open(): K = kind(UringHandles.handle(openDescriptor()))
  protected final def discard(handle: K): Unit = UringHandles.closeDescriptor(handle.fd)
}

/** A stream socket bound and listening on `address` (see `IpStreamOps.listen`). */
final class Listen(
    address: SocketAddress,
    into: Slot[UringKinds.Listener],
    options: SocketOptions = SocketOptions.default
) extends OpensAtOnce(into, UringKinds.listener) {
  protected def openDescriptor(): Int = UringHandles.listenStream(address, options)
}

/** A datagram socket, bound and connected where given (see `IpDatagramOps.connectDatagram`). */
final class OpenDatagram[K <: Handle](
    local: SocketAddress | Null,
    remote: SocketAddress | Null,
    into: Slot[K],
    kind: Handle => K,
    options: SocketOptions = SocketOptions.default
) extends OpensAtOnce(into, kind) {
  protected def openDescriptor(): Int = UringHandles.datagramSocket(local, remote, options)
}

/** A FIFO, opened without waiting for its other end (see `FifoOps.openFifoForReading` and `openFifoForWriting`). */
final class OpenFifo[K <: Handle](path: String, write: Boolean, into: Slot[K], kind: Handle => K)
    extends OpensAtOnce(into, kind) {
  gears.async.io.internal.Checks.checkPath(path)
  protected def openDescriptor(): Int = UringHandles.fifoDescriptor(path, write)
}

/** A pipe, its read end written into `read` and its write end into `write` (see `PipeOps.pipe`). */
final class OpenPipe(read: Slot[UringKinds.Readable], write: Slot[UringKinds.Writable])
    extends UringAtOnce
    with gears.async.io.internal.LoopOpensPipe[UringOp, UringKinds.Readable, UringKinds.Writable](read, write)
    with Opens {
  protected def open(): (UringKinds.Readable, UringKinds.Writable) = {
    val (in, out) = UringHandles.pipeDescriptors()
    (UringKinds.readable(UringHandles.handle(in)), UringKinds.writable(UringHandles.handle(out)))
  }
  protected def discard(end: UringKinds.Readable | UringKinds.Writable): Unit = UringHandles.closeDescriptor(end.fd)
}

/** A stream socket connected to `address` (see `IpStreamOps.connect` and `LoopConnects`): the socket is opened, and its
  * non-blocking connect started, when the op is submitted, and the op then waits in the kernel for the socket to be
  * writable, as a connect does. Done once connected, after writing the socket into `into`; if the connect fails, or the
  * submission is cancelled first, the socket is closed and `into` left empty.
  */
final class ConnectTo(
    address: SocketAddress,
    into: Slot[UringKinds.Connection],
    options: SocketOptions = SocketOptions.default
) extends Command
    with gears.async.io.internal.LoopConnects[UringOp, UringKinds.Connection](into)
    with Opens {
  def fd: Int = if socket == null then -1 else socket.nn.fd
  def interest: Interest = Interest.Write

  protected def open(): UringKinds.Connection =
    UringKinds.connection(UringHandles.handle(UringHandles.connectStream(address, options)).knownAs(Handle.Socket))
  protected def discard(socket: UringKinds.Connection): Unit = UringHandles.closeDescriptor(socket.fd)

  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit = io_uring_prep_poll_add(sqe, fd, Linux.POLLOUT.toUInt)

  protected def finish(res: Int): Boolean =
    if Results.transient(res) then false
    else {
      try
        if res < 0 then throw Results.failure("Failed to wait for connection", res)
        else gears.async.io.internal.PosixSockets.checkConnect(fd)
      catch {
        case t: Throwable =>
          abandon()
          throw t
      }
      finished()
      true
    }
}

/** Closes a handle (see `Ops.close` and `UringBackend.Handle`): at once in meaning, and the descriptor itself at once
  * too if no use of it is in progress, completing as an at-once op does; otherwise once the last use ends, as the
  * commands in the kernel on it are cancelled, on whose thread the descriptor is closed and this completed, as a
  * promise is from another thread.
  */
final class Close(val handle: Handle) extends UringPromise {
  override private[io] def closes: Boolean = true

  /** Starts closing: true if that finished here, the descriptor closed or the close failed, its failure in
    * `atOnceFailure`; false if the last use of the descriptor closes it, completing this.
    */
  private[uring] def start(): Boolean =
    try UringHandles.closeHandle(handle, failure => if failure == null then signal() else fail(failure.nn))
    catch {
      case t: Throwable =>
        atOnceFailure = t
        true
    }
}

/** Shuts a socket down for writing (see `StreamSocketOps.shutdownOutput`): at once, since the kernel sends what is
  * queued, then the end of the stream, after the call returns.
  */
final class ShutdownOutput(val handle: Handle) extends UringAtOnce {
  def perform(): Unit = UringHandles.inUse(handle)(gears.async.io.internal.PosixSockets.shutdownOutput(handle.fd))
}

/** An op io_uring runs: each submission of it is one submission queue entry, which `prepare` fills in, and the
  * completion queue entry that answers it is handed to `finish`. Every method runs on the loop thread. Commands are
  * `Repeatable`, but for those that open (`oneShot`).
  *
  * A command on a non-blocking descriptor that has nothing to do yet may complete with `EAGAIN`, where the kernel does
  * not wait for it itself: the command then waits for readiness with a poll for `readinessEvents`, and is submitted
  * again once the descriptor is ready, rather than spinning.
  */
trait Command extends UringOp with Repeatable {

  /** The descriptor the command works on, or -1 for none. */
  def fd: Int

  /** The direction whose turn the command takes on its handle (see `claim`). */
  def interest: Interest

  /** Whether the command takes its direction's turn on its handle: a file's reads and writes at an offset take none,
    * since they may run side by side.
    */
  protected def takesTurn: Boolean = true

  /** Whether a kernel worker runs it, as for a regular file, where nothing interrupts it once started, rather than the
    * kernel waiting for readiness, which a cancel ends at once (see `UringReactor.withdrawOwn`): by default, whether
    * its handle is a regular file's.
    */
  private[uring] def runsInWorker: Boolean = {
    val h = handle
    h != null && h.kind == UringBackend.Handle.RegularFile
  }

  /** Whether its handle is a socket's, on which a read is a receive and a write a send, which the kernel waits for
    * readiness for itself, where it gives back `EAGAIN` for a read or a write of a non-blocking descriptor.
    */
  protected final def onSocket: Boolean = {
    val h = handle
    h != null && h.kind == UringBackend.Handle.Socket
  }

  /** Set by the reactor for a submission right after `tryAtOnce` found nothing to do, so that a send or a receive waits
    * for readiness before trying again (`pollFirst`).
    */
  private[uring] var triedFirst = false

  /** Has a send or a receive wait for readiness first, if `tryAtOnce` just found it would block. */
  protected final def pollFirst(sqe: Ptr[io_uring_sqe]): Unit =
    if triedFirst then {
      sqe.ioprio = IORING_RECVSEND_POLL_FIRST.toUShort
      triedFirst = false
    }

  override private[io] def claim: gears.async.io.internal.Turns | Null = {
    val h = handle
    if h == null || !takesTurn then null else if interest == Interest.Read then h.reader else h.writer
  }

  /** The handle the command was built on, through `UringOps`, which the reactor takes from the command's entry's
    * preparation until the kernel has taken the entry, failing the command if it is closed, and counts the entry
    * against until the kernel lets go of it, so that a close cancels it (see `UringBackend.Handle`); null for a command
    * built on a descriptor directly, as io_uring-specific code may, or one that opens its own.
    */
  private[uring] var handle: UringBackend.Handle | Null = null

  /** Fills in `sqe` for one submission. Throwing fails the op, as it would at once: nothing has reached the kernel. */
  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit

  /** Handles the result of one submission, a count, a descriptor or a negated `errno`: true once the op is done, false
    * to submit it again. Throwing fails the op.
    */
  protected def finish(res: Int): Boolean

  /** Whether the command has nothing to do as it stands, such as a write with no bytes remaining: then it completes as
    * it is submitted, as one performed at once does, with no system call.
    */
  private[uring] def nothingToDo: Boolean = false

  /** For `trySubmit`: performs the operation at once with a non-blocking system call, and returns true if that finished
    * the op, or false, having done nothing, if it would block. Throwing fails the op. By default the command is never
    * tried, so it always goes to the ring.
    */
  def tryAtOnce(): Boolean = false

  /** Releases what a submission produced that nobody will see, because the op was withdrawn before its completion ran,
    * such as the descriptor an accept returned or the pool's buffer a read took; `res` is the submission's result, or
    * `Command.NoResult` for one that never reached the kernel. What the caller gave, its buffer's position among it, is
    * left as it was.
    */
  protected def discard(res: Int): Unit = ()

  /** The poll events to wait for when a submission completes with `EAGAIN`, or 0 for a command that never does. */
  protected def readinessEvents: Int = 0

  // The user data of the command's submission in flight, 0 when none is; loop thread only.
  private[uring] var userData = 0L

  // The commands in the kernel on the same descriptor number on the same reactor (`UringReactor.onFd`); loop thread only.
  private[uring] var prevOnFd: Command | Null = null
  private[uring] var nextOnFd: Command | Null = null

  private var awaitingReadiness = false

  /** Whether the submission in the kernel is a wait for readiness after an `EAGAIN`, for tests. */
  private[uring] def waitsForReadiness: Boolean = awaitingReadiness

  private[uring] final def prepareSubmission(sqe: Ptr[io_uring_sqe]): Unit =
    if awaitingReadiness then io_uring_prep_poll_add(sqe, fd, readinessEvents.toUInt)
    else prepare(sqe)

  private[uring] final def finishSubmission(res: Int): Boolean =
    if awaitingReadiness then {
      awaitingReadiness = false // the descriptor is ready: submitting again runs the command itself
      if res < 0 && !Results.transient(res) then throw Results.failure(s"Failed to wait for descriptor $fd", res)
      false
    } else if res == -errno.EAGAIN && readinessEvents != 0 then {
      awaitingReadiness = true
      false
    } else finish(res)

  /** Forgets a readiness wait the last result started, for a command withdrawn instead of submitted again. */
  private[uring] final def abandonRetry(): Unit = awaitingReadiness = false

  /** Takes in the result of a submission whose op was withdrawn: a readiness wait's is a poll mask, which holds
    * nothing, and anything else is the command's to discard.
    */
  private[uring] final def discardSubmission(res: Int): Unit =
    if awaitingReadiness then {
      awaitingReadiness = false
      discard(Command.NoResult)
    } else discard(res)
}

object Command {

  /** What `discard` is given for a submission that never reached the kernel, or whose result held nothing. */
  inline val NoResult = Int.MinValue
}

/** A one-shot timer, `nanos` from each submission, kept in the reactor's heap while pending (`TimerState`). */
sealed class UringTimer private[uring] (protected val nanos: Long)
    extends gears.async.io.internal.LoopTimer[UringOp]
    with UringOp {

  /** A timer of `milliseconds`, for io_uring-specific code. */
  def this(milliseconds: Int) = {
    this(milliseconds * 1_000_000L)
    require(milliseconds >= 0, "Timer duration must not be negative")
  }

  /** Its time, in whole milliseconds, for io_uring-specific code; a deadline timer's is negative. */
  def milliseconds: Int = if nanos < 0 then -1 else (nanos / 1_000_000L).toInt
}

/** A timer at a deadline that may move while it is pending (see `Ops.deadlineTimer`). */
final class UringDeadlineTimer private[uring] (initially: Long) extends UringTimer(-1L) with Deadline {
  at = initially
  def deadline: Long = at
  def moveTo(nanoTime: Long): Unit = move(nanoTime)
}

/** The reactor's side of a promise (`LoopPromise`): `UringBlocking` completes itself from the blocker pool, and
  * `Close`, when it cannot close at once, from the thread of the descriptor's last use.
  */
sealed abstract class UringPromise extends gears.async.io.internal.LoopPromise[UringOp] with UringOp

/** A promise the reactor completes by running `block` on the blocker pool, with `BlockingRun`'s cancellation: skipped
  * if not started, interrupted if running, and `onCancel` waits for `block` to return.
  */
abstract class UringBlocking extends UringPromise with gears.async.io.internal.LoopBlocking[UringOp]

/** A blocking task built from a function, for `BlockingOps.blocking`. */
final class BlockingTask(body: () => Unit) extends UringBlocking {
  def block(): Unit = body()
}

/** A blocking task whose function's value is written into `into`, for `BlockingOps.blocking`. */
final class BlockingResultTask[A <: AnyRef](body: () => A, into: Slot[A]) extends UringBlocking {
  override private[io] def slotRefusal: Throwable | Null = gears.async.io.internal.Checks.notEmpty(into)
  def block(): Unit = {
    publish(into, body())
  }
}
