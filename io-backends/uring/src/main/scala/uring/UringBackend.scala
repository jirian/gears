package gears.async.io
package uring

import gears.async.Async
import gears.async.AsyncSupport
import gears.async.Future

import java.nio.ByteBuffer
import scala.concurrent.duration.FiniteDuration

/** The io_uring backend (Linux 6.0 or later, Scala Native), through liburing.
  *
  * A program runs on a pool of workers (`UringScheduler`), one for each processor, each with its own ring, inbox and
  * heap of timers. An op is performed on the performer's own worker: tried at once with a non-blocking system call,
  * else prepared as an entry on that worker's ring, and the performer resumed once the entry's completion arrives. So
  * far it has IP stream and datagram sockets, Unix stream sockets, regular files, pipes, name resolution and blocking
  * ops.
  */
object UringBackend
    extends IpStreams
    with IpDatagrams
    with UnixStreams
    with RegularFiles
    with Pipes
    with Resolving
    with Blocking {
  type Op = UringOp
  type Handle = UringHandle
  type Readable = UringReadable
  type Writable = UringWritable
  type Connection = ConnectionHandle
  type Listener = ListenerHandle
  type BoundDatagram = BoundDatagramHandle
  type ConnectedDatagram = ConnectedDatagramHandle
  type File = FileHandle

  val ops: UringOps.type = UringOps

  def pool(bufferSize: Int, buffers: Int): BufferPool = new NativePool(bufferSize, buffers)

  /** Runs `program` on a pool of `GEARS_IO_URING_WORKERS` workers, one for each processor if it is not set, shutting it
    * down afterwards.
    */
  private[io] def run[T](program: Io[this.type] ?=> Async.Spawn ?=> T): T =
    runOn(defaultWorkers)(program)

  /** The workers of a run: `GEARS_IO_URING_WORKERS`, or one for each processor. */
  def defaultWorkers: Int =
    sys.env
      .get("GEARS_IO_URING_WORKERS")
      .flatMap(_.toIntOption)
      .filter(_ > 0)
      .getOrElse(Runtime.getRuntime.availableProcessors)

  /** Runs `program` over this backend as `Io.run` does, on a pool of `workers` workers, each with its own ring. */
  def runOn[T](workers: Int)(program: Io[this.type] ?=> Async.Spawn ?=> T): T = {
    require(workers > 0, s"A pool has at least one worker, not $workers")
    val pool = UringScheduler(workers)
    given poolSupport: (AsyncSupport { type Scheduler = pool.Scheduler }) = pool
    given poolScheduler: pool.Scheduler = pool
    // `Async.blocking` runs its body on the calling thread.
    try Async.blocking(Future(program(using pool)).await)
    finally pool.shutdown()
  }
}

/** The io_uring backend's ops. */
object UringOps
    extends IpStreamOps
    with IpDatagramOps
    with UnixStreamOps
    with FileOps
    with PipeOps
    with ResolveOps
    with BlockingOps {
  import scala.annotation.targetName
  type Op = UringOp
  type Handle = UringHandle
  type Readable = UringReadable
  type Writable = UringWritable
  type Connection = ConnectionHandle
  type Listener = ListenerHandle
  type BoundDatagram = BoundDatagramHandle
  type ConnectedDatagram = ConnectedDatagramHandle
  type File = FileHandle

  def read(handle: UringReadable, buf: ByteBuffer): UringOp & Repeatable = ReadIntoBuffer(handle, buf)
  def write(handle: UringWritable, buf: ByteBuffer): UringOp & Repeatable = WriteFromBuffer(handle, buf)
  def read(handle: UringReadable, bufs: Array[ByteBuffer]): UringOp & Repeatable = ReadIntoBuffers(handle, bufs)
  def write(handle: UringWritable, bufs: Array[ByteBuffer]): UringOp & Repeatable = WriteFromBuffers(handle, bufs)

  def timer(duration: FiniteDuration): UringOp & Repeatable = {
    if duration.length < 0 then throw new IllegalArgumentException(s"A timer's time is not negative: $duration")
    UringTimer(duration)
  }

  def deadlineTimer(nanoTime: Long): UringOp & Repeatable & Deadline = UringDeadline(nanoTime)

  def read(handle: UringReadable, pool: BufferPool, into: Slot[Received]): UringOp & Repeatable = pool match {
    case ours: NativePool => PooledRead(handle, ours, into)
    case other            => throw new IllegalArgumentException(s"Not a pool of this backend's: $other")
  }
  def close(handle: UringHandle): UringOp = CloseHandle(handle)

  def pipe(read: Slot[UringReadable], write: Slot[UringWritable]): UringOp =
    OpenPipe(read.asInstanceOf[Slot[PipeSource]], write.asInstanceOf[Slot[PipeSink]])

  // ---- Stream sockets

  def connect(address: IpSocketAddress, options: SocketOptions, into: Slot[ConnectionHandle]): UringOp = {
    Sockets.check(options, SocketOption.Kind.Connection)
    Connect(address, options, into)
  }

  def connect(address: UnixAddress, options: SocketOptions, into: Slot[ConnectionHandle]): UringOp = {
    Sockets.checkPath(address.path)
    Sockets.check(options, SocketOption.Kind.UnixConnection)
    Connect(address, options, into)
  }

  def listen(address: IpSocketAddress, options: SocketOptions, into: Slot[ListenerHandle]): UringOp = {
    Sockets.check(options, SocketOption.Kind.Listener)
    Listen(address, options, into)
  }

  def listen(address: UnixAddress, options: SocketOptions, into: Slot[ListenerHandle]): UringOp = {
    Sockets.checkPath(address.path)
    Sockets.check(options, SocketOption.Kind.UnixListener)
    Listen(address, options, into)
  }

  def accept(handle: ListenerHandle, into: Slot[ConnectionHandle]): UringOp & Repeatable = Accept(handle, into)

  def acceptAll(handle: ListenerHandle, into: Slot[Array[ConnectionHandle]], max: Int): UringOp & Repeatable = {
    if max <= 0 then throw new IllegalArgumentException(s"acceptAll takes at least one connection, not $max")
    AcceptAll(handle, into, max)
  }

  def shutdownOutput(handle: ConnectionHandle): UringOp = ShutdownOutput(handle)

  private def open[A](handle: UringHandle)(body: Int => A): A = {
    if !handle.enter() then throw handle.closedFailure
    try body(handle.fd)
    finally handle.leave()
  }

  def localAddress(handle: ListenerHandle | ConnectionHandle): IpSocketAddress | UnixAddress =
    open(handle)(Sockets.localAddress)

  def peerAddress(handle: ConnectionHandle): IpSocketAddress | UnixAddress = open(handle)(Sockets.peerAddress)

  // ---- Socket options

  def supports(option: SocketOption[?], kind: SocketOption.Kind): Boolean = Sockets.supports(option, kind)

  def canChange(option: SocketOption[?], kind: SocketOption.Kind): Boolean = Sockets.canChange(option, kind)

  private def kindOf(handle: UringHandle): SocketOption.Kind = handle match {
    case c: ConnectionHandle => c.kind
    case l: ListenerHandle   => l.kind
    case d: DatagramHandle   => d.optionKind
    case other               => throw new IllegalArgumentException(s"$other is not a socket")
  }

  def option[V](handle: UringHandle, option: SocketOption[V]): V = {
    val kind = kindOf(handle)
    if !SocketOption.onSocket(option, kind) then
      throw new IllegalArgumentException(s"$option is not read on a ${kind.description}")
    open(handle)(Sockets.get(_, option)).asInstanceOf[V]
  }

  def setOption[V](handle: UringHandle, option: SocketOption[V], value: V): Unit = {
    val kind = kindOf(handle)
    if !canChange(option, kind) then
      throw new IllegalArgumentException(s"$option is not changed on an open ${kind.description}")
    option.check(value)
    open(handle)(Sockets.set(_, option, value))
  }

  // ---- Datagram sockets

  def bindDatagram(local: IpSocketAddress, options: SocketOptions, into: Slot[BoundDatagramHandle]): UringOp = {
    Sockets.check(options, SocketOption.Kind.BoundDatagram)
    OpenDatagram(local, null, options, into)
  }

  def connectDatagram(
      local: IpSocketAddress | Null,
      remote: IpSocketAddress,
      options: SocketOptions,
      into: Slot[ConnectedDatagramHandle]
  ): UringOp = {
    Sockets.check(options, SocketOption.Kind.ConnectedDatagram)
    if local != null && local.address.protocol != remote.address.protocol then
      throw new IllegalArgumentException(s"$local and $remote are of different families")
    OpenDatagram(local, remote, options, into)
  }

  def receive(handle: BoundDatagramHandle, buf: ByteBuffer, from: Slot[IpSocketAddress]): UringOp & Repeatable =
    ReceiveFrom(handle, buf, from)

  def receive(handle: ConnectedDatagramHandle, buf: ByteBuffer): UringOp & Repeatable = ReceiveFrom(handle, buf, null)

  def send(handle: BoundDatagramHandle, buf: ByteBuffer, to: IpSocketAddress): UringOp & Repeatable =
    SendTo(handle, Array(buf), to)

  def send(handle: ConnectedDatagramHandle, buf: ByteBuffer): UringOp & Repeatable = SendTo(handle, Array(buf), null)

  def send(handle: BoundDatagramHandle, bufs: Array[ByteBuffer], to: IpSocketAddress): UringOp & Repeatable =
    SendTo(handle, bufs, to)

  def send(handle: ConnectedDatagramHandle, bufs: Array[ByteBuffer]): UringOp & Repeatable = SendTo(handle, bufs, null)

  @targetName("datagramLocalAddress")
  def localAddress(handle: BoundDatagramHandle | ConnectedDatagramHandle): IpSocketAddress =
    open(handle)(Sockets.localAddress).asInstanceOf[IpSocketAddress]

  @targetName("datagramPeerAddress")
  def peerAddress(handle: ConnectedDatagramHandle): IpSocketAddress =
    open(handle)(Sockets.peerAddress).asInstanceOf[IpSocketAddress]

  // ---- Files, names and blocking code

  def open(path: String, write: Boolean, into: Slot[FileHandle]): UringOp = {
    Files.checkPath(path)
    OpenFile(path, write, into)
  }

  private def offset(at: Long): Long = {
    if at < 0 then throw new IllegalArgumentException(s"A file offset must not be negative: $at")
    at
  }

  def read(handle: FileHandle, buf: ByteBuffer, at: Long): UringOp & Repeatable =
    PositionalRead(handle, buf, offset(at))

  def write(handle: FileHandle, buf: ByteBuffer, at: Long): UringOp & Repeatable =
    PositionalWrite(handle, buf, offset(at))

  def sync(handle: FileHandle): UringOp & Repeatable = FileSync(handle)

  def resolve(host: String, into: Slot[List[IpAddress]]): UringOp = {
    Resolve.check(host)
    Resolve(host, into)
  }

  def blocking(body: () => Unit): UringOp = BlockingTask[Unit](body, _ => (), Nil, interruptible = true)

  def blocking[A <: AnyRef](body: () => A, into: Slot[A]): UringOp =
    BlockingTask[A](body, into.set, List(into), interruptible = true)
}
