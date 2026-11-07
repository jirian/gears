package gears.async.io
package nio

import gears.async.Async
import gears.async.Future
import gears.async.VThreadScheduler
import gears.async.VThreadSupport

import java.nio.ByteBuffer
import java.nio.channels.NetworkChannel
import scala.annotation.targetName
import scala.concurrent.duration.FiniteDuration

/** NIO with no reactor: each op performed on the calling thread, gears' virtual threads, the JVM's default backend. Its
  * ops and handles are the reference backend's (`gears.async.io.reference`, by Jamie Thompson), with the way of waiting
  * of his earlier `NioOnCaller`: an op whose channel is not ready yields and tries again, up to `Retries` times,
  * because in request and response traffic the reply often arrives within microseconds; then it waits on its handle's
  * own selector, which the JDK's poller parks a virtual thread in, so no thread polls for it, and tries again when
  * woken. A run of ops done at once yields now and then (`HereBudget`), so it stays fair. Timers park the caller; files
  * are read and written by the caller, blocking code and name lookups on threads of their own. Nothing of the JDK
  * beyond its public interface, so it needs no flags.
  *
  * It has IP stream and datagram sockets, Unix stream sockets, regular files, pipes, name resolution and blocking ops:
  * what the JDK has, which has no Unix datagram sockets and no way to open a FIFO without blocking.
  */
object NioOnCaller
    extends IpStreams
    with IpDatagrams
    with UnixStreams
    with RegularFiles
    with Pipes
    with Resolving
    with Blocking {
  type Op = NioOp
  type Handle = NioHandle
  type Readable = ReadEnd
  type Writable = WriteEnd
  type Connection = ConnectionHandle
  type Listener = ListenerHandle
  type BoundDatagram = BoundDatagramHandle
  type ConnectedDatagram = ConnectedDatagramHandle
  type File = FileHandle

  val ops: NioOps.type = NioOps

  /** How many times an op whose channel is not ready yields and tries again before it waits on a selector
    * (`-Dgears.io.nio.retries`).
    */
  val Retries: Int = sys.props.get("gears.io.nio.retries").map(_.toInt).getOrElse(8)

  /** Performances of one op in a row done at once, its channel ready, before the caller yields, so that a run of them
    * gives up the thread now and then.
    */
  inline val HereBudget = 32

  def pool(bufferSize: Int, buffers: Int): BufferPool = new NioPool(bufferSize, buffers)

  /** Runs `program` on gears' virtual threads, each op performed by the future performing it. */
  private[io] def run[T](program: Io[this.type] ?=> Async.Spawn ?=> T): T = {
    given VThreadSupport.type = VThreadSupport
    given VThreadScheduler.type = VThreadScheduler
    val io: Io[this.type] = new Io[this.type] {
      val backend: NioOnCaller.type = NioOnCaller
      private[io] def performWithin(op: NioOp, timeoutMillis: Int)(using Async): Unit = op.perform(timeoutMillis)
    }
    // `Async.blocking` runs its body on the calling thread, which may be a platform thread; a future's is virtual.
    Async.blocking(Future(program(using io)).await)
  }
}

/** The reference backend's ops, each checked as it is built. */
object NioOps
    extends IpStreamOps[NioOp, NioHandle]
    with UnixStreamOps[NioOp, NioHandle]
    with IpDatagramOps[NioOp, NioHandle]
    with FileOps[NioOp, NioHandle]
    with PipeOps[NioOp, NioHandle]
    with ResolveOps[NioOp, NioHandle]
    with BlockingOps[NioOp, NioHandle] {
  import SocketOption.Kind

  type Readable = ReadEnd
  type Writable = WriteEnd
  type Connection = ConnectionHandle
  type Listener = ListenerHandle
  type BoundDatagram = BoundDatagramHandle
  type ConnectedDatagram = ConnectedDatagramHandle
  type File = FileHandle

  private def fillable(buf: ByteBuffer): ByteBuffer = {
    if buf.isReadOnly() then throw new IllegalArgumentException(s"A read-only buffer cannot be read into: $buf")
    buf
  }

  private def several(bufs: Array[ByteBuffer]): Array[ByteBuffer] = {
    if bufs.isEmpty then throw new IllegalArgumentException("A vectored op takes at least one buffer")
    bufs
  }

  private def offset(at: Long): Long = {
    if at < 0 then throw new IllegalArgumentException(s"A file offset must not be negative: $at")
    at
  }

  // ---- Every backend's

  def read(handle: ReadEnd, buf: ByteBuffer): NioOp & Repeatable = ReadOp(handle, fillable(buf))

  def write(handle: WriteEnd, buf: ByteBuffer): NioOp & Repeatable = WriteOp(handle, buf)

  def read(handle: ReadEnd, bufs: Array[ByteBuffer]): NioOp & Repeatable = {
    several(bufs).foreach(fillable)
    ScatterOp(handle, bufs)
  }

  def write(handle: WriteEnd, bufs: Array[ByteBuffer]): NioOp & Repeatable = GatherOp(handle, several(bufs))

  def timer(duration: FiniteDuration): NioOp & Repeatable = {
    if duration.length < 0 then throw new IllegalArgumentException(s"A timer's time is not negative: $duration")
    TimerOp(duration.toNanos)
  }

  def deadlineTimer(nanoTime: Long): NioOp & Repeatable & Deadline = DeadlineOp(nanoTime)

  def read(handle: ReadEnd, pool: BufferPool, into: Slot[Received]): NioOp & Repeatable = pool match {
    case ours: NioPool => PooledReadOp(handle, ours, into)
    case other         => throw new IllegalArgumentException(s"Not a pool of this backend's: $other")
  }

  def close(handle: NioHandle): NioOp = CloseOp(handle)

  // ---- Stream sockets

  def accept(handle: ListenerHandle, into: Slot[ConnectionHandle]): NioOp & Repeatable = AcceptOp(handle, into)

  def acceptAll(handle: ListenerHandle, into: Slot[Array[ConnectionHandle]], max: Int): NioOp & Repeatable = {
    if max <= 0 then throw new IllegalArgumentException(s"acceptAll takes at least one connection, not $max")
    AcceptAllOp(handle, into, max)
  }

  def shutdownOutput(handle: ConnectionHandle): NioOp = ShutdownOutputOp(handle)

  def localAddress(handle: ListenerHandle | ConnectionHandle): IpSocketAddress | UnixAddress = {
    handle.checkOpen()
    handle match {
      case l: ListenerHandle =>
        if l.unix then Addresses.unix(l.server.getLocalAddress()) else Addresses.ip(l.server.getLocalAddress())
      case c: ConnectionHandle =>
        if c.unix then Addresses.unix(c.socket.getLocalAddress()) else Addresses.ip(c.socket.getLocalAddress())
    }
  }

  def peerAddress(handle: ConnectionHandle): IpSocketAddress | UnixAddress = {
    handle.checkOpen()
    if handle.unix then Addresses.unix(handle.socket.getRemoteAddress())
    else Addresses.ip(handle.socket.getRemoteAddress())
  }

  def connect(address: IpSocketAddress, options: SocketOptions, into: Slot[ConnectionHandle]): NioOp = {
    Addresses.checked(address)
    Options.check(options, Kind.Connection)
    ConnectOp(address, options, into)
  }

  def listen(address: IpSocketAddress, options: SocketOptions, into: Slot[ListenerHandle]): NioOp = {
    Addresses.checked(address)
    Options.check(options, Kind.Listener)
    ListenOp(address, options, into)
  }

  def connect(address: UnixAddress, options: SocketOptions, into: Slot[ConnectionHandle]): NioOp = {
    val checked = Addresses.checked(address)
    Options.check(options, Kind.UnixConnection)
    ConnectOp(checked, options, into)
  }

  def listen(address: UnixAddress, options: SocketOptions, into: Slot[ListenerHandle]): NioOp = {
    val checked = Addresses.checked(address)
    Options.check(options, Kind.UnixListener)
    ListenOp(checked, options, into)
  }

  // ---- IP datagram sockets

  def bindDatagram(local: IpSocketAddress, options: SocketOptions, into: Slot[BoundDatagramHandle]): NioOp = {
    Addresses.checked(local)
    Options.check(options, Kind.BoundDatagram)
    OpenDatagramOp(local, null, options, into)
  }

  def connectDatagram(
      local: IpSocketAddress | Null,
      remote: IpSocketAddress,
      options: SocketOptions,
      into: Slot[ConnectedDatagramHandle]
  ): NioOp = {
    val (l, r) = Addresses.pair(local, remote)
    Options.check(options, Kind.ConnectedDatagram)
    OpenDatagramOp(l, r, options, into)
  }

  def receive(handle: BoundDatagramHandle, buf: ByteBuffer, from: Slot[IpSocketAddress]): NioOp & Repeatable =
    ReceiveOp(handle, fillable(buf), from)

  def receive(handle: ConnectedDatagramHandle, buf: ByteBuffer): NioOp & Repeatable =
    ReceiveOp(handle, fillable(buf), null)

  def send(handle: BoundDatagramHandle, buf: ByteBuffer, to: IpSocketAddress): NioOp & Repeatable =
    SendOp(handle, Array(buf), Addresses.checked(to))

  def send(handle: ConnectedDatagramHandle, buf: ByteBuffer): NioOp & Repeatable = SendOp(handle, Array(buf), null)

  def send(handle: BoundDatagramHandle, bufs: Array[ByteBuffer], to: IpSocketAddress): NioOp & Repeatable =
    SendOp(handle, several(bufs), Addresses.checked(to))

  def send(handle: ConnectedDatagramHandle, bufs: Array[ByteBuffer]): NioOp & Repeatable =
    SendOp(handle, several(bufs), null)

  @targetName("datagramLocalAddress")
  def localAddress(handle: BoundDatagramHandle | ConnectedDatagramHandle): IpSocketAddress = {
    handle.checkOpen()
    Addresses.ip(handle.datagrams.getLocalAddress())
  }

  @targetName("datagramPeerAddress")
  def peerAddress(handle: ConnectedDatagramHandle): IpSocketAddress = {
    handle.checkOpen()
    Addresses.ip(handle.datagrams.getRemoteAddress())
  }

  // ---- Socket options

  def supports(option: SocketOption[?], kind: Kind): Boolean = Options.supports(option, kind)

  def canChange(option: SocketOption[?], kind: Kind): Boolean = Options.canChange(option, kind)

  /** The socket `handle` names, and its kind. */
  private def socket(handle: NioHandle): (NetworkChannel, Kind) = handle match {
    case c: ConnectionHandle        => (c.socket, if c.unix then Kind.UnixConnection else Kind.Connection)
    case l: ListenerHandle          => (l.server, if l.unix then Kind.UnixListener else Kind.Listener)
    case b: BoundDatagramHandle     => (b.datagrams, Kind.BoundDatagram)
    case c: ConnectedDatagramHandle => (c.datagrams, Kind.ConnectedDatagram)
    case other                      => throw new IllegalArgumentException(s"$other is not a socket")
  }

  def option[V](handle: NioHandle, option: SocketOption[V]): V = {
    val (channel, kind) = socket(handle)
    if !Options.onOpen(option, kind) then
      throw new IllegalArgumentException(s"$option is not read on a ${kind.description} by this backend")
    handle.checkOpen()
    Options.get(channel, option).asInstanceOf[V]
  }

  def setOption[V](handle: NioHandle, option: SocketOption[V], value: V): Unit = {
    val (channel, kind) = socket(handle)
    if !Options.canChange(option, kind) then
      throw new IllegalArgumentException(s"$option is not changed on an open ${kind.description} by this backend")
    option.check(value)
    handle.checkOpen()
    Options.set(channel, option, value)
  }

  // ---- Files, pipes, names and blocking code

  def open(path: String, write: Boolean, into: Slot[FileHandle]): NioOp = {
    Addresses.checkPath(path)
    OpenFileOp(path, write, into)
  }

  def read(handle: FileHandle, buf: ByteBuffer, at: Long): NioOp & Repeatable =
    FileReadOp(handle, fillable(buf), offset(at))

  def write(handle: FileHandle, buf: ByteBuffer, at: Long): NioOp & Repeatable = FileWriteOp(handle, buf, offset(at))

  def sync(handle: FileHandle): NioOp & Repeatable = SyncOp(handle)

  def pipe(read: Slot[ReadEnd], write: Slot[WriteEnd]): NioOp = PipeOp(read, write)

  def resolve(host: String, into: Slot[List[IpAddress]]): NioOp = {
    if host.isEmpty then throw new IllegalArgumentException("An empty host name names no host")
    Addresses.checkPath(host)
    ResolveOp(host, into)
  }

  def blocking(body: () => Unit): NioOp = BlockingOp[AnyRef](() => { body(); "" }, null)

  def blocking[A <: AnyRef](body: () => A, into: Slot[A]): NioOp = BlockingOp(body, into)
}
