package gears.async.io
package nio

import java.nio.channels.Channel
import java.nio.channels.ClosedChannelException
import java.nio.channels.DatagramChannel
import java.nio.channels.FileChannel
import java.nio.channels.GatheringByteChannel
import java.nio.channels.Pipe
import java.nio.channels.ReadableByteChannel
import java.nio.channels.ScatteringByteChannel
import java.nio.channels.SelectableChannel
import java.nio.channels.Selector
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel
import java.nio.channels.WritableByteChannel
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributes
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

/** The failure of an op on a closed handle, naming it: a closed channel's, as `Ops.close` has it. Without a stack
  * trace, as every op a close fails makes one.
  */
final class HandleClosedException(handle: AnyRef) extends ClosedChannelException {
  override def getMessage: String = s"$handle is closed"
  override def fillInStackTrace(): Throwable = this
}

/** An op's time running out, without a stack trace: a read given a deadline is how a protocol waits, so it is common.
  */
private[nio] final class TimedOut(op: AnyRef) extends java.util.concurrent.TimeoutException(s"$op timed out") {
  override def fillInStackTrace(): Throwable = this
}

/** A handle of `NioOnCaller`: an I/O object of the JDK's, the ops in progress on it, which a close fails and waits for
  * before it closes the object, so that no op uses it once closed, and the selectors its ops wait on, one for reading
  * and accepting, one for writing and connecting, taken from `SelectorPool` when first waited on.
  */
sealed abstract class NioHandle(kind: String) {
  private val id = NioHandle.ids.incrementAndGet()
  private var closing = false
  private var active = 0
  private val waits = scala.collection.mutable.Set.empty[Wait]
  private var closer: Wait | Null = null
  private var readSelector: Selector | Null = null
  private var writeSelector: Selector | Null = null

  /** The I/O object, closed by `close`. */
  protected def channel: Channel

  /** Takes up an op performed on this handle, whose `wait` a close wakes; fails if the handle is closed. */
  private[nio] def enter(wait: Wait): Unit = synchronized {
    if closing then throw new HandleClosedException(this)
    active += 1
    waits += wait
  }

  /** Lets go of an op taken up by `enter`. */
  private[nio] def leave(wait: Wait): Unit = {
    val last = synchronized {
      active -= 1
      waits -= wait
      if active == 0 then closer else null
    }
    if last != null then last.signal()
  }

  /** The selector an op on this handle waits on for writing (or connecting), or for reading (or accepting): one op at a
    * time in each direction, since a stream has one read and one write at a time, and the rest take turns. Fails if the
    * handle is closed.
    */
  private[nio] def selector(write: Boolean): Selector = synchronized {
    if closing then throw new HandleClosedException(this)
    if write then {
      if writeSelector == null then writeSelector = SelectorPool.take()
      writeSelector.nn
    } else {
      if readSelector == null then readSelector = SelectorPool.take()
      readSelector.nn
    }
  }

  /** Throws the failure of an op on this handle if it has been closed. */
  private[nio] def checkOpen(): Unit = if synchronized(closing) then throw new HandleClosedException(this)

  /** Closes this handle: fails the ops in progress on it, waits for them to stop, then closes its I/O object. Fails if
    * it was closed already.
    */
  private[nio] def close(wait: Wait): Unit = {
    val woken = synchronized {
      if closing then throw new HandleClosedException(this)
      closing = true
      closer = wait
      waits.toList
    }
    woken.foreach(_.signal())
    while synchronized(active > 0) do {
      wait.reset()
      if synchronized(active > 0) then wait.parkUntilSignalled()
    }
    // No op is in progress, so none selects: the channel comes off its selectors first, so that closing it releases
    // its descriptor now.
    val selectors = synchronized {
      val both = List(readSelector, writeSelector).collect { case s: Selector => s }
      readSelector = null
      writeSelector = null
      both
    }
    try selectors.foreach(SelectorPool.release)
    finally channel.close()
    closed()
  }

  /** What else closing does once the I/O object is closed. */
  protected def closed(): Unit = ()

  /** One read at a time on a stream, and one write. */
  private val reading = new AtomicBoolean()
  private val writing = new AtomicBoolean()

  /** Runs `body` as the stream's one read, or write if `write`; fails if another is in progress. */
  private[nio] def exclusively[T](write: Boolean, op: AnyRef)(body: => T): T = {
    val flag = if write then writing else reading
    if !flag.compareAndSet(false, true) then
      throw new IllegalArgumentException(s"An op reading, or writing, as $op does, is already pending on its handle")
    try body
    finally flag.set(false)
  }

  override def toString: String = s"$kind #$id"
}

object NioHandle {
  private val ids = new AtomicLong()
}

/** A handle read as a stream. */
sealed trait ReadEnd extends NioHandle {
  private[nio] def in: ReadableByteChannel & ScatteringByteChannel

  /** What the poller waits on for it to be readable, or null if it always is. */
  private[nio] def inReady: SelectableChannel | Null
}

/** A handle written as a stream. */
sealed trait WriteEnd extends NioHandle {
  private[nio] def out: WritableByteChannel & GatheringByteChannel

  /** What the poller waits on for it to be writable, or null if it always is. */
  private[nio] def outReady: SelectableChannel | Null
}

/** A connected stream socket, IP or Unix. */
final class ConnectionHandle(private[nio] val socket: SocketChannel, private[nio] val unix: Boolean)
    extends NioHandle(if unix then "Unix connection" else "connection")
    with ReadEnd
    with WriteEnd {
  protected def channel: Channel = socket
  private[nio] def in = socket
  private[nio] def inReady = socket
  private[nio] def out = socket
  private[nio] def outReady = socket
}

/** A listening stream socket, IP or Unix, with the options it gives the connections it accepts, and for a Unix one the
  * socket file it bound, which it removes as it closes if the file there is still its own.
  */
final class ListenerHandle(
    private[nio] val server: ServerSocketChannel,
    private[nio] val unix: Boolean,
    private[nio] val options: SocketOptions,
    path: Path | Null
) extends NioHandle(if unix then "Unix listener" else "listener") {
  private val fileKey: AnyRef | Null = if path == null then null else ListenerHandle.fileKey(path)
  protected def channel: Channel = server
  private[nio] val accepts = new Turns

  override protected def closed(): Unit =
    if path != null && fileKey != null && ListenerHandle.fileKey(path) == fileKey then Files.deleteIfExists(path): Unit
}

object ListenerHandle {
  private[nio] def fileKey(path: Path): AnyRef | Null =
    try Files.readAttributes(path, classOf[BasicFileAttributes], LinkOption.NOFOLLOW_LINKS).fileKey()
    catch case _: java.io.IOException => null
}

/** An IP datagram socket: bound, or connected to one peer. Its receives and sends each take turns. */
sealed abstract class DatagramHandle(kind: String, private[nio] val datagrams: DatagramChannel)
    extends NioHandle(kind) {
  protected def channel: Channel = datagrams
  private[nio] val receives = new Turns
  private[nio] val sends = new Turns
}

final class BoundDatagramHandle(datagrams: DatagramChannel) extends DatagramHandle("bound datagram socket", datagrams)

final class ConnectedDatagramHandle(datagrams: DatagramChannel)
    extends DatagramHandle("connected datagram socket", datagrams)

/** A pipe's read end. */
final class PipeSourceHandle(source: Pipe.SourceChannel) extends NioHandle("pipe's read end") with ReadEnd {
  protected def channel: Channel = source
  private[nio] def in = source
  private[nio] def inReady = source
}

/** A pipe's write end. */
final class PipeSinkHandle(sink: Pipe.SinkChannel) extends NioHandle("pipe's write end") with WriteEnd {
  protected def channel: Channel = sink
  private[nio] def out = sink
  private[nio] def outReady = sink
}

/** A regular file, read and written as a stream at its own position, or at offsets. Always ready. */
final class FileHandle(private[nio] val file: FileChannel, path: String)
    extends NioHandle(s"file $path")
    with ReadEnd
    with WriteEnd {
  protected def channel: Channel = file
  private[nio] def in = file
  private[nio] def inReady = null
  private[nio] def out = file
  private[nio] def outReady = null
}
