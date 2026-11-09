package gears.async.io
package uring

import java.nio.channels.ClosedChannelException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import scala.scalanative.posix.unistd

/** The failure of an op on a closed handle, naming it: a closed channel's, as `Ops.close` has it. It has no stack
  * trace, which on Scala Native costs about 250 µs, a dozen round trips, and every op a close fails makes one.
  */
final class HandleClosedException(handle: AnyRef) extends ClosedChannelException {
  override def getMessage: String = s"$handle is closed"
  override def fillInStackTrace(): Throwable = this
}

/** A handle of the io_uring backend: a descriptor, the rings that have had entries on it, and the uses of its number in
  * progress. A command holds a use from its first system call on the descriptor until the kernel has let go of its
  * entry, so that the number is never closed, and reused for another file, under an entry that names it. A close, from
  * any thread, marks the handle closing, so that no use begins; cancels in the kernel, on every ring that has had
  * entries on it, those on the descriptor, which then complete with `ECANCELED` and fail as closed; and closes the
  * descriptor once the last use has ended, which then completes the close (`closed`).
  */
sealed abstract class UringHandle(val fd: Int, kind: String) {
  private val uses = new AtomicInteger()
  @volatile private var closing = false
  private val descriptorClosed = new AtomicBoolean()
  @volatile private var onClosed: (() => Unit) | Null = null

  /** How many commands have entries on the descriptor on each ring that has had one: a close cancels and wakes only the
    * rings with some, not every worker's that a future moving between workers ever used.
    */
  private val rings = new ConcurrentHashMap[Ring, AtomicInteger]()

  /** A command is about to have an entry on `ring`. Before its submission, which checks `isClosing`. */
  private[uring] def onRing(ring: Ring): Unit = {
    var n = rings.get(ring)
    if n == null then {
      rings.putIfAbsent(ring, new AtomicInteger()): Unit
      n = rings.get(ring)
    }
    n.nn.incrementAndGet(): Unit
  }

  /** A command's entries on `ring` are done with. */
  private[uring] def offRing(ring: Ring): Unit = {
    val n = rings.get(ring)
    if n != null then n.decrementAndGet(): Unit
  }

  /** One read at a time on a stream, and one write. */
  private[uring] val reading = new AtomicBoolean()
  private[uring] val writing = new AtomicBoolean()

  def isClosing: Boolean = closing

  /** Whether a command on it is tried at once with a non-blocking system call first: not on a regular file, where a
    * read of uncached data would block the worker on the disk.
    */
  private[uring] def triesAtOnce: Boolean = true

  /** Begins a use of the descriptor: false if the handle is closing, and the use not begun. */
  private[uring] def enter(): Boolean = {
    uses.incrementAndGet()
    if closing then {
      leave()
      false
    } else true
  }

  /** Ends a use: the last one of a closing handle closes its descriptor. */
  private[uring] def leave(): Unit =
    if uses.decrementAndGet() == 0 && closing then closeDescriptor()

  /** Begins closing, from any thread, failing if it was closing already. Cancels the entries on the descriptor on every
    * ring, and closes the descriptor once no use is left, at once or as the last ends, then calls `whenClosed`; true if
    * that has been done by the time this returns.
    */
  private[uring] def close(whenClosed: () => Unit): Boolean = {
    synchronized {
      if closing then throw closedFailure
      onClosed = whenClosed
      closing = true
    }
    beginClosing()
    // A command counted after this read sees `closing` as it submits, and its entry becomes a no-op.
    rings.forEach { (ring, n) =>
      if n.get() > 0 then {
        ring.cancelFd(fd)
        ring.wake() // so that it cancels an entry it submitted as the close began (`UringWorker`)
      }
    }
    if uses.get() == 0 then closeDescriptor()
    descriptorClosed.get()
  }

  /** Closes the descriptor, once, and tells the close. */
  private def closeDescriptor(): Unit =
    if descriptorClosed.compareAndSet(false, true) then {
      unistd.close(fd): Unit
      closed()
      val tell = onClosed
      onClosed = null
      if tell != null then tell()
    }

  /** Whether the descriptor has been closed. */
  private[uring] def isClosed: Boolean = descriptorClosed.get()

  /** What else closing does as it begins, before the entries on the descriptor are cancelled. */
  protected def beginClosing(): Unit = ()

  /** What else closing does once the descriptor is closed. */
  protected def closed(): Unit = ()

  /** The failure of an op on this handle once it is closing. */
  private[uring] def closedFailure: HandleClosedException = new HandleClosedException(this)

  override def toString: String = s"$kind $fd"
}

/** A handle read as a stream. */
trait UringReadable extends UringHandle

/** A handle written as a stream. */
trait UringWritable extends UringHandle

/** A pipe's read end. */
final class PipeSource private[uring] (fd: Int) extends UringHandle(fd, "pipe's read end") with UringReadable

/** A pipe's write end. */
final class PipeSink private[uring] (fd: Int) extends UringHandle(fd, "pipe's write end") with UringWritable

/** A connected stream socket, IP or Unix, from a connect or an accept. */
final class ConnectionHandle private[uring] (fd: Int, val unix: Boolean)
    extends UringHandle(fd, if unix then "Unix connection" else "connection")
    with UringReadable
    with UringWritable {
  def kind: SocketOption.Kind = if unix then SocketOption.Kind.UnixConnection else SocketOption.Kind.Connection
}

/** A listening stream socket, IP or Unix, with the options it gives the connections it accepts, and for a Unix one the
  * socket file it bound, which it removes as it closes if the file there is still its own.
  */
final class ListenerHandle private[uring] (fd: Int, val unix: Boolean, val options: SocketOptions, path: String | Null)
    extends UringHandle(fd, if unix then "Unix listener" else "listener") {
  private val file = if path == null then null else Sockets.fileId(path)
  def kind: SocketOption.Kind = if unix then SocketOption.Kind.UnixListener else SocketOption.Kind.Listener

  /** Its accepts take turns, in the order they began. */
  private[uring] val accepts = new Turns

  override protected def beginClosing(): Unit = accepts.failWaiting()

  override protected def closed(): Unit =
    if path != null && file != null && Sockets.fileId(path) == file then Sockets.unlink(path)
}

/** An IP datagram socket: bound, or connected to one peer. An IPv6 one is dual stack. */
sealed abstract class DatagramHandle(fd: Int, kind: String, val ipv6: Boolean) extends UringHandle(fd, kind) {
  def optionKind: SocketOption.Kind

  /** Its receives, and its sends, each take turns, in the order they began. */
  private[uring] val receives = new Turns
  private[uring] val sends = new Turns

  override protected def beginClosing(): Unit = {
    receives.failWaiting()
    sends.failWaiting()
  }
}

final class BoundDatagramHandle private[uring] (fd: Int, ipv6: Boolean)
    extends DatagramHandle(fd, "bound datagram socket", ipv6) {
  def optionKind = SocketOption.Kind.BoundDatagram
}

final class ConnectedDatagramHandle private[uring] (fd: Int, ipv6: Boolean)
    extends DatagramHandle(fd, "connected datagram socket", ipv6) {
  def optionKind = SocketOption.Kind.ConnectedDatagram
}

/** Commands taking turns on a handle, first come first served: only the one at the head is tried or in the kernel, and
  * when it ends, by its completion or a withdrawal, the next is started on its own worker. A withdrawal of one still
  * waiting takes it out; a close fails those waiting, each on its own worker.
  */
private[uring] final class Turns {
  private val queue = new java.util.ArrayDeque[Command]()

  /** Queues `command`: true if it holds the turn now. */
  def enter(command: Command): Boolean = synchronized {
    queue.addLast(command)
    queue.size == 1
  }

  /** Takes out `command` if it is waiting, not holding the turn: true if it was. */
  def leaveWaiting(command: Command): Boolean = synchronized {
    if queue.peekFirst() eq command then false else queue.remove(command)
  }

  /** Ends `command`'s turn, or its wait: the command whose turn it is now, to start, or null. */
  def pass(command: Command): Command | Null = synchronized {
    if queue.peekFirst() eq command then {
      queue.pollFirst()
      queue.peekFirst()
    } else {
      queue.remove(command)
      null
    }
  }

  /** Hands each command waiting, not holding the turn, to its worker, which fails it as its handle is closing. */
  def failWaiting(): Unit = {
    val waiting = synchronized {
      val all = queue.toArray(new Array[Command](0)).toList
      all.drop(1).foreach(queue.remove)
      all.drop(1)
    }
    waiting.foreach(c => c.owner.nn.startTurn(c))
  }
}

/** A regular file, read and written as a stream at its own position, or at offsets, and synced, by the kernel's
  * workers.
  */
final class FileHandle private[uring] (fd: Int, path: String)
    extends UringHandle(fd, s"file $path")
    with UringReadable
    with UringWritable {
  override private[uring] def triesAtOnce: Boolean = false
}
