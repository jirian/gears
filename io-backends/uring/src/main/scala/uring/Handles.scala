package gears.async.io
package uring

import java.nio.channels.ClosedChannelException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import scala.scalanative.posix.unistd

final class HandleClosedException(handle: AnyRef) extends ClosedChannelException {
  override def getMessage: String = s"$handle is closed"
  override def fillInStackTrace(): Throwable = this
}

sealed abstract class UringHandle(val fd: Int, kind: String) {
  // - a use lasts until the kernel has let go of the entry, so the descriptor's number is never reused under one
  private val uses = new AtomicInteger()
  @volatile private var closing = false
  private val descriptorClosed = new AtomicBoolean()
  @volatile private var onClosed: (() => Unit) | Null = null

  private val rings = new ConcurrentHashMap[Ring, AtomicInteger]()

  private[uring] def onRing(ring: Ring): Unit = {
    var n = rings.get(ring)
    if n == null then {
      rings.putIfAbsent(ring, new AtomicInteger()): Unit
      n = rings.get(ring)
    }
    n.nn.incrementAndGet(): Unit
  }

  private[uring] def entriesOnRings: Int = {
    var n = 0
    rings.forEach((_, count) => n += count.get())
    n
  }

  private[uring] def offRing(ring: Ring): Unit = {
    val n = rings.get(ring)
    if n != null then n.decrementAndGet(): Unit
  }

  private[uring] val reading = new AtomicBoolean()
  private[uring] val writing = new AtomicBoolean()

  @volatile private[uring] var parked: Command | Null = null

  def isClosing: Boolean = closing

  private[uring] def triesAtOnce: Boolean = true

  private[uring] def enter(): Boolean = {
    uses.incrementAndGet()
    if closing then {
      leave()
      false
    } else true
  }

  private[uring] def leave(): Unit =
    if uses.decrementAndGet() == 0 && closing then closeDescriptor()

  private[uring] def close(whenClosed: () => Unit): Boolean = {
    synchronized {
      if closing then throw closedFailure
      onClosed = whenClosed
      closing = true
    }
    beginClosing()
    val waiting = parked
    if waiting != null && waiting.leaveParking() then waiting.owner.nn.startTurn(waiting)
    rings.forEach { (ring, n) =>
      if n.get() > 0 then {
        ring.cancelFd(fd)
        ring.wake()
      }
    }
    if uses.get() == 0 then closeDescriptor()
    descriptorClosed.get()
  }

  private def closeDescriptor(): Unit =
    if descriptorClosed.compareAndSet(false, true) then {
      unistd.close(fd): Unit
      closed()
      val tell = onClosed
      onClosed = null
      if tell != null then tell()
    }

  protected def beginClosing(): Unit = ()

  protected def closed(): Unit = ()

  private[uring] def closedFailure: HandleClosedException = new HandleClosedException(this)

  override def toString: String = s"$kind $fd"
}

trait UringReadable extends UringHandle

trait UringWritable extends UringHandle

final class PipeSource private[uring] (fd: Int) extends UringHandle(fd, "pipe's read end") with UringReadable

final class PipeSink private[uring] (fd: Int) extends UringHandle(fd, "pipe's write end") with UringWritable

final class FifoSource private[uring] (fd: Int, path: String)
    extends UringHandle(fd, s"read end of FIFO $path")
    with UringReadable

final class FifoSink private[uring] (fd: Int, path: String)
    extends UringHandle(fd, s"write end of FIFO $path")
    with UringWritable

final class ConnectionHandle private[uring] (fd: Int, val unix: Boolean)
    extends UringHandle(fd, if unix then "Unix connection" else "connection")
    with UringReadable
    with UringWritable {
  def kind: SocketOption.Kind = if unix then SocketOption.Kind.UnixConnection else SocketOption.Kind.Connection
}

private[uring] final class SocketFile(path: String) {
  private val id = Sockets.fileId(path)

  def remove(): Unit = if id != null && Sockets.fileId(path) == id then Sockets.unlink(path)
}

final class ListenerHandle private[uring] (fd: Int, val unix: Boolean, val options: SocketOptions, path: String | Null)
    extends UringHandle(fd, if unix then "Unix listener" else "listener") {
  private val file = if path == null then null else SocketFile(path)
  def kind: SocketOption.Kind = if unix then SocketOption.Kind.UnixListener else SocketOption.Kind.Listener

  private[uring] val accepts = new Turns

  override protected def beginClosing(): Unit = accepts.failWaiting()

  override protected def closed(): Unit = if file != null then file.remove()
}

sealed abstract class DatagramHandle(fd: Int, kind: String, val ipv6: Boolean, path: String | Null = null)
    extends UringHandle(fd, kind) {
  private val file = if path == null then null else SocketFile(path)

  def optionKind: SocketOption.Kind

  def unix: Boolean = optionKind.unix

  private[uring] val receives = new Turns
  private[uring] val sends = new Turns

  override protected def beginClosing(): Unit = {
    receives.failWaiting()
    sends.failWaiting()
  }

  override protected def closed(): Unit = if file != null then file.remove()
}

final class BoundDatagramHandle private[uring] (fd: Int, ipv6: Boolean)
    extends DatagramHandle(fd, "bound datagram socket", ipv6) {
  def optionKind = SocketOption.Kind.BoundDatagram
}

final class ConnectedDatagramHandle private[uring] (fd: Int, ipv6: Boolean)
    extends DatagramHandle(fd, "connected datagram socket", ipv6) {
  def optionKind = SocketOption.Kind.ConnectedDatagram
}

final class UnixBoundDatagramHandle private[uring] (fd: Int, path: String)
    extends DatagramHandle(fd, "bound Unix datagram socket", ipv6 = false, path) {
  def optionKind = SocketOption.Kind.UnixBoundDatagram
}

final class UnixConnectedDatagramHandle private[uring] (fd: Int, path: String | Null)
    extends DatagramHandle(fd, "connected Unix datagram socket", ipv6 = false, path) {
  def optionKind = SocketOption.Kind.UnixConnectedDatagram
}

private[uring] final class Turns {
  private val queue = new java.util.ArrayDeque[Command]()

  def enter(command: Command): Boolean = synchronized {
    queue.addLast(command)
    queue.size == 1
  }

  def leaveWaiting(command: Command): Boolean = synchronized {
    if queue.peekFirst() eq command then false else queue.remove(command)
  }

  def pass(command: Command): Command | Null = synchronized {
    if queue.peekFirst() eq command then {
      queue.pollFirst()
      queue.peekFirst()
    } else {
      queue.remove(command)
      null
    }
  }

  def failWaiting(): Unit = {
    val waiting = synchronized {
      val all = queue.toArray(new Array[Command](0)).toList
      all.drop(1).foreach(queue.remove)
      all.drop(1)
    }
    waiting.foreach(c => c.owner.nn.startTurn(c))
  }
}

final class FileHandle private[uring] (fd: Int, path: String)
    extends UringHandle(fd, s"file $path")
    with UringReadable
    with UringWritable {
  override private[uring] def triesAtOnce: Boolean = false
}
