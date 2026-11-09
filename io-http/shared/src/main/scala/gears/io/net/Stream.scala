package gears.io.net

import gears.async.Async
import gears.async.CompletionGroup
import gears.async.io.Io
import gears.async.io.IpSocketAddress
import gears.async.io.Slot
import gears.async.io.StreamSockets
import gears.async.io.UnixAddress

import java.nio.ByteBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

/** A connected byte stream: a TCP or Unix socket, or TLS over one. Reads and writes suspend the calling future, and
  * each takes a time limit, after which it throws `java.util.concurrent.TimeoutException`: a read having moved no
  * bytes, so that a timed-out read is a wait that gave up, not a connection lost; a write having written what its
  * buffer's position says, so that it may be taken up again where it stopped. Failures are `IOException`s; a cancelled
  * caller's op stops and throws `CancellationException`, as a timed-out one does. One read and one write may be under
  * way at once, from different futures; not two of either.
  */
trait Stream {

  /** Reads into `buf`, between its position and limit, once at least one byte has arrived: how many, or -1 at the end
    * of the stream, which leaves `buf` as it was. `buf` must have room.
    */
  def read(buf: ByteBuffer, timeout: Duration = Duration.Inf)(using Async): Int

  /** Writes all of `buf`, however many writes it takes, each within `timeout`. */
  def write(buf: ByteBuffer, timeout: Duration = Duration.Inf)(using Async): Unit

  /** Writes all of `bufs`, in order, with as few writes as the stream can manage, each within `timeout`. */
  def write(bufs: Array[ByteBuffer], timeout: Duration)(using Async): Unit = {
    var i = 0
    while i < bufs.length do {
      write(bufs(i), timeout)
      i += 1
    }
  }

  /** Ends the stream for writing: the peer reads what was written, then the end of the stream, and this side may still
    * read.
    */
  def shutdownOutput()(using Async): Unit

  /** Closes the stream, once; later calls do nothing. It runs whether or not the caller has been cancelled, so that it
    * may be called from a `finally` as a future is cancelled.
    */
  def close()(using Async): Unit

  /** Whether the stream is TLS, whose end is authenticated: a peer's end without one is a truncation (see `Tls`). */
  def secure: Boolean = false

  /** The local and the peer's address. */
  def localAddress: IpSocketAddress | UnixAddress
  def peerAddress: IpSocketAddress | UnixAddress
}

object Stream {

  /** `timeout` as `Io.perform` takes it: none if infinite. */
  private[io] def limit(timeout: Duration): FiniteDuration | Null = timeout match {
    case finite: FiniteDuration => finite
    case _                      => null
  }

  /** `body` in an `Async` that is not cancelled with the caller, for an op that must run however the caller ends, such
    * as a close: it is at once in meaning, so it cannot keep a cancelled future waiting.
    */
  private[io] inline def uncancelled[A](inline body: Async ?=> A)(using ac: Async): A =
    body(using ac.withGroup(CompletionGroup.Unlinked))

  /** A failure to recover from: not fatal, and not the caller's cancellation, which gears' `CancellationException`, a
    * `RuntimeException`, would otherwise be taken for.
    */
  private[io] object Failed {
    def unapply(e: Throwable): Boolean =
      !e.isInstanceOf[java.util.concurrent.CancellationException] && scala.util.control.NonFatal(e)
  }

  /** `body`, holding one of `semaphore`'s permits. */
  private[io] inline def holding[A](semaphore: gears.async.Semaphore)(inline body: A)(using Async): A = {
    val guard = semaphore.acquire()
    try body
    finally guard.release()
  }
}

/** A stream over a connected socket of `io`'s backend: what `Tcp` and `Unix` connect and accept. The ops of the last
  * buffer read into and written from are kept, so that reading and writing the same buffer again, as a buffered reader
  * does, allocates nothing.
  */
final class SocketStream private[io] (val io: Io[StreamSockets], connection: AnyRef) extends Stream {
  private val handle = connection.asInstanceOf[io.backend.Connection]
  private val ops = io.backend.ops

  private val closed = new java.util.concurrent.atomic.AtomicBoolean(false)

  private var readBuffer: ByteBuffer | Null = null
  private var readOp: io.Op | Null = null
  private var writeBuffer: ByteBuffer | Null = null
  private var writeOp: io.Op | Null = null
  private var writeBuffers: Array[ByteBuffer] | Null = null
  private var writevOp: io.Op | Null = null

  private def perform(op: io.Op, timeout: Duration)(using Async): Unit = {
    val finite = Stream.limit(timeout)
    if finite == null then io.perform(op) else io.perform(op, finite.nn)
  }

  def read(buf: ByteBuffer, timeout: Duration)(using Async): Int = {
    require(buf.hasRemaining, "A read needs a buffer with room")
    if !(readBuffer eq buf) then {
      readOp = ops.read(handle, buf)
      readBuffer = buf
    }
    val before = buf.position()
    perform(readOp.nn, timeout)
    val n = buf.position() - before
    if n == 0 then -1 else n
  }

  def write(buf: ByteBuffer, timeout: Duration)(using Async): Unit =
    if buf.hasRemaining then {
      if !(writeBuffer eq buf) then {
        writeOp = ops.write(handle, buf)
        writeBuffer = buf
      }
      val op = writeOp.nn
      while buf.hasRemaining do perform(op, timeout)
    }

  override def write(bufs: Array[ByteBuffer], timeout: Duration)(using Async): Unit =
    if bufs.length == 1 then write(bufs(0), timeout)
    else {
      if !(writeBuffers eq bufs) then {
        writevOp = ops.write(handle, bufs)
        writeBuffers = bufs
      }
      val op = writevOp.nn
      while bufs.exists(_.hasRemaining) do perform(op, timeout)
    }

  def shutdownOutput()(using Async): Unit = io.perform(ops.shutdownOutput(handle))

  def close()(using Async): Unit =
    if closed.compareAndSet(false, true) then Stream.uncancelled(io.perform(ops.close(handle)))

  def localAddress: IpSocketAddress | UnixAddress = ops.localAddress(handle)
  def peerAddress: IpSocketAddress | UnixAddress = ops.peerAddress(handle)

  override def toString: String = s"SocketStream($handle)"
}

/** A listening socket of `io`'s backend, accepting `SocketStream`s. */
final class Listener private[io] (val io: Io[StreamSockets], listener: AnyRef) {
  private val handle = listener.asInstanceOf[io.backend.Listener]
  private val into = Slot[io.backend.Connection]()
  private val acceptOp = io.backend.ops.accept(handle, into)
  private val isClosed = new java.util.concurrent.atomic.AtomicBoolean(false)

  /** Whether `close` has been called. */
  def closed: Boolean = isClosed.get()

  /** Waits for the next connection and returns it. Throws a closed channel's `IOException` once the listener is closed,
    * from any future. One future accepts at a time: the op is the listener's, made once, and a second `accept` while
    * one waits fails, as performing an op twice at once does.
    */
  def accept()(using Async): SocketStream = {
    io.perform(acceptOp)
    new SocketStream(io, into.clear())
  }

  /** The address it listens on, with the port the system chose if it was asked for port 0. */
  def localAddress: IpSocketAddress | UnixAddress = io.backend.ops.localAddress(handle)

  /** Stops listening, once: a pending `accept` fails, and the port is free again as this returns. */
  def close()(using Async): Unit =
    if isClosed.compareAndSet(false, true) then Stream.uncancelled(io.perform(io.backend.ops.close(handle)))
}
