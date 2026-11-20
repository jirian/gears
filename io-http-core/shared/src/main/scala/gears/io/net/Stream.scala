package gears.io.net

import gears.async.Async
import gears.async.CompletionGroup
import gears.async.io.BufferPool
import gears.async.io.Io
import gears.async.io.IpSocketAddress
import gears.async.io.Received
import gears.async.io.Slot
import gears.async.io.StreamSockets
import gears.async.io.UnixAddress

import java.nio.ByteBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

trait Stream {

  def read(buf: ByteBuffer, timeout: Duration = Duration.Inf)(using Async): Int

  def write(buf: ByteBuffer, timeout: Duration = Duration.Inf)(using Async): Unit

  def pooledReads: Boolean = false

  def read(pool: BufferPool, timeout: Duration)(using Async): Received =
    throw new UnsupportedOperationException("No pooled reads on this stream")

  def write(bufs: Array[ByteBuffer], timeout: Duration)(using Async): Unit = {
    var i = 0
    while i < bufs.length do {
      write(bufs(i), timeout)
      i += 1
    }
  }

  def shutdownOutput()(using Async): Unit

  def close()(using Async): Unit

  def secure: Boolean = false

  def localAddress: IpSocketAddress | UnixAddress
  def peerAddress: IpSocketAddress | UnixAddress
}

object Stream {

  private[io] def limit(timeout: Duration): FiniteDuration | Null = timeout match {
    case finite: FiniteDuration => finite
    case _                      => null
  }

  private[io] inline def uncancelled[A](inline body: Async ?=> A)(using ac: Async): A =
    body(using ac.withGroup(CompletionGroup.Unlinked))

  private[io] object Failed {
    def unapply(e: Throwable): Boolean =
      !e.isInstanceOf[java.util.concurrent.CancellationException] && scala.util.control.NonFatal(e)
  }

  private[io] inline def holding[A](semaphore: gears.async.Semaphore)(inline body: A)(using Async): A = {
    val guard = semaphore.acquire()
    try body
    finally guard.release()
  }
}

final class SocketStream private[io] (val io: Io[StreamSockets])(handle: io.ops.Connection) extends Stream {
  private val ops: io.ops.type = io.ops

  private val closed = new java.util.concurrent.atomic.AtomicBoolean(false)

  // - ops kept per buffer: reading or writing the same buffer again allocates nothing
  private var readBuffer: ByteBuffer | Null = null
  private var readOp: io.Op | Null = null
  private var writeBuffer: ByteBuffer | Null = null
  private var writeOp: io.Op | Null = null
  private var writeBuffers: Array[ByteBuffer] | Null = null
  private var writevOp: io.Op | Null = null
  private var pool: BufferPool | Null = null
  private var pooledOp: io.Op | Null = null
  private val received = Slot[Received]()

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

  override def pooledReads: Boolean = true

  override def read(from: BufferPool, timeout: Duration)(using Async): Received = {
    if !(pool eq from) then {
      pooledOp = ops.read(handle, from, received)
      pool = from
    }
    perform(pooledOp.nn, timeout)
    received.clear()
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

final class Listener private[io] (val io: Io[StreamSockets])(handle: io.ops.Listener) {
  private val into = Slot[io.ops.Connection]()
  private val acceptOp = io.ops.accept(handle, into)
  private val isClosed = new java.util.concurrent.atomic.AtomicBoolean(false)

  def closed: Boolean = isClosed.get()

  def accept()(using Async): SocketStream = {
    io.perform(acceptOp)
    new SocketStream(io)(into.clear())
  }

  def localAddress: IpSocketAddress | UnixAddress = io.ops.localAddress(handle)

  def close()(using Async): Unit =
    if isClosed.compareAndSet(false, true) then Stream.uncancelled(io.perform(io.ops.close(handle)))
}
