package gears.async.io
package uring

import gears.async.io.Received
import gears.async.io.Slot
import gears.async.io.internal.Interest
import gears.async.io.internal.NativeBuffer
import gears.async.io.internal.NativePool
import gears.async.io.internal.NonBlocking

import java.io.IOException
import java.nio.ByteBuffer
import scala.scalanative.memory.PointerBufferOps.given
import scala.scalanative.posix.errno
import scala.scalanative.posix.string
import scala.scalanative.posix.sys.socket
import scala.scalanative.posix.sys.uio
import scala.scalanative.posix.unistd
import scala.scalanative.runtime.ByteArray
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import uring.*
import uringStructOps.*

// The io_uring commands behind `UringOps`. The kernel uses a command's buffers from the submission of its entry until
// its completion arrives, so the command keeps them, and anything else the kernel reads after a submission, such as a
// datagram's address, until its completion arrives (`Staging`, `MessageHeader`). On a socket a command is a send or a
// receive, for which the kernel waits for readiness itself, where a read or a write of a non-blocking descriptor would
// come back with `EAGAIN` (see `Command`).

/** What the kernel is given for `bufs`, for one submission at a time: `begin` makes it, and `commit` makes what the
  * kernel moved its positions over the buffers' own, as `NativeBuffer.staged` does for a system call; `end` lets it go
  * without that. A buffer with native memory stands for itself, and one over a heap array the kernel may write, as
  * `ByteBuffer.allocate` makes, for its array, both at no cost; only a read-only one, whose array is hidden, has a
  * stand-in over memory of its own, holding its remaining bytes if `copyIn`, and copied back if `copyOut`.
  *
  * The kernel is handed the address of a heap array for as long as the command is in the kernel: sound because Scala
  * Native's collectors never move an object, and the command, which keeps its buffers, keeps the array alive. The
  * reactor's own arrays (the ring, the completions' batch, `MessageHeader`'s, a path being opened) rely on the same.
  */
private[uring] final class Staging(bufs: Array[ByteBuffer], copyIn: Boolean, copyOut: Boolean) {
  private val direct = bufs.forall(_.hasPointer())
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
          if b.hasPointer() then b
          else if b.hasArray() && b.hasRemaining() then Staging.inArray(b)
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
        // One over the buffer's own array moved the bytes in place: only its position is still to move.
        if !(stands(i) eq b) then NativeBuffer.settle(b, stands(i), copyOut && !b.hasArray())
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

private object Staging {

  /** A buffer over `buf`'s remaining bytes, in its own array, positioned at 0. */
  def inArray(buf: ByteBuffer): ByteBuffer = {
    val at = buf.array().asInstanceOf[ByteArray].at(buf.arrayOffset() + buf.position())
    val stand = scala.scalanative.memory.PointerBuffer.wrap(at, buf.remaining())
    stand
  }
}

/** A `msghdr`, its `iovec`s, one per buffer, and room for a socket address if `named`, for `readv`, `writev`, `sendmsg`
  * and `recvmsg`: arrays the command owns, since the kernel reads them after the submitting call returns.
  */
private[uring] final class MessageHeader(buffers: Int, named: Boolean) {
  private val header = new Array[Byte](sizeof[msghdr].toInt)
  private val vectorStorage = new Array[Byte](sizeof[uio.iovec].toInt * math.max(buffers, 1))
  private val name = new Array[Byte](if named then sizeof[socket.sockaddr_storage].toInt else 1)

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

/** Fills `buf` between position and limit. Done once at least one byte has been read, or at end of stream; refused if
  * `buf` has no room, since a read of nothing would look like the end of the stream.
  */
final class ReadIntoBuffer(val fd: Int, buf: ByteBuffer) extends Command {
  gears.async.io.internal.Checks.checkFillable(buf)
  def interest: Interest = Interest.Read
  override private[io] def refusal: Throwable | Null =
    if buf.hasRemaining() then null else gears.async.io.internal.Checks.bufferFull(buf)
  override protected def readinessEvents: Int = Linux.POLLIN
  private val staging = new Staging(Array(buf), copyIn = false, copyOut = true)

  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit = {
    val b = staging.begin()(0)
    if onSocket then {
      io_uring_prep_recv(sqe, fd, NativeBuffer.atPosition(b), b.remaining().toUInt, 0)
      pollFirst(sqe)
    } else io_uring_prep_read(sqe, fd, NativeBuffer.atPosition(b), b.remaining().toUInt, Results.currentPosition)
  }

  protected def finish(res: Int): Boolean =
    if Results.transient(res) then false
    else if res < 0 then {
      staging.end()
      throw Results.failure(s"Failed to read from descriptor $fd", res)
    } else {
      staging.commit(res) // 0 at end of stream, which leaves the buffer as it was
      true
    }

  override def tryAtOnce(): Boolean = NonBlocking.read(fd, buf) != 0 // -1 at EOF, which leaves the buffer as it was
  override protected def discard(res: Int): Unit = staging.end()
}

/** Fills `bufs` in order with one `readv`, as `ReadIntoBuffer` fills one (see `Ops.read`). */
final class ReadIntoBuffers(val fd: Int, bufs: Array[ByteBuffer]) extends Command {
  gears.async.io.internal.Checks.checkFillable(bufs)
  def interest: Interest = Interest.Read
  override private[io] def refusal: Throwable | Null =
    if gears.async.io.internal.Checks.anyRemaining(bufs) then null else gears.async.io.internal.Checks.buffersFull(bufs)
  override protected def readinessEvents: Int = Linux.POLLIN
  private val staging = new Staging(bufs, copyIn = false, copyOut = true)
  private val header = new MessageHeader(math.min(bufs.length, NonBlocking.MaxBuffers), named = false)

  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit = {
    val count = header.fill(staging.begin(), room = true)
    if onSocket then {
      header.set(count, 0)
      io_uring_prep_recvmsg(sqe, fd, header.msg, 0)
      pollFirst(sqe)
    } else io_uring_prep_readv(sqe, fd, header.vectors.asInstanceOf[Ptr[iovec]], count.toUInt, Results.currentPosition)
  }

  protected def finish(res: Int): Boolean =
    if Results.transient(res) then false
    else if res < 0 then {
      staging.end()
      throw Results.failure(s"Failed to read from descriptor $fd", res)
    } else {
      staging.commit(res)
      true
    }

  override def tryAtOnce(): Boolean = NonBlocking.readv(fd, bufs) != 0
  override protected def discard(res: Int): Unit = staging.end()
}

/** Drains `buf` between position and limit. Done once at least one byte has been written, or, with nothing remaining,
  * at once, with no system call.
  */
final class WriteFromBuffer(val fd: Int, buf: ByteBuffer) extends Command {
  def interest: Interest = Interest.Write
  override protected def readinessEvents: Int = Linux.POLLOUT
  override private[uring] def nothingToDo: Boolean = !buf.hasRemaining()
  private val staging = new Staging(Array(buf), copyIn = true, copyOut = false)

  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit = {
    val b = staging.begin()(0)
    if onSocket then {
      io_uring_prep_send(sqe, fd, NativeBuffer.atPosition(b), b.remaining().toUInt, 0)
      pollFirst(sqe)
    } else io_uring_prep_write(sqe, fd, NativeBuffer.atPosition(b), b.remaining().toUInt, Results.currentPosition)
  }

  protected def finish(res: Int): Boolean =
    if Results.transient(res) then false
    else if res < 0 then {
      staging.end()
      throw Results.failure(s"Failed to write to descriptor $fd", res)
    } else {
      staging.commit(res)
      true
    }

  override def tryAtOnce(): Boolean = NonBlocking.write(fd, buf) >= 0
  override protected def discard(res: Int): Unit = staging.end()
}

/** Drains `bufs` in order with one `writev`, as `WriteFromBuffer` drains one (see `Ops.write`). */
final class WriteFromBuffers(val fd: Int, bufs: Array[ByteBuffer]) extends Command {
  def interest: Interest = Interest.Write
  override protected def readinessEvents: Int = Linux.POLLOUT
  override private[uring] def nothingToDo: Boolean = !gears.async.io.internal.Checks.anyRemaining(bufs)
  private val staging = new Staging(bufs, copyIn = true, copyOut = false)
  private val header = new MessageHeader(math.min(bufs.length, NonBlocking.MaxBuffers), named = false)

  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit = {
    val count = header.fill(staging.begin(), room = false)
    if onSocket then {
      header.set(count, 0)
      io_uring_prep_sendmsg(sqe, fd, header.msg, 0)
      pollFirst(sqe)
    } else io_uring_prep_writev(sqe, fd, header.vectors.asInstanceOf[Ptr[iovec]], count.toUInt, Results.currentPosition)
  }

  protected def finish(res: Int): Boolean =
    if Results.transient(res) then false
    else if res < 0 then {
      staging.end()
      throw Results.failure(s"Failed to write to descriptor $fd", res)
    } else {
      staging.commit(res)
      true
    }

  override def tryAtOnce(): Boolean = NonBlocking.writev(fd, bufs) >= 0
  override protected def discard(res: Int): Unit = staging.end()
}

/** Fills a buffer of `pool`'s, writing it into `into` (see `Ops.read`). While more than half the pool is free, the
  * kernel receives straight into a buffer taken as the read is submitted, in one round trip; otherwise, for anything
  * but a regular file, the kernel only waits for the descriptor to be readable, and the buffer is taken then, as
  * kqueue's is, and the bytes read without waiting, so that reads waiting on idle connections hold at most half the
  * pool between them and never exhaust it. A regular file, always readable, is always read straight into a buffer. A
  * buffer taken is given back if the read fails, finds nothing after all, or is withdrawn.
  */
final class PooledRead(val fd: Int, pool: NativePool, into: Slot[Received]) extends Command {
  override private[io] def slotRefusal: Throwable | Null = gears.async.io.internal.Checks.notEmpty(into)
  def interest: Interest = Interest.Read
  private var held: pool.NativeSlot | Null = null
  private var pollNext = false // a read straight into a buffer found nothing: wait for readiness first

  private def take(): pool.NativeSlot = {
    val slot = pool.take()
    if slot == null then throw new IOException("The pool has no buffer free")
    slot.nn
  }

  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit = {
    if held == null && (runsInWorker || (!pollNext && pool.available * 2 > pool.buffers)) then held = pool.take()
    val slot = held
    if slot == null then io_uring_prep_poll_add(sqe, fd, Linux.POLLIN.toUInt)
    else if onSocket then io_uring_prep_recv(sqe, fd, slot.address, slot.buffer.capacity().toUInt, 0)
    else io_uring_prep_read(sqe, fd, slot.address, slot.buffer.capacity().toUInt, Results.currentPosition)
  }

  protected def finish(res: Int): Boolean = {
    val slot = held
    if slot == null then {
      pollNext = false
      if Results.transient(res) then false
      else if res < 0 then throw Results.failure(s"Failed to wait for descriptor $fd", res)
      else tryAtOnce() // readable: false, to wait again, if another reader took the bytes first
    } else if Results.transient(res) && !runsInWorker then {
      held = null
      slot.release()
      pollNext = true
      false
    } else if Results.transient(res) then false
    else {
      held = null
      if res < 0 then {
        slot.release() // a failed read holds nothing
        throw Results.failure(s"Failed to read from descriptor $fd", res)
      }
      received(slot, res)
      true
    }
  }

  override def tryAtOnce(): Boolean = {
    val slot = take()
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

  /** Hands over `slot`, whose buffer the kernel filled with `n` bytes, 0 at the end of the stream. */
  private def received(slot: pool.NativeSlot, n: Int): Unit = {
    val buffer = slot.buffer
    buffer.clear()
    buffer.position(n)
    buffer.flip()
    into.set(slot)
  }

  override protected def discard(res: Int): Unit = {
    val slot = held
    held = null
    if slot != null then slot.release()
  }
}

/** Accepts one connection on a listening socket, writing its handle, non-blocking and close-on-exec, into `into`. Done
  * once a connection has been accepted; one aborted before it was is skipped. The same command may be submitted again,
  * after `into` is cleared, to accept the next connection. A connection accepted for a submission withdrawn meanwhile
  * is closed.
  */
final class Accept(val fd: Int, into: Slot[UringKinds.Connection]) extends Command {
  override private[io] def waitsItsTurn: Boolean = true
  override private[io] def slotRefusal: Throwable | Null = gears.async.io.internal.Checks.notEmpty(into)
  def interest: Interest = Interest.Read
  override protected def readinessEvents: Int = Linux.POLLIN

  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit =
    io_uring_prep_accept(sqe, fd, null, null, Linux.SOCK_NONBLOCK | Linux.SOCK_CLOEXEC)

  protected def finish(res: Int): Boolean =
    if Results.transient(res) || res == -errno.ECONNABORTED then false
    else if res < 0 then throw Results.failure("Failed to accept connection", res)
    else {
      into.set(UringKinds.connection(UringHandles.accepted(fd, res)))
      true
    }

  override def tryAtOnce(): Boolean = {
    val client = Accepts.acceptNow(fd)
    if client >= 0 then into.set(UringKinds.connection(UringHandles.accepted(fd, client)))
    client >= 0
  }

  override protected def discard(res: Int): Unit = if res >= 0 then unistd.close(res): Unit
}

/** Accepts the connections pending on a listening socket, at most `max`, writing their handles into `into` (see
  * `StreamSocketOps.acceptAll`): it waits in the kernel for the socket to be readable, then accepts until none waits.
  * Done once at least one has been accepted.
  */
final class AcceptAll(val fd: Int, into: Slot[Array[UringKinds.Connection]], max: Int) extends Command {
  override private[io] def waitsItsTurn: Boolean = true
  override private[io] def slotRefusal: Throwable | Null = gears.async.io.internal.Checks.notEmpty(into)
  def interest: Interest = Interest.Read

  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit = io_uring_prep_poll_add(sqe, fd, Linux.POLLIN.toUInt)

  protected def finish(res: Int): Boolean =
    if Results.transient(res) then false
    else if res < 0 then throw Results.failure(s"Failed to wait for descriptor $fd", res)
    else tryAtOnce()

  override def tryAtOnce(): Boolean = {
    val clients = gears.async.io.internal.AcceptLoop.acceptAll[UringKinds.Connection](max) {
      val client = Accepts.acceptNow(fd)
      if client < 0 then null else UringKinds.connection(UringHandles.accepted(fd, client))
    }
    if clients != null then into.set(clients)
    clients != null
  }
}

/** Accepting without waiting, for `Accept` and `AcceptAll`. */
private object Accepts {

  /** Accepts a connection on `fd`, non-blocking and close-on-exec: its descriptor, or -1 if none waits. One its client
    * reset while it was queued is skipped, as the JDK's accept skips it.
    */
  def acceptNow(fd: Int): Int = {
    var client = LinuxCalls.accept4(fd, null, null, Linux.SOCK_NONBLOCK | Linux.SOCK_CLOEXEC)
    while client < 0 && errno.errno == errno.ECONNABORTED do
      client = LinuxCalls.accept4(fd, null, null, Linux.SOCK_NONBLOCK | Linux.SOCK_CLOEXEC)
    if client < 0 && !Results.wouldBlock then
      throw new IOException(s"Failed to accept connection: ${gears.async.io.internal.PosixErr.cError()}")
    client
  }
}

/** Receives one packet into `buf`, cleared first and left flipped, and writes its sender into `from`, which holds a
  * Unix address if `unix`, and an IP one otherwise; on a connected socket, whose sender is its peer, `from` is null.
  * Done once a packet has arrived; the bytes beyond `buf`'s capacity are dropped (see `IpDatagramOps.receive`).
  */
final class ReceiveFrom(val fd: Int, buf: ByteBuffer, from: Slot[? <: SocketAddress] | Null, unix: Boolean)
    extends Command {
  gears.async.io.internal.Checks.checkFillable(buf)
  override private[io] def waitsItsTurn: Boolean = true
  override private[io] def slotRefusal: Throwable | Null =
    if from == null then null else gears.async.io.internal.Checks.notEmpty(from.nn)
  def interest: Interest = Interest.Read
  override private[io] def refusal: Throwable | Null =
    if buf.capacity() > 0 then null else gears.async.io.internal.Checks.noCapacity(buf)
  override protected def readinessEvents: Int = Linux.POLLIN
  private val staging = new Staging(Array(buf), copyIn = false, copyOut = true)
  private val header = new MessageHeader(1, named = true)

  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit = {
    buf.clear()
    header.set(header.fill(staging.begin(), room = true), header.capacity)
    io_uring_prep_recvmsg(sqe, fd, header.msg, 0)
  }

  protected def finish(res: Int): Boolean =
    if Results.transient(res) then false
    else if res < 0 then {
      staging.end()
      throw Results.failure("Failed to receive datagram", res)
    } else {
      staging.commit(res)
      buf.flip()
      if from != null then {
        val nameLength = header.msg.msg_namelen
        // An unnamed Unix sender comes with no address at all, not even its family.
        val sender =
          if nameLength.toInt < sizeof[socket.sa_family_t].toInt then gears.async.io.UnixAddress("")
          else UringHandles.decode(header.address, nameLength)
        if sender.isInstanceOf[gears.async.io.UnixAddress] != unix then
          throw new IOException(s"Received from $sender on a socket of the other family")
        from.asInstanceOf[Slot[SocketAddress]].set(sender)
      }
      true
    }

  override protected def discard(res: Int): Unit = staging.end()
}

/** Sends one packet, consuming `bufs`: from a bound socket to `to`, or from a connected one, `to` null, to its peer,
  * without an address. An empty packet is refused on a connected socket (see `IpDatagramOps.send`). Done once it has
  * gone.
  */
final class SendTo(val fd: Int, bufs: Array[ByteBuffer], to: SocketAddress | Null) extends Command {
  override private[io] def waitsItsTurn: Boolean = true
  def this(fd: Int, buf: ByteBuffer, to: SocketAddress | Null) = this(fd, Array(buf), to)
  def interest: Interest = Interest.Write
  override protected def readinessEvents: Int = Linux.POLLOUT

  override private[io] def refusal: Throwable | Null =
    if to == null && !gears.async.io.internal.Checks.anyRemaining(bufs) then
      gears.async.io.internal.Checks.emptyPacket(bufs(0))
    else if bufs.count(_.hasRemaining) > NonBlocking.MaxBuffers
    then // one sendmsg takes no more, and a packet goes whole
      new IllegalArgumentException(
        s"A datagram is sent from at most ${NonBlocking.MaxBuffers} buffers with bytes remaining, not ${bufs.count(_.hasRemaining)}"
      )
    else null

  private val staging = new Staging(bufs, copyIn = true, copyOut = false)
  private val header = new MessageHeader(math.min(bufs.length, NonBlocking.MaxBuffers), named = to != null)
  private var size = 0L

  protected def prepare(sqe: Ptr[io_uring_sqe]): Unit = {
    size = gears.async.io.internal.Checks.remaining(bufs)
    val nameLength =
      if to == null then 0 else UringHandles.encode(UringHandles.targetFor(fd, to), header.address, header.capacity)
    header.set(header.fill(staging.begin(), room = false), nameLength)
    io_uring_prep_sendmsg(sqe, fd, header.msg, 0)
  }

  protected def finish(res: Int): Boolean =
    if Results.transient(res) then false
    else {
      staging.end()
      if res < 0 then throw Results.failure("Failed to send datagram", res)
      if res.toLong != size then throw new IOException(s"Incomplete datagram: sent $res of $size bytes")
      for b <- bufs do b.position(b.limit()) // a datagram goes whole or not at all
      true
    }

  override protected def discard(res: Int): Unit = staging.end()
}

/** What the commands share: reading a completion's result. */
private[uring] object Results {

  /** The offset that makes a read or write use and advance the file position, as `read(2)` and `write(2)` do. */
  val currentPosition: __u64 = (-1L).toULong

  /** Whether a negative result only means "try again". */
  def transient(res: Int): Boolean = res == -errno.EINTR || res == -errno.EAGAIN

  /** Whether the calling thread's last system call would have blocked, or was interrupted. */
  def wouldBlock: Boolean = {
    val err = errno.errno
    err == errno.EAGAIN || err == errno.EWOULDBLOCK || err == errno.EINTR
  }

  /** The failure of a submission whose result is `res`, a negated `errno`. */
  def failure(what: String, res: Int): IOException =
    new IOException(s"$what: ${fromCString(string.strerror(-res))}")
}
