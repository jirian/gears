// From the old shared runtime (gears.async.io.internal on async-io, 1d7add2), now the io_uring backend's.
package gears.async.io
package uring

import gears.async.io.BufferPool
import gears.async.io.Received

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import scala.scalanative.memory.PointerBuffer
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

/** A `BufferPool` of `buffers` buffers of `bufferSize` bytes in C memory: one allocation for all of them, in a zone of
  * the pool's own that its close frees, sliced into a `ByteBuffer` per buffer, whose address a read hands the kernel to
  * receive straight into. Each buffer is a `Received` made once and reused, and the free list takes no lock, since a
  * `Received` may be released from any thread: a stack of buffer indexes whose head is a compare-and-set word, the
  * index tagged with a count of changes so that a buffer released and taken again meanwhile cannot be mistaken for the
  * one seen.
  *
  * A read that finds no buffer free parks here (`park`), and every release starts the reads parked again, on their
  * workers: all of them, since the one a buffer would go to may find nothing to read, and wait for its descriptor
  * holding none, while another has bytes waiting.
  */
private[uring] final class NativePool(val bufferSize: Int, val buffers: Int) extends BufferPool {
  require(bufferSize > 0 && buffers > 0, "A pool needs buffers of some size")

  private val zone = Zone.open()
  private val memory: Ptr[Byte] = alloc[Byte]((bufferSize.toLong * buffers).toCSize)(using zone)

  /** One of the pool's buffers, with its address, as the `Received` of a read that filled it. */
  final class PoolBuffer(val index: Int, val address: Ptr[Byte]) extends Received {
    val buffer: ByteBuffer = PointerBuffer.wrap(address, bufferSize)

    /** The index of the buffer below this one on the free stack, while it is on it. */
    @volatile private[NativePool] var next = -1

    /** Whether a read or a `Received` holds this buffer: set by `take`, cleared by `release`, which refuses a second.
      */
    private[NativePool] val held = new AtomicBoolean(false)

    def release(): Unit = NativePool.this.release(this)
  }

  private val all = Array.tabulate(buffers)(i => new PoolBuffer(i, memory + i.toLong * bufferSize))

  // The free stack: `head` holds the top buffer's index, -1 for none, in its low 32 bits and a change count above.
  private val head = new AtomicLong(NativePool.pack(-1, 0))
  private val free = new AtomicInteger(buffers)
  @volatile private var closed = false

  (buffers - 1 to 0 by -1).foreach(i => push(all(i))) // the first on top

  def available: Int = free.get()

  // Reads parked, and how many: a release looks at the count, and takes the lock only if there are some. A park counts
  // itself before it looks at `free`, and a release adds to `free` before it looks at the count, so one of them sees
  // the other.
  private val parked = new java.util.ArrayDeque[Command]()
  private val parkedCount = new AtomicInteger()

  /** Parks `read` until a buffer is released: false if one is free now, and it is not parked. Its worker's thread. */
  private[uring] def park(read: Command): Boolean = synchronized {
    parkedCount.incrementAndGet()
    if free.get() > 0 then {
      parkedCount.decrementAndGet()
      false
    } else {
      parked.addLast(read)
      true
    }
  }

  /** Takes `read` out, if it is parked here: false if a release has started it again already. Any thread. */
  private[uring] def leave(read: Command): Boolean = synchronized {
    parked.remove(read) && {
      parkedCount.decrementAndGet()
      true
    }
  }

  /** Starts every read parked here again, on its worker. */
  private def unpark(): Unit =
    if parkedCount.get() > 0 then {
      val reads = synchronized {
        val all = parked.toArray(new Array[Command](0))
        parked.clear()
        parkedCount.addAndGet(-all.length)
        all
      }
      // A worker that has stopped has no read to start: the runtime is ending.
      for read <- reads do
        try read.owner.nn.startTurn(read)
        catch case _: IllegalStateException => ()
    }

  /** Takes a free buffer, cleared, or null if none is. */
  def take(): PoolBuffer | Null = {
    if closed then throw new IllegalStateException("The pool is closed")
    var taken: PoolBuffer | Null = null
    var done = false
    while !done do {
      val seen = head.get()
      val index = NativePool.index(seen)
      if index < 0 then done = true
      else {
        val buffer = all(index)
        if head.compareAndSet(seen, NativePool.pack(buffer.next, NativePool.tag(seen) + 1)) then {
          taken = buffer
          done = true
        }
      }
    }
    if taken != null then {
      taken.held.set(true)
      free.decrementAndGet()
      taken.buffer.clear()
    }
    taken
  }

  private def release(buffer: PoolBuffer): Unit = {
    // A second release would push the buffer again, making a cycle of the free stack, so it fails instead.
    if !buffer.held.compareAndSet(true, false) then
      throw new IllegalStateException(s"Buffer ${buffer.index} of the pool was released already")
    push(buffer)
    free.incrementAndGet()
    unpark()
  }

  private def push(buffer: PoolBuffer): Unit = {
    var done = false
    while !done do {
      val seen = head.get()
      buffer.next = NativePool.index(seen)
      done = head.compareAndSet(seen, NativePool.pack(buffer.index, NativePool.tag(seen) + 1))
    }
  }

  def close(): Unit = synchronized {
    if !closed then {
      // Checked first, so that a pool closed too early stays open, for its buffers to be released and the close done.
      require(free.get() == buffers, s"${buffers - free.get()} of the pool's buffers are still held")
      closed = true
      zone.close()
    }
  }
}

private object NativePool {
  private inline def pack(index: Int, tag: Int): Long = (tag.toLong << 32) | (index.toLong & 0xffffffffL)
  private inline def index(word: Long): Int = word.toInt
  private inline def tag(word: Long): Int = (word >>> 32).toInt
}
