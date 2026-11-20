package gears.io.net

import gears.async.Async

import java.io.EOFException
import java.io.IOException
import gears.async.io.BufferPool

import java.nio.ByteBuffer
import java.util.concurrent.TimeoutException
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

final class BufferedInput(val stream: Stream, initial: Int = 4096, max: Int = 64 * 1024) {
  require(initial > 0 && max >= initial, s"A buffer of $initial bytes, growing to at most $max")

  private var buf = ByteBuffer.allocate(initial)
  private var start = 0
  private var end = 0
  private var atEnd = false

  def buffered: Int = end - start

  def exhausted: Boolean = atEnd && start == end

  def fill(timeout: Duration = Duration.Inf, grow: Boolean = false)(using Async): Boolean =
    if atEnd then false
    else {
      if start == end then {
        start = 0
        end = 0
        if buf.capacity() == 0 then buf = ByteBuffer.allocate(initial)
      } else if end == buf.capacity() then {
        if start > 0 then {
          System.arraycopy(buf.array(), start, buf.array(), 0, end - start)
          end -= start
          start = 0
        } else if grow && buf.capacity() < max then {
          val bigger = ByteBuffer.allocate(math.min(max, buf.capacity() * 2))
          System.arraycopy(buf.array(), 0, bigger.array(), 0, end)
          buf = bigger
        } else return false
      }
      buf.limit(buf.capacity())
      buf.position(end)
      val n = stream.read(buf, timeout)
      if n < 0 then {
        atEnd = true
        false
      } else {
        end += n
        true
      }
    }

  def holding: Int = buf.capacity()

  def release(): Unit =
    if start == end then {
      buf = BufferedInput.Empty
      start = 0
      end = 0
    }

  def awaitMore(idle: Duration, grace: FiniteDuration, pool: BufferPool | Null, released: () => Unit)(using
      Async
  ): Boolean =
    if atEnd then false
    else if pool == null || !stream.pooledReads || (idle match {
        case d: FiniteDuration => d <= grace
        case _                 => false
      })
    then fill(idle, grow = true)
    else {
      val deadline = Stream.limit(idle) match {
        case null => Long.MaxValue
        case d    => System.nanoTime() + d.nn.toNanos
      }
      def left: Duration =
        if deadline == Long.MaxValue then Duration.Inf
        else {
          val nanos = deadline - System.nanoTime()
          if nanos <= 0 then throw new BufferedInput.TimedOut(s"Nothing in $idle")
          Duration.fromNanos(nanos)
        }
      var quiet = false
      val early =
        try fill(grace, grow = true)
        catch
          case _: TimeoutException =>
            quiet = true
            false
      if !quiet then early
      else {
        release()
        released()
        val got =
          try stream.read(pool.nn, left)
          catch case _: IOException if pool.nn.available == 0 => null
        if got == null then fill(left, grow = true)
        else
          try {
            val b = got.buffer
            val n = b.remaining()
            if n == 0 then {
              atEnd = true
              false
            } else {
              buf = ByteBuffer.allocate(math.max(initial, n))
              b.get(buf.array(), 0, n)
              start = 0
              end = n
              true
            }
          } finally got.release()
      }
    }

  def readUntil(
      delimiter: Array[Byte],
      limit: Int,
      timeout: Duration = Duration.Inf
  )(using Async): Array[Byte] | Null = {
    val deadline = Stream.limit(timeout) match {
      case null => Long.MaxValue
      case d    => System.nanoTime() + d.nn.toNanos
    }
    var scanned = start
    var found = -1
    while found < 0 do {
      found = indexOf(delimiter, scanned)
      if found < 0 then {
        if end - start >= limit then throw new BufferedInput.TooLong(limit)
        scanned = math.max(start, end - delimiter.length + 1)
        val shift = start
        val left =
          if deadline == Long.MaxValue then Duration.Inf
          else {
            val nanos = deadline - System.nanoTime()
            if nanos <= 0 then throw new BufferedInput.TimedOut(s"No complete line in $timeout")
            Duration.fromNanos(nanos)
          }
        if !fill(left, grow = true) then {
          if start == end && atEnd then return null
          if atEnd then throw new EOFException("The stream ended in the middle of a line")
          throw new BufferedInput.TooLong(limit)
        }
        scanned -= shift - start
      }
    }
    val until = found + delimiter.length
    if until - start > limit then throw new BufferedInput.TooLong(limit)
    val out = java.util.Arrays.copyOfRange(buf.array(), start, until)
    start = until
    out
  }

  private def indexOf(delimiter: Array[Byte], from: Int): Int = {
    val bytes = buf.array()
    val first = delimiter(0)
    var i = from
    val last = end - delimiter.length
    while i <= last do {
      if bytes(i) == first then {
        var j = 1
        while j < delimiter.length && bytes(i + j) == delimiter(j) do j += 1
        if j == delimiter.length then return i
      }
      i += 1
    }
    -1
  }

  def read(dst: ByteBuffer, max: Int = Int.MaxValue, timeout: Duration = Duration.Inf)(using Async): Int = {
    val want = math.min(max, dst.remaining())
    if want == 0 then 0
    else if start < end then copyTo(dst, want)
    else if atEnd then -1
    else if want >= buf.capacity() && dst.remaining() == want then stream.read(dst, timeout)
    else if fill(timeout) then copyTo(dst, want)
    else -1
  }

  private def copyTo(dst: ByteBuffer, want: Int): Int = {
    val n = math.min(want, end - start)
    dst.put(buf.array(), start, n)
    start += n
    n
  }

  def readExactly(n: Int, timeout: Duration = Duration.Inf)(using Async): Array[Byte] = {
    val out = new Array[Byte](n)
    val into = ByteBuffer.wrap(out)
    while into.hasRemaining do
      if read(into, timeout = timeout) < 0 then
        throw new EOFException(s"The stream ended ${into.remaining()} bytes short")
    out
  }

  def skip(n: Long, timeout: Duration = Duration.Inf)(using Async): Long = {
    var left = n
    while left > 0 && (start < end || fill(timeout)) do {
      val k = math.min(left, (end - start).toLong).toInt
      start += k
      left -= k
    }
    n - left
  }

  def drainBuffered(): Array[Byte] = {
    val out = java.util.Arrays.copyOfRange(buf.array(), start, end)
    start = end
    out
  }
}

object BufferedInput {
  private val Empty = ByteBuffer.allocate(0)

  final class TooLong(val limit: Int) extends IOException(s"No delimiter within $limit bytes") {
    override def fillInStackTrace(): Throwable = this
  }

  private[io] final class TimedOut(message: String) extends java.util.concurrent.TimeoutException(message) {
    override def fillInStackTrace(): Throwable = this
  }
}
