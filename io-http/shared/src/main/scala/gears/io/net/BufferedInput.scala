package gears.io.net

import gears.async.Async

import java.io.EOFException
import java.io.IOException
import java.nio.ByteBuffer
import scala.concurrent.duration.Duration

/** Reads `stream` through a buffer of its own, for parsing: lines and delimited heads are found in place, and bytes
  * copied out in bulk. The buffer starts at `initial` bytes and grows, up to `max`, only while a delimited read needs
  * it to (`readUntil`). Not for use from two futures at once.
  */
final class BufferedInput(val stream: Stream, initial: Int = 4096, max: Int = 64 * 1024) {
  require(initial > 0 && max >= initial, s"A buffer of $initial bytes, growing to at most $max")

  private var buf = ByteBuffer.allocate(initial)
  private var start = 0 // the first byte not yet consumed
  private var end = 0 // after the last byte read
  private var atEnd = false

  /** The bytes read and not yet consumed. */
  def buffered: Int = end - start

  /** Whether the stream has ended and every byte has been consumed. */
  def exhausted: Boolean = atEnd && start == end

  /** Reads more from the stream, into room made by moving the unconsumed bytes to the front, or by growing the buffer
    * if there is none and `grow`: false at the end of the stream, or if there is no room.
    */
  def fill(timeout: Duration = Duration.Inf, grow: Boolean = false)(using Async): Boolean =
    if atEnd then false
    else {
      if start == end then {
        start = 0
        end = 0
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

  /** Consumes the bytes up to `delimiter` and returns them with it, reading more as needed: null if the stream ends
    * first with nothing buffered; an `EOFException` if it ends in the middle; `BufferedInput.TooLong` if `limit` bytes
    * come without it. Each read is given what is left of `timeout`, measured from the call, so that a peer that sends a
    * byte at a time cannot keep it going.
    */
  def readUntil(
      delimiter: Array[Byte],
      limit: Int,
      timeout: Duration = Duration.Inf
  )(using Async): Array[Byte] | Null = {
    val deadline = Stream.limit(timeout) match {
      case null => Long.MaxValue
      case d    => System.nanoTime() + d.nn.toNanos
    }
    var scanned = start // where the search for the delimiter goes on from
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
            if nanos <= 0 then throw new java.util.concurrent.TimeoutException(s"No complete line in $timeout")
            Duration.fromNanos(nanos)
          }
        if !fill(left, grow = true) then {
          if start == end && atEnd then return null
          if atEnd then throw new EOFException("The stream ended in the middle of a line")
          throw new BufferedInput.TooLong(limit)
        }
        scanned -= shift - start // the bytes moved to the front
      }
    }
    val until = found + delimiter.length
    if until - start > limit then throw new BufferedInput.TooLong(limit)
    val out = java.util.Arrays.copyOfRange(buf.array(), start, until)
    start = until
    out
  }

  /** The index in the buffer of the first `delimiter` at or after `from`, or -1. */
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

  /** Reads into `dst`, at most `max` bytes: what is buffered first, else straight from the stream if `dst` has room for
    * a buffer's worth, else through the buffer. How many, or -1 at the end of the stream.
    */
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

  /** Consumes exactly `n` bytes and returns them; an `EOFException` if the stream ends first. */
  def readExactly(n: Int, timeout: Duration = Duration.Inf)(using Async): Array[Byte] = {
    val out = new Array[Byte](n)
    val into = ByteBuffer.wrap(out)
    while into.hasRemaining do
      if read(into, timeout = timeout) < 0 then
        throw new EOFException(s"The stream ended ${into.remaining()} bytes short")
    out
  }

  /** Consumes and drops up to `n` bytes; how many, fewer only at the end of the stream. */
  def skip(n: Long, timeout: Duration = Duration.Inf)(using Async): Long = {
    var left = n
    while left > 0 && (start < end || fill(timeout)) do {
      val k = math.min(left, (end - start).toLong).toInt
      start += k
      left -= k
    }
    n - left
  }

  /** Takes the bytes buffered and not consumed, leaving none: what a protocol switching away from this reader, such as
    * a WebSocket after its upgrade, has already read.
    */
  def drainBuffered(): Array[Byte] = {
    val out = java.util.Arrays.copyOfRange(buf.array(), start, end)
    start = end
    out
  }
}

object BufferedInput {

  /** A delimited read that found no delimiter within its limit. */
  final class TooLong(val limit: Int) extends IOException(s"No delimiter within $limit bytes")
}
