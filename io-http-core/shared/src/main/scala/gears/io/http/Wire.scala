package gears.io.http

import gears.async.Async
import gears.io.net.BufferedInput
import gears.io.net.Stream

import java.io.EOFException
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

private[http] object Wire {
  val Crlf: Array[Byte] = "\r\n".getBytes(StandardCharsets.ISO_8859_1)
  val HeadEnd: Array[Byte] = "\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1)
  val LastChunk: Array[Byte] = "0\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1)
  val Continue: Array[Byte] = "HTTP/1.1 100 Continue\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1)

  enum Framing {
    case NoBody
    case Length(bytes: Long)
    case Chunked
    case UntilClose
  }

  final class Malformed(val status: Status, message: String) extends IOException(message) {
    override def fillInStackTrace(): Throwable = this
  }

  final case class RequestHead(method: Method, target: String, version: HttpVersion, headers: Headers)
  final case class ResponseHead(version: HttpVersion, status: Status, headers: Headers)

  val IdleGrace: FiniteDuration = 1.second

  def readRequestHead(
      input: BufferedInput,
      limits: Limits,
      idle: Duration,
      timeout: Duration,
      begun: () => Unit = () => (),
      pool: gears.async.io.BufferPool | Null = null,
      released: () => Unit = () => ()
  )(using Async): RequestHead | Null = {
    if input.buffered == 0 then {
      val more =
        try input.awaitMore(idle, IdleGrace, pool, released)
        catch case _: java.util.concurrent.TimeoutException => false
      if !more then return null
      begun()
    }
    val bytes =
      try input.readUntil(HeadEnd, limits.head, timeout)
      catch {
        case _: BufferedInput.TooLong =>
          throw new Malformed(Status.RequestHeaderFieldsTooLarge, "The head is too large")
        case _: EOFException => throw new Malformed(Status.BadRequest, "The stream ended within the head")
      }
    if bytes == null then return null
    val b = bytes.nn
    val end = b.length - 4
    val firstEnd = lineEnd(b, 0, end)
    val requestLine = latin1(b, 0, firstEnd)
    val first = requestLine.indexOf(' ')
    val second = requestLine.indexOf(' ', first + 1)
    if first <= 0 || second < 0 || requestLine.indexOf(' ', second + 1) >= 0 then
      throw new Malformed(Status.BadRequest, "A malformed request line")
    val methodToken = requestLine.substring(0, first)
    val target = requestLine.substring(first + 1, second)
    val version = requestLine.substring(second + 1)
    if !Headers.isToken(methodToken) then throw new Malformed(Status.BadRequest, "A malformed method")
    val spoken = checkVersion(version)
    var i = 0
    while i < target.length && { val c = target.charAt(i); c > ' ' && c < 127 } do i += 1
    if target.isEmpty || i < target.length then throw new Malformed(Status.BadRequest, "A malformed request target")
    if target.length > limits.head then throw new Malformed(Status.UriTooLong, "The target is too long")
    val headers = parseFields(b, firstEnd + 2, end, limits.headers)
    RequestHead(Method.parse(methodToken), target, spoken, headers)
  }

  def readResponseHead(input: BufferedInput, limit: Int, timeout: Duration)(using Async): ResponseHead = {
    var head: ResponseHead | Null = null
    while head == null do {
      val bytes =
        try input.readUntil(HeadEnd, limit, timeout)
        catch {
          case _: BufferedInput.TooLong => throw new IOException(s"A response head larger than $limit bytes")
          case _: EOFException          => throw new EOFException("The connection ended within a response head")
        }
      if bytes == null then throw new EOFException("The connection ended before a response")
      val b = bytes.nn
      val end = b.length - 4
      val firstEnd = lineEnd(b, 0, end)
      val statusLine = latin1(b, 0, firstEnd)
      val first = statusLine.indexOf(' ')
      if first < 0 then throw new IOException(s"A malformed status line: $statusLine")
      val version = checkVersion(statusLine.substring(0, first))
      val rest = statusLine.substring(first + 1)
      val codeText = if rest.length >= 3 then rest.substring(0, 3) else rest
      if codeText.length != 3 || !codeText.forall(_.isDigit) || (rest.length > 3 && rest.charAt(3) != ' ') then
        throw new IOException(s"A malformed status line: $statusLine")
      val code = codeText.toInt
      if code < 100 || code > 599 then throw new IOException(s"A status out of range: $code")
      val headers = parseFields(b, firstEnd + 2, end, 1000)
      if code >= 200 || code == 101 then head = ResponseHead(version, Status(code), headers)
    }
    head.nn
  }

  private def checkVersion(version: String): HttpVersion =
    if !(version.length == 8 && version.startsWith("HTTP/") && version.charAt(5).isDigit && version.charAt(6) == '.' &&
        version.charAt(7).isDigit)
    then throw new Malformed(Status.BadRequest, "A malformed HTTP version")
    else if version == "HTTP/1.1" then HttpVersion.Http11
    else if version == "HTTP/1.0" then HttpVersion.Http10
    else throw new Malformed(Status.HttpVersionNotSupported, s"$version is not supported")

  private def lineEnd(bytes: Array[Byte], from: Int, end: Int): Int = {
    var i = from
    while i < end do {
      val c = bytes(i)
      if c == '\r' then {
        if i + 1 < end && bytes(i + 1) == '\n' then return i
        throw new Malformed(Status.BadRequest, "A bare CR or LF in the head")
      }
      if c == '\n' then throw new Malformed(Status.BadRequest, "A bare CR or LF in the head")
      i += 1
    }
    end
  }

  private[http] def chunkSize(line: Array[Byte], end: Int): Long = {
    def malformed = new Malformed(Status.BadRequest, "A malformed chunk line")
    def spaces(from: Int): Int = {
      var i = from
      while i < end && (line(i) == ' ' || line(i) == '\t') do i += 1
      i
    }
    def token(from: Int): Int = {
      var i = from
      while i < end && Text.isTokenChar((line(i) & 0xff).toChar) do i += 1
      if i == from then throw malformed
      i
    }
    def quoted(from: Int): Int = {
      var i = from
      while i < end && line(i) != '"' do {
        val c = line(i) & 0xff
        if c == '\\' then {
          i += 1
          val q = if i < end then line(i) & 0xff else -1
          if !(q == '\t' || (q >= ' ' && q != 0x7f)) then throw malformed
        } else if !(c == '\t' || c == ' ' || c == 0x21 || (c >= 0x23 && c <= 0x5b) || (c >= 0x5d && c != 0x7f)) then
          throw malformed
        i += 1
      }
      if i == end then throw malformed
      i + 1
    }
    def hex(c: Int): Int =
      if c >= '0' && c <= '9' then c - '0'
      else if c >= 'a' && c <= 'f' then c - 'a' + 10
      else if c >= 'A' && c <= 'F' then c - 'A' + 10
      else -1
    var size = 0L
    var i = 0
    while i < end && hex(line(i) & 0xff) >= 0 do {
      if i == 15 then throw new Malformed(Status.BadRequest, "A chunk too large")
      size = size * 16 + hex(line(i) & 0xff)
      i += 1
    }
    if i == 0 then throw malformed
    while i < end do {
      i = spaces(i)
      if i == end || line(i) != ';' then throw malformed
      i = token(spaces(i + 1))
      val eq = spaces(i)
      if eq < end && line(eq) == '=' then {
        val value = spaces(eq + 1)
        i = if value < end && line(value) == '"' then quoted(value + 1) else token(value)
      }
    }
    size
  }

  private def latin1(bytes: Array[Byte], from: Int, to: Int): String = {
    val chars = new Array[Char](to - from)
    var i = 0
    while i < chars.length do {
      chars(i) = (bytes(from + i) & 0xff).toChar
      i += 1
    }
    new String(chars)
  }

  private def parseFields(bytes: Array[Byte], from: Int, end: Int, max: Int): Headers = {
    var fields = new Array[String](16)
    var n = 0
    var at = from
    while at <= end do {
      if n == 2 * max then throw new Malformed(Status.RequestHeaderFieldsTooLarge, s"More than $max header fields")
      val lineTo = lineEnd(bytes, at, end)
      if lineTo > at && (bytes(at) == ' ' || bytes(at) == '\t') then
        throw new Malformed(Status.BadRequest, "A folded header line")
      var colon = at
      while colon < lineTo && bytes(colon) != ':' do colon += 1
      if colon == at || colon == lineTo then throw new Malformed(Status.BadRequest, "A malformed header line")
      var i = at
      while i < colon && Text.isTokenChar((bytes(i) & 0xff).toChar) do i += 1
      val name = latin1(bytes, at, colon)
      if i < colon then throw new Malformed(Status.BadRequest, s"A malformed header name: $name")
      var vs = colon + 1
      var ve = lineTo
      while vs < ve && (bytes(vs) & 0xff) <= ' ' do vs += 1
      while ve > vs && (bytes(ve - 1) & 0xff) <= ' ' do ve -= 1
      i = vs
      while i < ve && { val c = bytes(i) & 0xff; c != 0 && c != 127 && (c >= ' ' || c == '\t') } do i += 1
      if i < ve then throw new Malformed(Status.BadRequest, s"A control character in header $name")
      if n == fields.length then fields = java.util.Arrays.copyOf(fields, 2 * n)
      fields(n) = name
      fields(n + 1) = latin1(bytes, vs, ve)
      n += 2
      at = lineTo + 2
    }
    Headers.trusted(if n == fields.length then fields else java.util.Arrays.copyOf(fields, n))
  }

  def framing(headers: Headers, orElse: Framing): Framing = {
    val te = headers.tokens("Transfer-Encoding")
    val lengths = headers.getAll("Content-Length").flatMap(Text.split(_, ',').map(_.trim)).distinct
    if te.isEmpty && headers.contains("Transfer-Encoding") then
      throw new Malformed(Status.BadRequest, "An empty Transfer-Encoding")
    if te.nonEmpty then {
      if lengths.nonEmpty then throw new Malformed(Status.BadRequest, "Both Transfer-Encoding and Content-Length")
      if !te.last.equalsIgnoreCase("chunked") || te.count(_.equalsIgnoreCase("chunked")) != 1 then
        throw new Malformed(Status.NotImplemented, s"Transfer-Encoding ${te.mkString(", ")} is not supported")
      if te.length != 1 then throw new Malformed(Status.NotImplemented, s"Transfer-Encoding ${te.mkString(", ")}")
      Framing.Chunked
    } else if lengths.nonEmpty then {
      if lengths.length != 1 then throw new Malformed(Status.BadRequest, "Content-Lengths that disagree")
      val text = lengths.head
      if text.isEmpty || text.length > 18 || !text.forall(c => c >= '0' && c <= '9') then
        throw new Malformed(Status.BadRequest, s"A malformed Content-Length: $text")
      val n = text.toLong
      if n == 0 then Framing.NoBody else Framing.Length(n)
    } else orElse
  }

  final class Source(
      input: BufferedInput,
      framing: Framing,
      val limit: Long,
      timeout: Duration,
      onFirstRead: () => Async ?=> Unit,
      tooLarge: Long => Throwable
  ) {
    private var started = false
    private var left: Long = framing match {
      case Framing.Length(n) => n
      case Framing.NoBody    => 0L
      case _                 => -1L
    }
    private var chunkLeft = 0L
    @volatile private var done = framing == Framing.NoBody
    private var total = 0L

    def complete: Boolean = done

    def failure: Throwable | Null = failed
    private var failed: Throwable | Null = null

    def read(buf: ByteBuffer)(using Async): Int =
      try readBody(buf)
      catch {
        case e: java.util.concurrent.CancellationException => throw e
        case e: Throwable                                  =>
          failed = e
          throw e
      }

    private def readBody(buf: ByteBuffer)(using Async): Int =
      if done then -1
      else {
        if !started then {
          started = true
          onFirstRead()
        }
        val n = framing match {
          case Framing.Length(_) =>
            val got = input.read(buf, math.min(left, Int.MaxValue.toLong).toInt, timeout)
            if got < 0 then throw new EOFException(s"The body ended $left bytes short")
            left -= got
            if left == 0 then done = true
            got
          case Framing.Chunked =>
            if chunkLeft == 0 then nextChunk()
            if done then -1
            else {
              val got = input.read(buf, math.min(chunkLeft, Int.MaxValue.toLong).toInt, timeout)
              if got < 0 then throw new EOFException("The body ended within a chunk")
              chunkLeft -= got
              if chunkLeft == 0 then expectCrlf()
              got
            }
          case Framing.UntilClose =>
            val got = input.read(buf, Int.MaxValue, timeout)
            if got < 0 then {
              if input.stream.secure && !closeNotified then
                throw new IOException(
                  "The connection ended without TLS's close notification: the body may be cut short"
                )
              done = true
            }
            got
          case Framing.NoBody => -1
        }
        if n > 0 then {
          total += n
          if total > limit then throw tooLarge(limit)
        }
        n
      }

    private def closeNotified: Boolean = input.stream match {
      case tls: gears.io.tls.TlsStream => tls.closeNotified
      case _                           => true
    }

    private def nextChunk()(using Async): Unit = {
      val line = input.readUntil(Crlf, 1024, timeout)
      if line == null then throw new EOFException("The body ended before its last chunk")
      chunkLeft = chunkSize(line.nn, line.nn.length - 2)
      if chunkLeft == 0 then {
        var trailers = 0
        var line2 = input.readUntil(Crlf, 8192, timeout)
        while line2 != null && line2.nn.length > 2 do {
          trailers += 1
          if trailers > 100 then throw new Malformed(Status.BadRequest, "Too many trailer fields")
          parseFields(line2.nn, 0, line2.nn.length - 2, 1): Unit
          line2 = input.readUntil(Crlf, 8192, timeout)
        }
        if line2 == null then throw new EOFException("The body ended within its trailers")
        done = true
      }
    }

    private def expectCrlf()(using Async): Unit = {
      val crlf = input.readExactly(2, timeout)
      if crlf(0) != '\r' || crlf(1) != '\n' then throw new Malformed(Status.BadRequest, "A chunk not ended by CRLF")
    }

    def drain(max: Long)(using Async): Boolean = {
      val scratch = ByteBuffer.allocate(8192)
      var dropped = 0L
      while !done && dropped <= max do {
        scratch.clear()
        val n = read(scratch)
        if n > 0 then dropped += n
      }
      done
    }
  }

  def head(firstLine: String, headers: Headers): ByteBuffer = {
    val out = new OutBuffer(firstLine.length + 32 * headers.size + 4)
    out.putHead(firstLine, headers)
    out.take().nn
  }

  final class OutBuffer(initial: Int) {
    private var bytes = new Array[Byte](initial)
    private var view = ByteBuffer.wrap(bytes)
    private var count = 0

    def size: Int = count
    def capacity: Int = bytes.length
    def isEmpty: Boolean = count == 0

    def reset(): Unit = count = 0

    def release(): Unit =
      if count == 0 && bytes.length > 0 then {
        bytes = new Array[Byte](0)
        view = ByteBuffer.wrap(bytes)
      }

    private def room(n: Int): Unit =
      if count + n > bytes.length then {
        bytes = java.util.Arrays.copyOf(bytes, math.max(math.max(2 * bytes.length, initial), count + n))
        view = ByteBuffer.wrap(bytes)
      }

    def put(from: Array[Byte], offset: Int, length: Int): Unit = {
      room(length)
      System.arraycopy(from, offset, bytes, count, length)
      count += length
    }

    def put(from: Array[Byte]): Unit = put(from, 0, from.length)

    def putLatin1(text: String): Unit = {
      room(text.length)
      var i = 0
      while i < text.length do {
        bytes(count + i) = text.charAt(i).toByte
        i += 1
      }
      count += text.length
    }

    def putCrlf(): Unit = {
      room(2)
      bytes(count) = '\r'
      bytes(count + 1) = '\n'
      count += 2
    }

    def putDecimal(n: Long): Unit =
      if n < 0 then putLatin1(java.lang.Long.toString(n))
      else {
        var digits = 1
        var rest = n / 10
        while rest > 0 do {
          digits += 1
          rest /= 10
        }
        room(digits)
        var v = n
        var i = count + digits - 1
        while i >= count do {
          bytes(i) = ('0' + (v % 10)).toByte
          v /= 10
          i -= 1
        }
        count += digits
      }

    def putHex(n: Int): Unit = {
      var digits = 1
      while digits < 8 && (n >>> (4 * digits)) != 0 do digits += 1
      room(digits)
      var i = digits - 1
      while i >= 0 do {
        val d = (n >>> (4 * (digits - 1 - i))) & 0xf
        bytes(count + i) = (if d < 10 then '0' + d else 'a' + d - 10).toByte
        i -= 1
      }
      count += digits
    }

    def putField(name: String, value: String): Unit = {
      putLatin1(name)
      room(2)
      bytes(count) = ':'
      bytes(count + 1) = ' '
      count += 2
      putLatin1(value)
      putCrlf()
    }

    def putHead(firstLine: String, headers: Headers): Unit = {
      var i = 0
      while i < firstLine.length do {
        val c = firstLine.charAt(i)
        if c > '\u00ff' || c == '\r' || c == '\n' || c == '\u0000' then
          throw new IllegalArgumentException(
            s"A character that may not go out in a message head: U+${c.toInt.toHexString}"
          )
        i += 1
      }
      putLatin1(firstLine)
      putCrlf()
      var f = 0
      while f < headers.size do {
        putField(headers.name(f), headers.value(f))
        f += 1
      }
      putCrlf()
    }

    def writeTo(stream: Stream, timeout: Duration)(using Async): Unit = {
      val v = view
      v.limit(count)
      v.position(0)
      count = 0
      stream.write(v, timeout)
    }

    def take(): ByteBuffer | Null =
      if count == 0 then null
      else {
        val v = view
        v.limit(count)
        v.position(0)
        count = 0
        v
      }
  }

  final class Out(
      stream: Stream,
      out: OutBuffer,
      chunked: Boolean,
      declared: Long,
      suppress: Boolean,
      timeout: Duration
  ) extends BodyWriter {
    private var written = 0L
    private var headOut = false

    def bytesWritten: Long = written

    def committed: Boolean = headOut

    private var dataArray: Array[Byte] | Null = null
    private var data: ByteBuffer | Null = null
    private var crlf: ByteBuffer | Null = null
    private var vector: Array[ByteBuffer] | Null = null

    private def dataOf(bytes: Array[Byte], offset: Int, length: Int): ByteBuffer = {
      if !(dataArray eq bytes) then {
        dataArray = bytes
        data = ByteBuffer.wrap(bytes)
      }
      val d = data.nn
      d.clear()
      d.position(offset)
      d.limit(offset + length)
      d
    }

    // - an op keeps the array it was built with: replace the array, never change it
    private def vectorOf(first: ByteBuffer, second: ByteBuffer, third: ByteBuffer | Null): Array[ByteBuffer] = {
      val v = vector
      val size = if third == null then 2 else 3
      if v != null && v.length == size && (v(0) eq first) && (v(1) eq second) && (third == null || (v(2) eq third))
      then v
      else {
        val made = if third == null then Array(first, second) else Array(first, second, third.nn)
        vector = made
        made
      }
    }

    def write(bytes: Array[Byte], offset: Int, length: Int)(using Async): Unit =
      if suppress || length == 0 then flushHead()
      else {
        if declared >= 0 && written + length > declared then
          throw new IllegalStateException(s"A body longer than its Content-Length of $declared")
        written += length
        headOut = true
        if chunked then {
          out.putHex(length)
          out.putCrlf()
        }
        if out.size + length + 2 <= out.capacity then {
          out.put(bytes, offset, length)
          if chunked then out.putCrlf()
          out.writeTo(stream, timeout)
        } else {
          val data = dataOf(bytes, offset, length)
          val pending = out.take()
          if chunked then {
            if crlf == null then crlf = ByteBuffer.wrap(Crlf)
            crlf.nn.clear()
            stream.write(vectorOf(pending.nn, data, crlf), timeout)
          } else if pending == null then stream.write(data, timeout)
          else stream.write(vectorOf(pending.nn, data, null), timeout)
        }
      }

    private def flushHead()(using Async): Unit =
      if !out.isEmpty then {
        headOut = true
        out.writeTo(stream, timeout)
      }

    def finish()(using Async): Unit = {
      if chunked && !suppress then out.put(LastChunk)
      flushHead()
    }
  }

  def date(): String = {
    val now = System.currentTimeMillis() / 1000
    val cached = dateCache
    if cached != null && cached.nn._1 == now then cached.nn._2
    else {
      val text = formatDate(now)
      dateCache = (now, text)
      text
    }
  }

  @volatile private var dateCache: (Long, String) | Null = null

  private val days = Array("Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed")
  private val months = Array("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")

  private[http] def formatDate(seconds: Long): String = {
    val dayNumber = Math.floorDiv(seconds, 86400L)
    val secondOfDay = Math.floorMod(seconds, 86400L)
    val z = dayNumber + 719468
    val era = Math.floorDiv(z, 146097L)
    val doe = z - era * 146097
    val yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365
    val doy = doe - (365 * yoe + yoe / 4 - yoe / 100)
    val mp = (5 * doy + 2) / 153
    val day = doy - (153 * mp + 2) / 5 + 1
    val month = if mp < 10 then mp + 3 else mp - 9
    val year = yoe + era * 400 + (if month <= 2 then 1 else 0)
    val weekday = days(Math.floorMod(dayNumber, 7L).toInt)
    f"$weekday, $day%02d ${months((month - 1).toInt)} $year%04d ${secondOfDay / 3600}%02d:${secondOfDay / 60 % 60}%02d:${secondOfDay % 60}%02d GMT"
  }
}
