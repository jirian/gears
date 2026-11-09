package gears.io.http

import gears.async.Async
import gears.io.net.BufferedInput
import gears.io.net.Stream

import java.io.EOFException
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.concurrent.duration.Duration

/** HTTP/1.1's syntax (RFC 9112), as the server and the client read and write it. Parsing is strict where leniency is
  * what request smuggling exploits: whitespace before a header's colon, folded lines, a `Transfer-Encoding` beside a
  * `Content-Length`, or two lengths, are refused rather than guessed at.
  */
private[http] object Wire {
  val Crlf: Array[Byte] = "\r\n".getBytes(StandardCharsets.ISO_8859_1)
  val HeadEnd: Array[Byte] = "\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1)
  val LastChunk: Array[Byte] = "0\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1)
  val Continue: Array[Byte] = "HTTP/1.1 100 Continue\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1)

  /** How a message's body is delimited (RFC 9112, 6.3). */
  enum Framing {
    case NoBody
    case Length(bytes: Long)
    case Chunked
    case UntilClose
  }

  /** A malformed message: what a server answers with `status`, and a client reports as an `IOException`. Any peer can
    * send one, so it has no stack trace, which costs as much as serving a request on Scala Native.
    */
  final class Malformed(val status: Status, message: String) extends IOException(message) {
    override def fillInStackTrace(): Throwable = this
  }

  final case class RequestHead(method: Method, target: String, version: String, headers: Headers)
  final case class ResponseHead(version: String, status: Status, headers: Headers)

  /** Reads a request's head, within `timeout` from its first byte; null if the stream ended, or `idle` passed, before
    * any byte came.
    */
  def readRequestHead(
      input: BufferedInput,
      limits: Limits,
      idle: Duration,
      timeout: Duration,
      begun: () => Unit = () => ()
  )(using Async): RequestHead | Null = {
    if input.buffered == 0 then {
      val more =
        try input.fill(idle, grow = true)
        catch case _: java.util.concurrent.TimeoutException => false
      if !more then return null
      begun() // the head's first bytes are in: the connection is no longer idle
    }
    val bytes =
      try input.readUntil(HeadEnd, limits.head, timeout)
      catch {
        case _: BufferedInput.TooLong =>
          throw new Malformed(Status.RequestHeaderFieldsTooLarge, "The head is too large")
        case _: EOFException => throw new Malformed(Status.BadRequest, "The stream ended within the head")
      }
    if bytes == null then return null
    val lines = splitLines(bytes.nn)
    val requestLine = lines.head
    val first = requestLine.indexOf(' ')
    val second = requestLine.indexOf(' ', first + 1)
    if first <= 0 || second < 0 || requestLine.indexOf(' ', second + 1) >= 0 then
      throw new Malformed(Status.BadRequest, "A malformed request line")
    val methodToken = requestLine.substring(0, first)
    val target = requestLine.substring(first + 1, second)
    val version = requestLine.substring(second + 1)
    if !Headers.isToken(methodToken) then throw new Malformed(Status.BadRequest, "A malformed method")
    checkVersion(version)
    if target.isEmpty || target.exists(c => c <= ' ' || c >= 127) then
      throw new Malformed(Status.BadRequest, "A malformed request target")
    if target.length > limits.head then throw new Malformed(Status.UriTooLong, "The target is too long")
    val headers = parseFields(lines.tail, limits.headers)
    RequestHead(Method.parse(methodToken), target, version, headers)
  }

  /** Reads a response's head, skipping interim (1xx) ones but `101 Switching Protocols`, each read within `timeout`
    * from the call.
    */
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
      val lines = splitLines(bytes.nn)
      val statusLine = lines.head
      val first = statusLine.indexOf(' ')
      if first < 0 then throw new IOException(s"A malformed status line: $statusLine")
      val version = statusLine.substring(0, first)
      checkVersion(version)
      val rest = statusLine.substring(first + 1)
      val codeText = if rest.length >= 3 then rest.substring(0, 3) else rest
      if codeText.length != 3 || !codeText.forall(_.isDigit) || (rest.length > 3 && rest.charAt(3) != ' ') then
        throw new IOException(s"A malformed status line: $statusLine")
      val code = codeText.toInt
      if code < 100 || code > 599 then throw new IOException(s"A status out of range: $code")
      val headers = parseFields(lines.tail, 1000)
      if code >= 200 || code == 101 then head = ResponseHead(version, Status(code), headers)
    }
    head.nn
  }

  private def checkVersion(version: String): Unit =
    if !(version.length == 8 && version.startsWith("HTTP/") && version.charAt(5).isDigit && version.charAt(6) == '.' &&
        version.charAt(7).isDigit)
    then throw new Malformed(Status.BadRequest, "A malformed HTTP version")
    else if version != "HTTP/1.1" && version != "HTTP/1.0" then
      throw new Malformed(Status.HttpVersionNotSupported, s"$version is not supported")

  /** A head's lines, without the final empty ones; a bare LF is refused, as is a bare CR. */
  private def splitLines(bytes: Array[Byte]): List[String] = {
    val text = new String(bytes, 0, bytes.length - 4, StandardCharsets.ISO_8859_1)
    val lines = Text.splitCrlf(text)
    if lines.exists(l => l.indexOf('\n') >= 0 || l.indexOf('\r') >= 0) then
      throw new Malformed(Status.BadRequest, "A bare CR or LF in the head")
    lines
  }

  /** Header fields from their lines: `name: value`, the name a token right before the colon, the value trimmed. */
  private def parseFields(lines: List[String], max: Int): Headers = {
    if lines.length > max then throw new Malformed(Status.RequestHeaderFieldsTooLarge, s"More than $max header fields")
    val fields = Vector.newBuilder[(String, String)]
    for line <- lines do {
      if line.nonEmpty && (line.charAt(0) == ' ' || line.charAt(0) == '\t') then
        throw new Malformed(Status.BadRequest, "A folded header line")
      val colon = line.indexOf(':')
      if colon <= 0 then throw new Malformed(Status.BadRequest, "A malformed header line")
      val name = line.substring(0, colon)
      val value = line.substring(colon + 1).trim
      if !Headers.isToken(name) then throw new Malformed(Status.BadRequest, s"A malformed header name: $name")
      if value.exists(c => c == 0 || c == 127 || (c < ' ' && c != '\t')) then
        throw new Malformed(Status.BadRequest, s"A control character in header $name")
      fields += (name -> value)
    }
    Headers.trusted(fields.result())
  }

  /** How a message with `headers` is framed, given that it may have a body at all: chunked, a length, or `orElse`.
    * Refuses a `Transfer-Encoding` other than `chunked`, an empty one, one beside a `Content-Length`, and lengths that
    * disagree.
    */
  def framing(headers: Headers, orElse: Framing): Framing = {
    val te = headers.tokens("Transfer-Encoding")
    val lengths = headers.getAll("Content-Length").flatMap(Text.split(_, ',').map(_.trim)).distinct
    // An empty one is refused too: a front end that takes any Transfer-Encoding for chunked would frame it otherwise.
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

  /** A body arriving on `input`, framed by `framing`, at most `limit` bytes, each read within `timeout`; `onFirstRead`
    * runs before its first read, for a server to send `100 Continue`. `truncatedAtEnd` says whether a close-delimited
    * body that ends without TLS's close notification is a truncation, an error, rather than its end.
    */
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
    // Volatile: a server's watch for the client going away reads it from another future (`Server.Connection`).
    @volatile private var done = framing == Framing.NoBody
    private var total = 0L

    /** Whether all of it has been read, so that the connection may carry another message. */
    def complete: Boolean = done

    def read(buf: ByteBuffer)(using Async): Int =
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
      val text = new String(line.nn, 0, line.nn.length - 2, StandardCharsets.ISO_8859_1)
      val sizeText = text.takeWhile(c => c != ';').trim
      if sizeText.isEmpty || sizeText.length > 15 || !sizeText.forall(c => Character.digit(c, 16) >= 0) then
        throw new Malformed(Status.BadRequest, s"A malformed chunk size: $sizeText")
      chunkLeft = java.lang.Long.parseLong(sizeText, 16)
      if chunkLeft == 0 then {
        var trailers = 0
        var line2 = input.readUntil(Crlf, 8192, timeout)
        while line2 != null && line2.nn.length > 2 do {
          trailers += 1
          if trailers > 100 then throw new Malformed(Status.BadRequest, "Too many trailer fields")
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

    /** Reads what is left and drops it, at most `max` bytes: whether it reached the end. */
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

  /** The head of a message: its first line and `headers`, as bytes, ISO-8859-1, as they go out. */
  def head(firstLine: String, headers: Headers): ByteBuffer = {
    val b = new java.lang.StringBuilder(firstLine.length + 32 * headers.size + 4)
    b.append(firstLine).append("\r\n")
    val it = headers.iterator
    while it.hasNext do {
      val (k, v) = it.next()
      b.append(k).append(": ").append(v).append("\r\n")
    }
    b.append("\r\n")
    val out = new Array[Byte](b.length)
    var i = 0
    while i < out.length do {
      val c = b.charAt(i)
      // Never truncated: U+010A would go out as LF. Headers are checked as they are built; this guards the first line.
      if c > '\u00ff' || (i < firstLine.length && (c == '\r' || c == '\n' || c == '\u0000')) then
        throw new IllegalArgumentException(
          s"A character that may not go out in a message head: U+${c.toInt.toHexString}"
        )
      out(i) = c.toByte
      i += 1
    }
    ByteBuffer.wrap(out)
  }

  /** Writes a body to `stream`, behind its `head`, which goes out with the first write: chunked, or as is, at most
    * `declared` bytes if that is not negative, or nothing at all if `suppress`, as for a `HEAD`. Each write within
    * `timeout`.
    */
  final class Out(
      stream: Stream,
      private var head: ByteBuffer | Null,
      chunked: Boolean,
      declared: Long,
      suppress: Boolean,
      timeout: Duration
  ) extends BodyWriter {
    private var written = 0L

    /** How many body bytes have been written. */
    def bytesWritten: Long = written

    /** Whether the head has gone out. */
    def committed: Boolean = head == null

    // A vectored write's buffers are an array of their own each time: an op keeps the array it was built with.
    def write(bytes: Array[Byte], offset: Int, length: Int)(using Async): Unit =
      if suppress || length == 0 then flushHead()
      else {
        if declared >= 0 && written + length > declared then
          throw new IllegalStateException(s"A body longer than its Content-Length of $declared")
        written += length
        val data = ByteBuffer.wrap(bytes, offset, length)
        val h = head
        head = null
        if chunked then {
          val size = ByteBuffer.wrap((Integer.toHexString(length) + "\r\n").getBytes(StandardCharsets.ISO_8859_1))
          val end = ByteBuffer.wrap(Crlf)
          stream.write(if h == null then Array(size, data, end) else Array(h, size, data, end), timeout)
        } else if h == null then stream.write(data, timeout)
        else stream.write(Array(h, data), timeout)
      }

    private def flushHead()(using Async): Unit = {
      val h = head
      if h != null then {
        head = null
        stream.write(h, timeout)
      }
    }

    /** Ends the body: the last chunk if chunked, and the head if nothing was written. */
    def finish()(using Async): Unit =
      if chunked && !suppress then {
        val h = head
        head = null
        val last = ByteBuffer.wrap(LastChunk)
        if h == null then stream.write(last, timeout) else stream.write(Array(h, last), timeout)
      } else flushHead()
  }

  /** Today's date as a `Date` header has it (RFC 9110, 5.6.7), formatted once a second. */
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

  /** `seconds` since the epoch as an IMF-fixdate, by the civil-from-days algorithm. */
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
