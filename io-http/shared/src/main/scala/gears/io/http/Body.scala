package gears.io.http

import gears.async.Async

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets

/** A message's body: bytes in memory, bytes a function writes as the message goes out (`Body.stream`), or bytes still
  * arriving with a request a server received or a response a client received. Any of them is read the same way: whole
  * (`bytes`, `text`, `as`), or a buffer at a time (`read`). One arriving is read once, as it arrives; reading it whole
  * keeps what it read, so that it may be read whole again.
  */
sealed trait Body {

  /** Its `Content-Type`, if it has one. */
  def contentType: Option[String]

  /** Its length, if known before it is written: what goes in `Content-Length`; else it is sent chunked. */
  def length: Option[Long]

  /** All of it: an `HttpError` of 413 if one arriving is longer than the limit it arrives with. */
  def bytes()(using Async): Array[Byte]

  /** All of it, decoded by its `Content-Type`'s charset, UTF-8 if it names none. */
  def text()(using Async): String = new String(bytes(), Body.charset(contentType))

  /** All of it, decoded by `A`'s decoder. */
  def as[A](using decoder: BodyDecoder[A], async: Async): A = decoder.decode(bytes(), contentType)

  /** Reads into `buf` once some bytes are there: how many, or -1 at the end. */
  def read(buf: ByteBuffer)(using Async): Int

  /** Whether it may be sent again, as a client resending a request after a redirect or a failure needs: bytes in memory
    * may; a stream, which is written once, may not.
    */
  def replayable: Boolean

  /** Writes it to `out`. */
  private[http] def writeTo(out: BodyWriter)(using Async): Unit
}

object Body {

  /** No body. */
  val empty: Body = new Bytes(Array.emptyByteArray, None)

  def apply(bytes: Array[Byte], contentType: String = "application/octet-stream"): Body =
    new Bytes(bytes, Some(contentType))

  /** `text` in UTF-8, `text/plain` unless `contentType` says otherwise. */
  def text(text: String, contentType: String = "text/plain; charset=utf-8"): Body =
    new Bytes(text.getBytes(charset(Some(contentType))), Some(contentType))

  /** `value`, encoded by its encoder, with the encoder's `Content-Type`. */
  def encode[A](value: A)(using encoder: BodyEncoder[A]): Body =
    new Bytes(encoder.encode(value), Some(encoder.contentType))

  /** A body `write` writes as the message goes out, chunked unless `length` is given, in which case `write` must write
    * exactly that many bytes. Each write waits until the peer has taken what it can, so a slow peer slows the writer,
    * and one that goes away fails the write, which ends `write`.
    */
  def stream(contentType: String, length: Option[Long] = None)(write: BodyWriter => Async ?=> Unit): Body =
    new Streamed(Some(contentType), length, write)

  /** The charset `contentType` names, UTF-8 if it names none or one this platform does not know. */
  def charset(contentType: Option[String]): Charset =
    contentType match {
      case Some(t) if t.indexOf(';') >= 0 =>
        Text
          .split(t, ';')
          .iterator
          .drop(1)
          .map(_.trim)
          .collectFirst {
            case p if p.regionMatches(true, 0, "charset=", 0, 8) =>
              p.substring(8).trim.stripPrefix("\"").stripSuffix("\"")
          }
          .map(charsetNamed)
          .getOrElse(StandardCharsets.UTF_8)
      case _ => StandardCharsets.UTF_8 // no parameters: no charset named
    }

  /** The charset `name` names, UTF-8 if none does; UTF-8 and Latin-1 without a lookup, which is slow on Scala Native.
    */
  private def charsetNamed(name: String): Charset =
    if name.equalsIgnoreCase("utf-8") || name.equalsIgnoreCase("utf8") then StandardCharsets.UTF_8
    else if name.equalsIgnoreCase("iso-8859-1") then StandardCharsets.ISO_8859_1
    else scala.util.Try(Charset.forName(name)).toOption.getOrElse(StandardCharsets.UTF_8)

  /** Bytes in memory. */
  private[http] final class Bytes(val content: Array[Byte], val contentType: Option[String]) extends Body {
    private var cursor = 0
    def length: Option[Long] = Some(content.length.toLong)
    def bytes()(using Async): Array[Byte] = content
    def read(buf: ByteBuffer)(using Async): Int =
      if cursor >= content.length then -1
      else {
        val n = math.min(buf.remaining(), content.length - cursor)
        buf.put(content, cursor, n)
        cursor += n
        n
      }
    def replayable: Boolean = true
    private[http] def writeTo(out: BodyWriter)(using Async): Unit = if content.nonEmpty then out.write(content)
    override def toString: String = s"Body(${content.length} bytes${contentType.fold("")(t => s", $t")})"
  }

  /** Bytes a function writes. */
  private[http] final class Streamed(
      val contentType: Option[String],
      val length: Option[Long],
      write: BodyWriter => Async ?=> Unit
  ) extends Body {
    private var written: Array[Byte] | Null = null
    def bytes()(using Async): Array[Byte] = {
      if written == null then {
        val out = new ByteArrayOutputStream()
        write(BodyWriter.to(out))
        written = out.toByteArray
      }
      written.nn
    }
    private var cursor = 0
    def read(buf: ByteBuffer)(using Async): Int = {
      val all = bytes()
      if cursor >= all.length then -1
      else {
        val n = math.min(buf.remaining(), all.length - cursor)
        buf.put(all, cursor, n)
        cursor += n
        n
      }
    }
    def replayable: Boolean = false
    private[http] def writeTo(out: BodyWriter)(using Async): Unit =
      if written != null then out.write(written.nn) else write(out)
    override def toString: String = s"Body.stream(${contentType.getOrElse("")})"
  }

  /** Bytes still arriving, a request's on a server or a response's on a client, through `source`, which delimits them
    * and holds them to its limit.
    */
  private[http] final class Incoming(val contentType: Option[String], val length: Option[Long], source: Wire.Source)
      extends Body {
    private var all: Array[Byte] | Null = null
    private var cursor = 0

    /** Whether every byte has arrived. */
    def complete: Boolean = all != null || source.complete

    def bytes()(using Async): Array[Byte] = {
      if all == null then {
        val out = new ByteArrayOutputStream(length.fold(8192)(n => math.min(n, 1L << 20).toInt))
        val chunk = ByteBuffer.allocate(16384)
        var n = source.read(chunk)
        while n >= 0 do {
          out.write(chunk.array(), 0, chunk.position())
          chunk.clear()
          n = source.read(chunk)
        }
        all = out.toByteArray
      }
      all.nn
    }

    def read(buf: ByteBuffer)(using Async): Int =
      if all == null then source.read(buf)
      else if cursor >= all.nn.length then -1
      else {
        val n = math.min(buf.remaining(), all.nn.length - cursor)
        buf.put(all.nn, cursor, n)
        cursor += n
        n
      }

    def replayable: Boolean = false

    private[http] def writeTo(out: BodyWriter)(using Async): Unit =
      if all != null then out.write(all.nn)
      else {
        val chunk = ByteBuffer.allocate(16384)
        while source.read(chunk) >= 0 do {
          out.write(chunk.array(), 0, chunk.position())
          chunk.clear()
        }
      }

    /** Reads what is left and drops it, at most `max` bytes: whether it reached the end. */
    private[http] def drain(max: Long)(using Async): Boolean = all != null || source.drain(max)

    override def toString: String = s"Body(arriving${contentType.fold("")(t => s", $t")})"
  }

  /** What takes the connection over once a server has written a `101 Switching Protocols` response, such as a
    * WebSocket's session: it is given the stream, and the server's reader of it, which may hold bytes beyond the
    * request.
    */
  private[http] final case class Upgrade(run: (gears.io.net.Stream, gears.io.net.BufferedInput) => Async ?=> Unit)
      extends Body {
    def contentType: Option[String] = None
    def length: Option[Long] = Some(0L)
    def bytes()(using Async): Array[Byte] = Array.emptyByteArray
    def read(buf: ByteBuffer)(using Async): Int = -1
    def replayable: Boolean = false
    private[http] def writeTo(out: BodyWriter)(using Async): Unit = ()
  }
}

/** Where a streamed body's bytes go (`Body.stream`). Each write returns once the peer has taken what it can. */
trait BodyWriter {
  def write(bytes: Array[Byte], offset: Int, length: Int)(using Async): Unit
  def write(bytes: Array[Byte])(using Async): Unit = write(bytes, 0, bytes.length)
  def write(text: String)(using Async): Unit = write(text.getBytes(StandardCharsets.UTF_8))
  def write(buf: ByteBuffer)(using Async): Unit = {
    val bytes = new Array[Byte](buf.remaining())
    buf.get(bytes)
    write(bytes)
  }

  /** Sends what has been written so far, for a stream whose reader is waiting, such as server-sent events: writes
    * already go out as they are made, so this is for a writer that buffers.
    */
  def flush()(using Async): Unit = ()
}

object BodyWriter {

  /** A writer into `out`, as a test, or a streamed body read whole, uses. */
  def to(out: java.io.OutputStream): BodyWriter = new BodyWriter {
    def write(bytes: Array[Byte], offset: Int, length: Int)(using Async): Unit = out.write(bytes, offset, length)
  }
}

/** How a value is encoded as a body: its bytes, and their `Content-Type`. */
trait BodyEncoder[-A] {
  def contentType: String
  def encode(value: A): Array[Byte]
}

/** How a body is decoded into a value. A failure is the decoder's exception, which a server answers with a 400. */
trait BodyDecoder[+A] {
  def decode(bytes: Array[Byte], contentType: Option[String]): A
}

/** Both, for a type that goes both ways, such as a JSON message: the one seam a JSON library plugs into, so that
  * `Response.json(user)`, `request.as[User]` and `client.send(...).as[User]` work with any of them:
  * {{{
  * given BodyCodec[User] = BodyCodec.json(writeToString(_), readFromString[User](_)) // jsoniter-scala
  * given BodyCodec[User] = BodyCodec.json(upickle.default.write(_), upickle.default.read[User](_))
  * }}}
  */
trait BodyCodec[A] extends BodyEncoder[A] with BodyDecoder[A]

object BodyCodec {

  /** A codec of JSON text, from a library's writer and reader, as `application/json` in UTF-8. */
  def json[A](write: A => String, read: String => A): BodyCodec[A] = new BodyCodec[A] {
    def contentType: String = "application/json"
    def encode(value: A): Array[Byte] = write(value).getBytes(StandardCharsets.UTF_8)
    def decode(bytes: Array[Byte], contentType: Option[String]): A = read(new String(bytes, StandardCharsets.UTF_8))
  }

  /** A codec of text in another format, `contentType`, from a writer and a reader. */
  def text[A](contentType: String, write: A => String, read: String => A): BodyCodec[A] = {
    val type0 = contentType
    new BodyCodec[A] {
      def contentType: String = type0
      def encode(value: A): Array[Byte] = write(value).getBytes(Body.charset(Some(type0)))
      def decode(bytes: Array[Byte], contentType: Option[String]): A = read(
        new String(bytes, Body.charset(contentType))
      )
    }
  }

  given string: BodyCodec[String] = new BodyCodec[String] {
    def contentType: String = "text/plain; charset=utf-8"
    def encode(value: String): Array[Byte] = value.getBytes(StandardCharsets.UTF_8)
    def decode(bytes: Array[Byte], contentType: Option[String]): String = new String(bytes, Body.charset(contentType))
  }

  given bytes: BodyCodec[Array[Byte]] = new BodyCodec[Array[Byte]] {
    def contentType: String = "application/octet-stream"
    def encode(value: Array[Byte]): Array[Byte] = value
    def decode(bytes: Array[Byte], contentType: Option[String]): Array[Byte] = bytes
  }
}
