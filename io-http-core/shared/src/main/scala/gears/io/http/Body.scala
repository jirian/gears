package gears.io.http

import gears.async.Async

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets

sealed trait Body {

  def contentType: Option[String]

  def length: Option[Long]

  def bytes()(using Async): Array[Byte]

  def text()(using Async): String = new String(bytes(), Body.charset(contentType))

  def as[A](using decoder: BodyDecoder[A], async: Async): A = decoder.decode(bytes(), contentType)

  def reader(): Body.Reader

  def replayable: Boolean

  private[http] def writeTo(out: BodyWriter)(using Async): Unit
}

object Body {

  val empty: Body = new Bytes(Array.emptyByteArray, None)

  def apply(bytes: Array[Byte], contentType: String = "application/octet-stream"): Body =
    new Bytes(bytes, Some(contentType))

  private[http] val PlainText = "text/plain; charset=utf-8"

  def text(text: String, contentType: String = PlainText): Body =
    new Bytes(Text.encode(text, charset(Some(contentType))), Some(contentType))

  def encode[A](value: A)(using encoder: BodyEncoder[A]): Body =
    new Bytes(encoder.encode(value), Some(encoder.contentType))

  def stream(contentType: String, length: Option[Long] = None)(write: BodyWriter => Async ?=> Unit): Body =
    new Streamed(Some(contentType), length, write)

  def charset(contentType: Option[String]): Charset =
    contentType match {
      case Some(t) if t eq PlainText      => StandardCharsets.UTF_8
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
      case _ => StandardCharsets.UTF_8
    }

  private def charsetNamed(name: String): Charset =
    if name.equalsIgnoreCase("utf-8") || name.equalsIgnoreCase("utf8") then StandardCharsets.UTF_8
    else if name.equalsIgnoreCase("iso-8859-1") then StandardCharsets.ISO_8859_1
    else scala.util.Try(Charset.forName(name)).toOption.getOrElse(StandardCharsets.UTF_8)

  trait Reader {

    def read(buf: ByteBuffer)(using Async): Int
  }

  private final class ArrayReader(content: Async ?=> Array[Byte]) extends Reader {
    private var cursor = 0
    def read(buf: ByteBuffer)(using Async): Int = {
      val all = content
      if cursor >= all.length then -1
      else {
        val n = math.min(buf.remaining(), all.length - cursor)
        buf.put(all, cursor, n)
        cursor += n
        n
      }
    }
  }

  private object Ended extends Reader {
    def read(buf: ByteBuffer)(using Async): Int = -1
  }

  private[http] final class Bytes(val content: Array[Byte], val contentType: Option[String]) extends Body {
    def length: Option[Long] = Some(content.length.toLong)
    def bytes()(using Async): Array[Byte] = content
    def reader(): Reader = new ArrayReader(content)
    def replayable: Boolean = true
    private[http] def writeTo(out: BodyWriter)(using Async): Unit = if content.nonEmpty then out.write(content)
    override def toString: String = s"Body(${content.length} bytes${contentType.fold("")(t => s", $t")})"
  }

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
    def reader(): Reader = new ArrayReader(bytes())
    def replayable: Boolean = false
    private[http] def writeTo(out: BodyWriter)(using Async): Unit =
      if written != null then out.write(written.nn) else write(out)
    override def toString: String = s"Body.stream(${contentType.getOrElse("")})"
  }

  private[http] def incoming(headers: Headers, framing: Wire.Framing, source: Wire.Source): Body =
    framing match {
      case Wire.Framing.NoBody    => empty
      case Wire.Framing.Length(n) => new Incoming(headers.get("Content-Type"), Some(n), source)
      case _                      => new Incoming(headers.get("Content-Type"), None, source)
    }

  private[http] final class Incoming(val contentType: Option[String], val length: Option[Long], source: Wire.Source)
      extends Body {
    private var all: Array[Byte] | Null = null
    private var cursor = 0

    def complete: Boolean = all != null || source.complete

    def bytes()(using Async): Array[Byte] = {
      if all == null then {
        val out = new ByteArrayOutputStream(length.fold(8192)(n => math.min(n, 1L << 16).toInt))
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

    private object arriving extends Reader {
      def read(buf: ByteBuffer)(using Async): Int =
        if all == null then source.read(buf)
        else if cursor >= all.nn.length then -1
        else {
          val n = math.min(buf.remaining(), all.nn.length - cursor)
          buf.put(all.nn, cursor, n)
          cursor += n
          n
        }
    }

    def reader(): Reader = arriving

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

    private[http] def drain(max: Long)(using Async): Boolean = all != null || source.drain(max)

    override def toString: String = s"Body(arriving${contentType.fold("")(t => s", $t")})"
  }

  private[http] final case class Upgrade(run: (gears.io.net.Stream, gears.io.net.BufferedInput) => Async ?=> Unit)
      extends Body {
    def contentType: Option[String] = None
    def length: Option[Long] = Some(0L)
    def bytes()(using Async): Array[Byte] = Array.emptyByteArray
    def reader(): Body.Reader = Body.Ended
    def replayable: Boolean = false
    private[http] def writeTo(out: BodyWriter)(using Async): Unit = ()
  }
}

trait BodyWriter {
  def write(bytes: Array[Byte], offset: Int, length: Int)(using Async): Unit
  def write(bytes: Array[Byte])(using Async): Unit = write(bytes, 0, bytes.length)
  def write(text: String)(using Async): Unit = write(text.getBytes(StandardCharsets.UTF_8))
  def write(buf: ByteBuffer)(using Async): Unit = {
    val bytes = new Array[Byte](buf.remaining())
    buf.get(bytes)
    write(bytes)
  }

  def flush()(using Async): Unit = ()
}

object BodyWriter {

  def to(out: java.io.OutputStream): BodyWriter = new BodyWriter {
    def write(bytes: Array[Byte], offset: Int, length: Int)(using Async): Unit = out.write(bytes, offset, length)
  }
}

trait BodyEncoder[-A] {
  def contentType: String
  def encode(value: A): Array[Byte]
}

trait BodyDecoder[+A] {
  def decode(bytes: Array[Byte], contentType: Option[String]): A
}

trait BodyCodec[A] extends BodyEncoder[A] with BodyDecoder[A]

object BodyCodec {

  def json[A](write: A => String, read: String => A): BodyCodec[A] = new BodyCodec[A] {
    def contentType: String = "application/json"
    def encode(value: A): Array[Byte] = write(value).getBytes(StandardCharsets.UTF_8)
    def decode(bytes: Array[Byte], contentType: Option[String]): A = read(new String(bytes, StandardCharsets.UTF_8))
  }

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
