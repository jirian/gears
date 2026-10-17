package gears.io.http

import gears.async.Async

import java.nio.charset.StandardCharsets

trait ResponseWriter:
  def header: MutableHeaders
  def writeHeader(status: HttpStatus): Unit
  def write(bytes: Array[Byte])(using Async): Unit
  final def write(text: String)(using Async): Unit = write(text.getBytes(StandardCharsets.UTF_8))

  private[http] def upgraded: Boolean = false

  private[http] def upgrade()(using Async): (gears.io.net.TcpStream, gears.io.Reader) =
    throw new UnsupportedOperationException("This response writer does not support protocol upgrades")

trait Handler:
  def serveHTTP(w: ResponseWriter, r: HttpRequest)(using Async): Unit

  private[http] def serveStream(w: ResponseWriter, r: HttpRequest, body: gears.io.Reader)(using
      Async
  ): Unit =
    val bytes = body match
      case framed: MessageBody if framed.complete => Array.emptyByteArray
      case _                                      => Bodies.collect(body)
    serveHTTP(w, r.copy(body = bytes))

object Handler:
  def streaming(body: (ResponseWriter, HttpRequest, gears.io.Reader) => Async ?=> Unit): Handler =
    new Handler:
      def serveHTTP(w: ResponseWriter, r: HttpRequest)(using Async): Unit = body(w, r, Bodies.bytes(r.body))
      override private[http] def serveStream(w: ResponseWriter, r: HttpRequest, input: gears.io.Reader)(using
          Async
      ): Unit =
        body(w, r, input)
  def of(f: HttpRequest => Async ?=> HttpResponse): Handler =
    new Handler:
      def serveHTTP(w: ResponseWriter, r: HttpRequest)(using Async): Unit =
        val response = f(r)
        response.headers.iterator.foreach((k, v) => w.header.add(k, v))
        w.header.set("Content-Length", response.body.length.toString)
        w.writeHeader(response.status)
        w.write(response.body)

  given fromFunction: Conversion[HttpRequest => HttpResponse, Handler] = f => of(r => f(r))
