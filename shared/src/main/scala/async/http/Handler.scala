package gears.async.http

import gears.async.Async

import java.nio.charset.StandardCharsets

trait ResponseWriter:
  def header: MutableHeaders
  def writeHeader(status: HttpStatus): Unit
  def write(bytes: Array[Byte])(using Async): Unit
  final def write(text: String)(using Async): Unit = write(text.getBytes(StandardCharsets.UTF_8))

trait Handler:
  def serveHTTP(w: ResponseWriter, r: HttpRequest)(using Async): Unit

object Handler:
  def of(f: HttpRequest => HttpResponse): Handler =
    new Handler:
      def serveHTTP(w: ResponseWriter, r: HttpRequest)(using Async): Unit =
        val response =
          try f(r)
          catch case e: Exception => HttpResponse.internalError(e)
        response.headers.iterator.foreach((k, v) => w.header.add(k, v))
        w.header.set("Content-Length", response.body.length.toString)
        w.writeHeader(response.status)
        w.write(response.body)

  given fromFunction: Conversion[HttpRequest => HttpResponse, Handler] = of(_)
