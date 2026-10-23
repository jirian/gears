package gears.io.http

import gears.async.Async

import java.nio.charset.StandardCharsets

/** How a handler sends its response, in the style of Go's `http.ResponseWriter`. Set headers in `header`, then call
  * `writeHeader` once, then `write` the body in as many pieces as needed. Without a `Content-Length`, the body is sent
  * chunked, or, when the client can't take chunks, ended by closing the connection. Writing before `writeHeader` sends
  * status 200.
  */
trait ResponseWriter:

  /** Returns the response headers. Changes after the status was sent have no effect. */
  def header: MutableHeaders

  /** Sends the status line and headers.
    *
    * @param status
    *   the response status
    */
  def writeHeader(status: HttpStatus): Unit

  /** Sends part of the body.
    *
    * @param bytes
    *   the next part of the body
    */
  def write(bytes: Array[Byte])(using Async): Unit

  /** Sends part of the body as UTF-8.
    *
    * @param text
    *   the next part of the body
    */
  final def write(text: String)(using Async): Unit = write(text.getBytes(StandardCharsets.UTF_8))

  private[http] def upgraded: Boolean = false

  private[http] def upgrade()(using Async): (gears.io.net.TcpStream, gears.io.Reader) =
    throw new UnsupportedOperationException("This response writer does not support protocol upgrades")

/** Serves HTTP requests. A plain handler gets the request with its whole body read; one made by `Handler.streaming`
  * reads the body itself as it arrives.
  */
trait Handler:

  /** Responds to a request. Throwing before anything was sent gives a 500; throwing later aborts the connection.
    *
    * @param w
    *   sends the response
    * @param r
    *   the request, with its whole body
    */
  def serveHTTP(w: ResponseWriter, r: HttpRequest)(using Async): Unit

  private[http] def serveStream(w: ResponseWriter, r: HttpRequest, body: gears.io.Reader)(using
      Async
  ): Unit =
    val bytes = body match
      case framed: MessageBody if framed.complete => Array.emptyByteArray
      case _                                      => Bodies.collect(body)
    serveHTTP(w, r.copy(body = bytes))

object Handler:

  /** Returns a handler that reads the request body as a stream instead of receiving it whole.
    *
    * @param body
    *   responds, given the response writer, the request without its body, and a reader of the body
    */
  def streaming(body: (ResponseWriter, HttpRequest, gears.io.Reader) => Async ?=> Unit): Handler =
    new Handler:
      def serveHTTP(w: ResponseWriter, r: HttpRequest)(using Async): Unit = body(w, r, Bodies.bytes(r.body))
      override private[http] def serveStream(w: ResponseWriter, r: HttpRequest, input: gears.io.Reader)(using
          Async
      ): Unit =
        body(w, r, input)

  /** Returns a handler that computes one whole response.
    *
    * @param f
    *   computes the response to a request
    */
  def of(f: HttpRequest => Async ?=> HttpResponse): Handler =
    new Handler:
      def serveHTTP(w: ResponseWriter, r: HttpRequest)(using Async): Unit =
        val response = f(r)
        response.headers.iterator.foreach((k, v) => w.header.add(k, v))
        w.header.set("Content-Length", response.body.length.toString)
        w.writeHeader(response.status)
        w.write(response.body)

  /** Lets a plain function be passed where a handler is expected. */
  given fromFunction: Conversion[HttpRequest => HttpResponse, Handler] = f => of(r => f(r))
