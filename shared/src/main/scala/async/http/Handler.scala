package gears.async.http

import gears.async.Async

import java.nio.charset.StandardCharsets

/** Writes a response incrementally - Go's `http.ResponseWriter`. [[header]]
  * is mutable until the first [[write]] (or an explicit [[writeHeader]]),
  * whichever comes first - that moment freezes and flushes the status line
  * and headers (defaulting to 200 if `writeHeader` was never called
  * explicitly, matching Go's own auto-200-on-first-`Write` rule), and every
  * `write` after that streams its bytes straight to the connection. No
  * full-response buffering is required the way building an [[HttpResponse]]
  * value up front does - genuinely large or unbounded bodies can be written
  * as they're produced.
  *
  * `Content-Length` framing only happens if it's set on [[header]] *before*
  * the first `write` (which [[Handler.of]]'s adapter always does, since it
  * already has the whole body in hand). A handler that streams without a
  * known length gets none sent, and the connection is closed at the end of
  * the response instead - the standard, RFC-compliant "read until close"
  * framing, not `Transfer-Encoding: chunked` (matching [[HttpServer]]'s
  * existing choice: this backend doesn't send chunked responses, the same
  * way it doesn't parse chunked request bodies).
  */
trait ResponseWriter:
  def header: MutableHeaders
  def writeHeader(status: HttpStatus): Unit
  def write(bytes: Array[Byte])(using Async): Unit
  final def write(text: String)(using Async): Unit = write(text.getBytes(StandardCharsets.UTF_8))

/** A request handler - Go's `http.Handler`. Most handlers only need to
  * compute a full response up front; those should use the implicit
  * conversion from a plain `HttpRequest => HttpResponse` (or [[Handler.of]]
  * directly) rather than implementing this trait by hand - see
  * [[gears.async.http.Router]], which every existing per-method handler
  * still targets unchanged.
  */
trait Handler:
  def serveHTTP(w: ResponseWriter, r: HttpRequest)(using Async): Unit

object Handler:
  /** Adapts a plain function - Go's `http.HandlerFunc` - into a [[Handler]]:
    * computes the whole response first (catching any exception into a 500,
    * the same recovery [[Router.dispatch]] used to do inline), then writes
    * it as one [[ResponseWriter.write]] call with `Content-Length` already
    * known, so the connection can still be kept alive afterward.
    */
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
