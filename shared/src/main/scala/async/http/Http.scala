package gears.async.http

import java.nio.charset.StandardCharsets

/** An HTTP request method. The common methods are named cases; anything
  * else parsed off the wire (or otherwise unrecognized) is [[Other]] rather
  * than a parse failure - an unknown method is a routing question (no
  * handler will match it, so [[Router]] answers 404/405), not a malformed
  * request.
  */
enum HttpMethod:
  case GET, HEAD, POST, PUT, DELETE, CONNECT, OPTIONS, TRACE, PATCH
  case Other(name: String)

  /** The wire representation, e.g. for the `Allow` header or logging. */
  def rendered: String = this match
    case Other(name) => name
    case known        => known.toString

object HttpMethod:
  def parse(token: String): HttpMethod = token match
    case "GET"     => GET
    case "HEAD"    => HEAD
    case "POST"    => POST
    case "PUT"     => PUT
    case "DELETE"  => DELETE
    case "CONNECT" => CONNECT
    case "OPTIONS" => OPTIONS
    case "TRACE"   => TRACE
    case "PATCH"   => PATCH
    case other      => Other(other)

/** An HTTP status: a code plus its standard reason phrase. */
final case class HttpStatus(code: Int, reason: String)

object HttpStatus:
  val Ok                    = HttpStatus(200, "OK")
  val NoContent              = HttpStatus(204, "No Content")
  val BadRequest             = HttpStatus(400, "Bad Request")
  val NotFound               = HttpStatus(404, "Not Found")
  val MethodNotAllowed       = HttpStatus(405, "Method Not Allowed")
  val PayloadTooLarge        = HttpStatus(413, "Payload Too Large")
  val InternalServerError    = HttpStatus(500, "Internal Server Error")
  val NotImplemented         = HttpStatus(501, "Not Implemented")
  val HttpVersionNotSupported = HttpStatus(505, "HTTP Version Not Supported")

/** A case-insensitive, order-preserving set of header fields (per RFC 7230,
  * field names are case-insensitive; values here are kept exactly as
  * written). Immutable - [[add]] returns a new [[Headers]].
  */
final class Headers private (private val entries: Vector[(String, String)]):
  def get(name: String): Option[String] =
    entries.collectFirst { case (k, v) if k.equalsIgnoreCase(name) => v }

  def getAll(name: String): Seq[String] =
    entries.collect { case (k, v) if k.equalsIgnoreCase(name) => v }

  def contains(name: String): Boolean = get(name).isDefined

  def add(name: String, value: String): Headers = new Headers(entries :+ (name -> value))

  def iterator: Iterator[(String, String)] = entries.iterator

object Headers:
  val empty: Headers = new Headers(Vector.empty)
  def apply(pairs: (String, String)*): Headers = new Headers(pairs.toVector)

/** A fully-parsed HTTP request: `path` and `query` are already split on the
  * request target's `?`, and `body` holds exactly `Content-Length` bytes
  * (empty if the request had none - chunked request bodies aren't
  * supported, see [[HttpServer]]).
  */
final case class HttpRequest(
    method: HttpMethod,
    path: String,
    query: String,
    version: String,
    headers: Headers,
    body: Array[Byte]
)

/** An HTTP response. `Content-Length` is computed from `body` and attached
  * automatically when the response is written - handlers never set it
  * themselves.
  */
final case class HttpResponse(status: HttpStatus, headers: Headers, body: Array[Byte]):
  def withHeader(name: String, value: String): HttpResponse = copy(headers = headers.add(name, value))

object HttpResponse:
  def apply(status: HttpStatus, body: String, contentType: String = "text/plain; charset=utf-8"): HttpResponse =
    HttpResponse(status, Headers("Content-Type" -> contentType), body.getBytes(StandardCharsets.UTF_8))

  def text(body: String): HttpResponse = apply(HttpStatus.Ok, body)

  def notFound(path: String): HttpResponse =
    apply(HttpStatus.NotFound, s"404 Not Found: $path\n")

  def methodNotAllowed(allowed: Seq[HttpMethod]): HttpResponse =
    apply(HttpStatus.MethodNotAllowed, "405 Method Not Allowed\n")
      .withHeader("Allow", allowed.map(_.rendered).distinct.mkString(", "))

  def badRequest(reason: String): HttpResponse =
    apply(HttpStatus.BadRequest, s"400 Bad Request: $reason\n")

  def notImplemented(reason: String): HttpResponse =
    apply(HttpStatus.NotImplemented, s"501 Not Implemented: $reason\n")

  def httpVersionNotSupported(): HttpResponse =
    apply(HttpStatus.HttpVersionNotSupported, "505 HTTP Version Not Supported\n")

  def internalError(e: Throwable): HttpResponse =
    apply(HttpStatus.InternalServerError, "500 Internal Server Error\n")
