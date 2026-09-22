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
  val Ok                     = HttpStatus(200, "OK")
  val Created                = HttpStatus(201, "Created")
  val Accepted               = HttpStatus(202, "Accepted")
  val NoContent               = HttpStatus(204, "No Content")
  val MovedPermanently        = HttpStatus(301, "Moved Permanently")
  val Found                   = HttpStatus(302, "Found")
  val SeeOther                 = HttpStatus(303, "See Other")
  val NotModified              = HttpStatus(304, "Not Modified")
  val TemporaryRedirect         = HttpStatus(307, "Temporary Redirect")
  val PermanentRedirect          = HttpStatus(308, "Permanent Redirect")
  val BadRequest              = HttpStatus(400, "Bad Request")
  val Unauthorized             = HttpStatus(401, "Unauthorized")
  val Forbidden                 = HttpStatus(403, "Forbidden")
  val NotFound                = HttpStatus(404, "Not Found")
  val MethodNotAllowed        = HttpStatus(405, "Method Not Allowed")
  val Conflict                 = HttpStatus(409, "Conflict")
  val PayloadTooLarge         = HttpStatus(413, "Payload Too Large")
  val UnsupportedMediaType      = HttpStatus(415, "Unsupported Media Type")
  val TooManyRequests            = HttpStatus(429, "Too Many Requests")
  val InternalServerError     = HttpStatus(500, "Internal Server Error")
  val NotImplemented          = HttpStatus(501, "Not Implemented")
  val BadGateway                = HttpStatus(502, "Bad Gateway")
  val ServiceUnavailable         = HttpStatus(503, "Service Unavailable")
  val GatewayTimeout             = HttpStatus(504, "Gateway Timeout")
  val HttpVersionNotSupported = HttpStatus(505, "HTTP Version Not Supported")

  /** Whether `code` is one of the redirect statuses [[Client]] follows. */
  private[http] def isRedirect(code: Int): Boolean =
    code == 301 || code == 302 || code == 303 || code == 307 || code == 308

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

  /** Appends `value` as an additional occurrence of `name`, keeping any
    * existing ones (multiple `Set-Cookie` headers, e.g.) - see [[set]] to
    * replace instead.
    */
  def add(name: String, value: String): Headers = new Headers(entries :+ (name -> value))

  /** Replaces every existing occurrence of `name` with a single `value`
    * (Go's `Header.Set`, as opposed to `Header.Add`) - the right choice for
    * headers a response/request should only ever carry one of
    * (`Content-Length`, `Connection`, `Content-Type`, ...), where `add`
    * would silently duplicate one already set by a handler.
    */
  def set(name: String, value: String): Headers =
    new Headers(entries.filterNot((k, _) => k.equalsIgnoreCase(name)) :+ (name -> value))

  def remove(name: String): Headers = new Headers(entries.filterNot((k, _) => k.equalsIgnoreCase(name)))

  def iterator: Iterator[(String, String)] = entries.iterator

object Headers:
  val empty: Headers = new Headers(Vector.empty)
  def apply(pairs: (String, String)*): Headers = new Headers(pairs.toVector)

/** A mutable header collection, for building up a set of headers
  * incrementally before they're frozen into an immutable [[Headers]] (e.g.
  * [[ResponseWriter.header]], mutable up until the first [[ResponseWriter.write]]
  * or [[ResponseWriter.writeHeader]] call, matching Go's `http.Header` -
  * itself always a plain mutable map, including for outgoing requests/
  * responses). [[Headers]] stays the immutable value type everywhere a
  * fully-formed header set is passed around (a parsed request, a finished
  * response) - this exists only for the "still being assembled" case.
  */
final class MutableHeaders:
  private val entries = scala.collection.mutable.ArrayBuffer[(String, String)]()

  def get(name: String): Option[String] =
    entries.collectFirst { case (k, v) if k.equalsIgnoreCase(name) => v }

  def add(name: String, value: String): this.type =
    entries += (name -> value)
    this

  def set(name: String, value: String): this.type =
    entries.filterInPlace((k, _) => !k.equalsIgnoreCase(name))
    entries += (name -> value)
    this

  def remove(name: String): this.type =
    entries.filterInPlace((k, _) => !k.equalsIgnoreCase(name))
    this

  def toHeaders: Headers = Headers(entries.toSeq*)

object MutableHeaders:
  def apply(): MutableHeaders = new MutableHeaders
  def from(headers: Headers): MutableHeaders =
    val m = new MutableHeaders
    headers.iterator.foreach((k, v) => m.add(k, v))
    m

/** A single `Set-Cookie`/`Cookie` value. Only the attributes `net/http`'s
  * own `Cookie` type carries are modeled; unrecognized `Set-Cookie`
  * attributes are ignored rather than rejected. No jar/persistence -
  * matching Go, where that's the separate `net/http/cookiejar` package, not
  * `net/http` itself: [[Client]] surfaces cookies via [[HttpResponse]]'s
  * headers and [[ClientRequest.addCookie]] lets a caller send them back,
  * but nothing here remembers a cookie between requests automatically.
  */
final case class Cookie(
    name: String,
    value: String,
    path: Option[String] = None,
    domain: Option[String] = None,
    maxAge: Option[Int] = None,
    secure: Boolean = false,
    httpOnly: Boolean = false
):
  /** The `Cookie:` request-header rendering - just `name=value`. */
  def rendered: String = s"$name=$value"

object Cookie:
  /** Parses one `Set-Cookie` header value. Permissive by design (matching
    * `net/http`): an attribute it doesn't recognize is skipped, not treated
    * as a parse failure.
    */
  def parse(setCookieValue: String): Option[Cookie] =
    val parts = setCookieValue.split(";").map(_.trim).toList
    parts match
      case first :: attrs =>
        first.split("=", 2) match
          case Array(name, value) if name.nonEmpty =>
            var path: Option[String] = None
            var domain: Option[String] = None
            var maxAge: Option[Int] = None
            var secure = false
            var httpOnly = false
            attrs.foreach { attr =>
              val kv = attr.split("=", 2)
              val key = kv(0).trim.toLowerCase
              val v = if kv.length > 1 then Some(kv(1).trim) else None
              key match
                case "path"     => path = v
                case "domain"   => domain = v
                case "max-age"  => maxAge = v.flatMap(_.toIntOption)
                case "secure"   => secure = true
                case "httponly" => httpOnly = true
                case _           => ()
            }
            Some(Cookie(name, value, path, domain, maxAge, secure, httpOnly))
          case _ => None
      case Nil => None

/** A fully-parsed HTTP request: `path` and `query` are already split on the
  * request target's `?`, and `body` holds exactly `Content-Length` bytes
  * (empty if the request had none - chunked request bodies aren't
  * supported, see [[HttpServer]]). `pathValues` holds whatever
  * [[Router]] extracted from `{name}` wildcard segments in the pattern that
  * matched, if any - Go's (1.22+) `Request.PathValue`.
  */
final case class HttpRequest(
    method: HttpMethod,
    path: String,
    query: String,
    version: String,
    headers: Headers,
    body: Array[Byte],
    pathValues: Map[String, String] = Map.empty
):
  def pathValue(name: String): Option[String] = pathValues.get(name)

/** An HTTP response. `Content-Length` is computed from `body` and attached
  * automatically when the response is written - handlers never set it
  * themselves.
  */
final case class HttpResponse(status: HttpStatus, headers: Headers, body: Array[Byte]):
  /** Replaces (not appends - see [[Headers.set]]) `name`, so a second call
    * for a header a handler already set (or [[HttpServer]]'s own later
    * `Connection`/`Content-Length` finalization) doesn't silently duplicate
    * it on the wire.
    */
  def withHeader(name: String, value: String): HttpResponse = copy(headers = headers.set(name, value))

  /** The [[Cookie]]s this response asked the client to set, parsed from
    * every `Set-Cookie` header present.
    */
  def cookies: Seq[Cookie] = headers.getAll("Set-Cookie").flatMap(Cookie.parse)

object HttpResponse:
  def apply(status: HttpStatus, body: String, contentType: String = "text/plain; charset=utf-8"): HttpResponse =
    HttpResponse(status, Headers("Content-Type" -> contentType), body.getBytes(StandardCharsets.UTF_8))

  def text(body: String): HttpResponse = apply(HttpStatus.Ok, body)

  /** A redirect response - `status` must be one of the 3xx codes
    * [[Client]] recognizes as a redirect (301/302/303/307/308); defaults to
    * 302, matching Go's `http.Redirect`'s own default.
    */
  def redirect(location: String, status: HttpStatus = HttpStatus.Found): HttpResponse =
    apply(status, s"redirecting to $location\n").withHeader("Location", location)

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
