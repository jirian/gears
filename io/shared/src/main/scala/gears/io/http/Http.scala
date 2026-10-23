package gears.io.http

import java.nio.charset.StandardCharsets

/** An HTTP request method. Methods without a case of their own are `Other`. */
enum HttpMethod:
  case GET, HEAD, POST, PUT, DELETE, CONNECT, OPTIONS, TRACE, PATCH
  case Other(name: String)

  /** Returns this method as it appears on the request line. */
  def rendered: String = this match
    case Other(name) => name
    case known       => known.toString

object HttpMethod:

  /** Returns the method that a request-line token names. Method names are case-sensitive.
    *
    * @param token
    *   the method token from a request line
    */
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
    case other     => Other(other)

/** A response status: its code and the reason phrase sent with it. */
final case class HttpStatus(code: Int, reason: String)

/** The common statuses. */
object HttpStatus:
  val Ok = HttpStatus(200, "OK")
  val Created = HttpStatus(201, "Created")
  val Accepted = HttpStatus(202, "Accepted")
  val NoContent = HttpStatus(204, "No Content")
  val MovedPermanently = HttpStatus(301, "Moved Permanently")
  val Found = HttpStatus(302, "Found")
  val SeeOther = HttpStatus(303, "See Other")
  val NotModified = HttpStatus(304, "Not Modified")
  val TemporaryRedirect = HttpStatus(307, "Temporary Redirect")
  val PermanentRedirect = HttpStatus(308, "Permanent Redirect")
  val BadRequest = HttpStatus(400, "Bad Request")
  val Unauthorized = HttpStatus(401, "Unauthorized")
  val Forbidden = HttpStatus(403, "Forbidden")
  val NotFound = HttpStatus(404, "Not Found")
  val MethodNotAllowed = HttpStatus(405, "Method Not Allowed")
  val Conflict = HttpStatus(409, "Conflict")
  val PayloadTooLarge = HttpStatus(413, "Payload Too Large")
  val UnsupportedMediaType = HttpStatus(415, "Unsupported Media Type")
  val ExpectationFailed = HttpStatus(417, "Expectation Failed")
  val TooManyRequests = HttpStatus(429, "Too Many Requests")
  val InternalServerError = HttpStatus(500, "Internal Server Error")
  val NotImplemented = HttpStatus(501, "Not Implemented")
  val BadGateway = HttpStatus(502, "Bad Gateway")
  val ServiceUnavailable = HttpStatus(503, "Service Unavailable")
  val GatewayTimeout = HttpStatus(504, "Gateway Timeout")
  val HttpVersionNotSupported = HttpStatus(505, "HTTP Version Not Supported")

  private[http] def isRedirect(code: Int): Boolean =
    code == 301 || code == 302 || code == 303 || code == 307 || code == 308

/** Header syntax checks from RFC 9110. Every way of building headers runs them, so a header cannot smuggle a line break
  * into a request or response.
  */
object HeaderSyntax:
  private val separators = "()<>@,;:\\\"/[]?={} \t"

  /** Returns whether `name` is a token: printable ASCII without separators.
    *
    * @param name
    *   a header name
    */
  def isValidName(name: String): Boolean =
    name.nonEmpty && name.forall(c => c > ' ' && c < 127 && separators.indexOf(c) < 0)

  /** Returns whether `value` has no CR, LF or NUL.
    *
    * @param value
    *   a header value
    */
  def isValidValue(value: String): Boolean =
    value.forall(c => c != '\r' && c != '\n' && c != '\u0000')

  /** Checks a header.
    *
    * @param name
    *   the header's name
    * @param value
    *   the header's value
    * @throws IllegalArgumentException
    *   unless both are valid
    */
  def check(name: String, value: String): Unit =
    if !isValidName(name) then throw new IllegalArgumentException(s"invalid header name: '$name'")
    if !isValidValue(value) then throw new IllegalArgumentException(s"invalid value for header '$name'")

/** An immutable list of headers in order. Names compare case-insensitively, and a name may repeat. */
final class Headers private (private val entries: Vector[(String, String)]):
  /** Returns the first value of `name`.
    *
    * @param name
    *   the header name, compared case-insensitively
    */
  def get(name: String): Option[String] =
    entries.collectFirst { case (k, v) if k.equalsIgnoreCase(name) => v }

  /** Returns every value of `name`, in order.
    *
    * @param name
    *   the header name, compared case-insensitively
    */
  def getAll(name: String): Seq[String] =
    entries.collect { case (k, v) if k.equalsIgnoreCase(name) => v }

  /** Returns whether a header named `name` is present.
    *
    * @param name
    *   the header name, compared case-insensitively
    */
  def contains(name: String): Boolean = get(name).isDefined

  /** Returns these headers plus one more, keeping any others with the same name.
    *
    * @param name
    *   the header's name
    * @param value
    *   the header's value
    * @throws IllegalArgumentException
    *   if the name or value is not valid
    */
  def add(name: String, value: String): Headers =
    HeaderSyntax.check(name, value)
    new Headers(entries :+ (name -> value))

  /** Returns these headers with every `name` replaced by this one value.
    *
    * @param name
    *   the header's name
    * @param value
    *   the header's value
    * @throws IllegalArgumentException
    *   if the name or value is not valid
    */
  def set(name: String, value: String): Headers =
    HeaderSyntax.check(name, value)
    new Headers(entries.filterNot((k, _) => k.equalsIgnoreCase(name)) :+ (name -> value))

  /** Returns these headers without any named `name`.
    *
    * @param name
    *   the header name, compared case-insensitively
    */
  def remove(name: String): Headers = new Headers(entries.filterNot((k, _) => k.equalsIgnoreCase(name)))

  /** Returns the headers as name and value pairs, in order. */
  def iterator: Iterator[(String, String)] = entries.iterator

object Headers:
  val empty: Headers = new Headers(Vector.empty)

  /** Returns headers made of `pairs`, in order.
    *
    * @param pairs
    *   names and values
    * @throws IllegalArgumentException
    *   if a name or value is not valid
    */
  def apply(pairs: (String, String)*): Headers =
    pairs.foreach((name, value) => HeaderSyntax.check(name, value))
    new Headers(pairs.toVector)

/** A mutable header list, for building a response's headers in a handler. */
final class MutableHeaders:
  private val entries = scala.collection.mutable.ArrayBuffer[(String, String)]()

  /** Returns the first value of `name`.
    *
    * @param name
    *   the header name, compared case-insensitively
    */
  def get(name: String): Option[String] =
    entries.collectFirst { case (k, v) if k.equalsIgnoreCase(name) => v }

  /** Adds a header, keeping any others with the same name.
    *
    * @param name
    *   the header's name
    * @param value
    *   the header's value
    * @return
    *   this list
    * @throws IllegalArgumentException
    *   if the name or value is not valid
    */
  def add(name: String, value: String): this.type =
    HeaderSyntax.check(name, value)
    entries += (name -> value)
    this

  /** Replaces every header named `name` with this one value.
    *
    * @param name
    *   the header's name
    * @param value
    *   the header's value
    * @return
    *   this list
    * @throws IllegalArgumentException
    *   if the name or value is not valid
    */
  def set(name: String, value: String): this.type =
    HeaderSyntax.check(name, value)
    entries.filterInPlace((k, _) => !k.equalsIgnoreCase(name))
    entries += (name -> value)
    this

  /** Removes every header named `name`.
    *
    * @param name
    *   the header name, compared case-insensitively
    * @return
    *   this list
    */
  def remove(name: String): this.type =
    entries.filterInPlace((k, _) => !k.equalsIgnoreCase(name))
    this

  /** Returns an immutable copy of this list. */
  def toHeaders: Headers = Headers(entries.toSeq*)

object MutableHeaders:
  /** Returns an empty list. */
  def apply(): MutableHeaders = new MutableHeaders

  /** Returns a mutable copy of `headers`.
    *
    * @param headers
    *   the headers to copy
    */
  def from(headers: Headers): MutableHeaders =
    val m = new MutableHeaders
    headers.iterator.foreach((k, v) => m.add(k, v))
    m

/** A cookie, as set by a `Set-Cookie` header. */
final case class Cookie(
    name: String,
    value: String,
    path: Option[String] = None,
    domain: Option[String] = None,
    maxAge: Option[Int] = None,
    secure: Boolean = false,
    httpOnly: Boolean = false
):
  /** Returns this cookie as sent back in a `Cookie` request header. */
  def rendered: String = s"$name=$value"

object Cookie:

  /** Parses a `Set-Cookie` value. Unknown attributes are ignored.
    *
    * @param setCookieValue
    *   the header's value
    * @return
    *   the cookie, or `None` for a value without `name=`
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
                case _          => ()
            }
            Some(Cookie(name, value, path, domain, maxAge, secure, httpOnly))
          case _ => None
      case Nil => None

/** A request with its whole body. `path` is still percent-encoded and excludes the query, which has no `?`.
  * `pathValues` holds the values of the route's `{name}` segments.
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
  /** Returns the value of the route's `{name}` segment, if the route has one.
    *
    * @param name
    *   the segment's name
    */
  def pathValue(name: String): Option[String] = pathValues.get(name)

/** A response with its whole body. The server adds `Content-Length` and framing headers itself. */
final case class HttpResponse(status: HttpStatus, headers: Headers, body: Array[Byte]):

  /** Returns this response with `name` set to `value`, replacing earlier values.
    *
    * @param name
    *   the header's name
    * @param value
    *   the header's value
    */
  def withHeader(name: String, value: String): HttpResponse = copy(headers = headers.set(name, value))

  /** Returns the cookies that this response's `Set-Cookie` headers set. */
  def cookies: Seq[Cookie] = headers.getAll("Set-Cookie").flatMap(Cookie.parse)

/** Responses with a text body. */
object HttpResponse:

  /** Returns a response with a UTF-8 body.
    *
    * @param status
    *   the status
    * @param body
    *   the body text
    * @param contentType
    *   the `Content-Type`, `text/plain` by default
    */
  def apply(status: HttpStatus, body: String, contentType: String = "text/plain; charset=utf-8"): HttpResponse =
    HttpResponse(status, Headers("Content-Type" -> contentType), body.getBytes(StandardCharsets.UTF_8))

  /** Returns a 200 response with a plain-text body.
    *
    * @param body
    *   the body text
    */
  def text(body: String): HttpResponse = apply(HttpStatus.Ok, body)

  /** Returns a redirect to `location`.
    *
    * @param location
    *   the `Location` header's value
    * @param status
    *   the redirect status, 302 by default
    */
  def redirect(location: String, status: HttpStatus = HttpStatus.Found): HttpResponse =
    apply(status, s"redirecting to $location\n").withHeader("Location", location)

  /** Returns a 404 response naming `path`.
    *
    * @param path
    *   the path that was not found
    */
  def notFound(path: String): HttpResponse =
    apply(HttpStatus.NotFound, s"404 Not Found: $path\n")

  /** Returns a 405 response with an `Allow` header.
    *
    * @param allowed
    *   the methods the path does support
    */
  def methodNotAllowed(allowed: Seq[HttpMethod]): HttpResponse =
    apply(HttpStatus.MethodNotAllowed, "405 Method Not Allowed\n")
      .withHeader("Allow", allowed.map(_.rendered).distinct.mkString(", "))

  /** Returns a 400 response.
    *
    * @param reason
    *   what was wrong with the request
    */
  def badRequest(reason: String): HttpResponse =
    apply(HttpStatus.BadRequest, s"400 Bad Request: $reason\n")

  /** Returns a 501 response.
    *
    * @param reason
    *   what is not implemented
    */
  def notImplemented(reason: String): HttpResponse =
    apply(HttpStatus.NotImplemented, s"501 Not Implemented: $reason\n")

  /** Returns a 505 response. */
  def httpVersionNotSupported(): HttpResponse =
    apply(HttpStatus.HttpVersionNotSupported, "505 HTTP Version Not Supported\n")

  /** Returns a 500 response. */
  def internalError(): HttpResponse =
    apply(HttpStatus.InternalServerError, "500 Internal Server Error\n")
