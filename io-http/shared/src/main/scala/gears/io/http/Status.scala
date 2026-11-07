package gears.io.http

/** A response status, by its code, 100 to 599. */
final case class Status(code: Int) {
  require(code >= 100 && code <= 599, s"A status code is 100 to 599, not $code")

  /** The reason phrase sent with it: the standard one, or `Status <code>` for a code without one. */
  def reason: String = Status.reasons.getOrElse(code, s"Status $code")

  def isInformational: Boolean = code < 200
  def isSuccess: Boolean = code >= 200 && code < 300
  def isRedirect: Boolean = code == 301 || code == 302 || code == 303 || code == 307 || code == 308
  def isClientError: Boolean = code >= 400 && code < 500
  def isServerError: Boolean = code >= 500

  /** Whether a response with it has no body, whatever its headers say (RFC 9110, 6.4.1). */
  def forbidsBody: Boolean = code < 200 || code == 204 || code == 304

  override def toString: String = s"$code $reason"
}

object Status {
  val Continue = Status(100)
  val SwitchingProtocols = Status(101)
  val Ok = Status(200)
  val Created = Status(201)
  val Accepted = Status(202)
  val NoContent = Status(204)
  val PartialContent = Status(206)
  val MovedPermanently = Status(301)
  val Found = Status(302)
  val SeeOther = Status(303)
  val NotModified = Status(304)
  val TemporaryRedirect = Status(307)
  val PermanentRedirect = Status(308)
  val BadRequest = Status(400)
  val Unauthorized = Status(401)
  val Forbidden = Status(403)
  val NotFound = Status(404)
  val MethodNotAllowed = Status(405)
  val NotAcceptable = Status(406)
  val RequestTimeout = Status(408)
  val Conflict = Status(409)
  val Gone = Status(410)
  val LengthRequired = Status(411)
  val PreconditionFailed = Status(412)
  val ContentTooLarge = Status(413)
  val UriTooLong = Status(414)
  val UnsupportedMediaType = Status(415)
  val ExpectationFailed = Status(417)
  val UnprocessableContent = Status(422)
  val UpgradeRequired = Status(426)
  val TooManyRequests = Status(429)
  val RequestHeaderFieldsTooLarge = Status(431)
  val InternalServerError = Status(500)
  val NotImplemented = Status(501)
  val BadGateway = Status(502)
  val ServiceUnavailable = Status(503)
  val GatewayTimeout = Status(504)
  val HttpVersionNotSupported = Status(505)

  private val reasons: Map[Int, String] = Map(
    100 -> "Continue",
    101 -> "Switching Protocols",
    200 -> "OK",
    201 -> "Created",
    202 -> "Accepted",
    203 -> "Non-Authoritative Information",
    204 -> "No Content",
    205 -> "Reset Content",
    206 -> "Partial Content",
    300 -> "Multiple Choices",
    301 -> "Moved Permanently",
    302 -> "Found",
    303 -> "See Other",
    304 -> "Not Modified",
    307 -> "Temporary Redirect",
    308 -> "Permanent Redirect",
    400 -> "Bad Request",
    401 -> "Unauthorized",
    402 -> "Payment Required",
    403 -> "Forbidden",
    404 -> "Not Found",
    405 -> "Method Not Allowed",
    406 -> "Not Acceptable",
    407 -> "Proxy Authentication Required",
    408 -> "Request Timeout",
    409 -> "Conflict",
    410 -> "Gone",
    411 -> "Length Required",
    412 -> "Precondition Failed",
    413 -> "Content Too Large",
    414 -> "URI Too Long",
    415 -> "Unsupported Media Type",
    416 -> "Range Not Satisfiable",
    417 -> "Expectation Failed",
    421 -> "Misdirected Request",
    422 -> "Unprocessable Content",
    425 -> "Too Early",
    426 -> "Upgrade Required",
    428 -> "Precondition Required",
    429 -> "Too Many Requests",
    431 -> "Request Header Fields Too Large",
    451 -> "Unavailable For Legal Reasons",
    500 -> "Internal Server Error",
    501 -> "Not Implemented",
    502 -> "Bad Gateway",
    503 -> "Service Unavailable",
    504 -> "Gateway Timeout",
    505 -> "HTTP Version Not Supported"
  )
}
