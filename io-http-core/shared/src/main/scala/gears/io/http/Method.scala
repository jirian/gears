package gears.io.http

/** A request method. Methods with no case of their own are `Other`, by their name, which is case-sensitive; `parse`
  * gives a known method its own case, and a client refuses to send an `Other` whose name is not a token.
  */
enum Method {
  case Get, Head, Post, Put, Delete, Patch, Options, Trace, Connect
  case Other(token: String)

  /** The method as it appears on the request line. */
  def name: String = this match {
    case Other(token) => token
    case known        => known.productPrefix.toUpperCase
  }

  /** Whether repeating it has the effect of doing it once (RFC 9110, 9.2.2), so that a client may send it again on a
    * fresh connection when one it reused fails before any response.
    */
  def idempotent: Boolean = this match {
    case Get | Head | Put | Delete | Options | Trace => true
    case _                                           => false
  }

  override def toString: String = name

  // `Other("GET")` would not equal `Get`: a known method is only its own case.
  if isInstanceOf[Other] then require(!Method.knownNames.contains(name), s"$name is a known method: use its case")
}

/** The HTTP versions spoken here: 1.1, and 1.0 for a server's older clients. */
enum HttpVersion(val text: String) {
  case Http10 extends HttpVersion("HTTP/1.0")
  case Http11 extends HttpVersion("HTTP/1.1")

  override def toString: String = text
}

object Method {

  /** The names of the methods with cases of their own. */
  private val knownNames = Set("GET", "HEAD", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "TRACE", "CONNECT")

  /** The method a request line's token names. */
  def parse(token: String): Method = token match {
    case "GET"     => Get
    case "HEAD"    => Head
    case "POST"    => Post
    case "PUT"     => Put
    case "DELETE"  => Delete
    case "PATCH"   => Patch
    case "OPTIONS" => Options
    case "TRACE"   => Trace
    case "CONNECT" => Connect
    case other     =>
      require(Headers.isToken(other), s"Not a method token: $other")
      Other(other)
  }
}
