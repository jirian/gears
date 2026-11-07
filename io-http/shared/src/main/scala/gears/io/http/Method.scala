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
}

object Method {

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
