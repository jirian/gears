package gears.io.http

enum Method {
  case Get, Head, Post, Put, Delete, Patch, Options, Trace, Connect
  case Other(token: String)

  def name: String = this match {
    case Other(token) => token
    case known        => known.productPrefix.toUpperCase
  }

  def idempotent: Boolean = this match {
    case Get | Head | Put | Delete | Options | Trace => true
    case _                                           => false
  }

  override def toString: String = name

  if isInstanceOf[Other] then require(!Method.knownNames.contains(name), s"$name is a known method: use its case")
}

enum HttpVersion(val text: String) {
  case Http10 extends HttpVersion("HTTP/1.0")
  case Http11 extends HttpVersion("HTTP/1.1")

  override def toString: String = text
}

object Method {

  private val knownNames = Set("GET", "HEAD", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "TRACE", "CONNECT")

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
