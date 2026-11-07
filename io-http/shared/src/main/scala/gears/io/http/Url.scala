package gears.io.http

import java.net.URI
import java.net.URISyntaxException
import java.nio.charset.StandardCharsets

/** An absolute `http`, `https`, `ws` or `wss` URL, as a client sends a request to: its host lower-cased, its port
  * explicit or the scheme's, its path and query still percent-encoded, as they go on the request line, with no
  * fragment. `Url.parse` checks it (RFC 3986), and `resolve` follows a reference from it, as a redirect's `Location`.
  */
final case class Url private (scheme: String, host: String, port: Int, rawPath: String, rawQuery: Option[String]) {

  /** Whether it is `https` or `wss`. */
  def secure: Boolean = scheme == "https" || scheme == "wss"

  /** The request target: the path, and the query after a `?` if there is one. */
  def target: String = rawQuery.fold(rawPath)(q => s"$rawPath?$q")

  /** The `Host` header's value: the host, and the port unless it is the scheme's. */
  def authority: String = {
    val h = if host.contains(':') then s"[$host]" else host
    if port == Url.defaultPort(scheme) then h else s"$h:$port"
  }

  /** The scheme, host and port: whether two URLs are the same origin, for credentials and pooled connections. */
  def origin: (String, String, Int) = (scheme, host, port)

  /** The path, percent-decoded. */
  def path: String = Url.decode(rawPath, plusIsSpace = false)

  /** The query's parameters, decoded, in order. */
  def query: Seq[(String, String)] = rawQuery.fold(Seq.empty)(Url.parseQuery)

  /** This URL with `name` and `value` appended to its query, encoded. */
  def withQueryParam(name: String, value: String): Url = {
    val pair = s"${Url.encodeQueryPart(name)}=${Url.encodeQueryPart(value)}"
    copy(rawQuery = Some(rawQuery.fold(pair)(q => if q.isEmpty then pair else s"$q&$pair")))
  }

  /** The URL `reference` names, relative to this one (RFC 3986, 5.2), as a redirect's `Location` is: absolute, with
    * another scheme or host (`//cdn.example/x`), or a path or query on this one.
    */
  def resolve(reference: String): Url =
    try Url.fromUri(new URI(toString).resolve(new URI(reference.trim)).normalize())
    catch case e: URISyntaxException => throw new IllegalArgumentException(s"Not a URL reference: $reference", e)

  override def toString: String = s"$scheme://$authority$target"
}

object Url {

  /** `text` as a URL; an `IllegalArgumentException` if it is not an absolute `http`, `https`, `ws` or `wss` one with a
    * host, or if it has user information, which a URL must not carry (RFC 9110, 4.2.4): credentials go in a header.
    */
  def parse(text: String): Url =
    try fromUri(new URI(text.trim))
    catch case e: URISyntaxException => throw new IllegalArgumentException(s"Not a URL: $text", e)

  private def fromUri(uri: URI): Url = {
    val scheme =
      Option(uri.getScheme).map(_.toLowerCase).getOrElse(throw new IllegalArgumentException(s"No scheme in $uri"))
    if !Set("http", "https", "ws", "wss").contains(scheme) then
      throw new IllegalArgumentException(s"Not an http, https, ws or wss URL: $uri")
    if uri.getRawUserInfo != null then throw new IllegalArgumentException(s"A URL with user information: $uri")
    val rawHost = uri.getHost
    if rawHost == null || rawHost.isEmpty then throw new IllegalArgumentException(s"No host in $uri")
    val host = (if rawHost.startsWith("[") then rawHost.substring(1, rawHost.length - 1) else rawHost).toLowerCase
    val port = if uri.getPort == -1 then defaultPort(scheme) else uri.getPort
    if port < 1 || port > 65535 then throw new IllegalArgumentException(s"A port is 1 to 65535, not $port, in $uri")
    // Characters outside ASCII, which `URI` takes in a path or query as they are, go out percent-encoded as UTF-8.
    val ascii = new URI(uri.toASCIIString)
    val path = Option(ascii.getRawPath).filter(_.nonEmpty).getOrElse("/")
    new Url(scheme, host, port, path, Option(ascii.getRawQuery))
  }

  def defaultPort(scheme: String): Int = if scheme == "https" || scheme == "wss" then 443 else 80

  /** The parameters of a raw query, `+` and percent-escapes decoded; an `IllegalArgumentException` if an escape is
    * malformed.
    */
  def parseQuery(raw: String): Seq[(String, String)] =
    if raw.isEmpty then Seq.empty
    else
      Text
        .split(raw, '&')
        .iterator
        .filter(_.nonEmpty)
        .map { pair =>
          val eq = pair.indexOf('=')
          if eq < 0 then (decode(pair, plusIsSpace = true), "")
          else (decode(pair.substring(0, eq), plusIsSpace = true), decode(pair.substring(eq + 1), plusIsSpace = true))
        }
        .toSeq

  /** `s` with its percent-escapes decoded as UTF-8, and `+` as a space if `plusIsSpace`; an `IllegalArgumentException`
    * if an escape is malformed or the bytes are not UTF-8.
    */
  def decode(s: String, plusIsSpace: Boolean): String =
    if s.indexOf('%') < 0 && (!plusIsSpace || s.indexOf('+') < 0) then s
    else {
      val out = new java.io.ByteArrayOutputStream(s.length)
      var i = 0
      while i < s.length do {
        val c = s.charAt(i)
        if c == '%' then {
          if i + 2 >= s.length then throw new IllegalArgumentException(s"A truncated percent-escape in $s")
          val hi = Character.digit(s.charAt(i + 1), 16)
          val lo = Character.digit(s.charAt(i + 2), 16)
          if hi < 0 || lo < 0 then throw new IllegalArgumentException(s"A malformed percent-escape in $s")
          out.write(hi * 16 + lo)
          i += 3
        } else {
          if c == '+' && plusIsSpace then out.write(' ')
          else if c < 0x80 then out.write(c)
          else out.write(c.toString.getBytes(StandardCharsets.UTF_8))
          i += 1
        }
      }
      val decoder = StandardCharsets.UTF_8
        .newDecoder()
        .onMalformedInput(java.nio.charset.CodingErrorAction.REPORT)
        .onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT)
      try decoder.decode(java.nio.ByteBuffer.wrap(out.toByteArray)).toString
      catch case e: java.nio.charset.CharacterCodingException => throw new IllegalArgumentException(s"Not UTF-8: $s", e)
    }

  private val hex = "0123456789ABCDEF"

  /** `s` percent-encoded as UTF-8, every byte but the unreserved ones (RFC 3986, 2.3) and those in `keep`. */
  def encode(s: String, keep: String = ""): String = {
    val bytes = s.getBytes(StandardCharsets.UTF_8)
    val out = new StringBuilder(bytes.length)
    for b <- bytes do {
      val c = (b & 0xff).toChar
      if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || "-._~".indexOf(c) >= 0 ||
        (c < 0x80 && keep.indexOf(c) >= 0)
      then out.append(c)
      else out.append('%').append(hex((b >> 4) & 15)).append(hex(b & 15))
    }
    out.toString
  }

  /** A path segment or a query's name or value, encoded to go between delimiters. */
  def encodeSegment(s: String): String = encode(s)
  private[http] def encodeQueryPart(s: String): String = encode(s)
}

/** The `url"..."` interpolator: an absolute URL whose interpolated values are percent-encoded, so that a value cannot
  * change the URL's shape: `url"https://api.example.com/users/$name?q=$query"` names one user, whatever `name` holds.
  */
extension (sc: StringContext) {
  def url(args: Any*): Url = {
    val parts = sc.parts.iterator
    val out = new StringBuilder(parts.next())
    for arg <- args do {
      out.append(Url.encodeSegment(String.valueOf(arg)))
      out.append(parts.next())
    }
    Url.parse(out.toString)
  }
}
