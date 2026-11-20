package gears.io.http

import java.net.URI
import java.net.URISyntaxException
import java.nio.charset.StandardCharsets

final case class Url private (scheme: String, host: String, port: Int, rawPath: String, rawQuery: Option[String]) {

  def secure: Boolean = scheme == "https" || scheme == "wss"

  def target: String = rawQuery.fold(rawPath)(q => s"$rawPath?$q")

  def authority: String = {
    val h = if host.contains(':') then s"[$host]" else host
    if port == Url.defaultPort(scheme) then h else s"$h:$port"
  }

  def origin: Url.Origin = Url.Origin(scheme, host, port)

  def path: String = Url.decode(rawPath, plusIsSpace = false)

  def query: Seq[(String, String)] = rawQuery.fold(Seq.empty)(Url.parseQuery)

  def withQueryParam(name: String, value: String): Url = {
    val pair = s"${Url.encodeQueryPart(name)}=${Url.encodeQueryPart(value)}"
    copy(rawQuery = Some(rawQuery.fold(pair)(q => if q.isEmpty then pair else s"$q&$pair")))
  }

  def resolve(reference: String): Url =
    try Url.fromUri(new URI(toString).resolve(new URI(reference.trim)).normalize())
    catch case e: URISyntaxException => throw new IllegalArgumentException(s"Not a URL reference: $reference", e)

  override def toString: String = s"$scheme://$authority$target"
}

object Url {

  final case class Origin(scheme: String, host: String, port: Int)

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
    val (host, port) =
      if rawHost == null || rawHost.isEmpty then {
        val authority = uri.getRawAuthority
        if authority == null || authority.isEmpty then throw new IllegalArgumentException(s"No host in $uri")
        splitAuthority(scheme, authority, what => new IllegalArgumentException(s"$what in $uri"))
      } else {
        val port = if uri.getPort == -1 then defaultPort(scheme) else uri.getPort
        if port < 1 || port > 65535 then throw new IllegalArgumentException(s"A port is 1 to 65535, not $port, in $uri")
        ((if rawHost.startsWith("[") then rawHost.substring(1, rawHost.length - 1) else rawHost).toLowerCase, port)
      }
    val ascii = new URI(uri.toASCIIString)
    val path = Option(ascii.getRawPath).filter(_.nonEmpty).getOrElse("/")
    new Url(scheme, host, port, path, Option(ascii.getRawQuery))
  }

  def defaultPort(scheme: String): Int = if scheme == "https" || scheme == "wss" then 443 else 80

  private[http] def ofRequest(scheme: String, authority: String, target: String): Url = {
    if !originForm(target) then throw new Wire.Malformed(Status.BadRequest, s"A malformed request target: $target")
    val (host, port) = splitAuthority(scheme, authority, what => new Wire.Malformed(Status.BadRequest, what))
    val question = target.indexOf('?')
    val path = if question < 0 then target else target.substring(0, question)
    val query = if question < 0 then None else Some(target.substring(question + 1))
    new Url(scheme, host, port, path, query)
  }

  private def splitAuthority(scheme: String, authority: String, malformed: String => Exception): (String, Int) = {
    def refused = malformed(s"A malformed host: $authority")
    var host = authority
    var port = ""
    if authority.startsWith("[") then {
      val close = authority.indexOf(']')
      val after = close + 1
      if close < 0 || after < authority.length && authority.charAt(after) != ':' then throw refused
      host = authority.substring(1, close)
      if !host.contains(':') || gears.io.net.IpLiteral.parse(host).isEmpty then throw refused
      if after < authority.length then port = authority.substring(after + 1)
    } else {
      val colon = authority.lastIndexOf(':')
      if colon >= 0 then {
        host = authority.substring(0, colon)
        port = authority.substring(colon + 1)
      }
      if host.isEmpty || !Text.forall(host)(c => c < 128 && Character.isLetterOrDigit(c) || "-._~".indexOf(c) >= 0)
      then throw refused
    }
    val number =
      if port.isEmpty then defaultPort(scheme)
      else if port.length <= 5 && Text.forall(port)(c => c >= '0' && c <= '9') && port.toInt >= 1 && port.toInt <= 65535
      then port.toInt
      else throw refused
    (Text.asciiLower(host), number)
  }

  private def originForm(target: String): Boolean = {
    if target.isEmpty || target.charAt(0) != '/' then return false
    var i = 0
    while i < target.length do {
      val c = target.charAt(i)
      if c == '%' then {
        if i + 2 >= target.length || !isHex(target.charAt(i + 1)) || !isHex(target.charAt(i + 2)) then return false
        i += 2
      } else if c >= 128 || !targetChars(c) then return false
      i += 1
    }
    true
  }

  private def isHex(c: Char): Boolean = c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'

  private val targetChars: Array[Boolean] = {
    val ok = new Array[Boolean](128)
    for c <- ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9') ++ "-._~!$&'()*+,;=:@/" do ok(c) = true
    ok('?') = true
    ok
  }

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

  def decode(s: String, plusIsSpace: Boolean): String =
    if s.indexOf('%') < 0 && (!plusIsSpace || s.indexOf('+') < 0) then s
    else {
      val out = new java.io.ByteArrayOutputStream(s.length)
      var i = 0
      while i < s.length do {
        val c = s.charAt(i)
        if c == '%' then {
          if i + 2 >= s.length then throw new IllegalArgumentException(s"A truncated percent-escape in $s")
          val hi = hexDigit(s.charAt(i + 1))
          val lo = hexDigit(s.charAt(i + 2))
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

  private def hexDigit(c: Char): Int =
    if c >= '0' && c <= '9' then c - '0'
    else if (c | 0x20) >= 'a' && (c | 0x20) <= 'f' then (c | 0x20) - 'a' + 10
    else -1

  private val hex = "0123456789ABCDEF"

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

  def encodeSegment(s: String): String = encode(s)
  private[http] def encodeQueryPart(s: String): String = encode(s)
}

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
