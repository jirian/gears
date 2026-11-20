package gears.io.http

import java.nio.charset.StandardCharsets

private[http] object Text {

  def split(text: String, separator: Char): List[String] = {
    val out = List.newBuilder[String]
    var start = 0
    var at = text.indexOf(separator)
    while at >= 0 do {
      out += text.substring(start, at)
      start = at + 1
      at = text.indexOf(separator, start)
    }
    out += text.substring(start)
    out.result()
  }

  def splitCrlf(text: String): List[String] = {
    val out = List.newBuilder[String]
    var start = 0
    var at = text.indexOf("\r\n")
    while at >= 0 do {
      out += text.substring(start, at)
      start = at + 2
      at = text.indexOf("\r\n", start)
    }
    out += text.substring(start)
    out.result()
  }

  def sameName(a: String, b: String): Boolean =
    (a eq b) || a.length == b.length && regionSame(a, 0, b, b.length)

  private def regionSame(a: String, at: Int, b: String, n: Int): Boolean = {
    var i = 0
    while i < n do {
      val x = a.charAt(at + i)
      val y = b.charAt(i)
      if x != y then {
        if (x | y) < 128 then {
          val lx = x | 0x20
          if lx != (y | 0x20) || lx < 'a' || lx > 'z' then return false
        } else {
          val ux = Character.toUpperCase(x)
          val uy = Character.toUpperCase(y)
          if ux != uy && Character.toLowerCase(ux) != Character.toLowerCase(uy) then return false
        }
      }
      i += 1
    }
    true
  }

  def hasElement(value: String, token: String): Boolean = {
    var start = 0
    while start <= value.length do {
      var end = value.indexOf(',', start)
      if end < 0 then end = value.length
      var s = start
      var e = end
      while s < e && value.charAt(s) <= ' ' do s += 1
      while e > s && value.charAt(e - 1) <= ' ' do e -= 1
      if e > s && e - s == token.length && regionSame(value, s, token, token.length) then return true
      start = end + 1
    }
    false
  }

  private val tokenChars: Array[Boolean] = {
    val t = new Array[Boolean](128)
    for c <- '!' to '~' do t(c) = "()<>@,;:\\\"/[]?={}".indexOf(c) < 0
    t
  }

  def isTokenChar(c: Char): Boolean = c < 128 && tokenChars(c)

  inline def forall(s: String)(inline p: Char => Boolean): Boolean = {
    var i = 0
    while i < s.length && p(s.charAt(i)) do i += 1
    i == s.length
  }

  def encode(text: String, charset: java.nio.charset.Charset): Array[Byte] = {
    val out = new Array[Byte](text.length)
    var i = 0
    while i < out.length && text.charAt(i) < 128 do {
      out(i) = text.charAt(i).toByte
      i += 1
    }
    val ascii = charset == StandardCharsets.UTF_8 || charset == StandardCharsets.ISO_8859_1 ||
      charset == StandardCharsets.US_ASCII
    if i == out.length && ascii then out else text.getBytes(charset)
  }

  def asciiLower(s: String): String = {
    var i = 0
    while i < s.length && { val c = s.charAt(i); c < 128 && (c < 'A' || c > 'Z') } do i += 1
    if i == s.length then s else s.toLowerCase
  }
}
