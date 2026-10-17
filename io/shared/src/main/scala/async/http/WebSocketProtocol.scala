package gears.async.http

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.charset.{CodingErrorAction, StandardCharsets}

private[http] object WebSocketProtocol:
  private val alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

  def base64(bytes: Array[Byte]): String =
    val out = new StringBuilder
    var i = 0
    while i < bytes.length do
      val n = ((bytes(i) & 255) << 16) |
        (if i + 1 < bytes.length then (bytes(i + 1) & 255) << 8 else 0) |
        (if i + 2 < bytes.length then bytes(i + 2) & 255 else 0)
      out.append(alphabet((n >>> 18) & 63)).append(alphabet((n >>> 12) & 63))
      out.append(if i + 1 < bytes.length then alphabet((n >>> 6) & 63) else '=')
      out.append(if i + 2 < bytes.length then alphabet(n & 63) else '=')
      i += 3
    out.toString

  def validKey(key: String): Boolean =
    key.length == 24 && key.endsWith("==") && key.take(22).forall(alphabet.contains(_)) &&
      (alphabet.indexOf(key(21)) & 15) == 0

  def accept(key: String): String =
    val input = (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").getBytes(StandardCharsets.US_ASCII)
    val padded = new Array[Byte](((input.length + 9 + 63) / 64) * 64)
    System.arraycopy(input, 0, padded, 0, input.length)
    padded(input.length) = 0x80.toByte
    ByteBuffer.wrap(padded).putLong(padded.length - 8, input.length.toLong * 8)
    val h = Array(0x67452301, 0xefcdab89, 0x98badcfe, 0x10325476, 0xc3d2e1f0)
    val words = new Array[Int](80)
    val data = ByteBuffer.wrap(padded)
    while data.hasRemaining do
      var i = 0
      while i < 16 do
        words(i) = data.getInt()
        i += 1
      while i < 80 do
        words(i) = Integer.rotateLeft(words(i - 3) ^ words(i - 8) ^ words(i - 14) ^ words(i - 16), 1)
        i += 1
      var a = h(0)
      var b = h(1)
      var c = h(2)
      var d = h(3)
      var e = h(4)
      i = 0
      while i < 80 do
        val (f, k) =
          if i < 20 then ((b & c) | (~b & d), 0x5a827999)
          else if i < 40 then (b ^ c ^ d, 0x6ed9eba1)
          else if i < 60 then ((b & c) | (b & d) | (c & d), 0x8f1bbcdc)
          else (b ^ c ^ d, 0xca62c1d6)
        val next = Integer.rotateLeft(a, 5) + f + e + k + words(i)
        e = d
        d = c
        c = Integer.rotateLeft(b, 30)
        b = a
        a = next
        i += 1
      h(0) += a
      h(1) += b
      h(2) += c
      h(3) += d
      h(4) += e
    val digest = ByteBuffer.allocate(20)
    h.foreach(digest.putInt(_))
    base64(digest.array())

  def tokens(headers: Headers, name: String): Seq[String] =
    headers.getAll(name).flatMap(_.split(",").map(_.trim))

  def hasToken(headers: Headers, name: String, token: String): Boolean =
    tokens(headers, name).exists(_.equalsIgnoreCase(token))

  def validToken(value: String): Boolean = HeaderSyntax.isValidName(value)

  def utf8(bytes: Array[Byte]): String =
    try
      StandardCharsets.UTF_8
        .newDecoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT)
        .decode(ByteBuffer.wrap(bytes))
        .toString
    catch case e: java.nio.charset.CharacterCodingException => throw IOException("Invalid WebSocket UTF-8", e)

  def validCloseCode(code: Int): Boolean =
    (code >= 1000 && code <= 1014 && code != 1004 && code != 1005 && code != 1006) ||
      (code >= 3000 && code <= 4999)
