package gears.io.http

/** An immutable list of header fields, in order. Names compare case-insensitively, and a name may repeat. Every way of
  * building one checks each field (RFC 9110, 5.1 and 5.5): a name is a token, and a value has no CR, LF or NUL, so that
  * a header cannot smuggle a line into a request or a response.
  */
final class Headers private (private val fields: Array[String]) {
  // Names at even indices, each one's value after it; never changed once built.

  /** The first value of `name`. */
  def get(name: String): Option[String] = {
    val i = indexOf(name, 0)
    if i < 0 then None else Some(fields(i + 1))
  }

  /** Every value of `name`, in order. */
  def getAll(name: String): Seq[String] = {
    var i = indexOf(name, 0)
    if i < 0 then Nil
    else {
      val out = List.newBuilder[String]
      while i >= 0 do {
        out += fields(i + 1)
        i = indexOf(name, i + 2)
      }
      out.result()
    }
  }

  /** The comma-separated elements of every value of `name`, trimmed, such as `Connection`'s options. */
  def tokens(name: String): Seq[String] =
    getAll(name).flatMap(Text.split(_, ',').iterator.map(_.trim).filter(_.nonEmpty))

  /** Whether `name` has `token` among its elements, compared case-insensitively. */
  def hasToken(name: String, token: String): Boolean = {
    var i = indexOf(name, 0)
    while i >= 0 do {
      if Text.hasElement(fields(i + 1), token) then return true
      i = indexOf(name, i + 2)
    }
    false
  }

  def contains(name: String): Boolean = indexOf(name, 0) >= 0

  /** These headers and one more, after any others of the same name. */
  def add(name: String, value: String): Headers = {
    Headers.check(name, value)
    val more = java.util.Arrays.copyOf(fields, fields.length + 2)
    more(fields.length) = name
    more(fields.length + 1) = value
    new Headers(more)
  }

  /** These headers with every `name` replaced by this one value. */
  def set(name: String, value: String): Headers = {
    Headers.check(name, value)
    without(name).add(name, value)
  }

  /** These headers with `name` set to `value` unless it has a value already. */
  def setIfAbsent(name: String, value: String): Headers = if contains(name) then this else set(name, value)

  def remove(name: String): Headers = if contains(name) then without(name) else this

  /** These headers, then `other`'s. */
  def ++(other: Headers): Headers =
    if other.fields.length == 0 then this
    else if fields.length == 0 then other
    else {
      val both = java.util.Arrays.copyOf(fields, fields.length + other.fields.length)
      System.arraycopy(other.fields, 0, both, fields.length, other.fields.length)
      new Headers(both)
    }

  def iterator: Iterator[(String, String)] = Iterator.range(0, size).map(i => (fields(2 * i), fields(2 * i + 1)))
  def size: Int = fields.length / 2
  def isEmpty: Boolean = fields.length == 0

  /** The `i`th field's name and value, for the writer, which needs no pair. */
  private[http] def name(i: Int): String = fields(2 * i)
  private[http] def value(i: Int): String = fields(2 * i + 1)

  /** The index in `fields` of the first `name` at or after `from`, or -1. */
  private def indexOf(name: String, from: Int): Int = {
    var i = from
    while i < fields.length do {
      if Text.sameName(fields(i), name) then return i
      i += 2
    }
    -1
  }

  private def without(name: String): Headers = {
    val kept = new Array[String](fields.length)
    var n = 0
    var i = 0
    while i < fields.length do {
      if !Text.sameName(fields(i), name) then {
        kept(n) = fields(i)
        kept(n + 1) = fields(i + 1)
        n += 2
      }
      i += 2
    }
    new Headers(java.util.Arrays.copyOf(kept, n))
  }

  override def equals(other: Any): Boolean = other match {
    case h: Headers => java.util.Arrays.equals(h.fields.asInstanceOf[Array[AnyRef]], fields.asInstanceOf[Array[AnyRef]])
    case _          => false
  }
  override def hashCode: Int = java.util.Arrays.hashCode(fields.asInstanceOf[Array[AnyRef]])
  override def toString: String = iterator.map((k, v) => s"$k: $v").mkString("Headers(", ", ", ")")
}

object Headers {
  val empty: Headers = new Headers(new Array[String](0))

  def apply(fields: (String, String)*): Headers = {
    val flat = new Array[String](2 * fields.length)
    var i = 0
    for (k, v) <- fields do {
      check(k, v)
      flat(i) = k
      flat(i + 1) = v
      i += 2
    }
    new Headers(flat)
  }

  private val plainText = new Headers(Array("Content-Type", Body.PlainText))

  /** A `Content-Type` of `value` alone, the most common headers, built with no check of the name, a token. */
  private[http] def contentType(value: String): Headers =
    if value eq Body.PlainText then plainText
    else {
      if !isValue(value) then
        throw new IllegalArgumentException(
          "A value of header 'Content-Type' with a control character or one above U+00FF"
        )
      new Headers(Array("Content-Type", value))
    }

  /** Headers already checked, as the parser builds them. */
  private[http] def trusted(fields: Array[String]): Headers = new Headers(fields)

  /** Whether `s` is a token: one or more printable ASCII characters, none a separator. */
  def isToken(s: String): Boolean = {
    var i = 0
    while i < s.length && Text.isTokenChar(s.charAt(i)) do i += 1
    s.nonEmpty && i == s.length
  }

  /** Whether `value` may be a field's value (RFC 9110, 5.5): visible ASCII, spaces, tabs and the bytes 0x80 to 0xFF,
    * each one byte on the wire. No other control character, CR, LF and NUL among them, and no character above 0xFF,
    * which would not fit in a byte: U+010A would otherwise go out as LF.
    */
  def isValue(value: String): Boolean = {
    var i = 0
    while i < value.length && { val c = value.charAt(i); c == '\t' || (c >= ' ' && c != '\u007f' && c <= '\u00ff') } do
      i += 1
    i == value.length
  }

  /** Throws an `IllegalArgumentException` unless `name` and `value` make a field. */
  def check(name: String, value: String): Unit = {
    if !isToken(name) then throw new IllegalArgumentException(s"Not a header name: '$name'")
    if !isValue(value) then
      throw new IllegalArgumentException(s"A value of header '$name' with a control character or one above U+00FF")
  }
}
