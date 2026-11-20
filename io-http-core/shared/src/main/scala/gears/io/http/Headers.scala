package gears.io.http

final class Headers private (private val fields: Array[String]) {

  def get(name: String): Option[String] = {
    val i = indexOf(name, 0)
    if i < 0 then None else Some(fields(i + 1))
  }

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

  def tokens(name: String): Seq[String] =
    getAll(name).flatMap(Text.split(_, ',').iterator.map(_.trim).filter(_.nonEmpty))

  def hasToken(name: String, token: String): Boolean = {
    var i = indexOf(name, 0)
    while i >= 0 do {
      if Text.hasElement(fields(i + 1), token) then return true
      i = indexOf(name, i + 2)
    }
    false
  }

  def contains(name: String): Boolean = indexOf(name, 0) >= 0

  def add(name: String, value: String): Headers = {
    Headers.check(name, value)
    val more = java.util.Arrays.copyOf(fields, fields.length + 2)
    more(fields.length) = name
    more(fields.length + 1) = value
    new Headers(more)
  }

  def set(name: String, value: String): Headers = {
    Headers.check(name, value)
    without(name).add(name, value)
  }

  def setIfAbsent(name: String, value: String): Headers = if contains(name) then this else set(name, value)

  def remove(name: String): Headers = if contains(name) then without(name) else this

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

  private[http] def name(i: Int): String = fields(2 * i)
  private[http] def value(i: Int): String = fields(2 * i + 1)

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

  private[http] def contentType(value: String): Headers =
    if value eq Body.PlainText then plainText
    else {
      if !isValue(value) then
        throw new IllegalArgumentException(
          "A value of header 'Content-Type' with a control character or one above U+00FF"
        )
      new Headers(Array("Content-Type", value))
    }

  private[http] def trusted(fields: Array[String]): Headers = new Headers(fields)

  def isToken(s: String): Boolean = {
    var i = 0
    while i < s.length && Text.isTokenChar(s.charAt(i)) do i += 1
    s.nonEmpty && i == s.length
  }

  def isValue(value: String): Boolean = {
    var i = 0
    while i < value.length && { val c = value.charAt(i); c == '\t' || (c >= ' ' && c != '\u007f' && c <= '\u00ff') } do
      i += 1
    i == value.length
  }

  def check(name: String, value: String): Unit = {
    if !isToken(name) then throw new IllegalArgumentException(s"Not a header name: '$name'")
    if !isValue(value) then
      throw new IllegalArgumentException(s"A value of header '$name' with a control character or one above U+00FF")
  }
}
