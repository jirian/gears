package gears.io.http

/** An immutable list of header fields, in order. Names compare case-insensitively, and a name may repeat. Every way of
  * building one checks each field (RFC 9110, 5.1 and 5.5): a name is a token, and a value has no CR, LF or NUL, so that
  * a header cannot smuggle a line into a request or a response.
  */
final class Headers private (private val fields: Vector[(String, String)]) {

  /** The first value of `name`. */
  def get(name: String): Option[String] = {
    var i = 0
    while i < fields.length do {
      val (k, v) = fields(i)
      if k.equalsIgnoreCase(name) then return Some(v)
      i += 1
    }
    None
  }

  /** Every value of `name`, in order. */
  def getAll(name: String): Seq[String] = fields.collect { case (k, v) if k.equalsIgnoreCase(name) => v }

  /** The comma-separated elements of every value of `name`, trimmed, such as `Connection`'s options. */
  def tokens(name: String): Seq[String] =
    getAll(name).flatMap(Text.split(_, ',').iterator.map(_.trim).filter(_.nonEmpty))

  /** Whether `name` has `token` among its elements, compared case-insensitively. */
  def hasToken(name: String, token: String): Boolean = tokens(name).exists(_.equalsIgnoreCase(token))

  def contains(name: String): Boolean = get(name).isDefined

  /** These headers and one more, after any others of the same name. */
  def add(name: String, value: String): Headers = {
    Headers.check(name, value)
    new Headers(fields :+ (name -> value))
  }

  /** These headers with every `name` replaced by this one value. */
  def set(name: String, value: String): Headers = {
    Headers.check(name, value)
    new Headers(fields.filterNot(_._1.equalsIgnoreCase(name)) :+ (name -> value))
  }

  /** These headers with `name` set to `value` unless it has a value already. */
  def setIfAbsent(name: String, value: String): Headers = if contains(name) then this else set(name, value)

  def remove(name: String): Headers =
    if contains(name) then new Headers(fields.filterNot(_._1.equalsIgnoreCase(name))) else this

  /** These headers, then `other`'s. */
  def ++(other: Headers): Headers = new Headers(fields ++ other.fields)

  def iterator: Iterator[(String, String)] = fields.iterator
  def size: Int = fields.size
  def isEmpty: Boolean = fields.isEmpty

  override def equals(other: Any): Boolean = other match {
    case h: Headers => h.fields == fields
    case _          => false
  }
  override def hashCode: Int = fields.hashCode
  override def toString: String = fields.map((k, v) => s"$k: $v").mkString("Headers(", ", ", ")")
}

object Headers {
  val empty: Headers = new Headers(Vector.empty)

  def apply(fields: (String, String)*): Headers = {
    fields.foreach(check(_, _))
    new Headers(fields.toVector)
  }

  /** Headers already checked, as the parser builds them. */
  private[http] def trusted(fields: Vector[(String, String)]): Headers = new Headers(fields)

  private val separators = "()<>@,;:\\\"/[]?={} \t"

  /** Whether `s` is a token: one or more printable ASCII characters, none a separator. */
  def isToken(s: String): Boolean =
    s.nonEmpty && s.forall(c => c > ' ' && c < 127 && separators.indexOf(c) < 0)

  /** Whether `value` may be a field's value (RFC 9110, 5.5): visible ASCII, spaces, tabs and the bytes 0x80 to 0xFF,
    * each one byte on the wire. No other control character, CR, LF and NUL among them, and no character above 0xFF,
    * which would not fit in a byte: U+010A would otherwise go out as LF.
    */
  def isValue(value: String): Boolean =
    value.forall(c => c == '\t' || (c >= ' ' && c != '\u007f' && c <= '\u00ff'))

  /** Throws an `IllegalArgumentException` unless `name` and `value` make a field. */
  def check(name: String, value: String): Unit = {
    if !isToken(name) then throw new IllegalArgumentException(s"Not a header name: '$name'")
    if !isValue(value) then
      throw new IllegalArgumentException(s"A value of header '$name' with a control character or one above U+00FF")
  }
}
