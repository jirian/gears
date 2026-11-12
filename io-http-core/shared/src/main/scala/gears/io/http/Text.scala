package gears.io.http

/** Splitting header and path text without regular expressions: `String.split`, which Scala's `split(Char)` calls too,
  * runs a regex on Scala Native for every call, where the JVM has a fast path for one literal character; on the request
  * path that was a third of a server's time.
  */
private[http] object Text {

  /** `text`'s parts between each `separator`, every one kept, empty and trailing ones too, as `split(sep, -1)`. */
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

  /** `text`'s lines between each CRLF, every one kept, as `split("\r\n", -1)`. */
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
}
