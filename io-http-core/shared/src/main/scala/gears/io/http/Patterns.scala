package gears.io.http

import scala.quoted.*

/** Route patterns read when compiling (see `GET`): a pattern's `{name}` and `{name:Type}` segments become the names and
  * types of the named tuple its handler is given, so that a misspelt name, or a type no decoder reads, is an error
  * where the route is written rather than a failure when a request comes.
  */
private[http] object Patterns {

  /** The types a segment may name, each a `ParamDecoder`'s; `{name}` alone is a `String`. */
  val typeNames: List[String] = List("String", "Int", "Long", "Double", "Boolean", "BigDecimal", "UUID")

  /** A `{name}` or `{name:Type}` segment, or `{name...}`, which is always a `String`. */
  final case class Param(name: String, tpe: String)

  /** `pattern` as the route tree takes it, each type removed, and its segments' parameters in order; or what is wrong
    * with it.
    */
  def parse(pattern: String): Either[String, (String, List[Param])] =
    if !pattern.startsWith("/") then Left(s"A pattern starts with /: $pattern")
    else {
      val parts = if pattern == "/" then Nil else pattern.substring(1).split("/", -1).toList
      val last = parts.length - 1
      parts.zipWithIndex.foldLeft[Either[String, (List[String], List[Param])]](Right((Nil, Nil))) {
        case (Right((segments, params)), (part, i)) =>
          if !part.startsWith("{") then {
            if part.contains('{') || part.contains('}') then Left(s"A segment is a literal or one {name}: $part")
            else Right((segments :+ part, params))
          } else if !part.endsWith("}") then Left(s"A segment is a literal or one {name}: $part")
          else {
            val inner = part.substring(1, part.length - 1)
            val (name, tpe, rest) =
              if inner.endsWith("...") then (inner.dropRight(3), "String", true)
              else
                inner.indexOf(':') match {
                  case -1 => (inner, "String", false)
                  case at => (inner.substring(0, at), inner.substring(at + 1), false)
                }
            if name.isEmpty || !Headers.isToken(name) then Left(s"Not a segment name: $name")
            else if rest && i != last then Left(s"A {name...} segment comes last, in $pattern")
            else if !typeNames.contains(tpe) then
              Left(s"No decoder for {$name:$tpe}: the types are ${typeNames.mkString(", ")}")
            else if params.exists(_.name == name) then Left(s"Two segments named $name, in $pattern")
            else Right((segments :+ (if rest then s"{$name...}" else s"{$name}"), params :+ Param(name, tpe)))
          }
        case (wrong, _) => wrong
      } match {
        case Right((segments, params)) => Right((segments.mkString("/", "/", ""), params))
        case Left(message)             => Left(message)
      }
    }

  /** An `Endpoint` for `method` on the literal `pattern`, or, if it has parameters, an `EndpointWith` typed by them. */
  def endpoint(method: Expr[Method], pattern: Expr[String])(using Quotes): Expr[Any] = {
    import quotes.reflect.*
    val text = pattern.value.getOrElse(
      report.errorAndAbort("A route's pattern is a literal, read when compiling; `route` takes one computed", pattern)
    )
    parse(text) match {
      case Left(message)               => report.errorAndAbort(message, pattern)
      case Right((normalized, Nil))    => '{ new Endpoint($method, ${ Expr(normalized) }) }
      case Right((normalized, params)) =>
        def tuple(types: List[TypeRepr]): TypeRepr =
          types.foldRight(TypeRepr.of[EmptyTuple])((t, rest) => TypeRepr.of[*:].appliedTo(List(t, rest)))
        def valueType(name: String): TypeRepr = name match {
          case "Int"        => TypeRepr.of[Int]
          case "Long"       => TypeRepr.of[Long]
          case "Double"     => TypeRepr.of[Double]
          case "Boolean"    => TypeRepr.of[Boolean]
          case "BigDecimal" => TypeRepr.of[BigDecimal]
          case "UUID"       => TypeRepr.of[java.util.UUID]
          case _            => TypeRepr.of[String]
        }
        val names = tuple(params.map(p => ConstantType(StringConstant(p.name))))
        val values = tuple(params.map(p => valueType(p.tpe)))
        (names.asType, values.asType) match {
          case ('[type n <: Tuple; n], '[type v <: Tuple; v]) =>
            '{
              new EndpointWith[n, v](
                $method,
                ${ Expr(normalized) },
                ${ Expr(params.map(_.name)) },
                ${ Expr(params.map(_.tpe)) }
              )
            }
          case _ => report.errorAndAbort(s"The parameters of $text are not a tuple")
        }
    }
  }
}
