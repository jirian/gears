package gears.io.http

import gears.async.Async

import scala.concurrent.duration.FiniteDuration

/** A request, the same on both sides: a client builds one and sends it (`HttpClient.send`), and a server's handler is
  * given one and answers it with a `Response`. A server's has the URL its client named, by the `Host` header, the
  * values its route's `{name}` segments matched (`pathParam`), and the attributes its middleware set (`attr`).
  * Immutable: each builder method returns a changed copy.
  */
final case class Request(
    method: Method,
    url: Url,
    headers: Headers = Headers.empty,
    body: Body = Body.empty,
    timeout: Option[FiniteDuration] = None,
    version: String = "HTTP/1.1",
    pathParams: Map[String, String] = Map.empty,
    attributes: Map[Attribute[?], Any] = Map.empty
) {

  /** The path, percent-decoded. */
  def path: String = url.path

  /** The value of the route's `{name}` segment, decoded as an `A`: a 400 `HttpError` if it is not one. */
  def pathParam[A](name: String)(using decoder: ParamDecoder[A]): A = {
    val raw = pathParams.getOrElse(name, throw new IllegalArgumentException(s"The route has no {$name} segment"))
    ParamDecoder.decode(name, raw)
  }

  /** The first value of query parameter `name`, decoded as an `A`, if there is one: a 400 `HttpError` if it is not one.
    */
  def query[A](name: String)(using decoder: ParamDecoder[A]): Option[A] =
    url.query.collectFirst { case (k, v) if k == name => v }.map(ParamDecoder.decode(name, _))

  /** Every value of query parameter `name`, decoded as `A`s. */
  def queryAll[A](name: String)(using decoder: ParamDecoder[A]): Seq[A] =
    url.query.collect { case (k, v) if k == name => ParamDecoder.decode(name, v) }

  def header(name: String): Option[String] = headers.get(name)

  /** The cookies the `Cookie` header sends, by name. */
  def cookies: Map[String, String] =
    headers
      .getAll("Cookie")
      .flatMap(Text.split(_, ';').iterator.map(_.trim).filter(_.nonEmpty))
      .flatMap { pair =>
        val eq = pair.indexOf('=')
        if eq <= 0 then None
        else Some(pair.substring(0, eq) -> pair.substring(eq + 1).stripPrefix("\"").stripSuffix("\""))
      }
      .toMap

  /** The body decoded as an `A`: a 400 `HttpError` if a decoder fails on it, as a server answers a body it cannot read.
    */
  def as[A](using decoder: BodyDecoder[A], async: Async): A =
    try body.as[A]
    catch {
      case e: HttpError => throw e
      case e: Exception
          if !e.isInstanceOf[java.io.IOException] && !e.isInstanceOf[java.util.concurrent.CancellationException] =>
        throw HttpError(Status.BadRequest, s"Malformed body: ${e.getMessage}")
    }

  def text()(using Async): String = body.text()
  def bytes()(using Async): Array[Byte] = body.bytes()

  /** The value of `key` that middleware set. */
  def attr[A](key: Attribute[A]): Option[A] = attributes.get(key).map(_.asInstanceOf[A])

  def withAttr[A](key: Attribute[A], value: A): Request = copy(attributes = attributes.updated(key, value))
  def withHeader(name: String, value: String): Request = copy(headers = headers.set(name, value))

  /** This request with a field `name: value` added beside any it has: `withHeader` replaces them. */
  def addHeader(name: String, value: String): Request = copy(headers = headers.add(name, value))
  def withHeaders(fields: (String, String)*): Request =
    copy(headers = fields.foldLeft(headers)((h, f) => h.set(f._1, f._2)))
  def withBody(body: Body): Request = copy(body = body)

  /** This request with `value` as its body, encoded by its encoder: `Request.post(url).json(item)`. */
  def json[A](value: A)(using BodyEncoder[A]): Request = copy(body = Body.encode(value))

  /** This request with a form body, `application/x-www-form-urlencoded`. */
  def form(fields: (String, String)*): Request = copy(body =
    Body.text(
      fields.map((k, v) => s"${Url.encodeSegment(k)}=${Url.encodeSegment(v)}").mkString("&"),
      "application/x-www-form-urlencoded"
    )
  )

  /** This request with `name` and `value` added to its URL's query. */
  def withQueryParam(name: String, value: String): Request = copy(url = url.withQueryParam(name, value))

  /** This request with a time limit on all of it, connecting to reading the whole response, overriding the client's. */
  def withTimeout(limit: FiniteDuration): Request = copy(timeout = Some(limit))

  override def toString: String = s"Request($method $url)"
}

object Request {

  /** A request for `target`: an absolute URL, or, for a request handed to routes in a test, a path, on `localhost`. */
  def apply(method: Method, target: String): Request = Request(method, toUrl(target))

  def get(target: String | Url): Request = Request(Method.Get, urlOf(target))
  def head(target: String | Url): Request = Request(Method.Head, urlOf(target))
  def post(target: String | Url, body: Body = Body.empty): Request = Request(Method.Post, urlOf(target), body = body)
  def put(target: String | Url, body: Body = Body.empty): Request = Request(Method.Put, urlOf(target), body = body)
  def patch(target: String | Url, body: Body = Body.empty): Request = Request(Method.Patch, urlOf(target), body = body)
  def delete(target: String | Url): Request = Request(Method.Delete, urlOf(target))
  def options(target: String | Url): Request = Request(Method.Options, urlOf(target))

  private def urlOf(target: String | Url): Url = target match {
    case url: Url  => url
    case s: String => toUrl(s)
  }

  private def toUrl(target: String): Url =
    if target.startsWith("/") then Url.parse(s"http://localhost$target") else Url.parse(target)
}

/** A key for a value middleware attaches to a request, for the handlers after it: `val User = Attribute[User]("user")`,
  * then `req.withAttr(User, user)` and `req.attr(User)`. Keys compare by identity.
  */
final class Attribute[A](val name: String) {
  override def toString: String = s"Attribute($name)"
}

/** A response, the same on both sides: a handler returns one, and a client is given one. A client's body is still
  * arriving when `HttpClient.stream` hands it over, and read whole when `HttpClient.send` returns it.
  */
final case class Response(status: Status, headers: Headers = Headers.empty, body: Body = Body.empty) {

  def isSuccess: Boolean = status.isSuccess

  /** This response, if its status is a success; else an `HttpStatusException` with its status and the start of its
    * body.
    */
  def ensureSuccess()(using Async): Response =
    if status.isSuccess then this
    else {
      val preview =
        try new String(body.bytes().take(512), Body.charset(body.contentType))
        catch case _: Exception => ""
      throw new HttpStatusException(status, headers, preview)
    }

  def header(name: String): Option[String] = headers.get(name)
  def text()(using Async): String = body.text()
  def bytes()(using Async): Array[Byte] = body.bytes()
  def as[A](using BodyDecoder[A], Async): A = body.as[A]

  def withHeader(name: String, value: String): Response = copy(headers = headers.set(name, value))

  /** This response with a field `name: value` added beside any it has: `withHeader` replaces them. */
  def addHeader(name: String, value: String): Response = copy(headers = headers.add(name, value))
  def withStatus(status: Status): Response = copy(status = status)

  /** This response with a `Set-Cookie` header for `cookie`. */
  def withCookie(cookie: Cookie): Response = copy(headers = headers.add("Set-Cookie", cookie.rendered))

  /** The cookies its `Set-Cookie` headers set. */
  def cookies: Seq[Cookie] = headers.getAll("Set-Cookie").flatMap(Cookie.parse)

  override def toString: String = s"Response($status)"
}

object Response {

  /** A response with `body`, and its `Content-Type`. */
  def apply(status: Status, body: Body): Response =
    Response(status, body.contentType.fold(Headers.empty)(t => Headers("Content-Type" -> t)), body)

  def ok(body: Body): Response = Response(Status.Ok, body)
  def ok(text: String): Response = Response(Status.Ok, Body.text(text))
  def ok(bytes: Array[Byte]): Response = Response(Status.Ok, Body(bytes))

  /** `text` in UTF-8, `text/plain` unless `contentType` says otherwise. */
  def text(text: String, status: Status = Status.Ok, contentType: String = "text/plain; charset=utf-8"): Response =
    Response(status, Body.text(text, contentType))

  def html(html: String, status: Status = Status.Ok): Response =
    Response(status, Body.text(html, "text/html; charset=utf-8"))

  /** `value`, encoded by its encoder, which for a JSON type is `BodyCodec.json`'s. */
  def json[A](value: A, status: Status = Status.Ok)(using BodyEncoder[A]): Response =
    Response(status, Body.encode(value))

  /** A response whose body `write` writes as it goes out, chunked unless `length` is given: a large file, or events. */
  def stream(contentType: String, status: Status = Status.Ok, length: Option[Long] = None)(
      write: BodyWriter => Async ?=> Unit
  ): Response = Response(status, Body.stream(contentType, length)(write))

  def noContent: Response = Response(Status.NoContent)

  def redirect(location: String, status: Status = Status.Found): Response =
    Response(status, Headers("Location" -> location))

  def notFound: Response = text("Not Found\n", Status.NotFound)

  def status(status: Status): Response = Response(status)
}

/** Thrown by a handler to answer with `status`, and `message` as a plain-text body, and `headers`: `throw
  * HttpError(Status.NotFound)`. Middleware and routes rethrow it, and the server renders it. It carries no stack trace,
  * which would cost more than the answer.
  */
final class HttpError(val status: Status, message: String, val headers: Headers)
    extends RuntimeException(message, null, false, false) {
  def response: Response = {
    val body = Body.text(if message.isEmpty then s"${status.reason}\n" else s"$message\n")
    Response(status, headers.setIfAbsent("Content-Type", body.contentType.get), body)
  }
}

object HttpError {
  def apply(status: Status, message: String = "", headers: Headers = Headers.empty): HttpError =
    new HttpError(status, message, headers)
}

/** A response whose status was not a success, as `Response.ensureSuccess` reports it. */
final class HttpStatusException(val status: Status, val headers: Headers, val bodyPreview: String)
    extends java.io.IOException(s"HTTP $status${if bodyPreview.isEmpty then "" else s": $bodyPreview"}")

/** How a path segment's or a query parameter's text is decoded into a value. */
trait ParamDecoder[A] {

  /** The value, or None if `text` is not one. */
  def decode(text: String): Option[A]
}

object ParamDecoder {
  def apply[A](f: String => Option[A]): ParamDecoder[A] = text => f(text)

  given ParamDecoder[String] = Some(_)
  given ParamDecoder[Int] = _.toIntOption
  given ParamDecoder[Long] = _.toLongOption
  given ParamDecoder[Double] = _.toDoubleOption
  given ParamDecoder[BigDecimal] = s => scala.util.Try(BigDecimal(s)).toOption
  given ParamDecoder[Boolean] = _.toLowerCase match {
    case "true" | "1" | "yes" | "on"  => Some(true)
    case "false" | "0" | "no" | "off" => Some(false)
    case _                            => None
  }
  given ParamDecoder[java.util.UUID] = s => scala.util.Try(java.util.UUID.fromString(s)).toOption

  /** The decoder of a type that a route's pattern names (see `Patterns.typeNames`). */
  private[http] def named(tpe: String): ParamDecoder[Any] = (tpe match {
    case "Int"        => summon[ParamDecoder[Int]]
    case "Long"       => summon[ParamDecoder[Long]]
    case "Double"     => summon[ParamDecoder[Double]]
    case "Boolean"    => summon[ParamDecoder[Boolean]]
    case "BigDecimal" => summon[ParamDecoder[BigDecimal]]
    case "UUID"       => summon[ParamDecoder[java.util.UUID]]
    case _            => summon[ParamDecoder[String]]
  }).asInstanceOf[ParamDecoder[Any]]

  private[http] def decode[A](name: String, text: String)(using decoder: ParamDecoder[A]): A =
    decoder.decode(text).getOrElse(throw HttpError(Status.BadRequest, s"Malformed parameter $name: $text"))
}

/** A cookie, as a `Set-Cookie` header sets it. */
final case class Cookie(
    name: String,
    value: String,
    path: Option[String] = None,
    domain: Option[String] = None,
    maxAge: Option[Long] = None,
    secure: Boolean = false,
    httpOnly: Boolean = false,
    sameSite: Option[String] = None
) {
  require(Headers.isToken(name), s"Not a cookie name: $name")
  require(value.forall(c => c > ' ' && c < 127 && c != '"' && c != ',' && c != ';' && c != '\\'), s"Not a cookie value")
  // An attribute's value ends at a `;`, so one in it would add attributes of its own (RFC 6265, 4.1.1).
  require(path.forall(Cookie.isAttributeValue), s"Not a cookie path: ${path.get}")
  require(
    domain.forall(d => d.nonEmpty && d.forall(c => c.isLetterOrDigit && c < 127 || c == '-' || c == '.')),
    s"Not a cookie domain: ${domain.get}"
  )
  require(
    sameSite.forall(s => Set("strict", "lax", "none").contains(s.toLowerCase)),
    s"Not a SameSite value: ${sameSite.get}"
  )

  /** The `Set-Cookie` value. */
  def rendered: String = {
    val b = new StringBuilder(s"$name=$value")
    path.foreach(p => b.append("; Path=").append(p))
    domain.foreach(d => b.append("; Domain=").append(d))
    maxAge.foreach(a => b.append("; Max-Age=").append(a))
    if secure then b.append("; Secure")
    if httpOnly then b.append("; HttpOnly")
    sameSite.foreach(s => b.append("; SameSite=").append(s))
    b.toString
  }
}

object Cookie {

  /** Printable ASCII, no `;`. */
  private def isAttributeValue(v: String): Boolean = v.forall(c => c >= ' ' && c < 127 && c != ';')

  /** A `Set-Cookie` value's cookie, if it has a `name=value`; unknown attributes are ignored. */
  def parse(setCookie: String): Option[Cookie] = {
    val parts = Text.split(setCookie, ';').map(_.trim).toList // not String.split, a regular expression on Native
    parts match {
      case first :: attrs =>
        val eq = first.indexOf('=')
        if eq <= 0 then None
        else
          scala.util.Try {
            attrs.foldLeft(Cookie(first.substring(0, eq), first.substring(eq + 1))) { (c, attr) =>
              val eq = attr.indexOf('=')
              val kv = if eq < 0 then Array(attr) else Array(attr.substring(0, eq), attr.substring(eq + 1))
              val v = if kv.length > 1 then Some(kv(1).trim) else None
              // An attribute that is not valid is ignored, as an unknown one is.
              scala.util
                .Try(kv(0).trim.toLowerCase match {
                  case "path"     => c.copy(path = v)
                  case "domain"   => c.copy(domain = v.map(_.stripPrefix(".")))
                  case "max-age"  => c.copy(maxAge = v.flatMap(_.toLongOption))
                  case "secure"   => c.copy(secure = true)
                  case "httponly" => c.copy(httpOnly = true)
                  case "samesite" => c.copy(sameSite = v)
                  case _          => c
                })
                .getOrElse(c)
            }
          }.toOption
      case Nil => None
    }
  }
}
