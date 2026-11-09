package gears.io.http

import gears.async.Async
import gears.async.io.Io
import gears.io.net.DefaultCapabilities

import scala.annotation.publicInBinary

/** What answers a request: a function, which may suspend. It runs with its `Async`, and the server's `Io`, which is its
  * timers (`AsyncOperations.sleep`, `withTimeout`), and what it reaches other services with (`Http.get`,
  * `Tcp.connect`). A failure it throws is the server's to answer: an `HttpError` with its status, anything else with a
  * 500.
  */
type Handler = Request => (Async, Io[DefaultCapabilities]) ?=> Response

/** A function from a handler to a handler: what runs around the handlers of `Routes.withMiddleware`, before and after
  * them, as logging, authentication or a time limit does:
  * {{{
  * val timing: Middleware = next => req =>
  *   val start = System.nanoTime()
  *   val res = next(req)
  *   println(s"${req.method} ${req.path} -> ${res.status.code} in ${(System.nanoTime() - start) / 1000} µs")
  *   res
  * }}}
  */
type Middleware = Handler => Handler

/** Middleware most servers want. */
object Middleware {

  /** Answers 503 if the handler takes longer than `limit`, cancelling it. */
  def timeout(limit: scala.concurrent.duration.FiniteDuration): Middleware = next =>
    req =>
      gears.async
        .withTimeoutOption(limit)(next(req))
        .getOrElse(Response.text("The request took too long\n", Status.ServiceUnavailable))

  /** Reports each request's method, path, status and time to `log`, failures included. */
  def logging(log: String => Unit = println): Middleware = next =>
    req => {
      val start = System.nanoTime()
      def report(status: String): Unit =
        log(s"${req.method} ${req.url.rawPath} -> $status in ${(System.nanoTime() - start) / 1000} µs")
      try {
        val res = next(req)
        report(res.status.code.toString)
        res
      } catch {
        case e: HttpError =>
          report(e.status.code.toString)
          throw e
        case e: Throwable =>
          report(s"failed: $e")
          throw e
      }
    }
}

/** What a handler may return: a `Response`, or what one is made of, such as a `String`, which answers 200 with it as
  * plain text, or nothing, for a handler that only throws.
  */
trait ToResponse[A] {
  def apply(value: A): Response
}

// The rest of the givens are of lower priority than `Response`'s, so that a handler that only throws, whose type is
// not determined, is taken as returning a `Response`.
object ToResponse extends ToResponseLow {
  given ToResponse[Response] = r => r
}

trait ToResponseLow {
  given ToResponse[String] = s => Response.text(s)
  given ToResponse[Array[Byte]] = b => Response.ok(b)
  given ToResponse[Body] = b => Response.ok(b)
  given ToResponse[Status] = s => Response.status(s)
  given ToResponse[Unit] = _ => Response.noContent
}

/** One route of `Routes`: a method and a path pattern, and what answers them; or a mount, routes under a prefix. */
sealed trait Route

object Route {
  private[http] final case class Endpoint(method: Method, pattern: String, handler: Handler) extends Route
  private[http] final case class Mount(prefix: String, routes: Routes) extends Route
}

/** A route for `GET` on `pattern`, given its handler next: `GET("/items/{id:Long}") { (req, p) => items.get(p.id) }`.
  *
  * The pattern is read when compiling. A pattern with no `{name}` segments takes a handler of the request alone; one
  * with them, a handler of the request and a named tuple of their values, each decoded as its segment's type, `String`
  * unless it names one (`{id:Long}`; see `Routes` for the types), so that a misspelt name does not compile and a
  * segment that does not decode is answered 400 before the handler runs. A pattern computed when running is `route`'s.
  */
transparent inline def GET(inline pattern: String): Any = ${ Patterns.endpoint('{ Method.Get }, 'pattern) }
transparent inline def POST(inline pattern: String): Any = ${ Patterns.endpoint('{ Method.Post }, 'pattern) }
transparent inline def PUT(inline pattern: String): Any = ${ Patterns.endpoint('{ Method.Put }, 'pattern) }
transparent inline def PATCH(inline pattern: String): Any = ${ Patterns.endpoint('{ Method.Patch }, 'pattern) }
transparent inline def DELETE(inline pattern: String): Any = ${ Patterns.endpoint('{ Method.Delete }, 'pattern) }
transparent inline def HEAD(inline pattern: String): Any = ${ Patterns.endpoint('{ Method.Head }, 'pattern) }
transparent inline def OPTIONS(inline pattern: String): Any = ${ Patterns.endpoint('{ Method.Options }, 'pattern) }

/** A route for `method`, any method, on `pattern`, which may be computed when running: its `{name}` segments are the
  * handler's to read, as strings, with `Request.pathParam`.
  */
def route[A](method: Method, pattern: String)(handler: Request => (Async, Io[DefaultCapabilities]) ?=> A)(using
    out: ToResponse[A]
): Route = Route.Endpoint(method, pattern, req => out(handler(req)))

/** A method and a pattern with no `{name}` segments, waiting for the handler that answers them (see `GET`). */
final class Endpoint @publicInBinary private[http] (method: Method, pattern: String) {
  def apply[A](handler: Request => (Async, Io[DefaultCapabilities]) ?=> A)(using out: ToResponse[A]): Route =
    Route.Endpoint(method, pattern, req => out(handler(req)))
}

/** A method and a pattern with `{name}` segments, named `N` and of types `V`, waiting for the handler that answers
  * them, which is given their values as a named tuple (see `GET`).
  */
final class EndpointWith[N <: Tuple, V <: Tuple] @publicInBinary private[http] (
    method: Method,
    pattern: String,
    names: List[String],
    types: List[String]
) {
  private val params = names.toArray
  private val decoders = types.map(ParamDecoder.named).toArray

  def apply[A](handler: (Request, NamedTuple.NamedTuple[N, V]) => (Async, Io[DefaultCapabilities]) ?=> A)(using
      out: ToResponse[A]
  ): Route = Route.Endpoint(method, pattern, req => out(handler(req, values(req))))

  /** The segments' values, decoded: a 400 `HttpError` if one does not decode. */
  private def values(req: Request): NamedTuple.NamedTuple[N, V] = {
    val out = new Array[AnyRef](params.length)
    var i = 0
    while i < params.length do {
      val name = params(i)
      out(i) = ParamDecoder.decode(name, req.pathParams(name))(using decoders(i)).asInstanceOf[AnyRef]
      i += 1
    }
    Tuple.fromArray(out).asInstanceOf[NamedTuple.NamedTuple[N, V]]
  }
}

extension (prefix: String) {

  /** `routes` mounted under `prefix`, a pattern of literal segments: `"/api" / Routes(GET("/items")(...))` answers
    * `GET /api/items`.
    */
  def /(routes: Routes): Route = Route.Mount(prefix, routes)
}

/** Routes: a value that answers requests (`apply`), so that it is tested by calling it, with no server and no socket,
  * and served by `Http.serve` or `Http.server`.
  *
  * A pattern is a path of segments: a literal, `{name}`, which matches one segment, or, last, `{name...}`, which
  * matches the rest of the path, one or more segments. A `{name}` segment may name the type its value is decoded as,
  * `{name:Type}`, one of `String` (the default), `Int`, `Long`, `Double`, `Boolean`, `BigDecimal` and `UUID`; the
  * handler gets the values as a named tuple (see `GET`), and `Request.pathParam` has each as text. A literal wins over
  * `{name}`, which wins over `{name...}`, whatever the order the routes are given in, and two routes for one method and
  * pattern are refused as they are built. A path is matched by its segments, each percent-decoded, so that
  * `/users/J%C3%BCrgen` gives `{name}` the value `Jürgen`, and `%2F` within a segment does not split it; a segment
  * whose `%2F` would make a dot segment once split, `..%2F..`, or that has a NUL, is a 400. A path with `.` or `..`
  * segments, or an empty one between two slashes, which no route matches as it is, is redirected (308) to the path
  * cleaned of them, as Go's `ServeMux` does, so that `/static/../admin` is never served from under `/static`.
  *
  * A `HEAD` with no route of its own is answered by the `GET` route, with no body. Precedence is per method: a route
  * that wins for one method does not hide another that matches the path for the request's (`GET /a/{x}` and
  * `POST /{y}/b` both serve `/a/b`, each its own method). A path routes match only for other methods is answered 405,
  * with `Allow`; one no route matches, 404. Middleware (`withMiddleware`) runs around all of it, and is given these,
  * and a handler's `HttpError`, as the responses they are, so that a logger sees them.
  */
final class Routes private (routes: Seq[Route], middleware: List[Middleware]) {
  private val root: Routes.Node = Routes.compile(routes)
  private val dispatch: Handler = middleware.foldRight[Handler](req => route(req))((m, next) => m(next))

  /** Answers `request`, as the server does, the handler's `HttpError`s rendered as responses: how routes are tested, in
    * `Http.run`, which gives the `Io`.
    */
  def apply(request: Request)(using Async, Io[DefaultCapabilities]): Response =
    try dispatch(request)
    catch case e: HttpError => e.response // thrown by middleware

  /** These routes with `middleware` around them, the first outermost. */
  def withMiddleware(middleware: Middleware*): Routes = new Routes(routes, middleware.toList ++ this.middleware)

  private def route(request: Request)(using Async, Io[DefaultCapabilities]): Response = {
    val raw = request.url.rawPath
    val segments =
      try Routes.segments(raw)
      catch case e: IllegalArgumentException => throw HttpError(Status.BadRequest, e.getMessage)
    if segments.exists(Routes.escapes) then throw HttpError(Status.BadRequest, "A path segment that escapes its place")
    val clean = Routes.clean(segments)
    if clean ne segments then {
      val location = clean.map(Url.encodeSegment).mkString("/", "/", "") + request.url.rawQuery.fold("")("?" + _)
      Response.redirect(location, Status.PermanentRedirect)
    } else routeSegments(request, segments.toArray, 0)
  }

  /** Answers `request`, whose path from segment `from` on is these routes' to match. */
  private[http] def routeSegments(request: Request, segments: Array[String], from: Int)(using
      Async,
      Io[DefaultCapabilities]
  ): Response =
    Routes.find(root, segments, from, Map.empty, request.method) match {
      case Routes.Found(endpoints, params, mount, rest) =>
        if mount != null then mount.nn.routeSegmentsWithin(request, segments, rest)
        else {
          val h = endpoints.getOrElse(request.method, endpoints(Method.Get)) // a HEAD with no route of its own
          val answered =
            try h(if params.isEmpty then request else request.copy(pathParams = request.pathParams ++ params))
            catch case e: HttpError => e.response // so that middleware sees it as the response it is
          if request.method == Method.Head && !endpoints.contains(Method.Head) then answered.copy(body = Body.empty)
          else answered
        }
      case Routes.OtherMethods(methods) =>
        val allowed = (methods.toSeq ++ (if methods.contains(Method.Get) then Seq(Method.Head) else Nil))
          .map(_.name)
          .distinct
          .sorted
        HttpError(Status.MethodNotAllowed, "", Headers("Allow" -> allowed.mkString(", "))).response
      case Routes.NotFound => HttpError(Status.NotFound).response
    }

  /** As `routeSegments`, with these routes' middleware around it, for a mount. */
  private def routeSegmentsWithin(request: Request, segments: Array[String], from: Int)(using
      Async,
      Io[DefaultCapabilities]
  ): Response =
    if middleware.isEmpty then routeSegments(request, segments, from)
    else middleware.foldRight[Handler](req => routeSegments(req, segments, from))((m, next) => m(next))(request)
}

object Routes {
  def apply(routes: Route*): Routes = new Routes(routes, Nil)

  /** A route tree's node: its literal children, its `{name}` child, its `{name...}` routes, the routes that end here,
    * and routes mounted here.
    */
  private[http] final class Node {
    val literals = scala.collection.mutable.HashMap.empty[String, Node]
    var param: (String, Node) | Null = null
    var rest: (String, scala.collection.mutable.Map[Method, Handler]) | Null = null
    val endpoints = scala.collection.mutable.LinkedHashMap.empty[Method, Handler]
    var mount: Routes | Null = null
  }

  private[http] sealed trait Lookup
  private[http] final case class Found(
      endpoints: scala.collection.Map[Method, Handler],
      params: Map[String, String],
      mount: Routes | Null,
      rest: Int
  ) extends Lookup
  private[http] case object NotFound extends Lookup

  /** Routes match the path, none of them for the request's method: these are the methods they take. */
  private[http] final case class OtherMethods(allowed: Set[Method]) extends Lookup

  /** Whether `endpoints` answer `method`: a `HEAD` is answered by a `GET` too. */
  private def answers(endpoints: scala.collection.Map[Method, Handler], method: Method): Boolean =
    endpoints.contains(method) || (method == Method.Head && endpoints.contains(Method.Get))

  /** The route of `endpoints`, for a path they match: found if they answer `method`. */
  private def matched(
      endpoints: scala.collection.Map[Method, Handler],
      params: Map[String, String],
      rest: Int,
      method: Method
  ): Lookup =
    if endpoints.isEmpty then NotFound
    else if answers(endpoints, method) then Found(endpoints, params, null, rest)
    else OtherMethods(endpoints.keySet.toSet)

  /** The better of two lookups of one path: one found, else the methods of both. */
  private def either(a: Lookup, b: => Lookup): Lookup = a match {
    case found: Found       => found
    case NotFound           => b
    case OtherMethods(some) =>
      b match {
        case found: Found        => found
        case NotFound            => a
        case OtherMethods(other) => OtherMethods(some ++ other)
      }
  }

  private def compile(routes: Seq[Route]): Node = {
    val root = new Node
    def add(prefix: List[String], route: Route): Unit = route match {
      case Route.Endpoint(method, pattern, handler) =>
        val parts = prefix ++ patternSegments(pattern)
        var node = root
        var i = 0
        var placed = false
        while !placed && i < parts.length do {
          val part = parts(i)
          if part.startsWith("{") && part.endsWith("...}") then {
            require(i == parts.length - 1, s"A {name...} segment comes last, in $pattern")
            val name = part.substring(1, part.length - 4)
            if node.rest == null then node.rest = (name, scala.collection.mutable.LinkedHashMap.empty)
            else require(node.rest.nn._1 == name, s"Two names for one {name...} segment: ${node.rest.nn._1}, $name")
            val methods = node.rest.nn._2
            require(!methods.contains(method), s"Two routes for $method $pattern")
            methods(method) = handler
            placed = true
          } else if part.startsWith("{") && part.endsWith("}") then {
            val name = part.substring(1, part.length - 1)
            require(Headers.isToken(name), s"Not a segment name: $name, in $pattern")
            if node.param == null then node.param = (name, new Node)
            else require(node.param.nn._1 == name, s"Two names for one segment: {${node.param.nn._1}}, {$name}")
            node = node.param.nn._2
          } else node = node.literals.getOrElseUpdate(part, new Node)
          i += 1
        }
        if !placed then {
          require(!node.endpoints.contains(method), s"Two routes for $method $pattern")
          node.endpoints(method) = handler
        }
      case Route.Mount(at, routes) =>
        val parts = prefix ++ patternSegments(at)
        require(
          parts.nonEmpty && parts.forall(p => p.nonEmpty && !p.startsWith("{")),
          s"A mount's prefix is literal: $at"
        )
        var node = root
        for part <- parts do node = node.literals.getOrElseUpdate(part, new Node)
        require(node.mount == null, s"Two mounts at $at")
        node.mount = routes
    }
    routes.foreach(add(Nil, _))
    root
  }

  /** A pattern's segments: `/` alone has none; it must start with `/`. */
  private def patternSegments(pattern: String): List[String] = {
    require(pattern.startsWith("/"), s"A pattern starts with /: $pattern")
    if pattern == "/" then Nil else pattern.substring(1).split("/", -1).toList
  }

  /** A raw path's segments, each percent-decoded: `/` has none, `/a/` has `a` and an empty one. */
  private[http] def segments(raw: String): List[String] =
    if raw.isEmpty || raw == "/" then Nil
    else {
      val trimmed = if raw.startsWith("/") then raw.substring(1) else raw
      Text.split(trimmed, '/').map(Url.decode(_, plusIsSpace = false))
    }

  /** Whether a decoded segment could step out of where it is once a handler joins it into a path: one with a NUL, or
    * with an encoded `/` (`%2F`) between `.` or `..` parts, such as `..%2F..%2Fetc`, which `clean` does not see as dot
    * segments. `{name}` and `{name...}` values then never carry a dot segment of their own.
    */
  private[http] def escapes(segment: String): Boolean =
    segment.indexOf('\u0000') >= 0 ||
      (segment.indexOf('/') >= 0 && Text.split(segment, '/').exists(p => p == "." || p == ".."))

  /** `segments` with `.`, `..` and empty segments resolved, as RFC 3986's dot-segment removal does, keeping a final
    * empty one, a trailing slash; `segments` itself, the same list, if there were none.
    */
  private[http] def clean(segments: List[String]): List[String] = {
    var dirty = false
    var rest = segments
    while !dirty && rest.nonEmpty do {
      val s = rest.head
      dirty = s == "." || s == ".." || (s.isEmpty && rest.tail.nonEmpty)
      rest = rest.tail
    }
    if !dirty then segments
    else {
      val last = segments.length - 1
      val out = scala.collection.mutable.ListBuffer.empty[String]
      for (s, i) <- segments.zipWithIndex do
        s match {
          case "."  => if i == last then out += ""
          case ".." =>
            if out.nonEmpty then out.remove(out.length - 1)
            if i == last then out += ""
          case "" if i != last => ()
          case other           => out += other
        }
      out.toList
    }
  }

  /** The route for `method` that matches `segments` from `i` on, trying a literal first, then `{name}`, then
    * `{name...}`, and going on to the next if the one tried matches the path but takes other methods only, so that a
    * route is never hidden from its method by one that wins for another.
    */
  private def find(node: Node, segments: Array[String], i: Int, params: Map[String, String], method: Method): Lookup =
    if node.mount != null then Found(Map.empty, params, node.mount, i)
    else if i == segments.length then matched(node.endpoints, params, i, method)
    else {
      val segment = segments(i)
      either(
        node.literals.get(segment) match {
          case Some(child) => find(child, segments, i + 1, params, method)
          case None        => NotFound
        },
        either(
          if node.param != null && segment.nonEmpty then {
            val (name, child) = node.param.nn
            find(child, segments, i + 1, params.updated(name, segment), method)
          } else NotFound,
          if node.rest != null && segments.drop(i).exists(_.nonEmpty) then {
            val (name, methods) = node.rest.nn
            matched(methods, params.updated(name, segments.drop(i).mkString("/")), segments.length, method)
          } else NotFound
        )
      )
    }
}
