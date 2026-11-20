package gears.io.http

import gears.async.Async
import gears.async.io.Io
import gears.io.net.DefaultCapabilities

import scala.annotation.publicInBinary

type Handler = Request => (Async, Io[DefaultCapabilities]) ?=> Response

type Middleware = Handler => Handler

object Middleware {

  def timeout(limit: scala.concurrent.duration.FiniteDuration): Middleware = next =>
    req =>
      gears.async
        .withTimeoutOption(limit)(next(req))
        .getOrElse(Response.text("The request took too long\n", Status.ServiceUnavailable))

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

trait ToResponse[A] {
  def apply(value: A): Response
}

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

sealed trait Route

object Route {
  private[http] final case class Endpoint(method: Method, pattern: String, handler: Handler) extends Route
  private[http] final case class Mount(prefix: String, routes: Routes) extends Route
}

transparent inline def GET(inline pattern: String): Any = ${ Patterns.endpoint('{ Method.Get }, 'pattern) }
transparent inline def POST(inline pattern: String): Any = ${ Patterns.endpoint('{ Method.Post }, 'pattern) }
transparent inline def PUT(inline pattern: String): Any = ${ Patterns.endpoint('{ Method.Put }, 'pattern) }
transparent inline def PATCH(inline pattern: String): Any = ${ Patterns.endpoint('{ Method.Patch }, 'pattern) }
transparent inline def DELETE(inline pattern: String): Any = ${ Patterns.endpoint('{ Method.Delete }, 'pattern) }
transparent inline def HEAD(inline pattern: String): Any = ${ Patterns.endpoint('{ Method.Head }, 'pattern) }
transparent inline def OPTIONS(inline pattern: String): Any = ${ Patterns.endpoint('{ Method.Options }, 'pattern) }

def route[A](method: Method, pattern: String)(handler: Request => (Async, Io[DefaultCapabilities]) ?=> A)(using
    out: ToResponse[A]
): Route = Route.Endpoint(method, pattern, req => out(handler(req)))

final class Endpoint @publicInBinary private[http] (method: Method, pattern: String) {
  def apply[A](handler: Request => (Async, Io[DefaultCapabilities]) ?=> A)(using out: ToResponse[A]): Route =
    Route.Endpoint(method, pattern, req => out(handler(req)))
}

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

  def /(routes: Routes): Route = Route.Mount(prefix, routes)
}

final class Routes private (routes: Seq[Route], middleware: List[Middleware]) {
  private val root: Routes.Node = Routes.compile(routes)
  private val dispatch: Handler = middleware.foldRight[Handler](req => route(req))((m, next) => m(next))

  def apply(request: Request)(using Async, Io[DefaultCapabilities]): Response =
    try dispatch(request)
    catch case e: HttpError => e.response

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

  private[http] def routeSegments(request: Request, segments: Array[String], from: Int)(using
      Async,
      Io[DefaultCapabilities]
  ): Response =
    Routes.find(root, segments, from, Map.empty, request.method) match {
      case Routes.Found(endpoints, params, mount, rest) =>
        if mount != null then mount.nn.routeSegmentsWithin(request, segments, rest)
        else {
          val h = endpoints.getOrElse(request.method, endpoints(Method.Get))
          val answered =
            try h(if params.isEmpty then request else request.copy(pathParams = request.pathParams ++ params))
            catch case e: HttpError => e.response
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

  private def routeSegmentsWithin(request: Request, segments: Array[String], from: Int)(using
      Async,
      Io[DefaultCapabilities]
  ): Response =
    if middleware.isEmpty then routeSegments(request, segments, from)
    else mounted(request.withAttr(Routes.rest, new Routes.Rest(segments, from)))

  private lazy val mounted: Handler = middleware.foldRight[Handler] { req =>
    val rest = req.attr(Routes.rest).get
    routeSegments(req, rest.segments, rest.from)
  }((m, next) => m(next))
}

object Routes {
  def apply(routes: Route*): Routes = new Routes(routes, Nil)

  private final class Rest(val segments: Array[String], val from: Int)
  private val rest = new Attribute[Rest]("mount")

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

  private[http] final case class OtherMethods(allowed: Set[Method]) extends Lookup

  private def answers(endpoints: scala.collection.Map[Method, Handler], method: Method): Boolean =
    endpoints.contains(method) || (method == Method.Head && endpoints.contains(Method.Get))

  private def matched(
      endpoints: scala.collection.Map[Method, Handler],
      params: Map[String, String],
      rest: Int,
      method: Method
  ): Lookup =
    if endpoints.isEmpty then NotFound
    else if answers(endpoints, method) then Found(endpoints, params, null, rest)
    else OtherMethods(endpoints.keySet.toSet)

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

  private def patternSegments(pattern: String): List[String] = {
    require(pattern.startsWith("/"), s"A pattern starts with /: $pattern")
    if pattern == "/" then Nil else pattern.substring(1).split("/", -1).toList
  }

  private[http] def segments(raw: String): List[String] =
    if raw.isEmpty || raw == "/" then Nil
    else {
      val trimmed = if raw.startsWith("/") then raw.substring(1) else raw
      Text.split(trimmed, '/').map(Url.decode(_, plusIsSpace = false))
    }

  private[http] def escapes(segment: String): Boolean =
    segment.indexOf('\u0000') >= 0 ||
      (segment.indexOf('/') >= 0 && Text.split(segment, '/').exists(p => p == "." || p == ".."))

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
