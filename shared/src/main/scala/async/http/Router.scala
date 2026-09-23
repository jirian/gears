package gears.async.http

import gears.async.Async

import scala.collection.mutable

final class Router extends Handler:
  private final case class Registration(
      pattern: String,
      handlers: mutable.LinkedHashMap[HttpMethod, Handler] = mutable.LinkedHashMap.empty,
      var anyMethod: Option[Handler] = None
  )

  private val registrations = mutable.ArrayBuffer[Registration]()
  private val byPattern = mutable.LinkedHashMap[String, Registration]()

  private def registrationFor(pattern: String): Registration =
    byPattern.getOrElseUpdate(
      pattern, {
        val r = Registration(pattern)
        registrations += r
        r
      }
    )

  def handle(pattern: String)(handler: Handler): this.type =
    registrationFor(pattern).anyMethod = Some(handler)
    this

  private def register(method: HttpMethod, pattern: String, handler: Handler): this.type =
    registrationFor(pattern).handlers.update(method, handler)
    this

  def get(pattern: String)(handler: HttpRequest => HttpResponse): this.type =
    register(HttpMethod.GET, pattern, Handler.of(handler))
  def post(pattern: String)(handler: HttpRequest => HttpResponse): this.type =
    register(HttpMethod.POST, pattern, Handler.of(handler))
  def put(pattern: String)(handler: HttpRequest => HttpResponse): this.type =
    register(HttpMethod.PUT, pattern, Handler.of(handler))
  def delete(pattern: String)(handler: HttpRequest => HttpResponse): this.type =
    register(HttpMethod.DELETE, pattern, Handler.of(handler))
  def patch(pattern: String)(handler: HttpRequest => HttpResponse): this.type =
    register(HttpMethod.PATCH, pattern, Handler.of(handler))

  private def isWildcard(pattern: String): Boolean = pattern.contains("{")
  private def isSubtree(pattern: String): Boolean = pattern != "/" && pattern.endsWith("/")

  private def segments(path: String): Array[String] = path.split("/", -1).filter(_.nonEmpty)

  private def matchWildcard(pattern: String, path: String): Option[Map[String, String]] =
    val patternSegs = segments(pattern)
    val pathSegs = segments(path)
    if patternSegs.length != pathSegs.length then None
    else
      val extracted = mutable.LinkedHashMap[String, String]()
      val ok = patternSegs.lazyZip(pathSegs).forall { (p, s) =>
        if p.startsWith("{") && p.endsWith("}") then
          extracted(p.substring(1, p.length - 1)) = s
          true
        else p == s
      }
      if ok then Some(extracted.toMap) else None

  private def candidates(path: String): Seq[(Registration, Map[String, String])] =
    registrations.toSeq.flatMap { reg =>
      val pattern = reg.pattern
      if isWildcard(pattern) then matchWildcard(pattern, path).map(reg -> _)
      else if isSubtree(pattern) then Option.when(path.startsWith(pattern))(reg -> Map.empty[String, String])
      else Option.when(pattern == path)(reg -> Map.empty[String, String])
    }

  def serveHTTP(w: ResponseWriter, request: HttpRequest)(using Async): Unit =
    candidates(request.path).sortBy(-_._1.pattern.length).headOption match
      case None => dispatchTo(Handler.of(_ => HttpResponse.notFound(request.path)), w, request)
      case Some((reg, pathValues)) =>
        val matched = request.copy(pathValues = pathValues)
        reg.anyMethod match
          case Some(handler) => dispatchTo(handler, w, matched)
          case None =>
            val lookupMethod = if request.method == HttpMethod.HEAD then HttpMethod.GET else request.method
            reg.handlers.get(lookupMethod) match
              case Some(handler) => dispatchTo(handler, w, matched)
              case None =>
                val allowed = reg.handlers.keys.toSeq
                val withHead = if allowed.contains(HttpMethod.GET) then allowed :+ HttpMethod.HEAD else allowed
                dispatchTo(Handler.of(_ => HttpResponse.methodNotAllowed(withHead)), w, matched)

  private def dispatchTo(handler: Handler, w: ResponseWriter, request: HttpRequest)(using Async): Unit =
    try handler.serveHTTP(w, request)
    catch case e: Exception => Handler.of(_ => HttpResponse.internalError(e)).serveHTTP(w, request)

  def dispatch(request: HttpRequest)(using Async): HttpResponse =
    var captured: HttpResponse = null
    val w = new ResponseWriter:
      val header: MutableHeaders = MutableHeaders()
      private var status: HttpStatus = HttpStatus.Ok
      def writeHeader(s: HttpStatus): Unit = status = s
      def write(bytes: Array[Byte])(using Async): Unit =
        captured = HttpResponse(status, header.toHeaders, bytes)
    serveHTTP(w, request)
    captured

object Router:
  def apply(): Router = new Router
