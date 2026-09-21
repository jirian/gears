package gears.async.http

import scala.collection.mutable

/** A request handler: computes a response for a request. Synchronous by
  * design (routing itself never needs to suspend) - a handler that needs to
  * do its own async I/O can still take `(using Async)` and summon it, since
  * it always runs on the connection's own fiber.
  */
type Handler = HttpRequest => HttpResponse

/** A minimal method+path router: exact-match dispatch to a registered
  * [[Handler]]. A path with no registered handler at all answers 404; a
  * path registered for other methods but not this one answers 405 with an
  * `Allow` header listing what is registered.
  */
final class Router:
  private val routes = mutable.LinkedHashMap[String, mutable.LinkedHashMap[HttpMethod, Handler]]()

  private def register(method: HttpMethod, path: String, handler: Handler): this.type =
    routes.getOrElseUpdate(path, mutable.LinkedHashMap.empty).update(method, handler)
    this

  def get(path: String)(handler: Handler): this.type = register(HttpMethod.GET, path, handler)
  def post(path: String)(handler: Handler): this.type = register(HttpMethod.POST, path, handler)
  def put(path: String)(handler: Handler): this.type = register(HttpMethod.PUT, path, handler)
  def delete(path: String)(handler: Handler): this.type = register(HttpMethod.DELETE, path, handler)
  def patch(path: String)(handler: Handler): this.type = register(HttpMethod.PATCH, path, handler)

  /** Dispatches `request` to its registered handler. `HEAD` is served by
    * the path's `GET` handler - [[HttpServer]] strips the body afterward,
    * once it knows the real `Content-Length` to report. A handler that
    * throws is turned into a 500 response here rather than propagating and
    * killing the connection's fiber.
    */
  def dispatch(request: HttpRequest): HttpResponse =
    routes.get(request.path) match
      case None => HttpResponse.notFound(request.path)
      case Some(byMethod) =>
        val lookupMethod = if request.method == HttpMethod.HEAD then HttpMethod.GET else request.method
        byMethod.get(lookupMethod) match
          case Some(handler) =>
            try handler(request)
            catch case e: Exception => HttpResponse.internalError(e)
          case None =>
            val allowed = byMethod.keys.toSeq
            val withHead = if allowed.contains(HttpMethod.GET) then allowed :+ HttpMethod.HEAD else allowed
            HttpResponse.methodNotAllowed(withHead)

object Router:
  def apply(): Router = new Router
