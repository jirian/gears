package gears.io.http

import munit.FunSuite

/** Routes as values: matched, decoded and answered with no server and no socket. */
class RoutesSuite extends FunSuite {
  private def call(routes: Routes, request: Request): Response = Http.run(routes(request))

  test("a literal beats {name}, which beats {name...}, whatever the order") {
    val routes = Routes(
      GET("/files/{rest...}") { (_, p) => s"rest ${p.rest}" },
      GET("/files/{name}") { (_, p) => s"name ${p.name}" },
      GET("/files/readme") { _ => "literal" }
    )
    assertEquals(call(routes, Request.get("/files/readme")).text0, "literal")
    assertEquals(call(routes, Request.get("/files/a")).text0, "name a")
    assertEquals(call(routes, Request.get("/files/a/b/c")).text0, "rest a/b/c")
  }

  test("segments are percent-decoded after matching, and an encoded slash does not split one") {
    val routes = Routes(GET("/users/{name}") { (_, p) => p.name })
    assertEquals(call(routes, Request.get("/users/J%C3%BCrgen")).text0, "Jürgen")
    assertEquals(call(routes, Request.get("/users/a%2Fb")).text0, "a/b")
    assertEquals(call(routes, Request.get("/users/%C3%28")).status, Status.BadRequest, "not UTF-8")
  }

  test("dot segments and empty ones are redirected to the clean path, so nothing escapes a prefix") {
    val routes = Routes(GET("/static/{path...}") { (_, p) => p.path }, GET("/admin") { _ => "admin" })
    for raw <- Seq("/static/../admin", "/static/%2e%2e/admin", "/static//../admin") do {
      val response = call(routes, Request.get(raw))
      assertEquals(response.status, Status.PermanentRedirect, raw)
      assertEquals(response.header("Location"), Some("/admin"), raw)
    }
    assertEquals(call(routes, Request.get("/static/./a?x=1")).header("Location"), Some("/static/a?x=1"))
  }

  test("404 for no route, 405 with Allow for another method's, and HEAD answered by GET without a body") {
    val routes = Routes(GET("/x") { _ => "x" }, POST("/x") { _ => "posted" })
    assertEquals(call(routes, Request.get("/y")).status, Status.NotFound)
    val refused = call(routes, Request.delete("/x"))
    assertEquals(refused.status, Status.MethodNotAllowed)
    assertEquals(refused.header("Allow"), Some("GET, HEAD, POST"))
    val head = call(routes, Request.head("/x"))
    assertEquals(head.status, Status.Ok)
    assertEquals(head.body.length, Some(0L))
  }

  test("typed parameters: a malformed one is a 400, query parameters are optional") {
    val routes = Routes(GET("/items/{id:Long}") { (r, p) =>
      val id: Long = p.id
      val limit = r.query[Int]("limit").getOrElse(50)
      s"$id $limit"
    })
    assertEquals(call(routes, Request.get("/items/7?limit=3")).text0, "7 3")
    assertEquals(call(routes, Request.get("/items/7")).text0, "7 50")
    assertEquals(call(routes, Request.get("/items/seven")).status, Status.BadRequest)
    assertEquals(call(routes, Request.get("/items/7?limit=many")).status, Status.BadRequest)
  }

  test("middleware runs around everything, the first outermost, and sees 404s; mounts keep their own") {
    val trail = scala.collection.mutable.ListBuffer.empty[String]
    def tag(name: String): Middleware = next =>
      req => {
        trail += s"$name>"
        val res = next(req)
        trail += s"<$name"
        res
      }
    val api = Routes(GET("/items") { _ => "items" }).withMiddleware(tag("api"))
    val routes = Routes(GET("/health") { _ => "ok" }, "/api" / api).withMiddleware(tag("outer"), tag("inner"))
    assertEquals(call(routes, Request.get("/api/items")).text0, "items")
    assertEquals(trail.toList, List("outer>", "inner>", "api>", "<api", "<inner", "<outer"))
    trail.clear()
    assertEquals(call(routes, Request.get("/nope")).status, Status.NotFound)
    assertEquals(trail.toList, List("outer>", "inner>", "<inner", "<outer"))
  }

  test("a handler's HttpError is its response, and an authentication middleware can refuse") {
    val auth: Middleware = next =>
      req =>
        req.header("Authorization") match {
          case Some(s"Bearer $token") if token == "t" => next(req)
          case _                                      => throw HttpError(Status.Unauthorized)
        }
    val routes = Routes(GET("/me") { _ => "me" }).withMiddleware(auth)
    assertEquals(call(routes, Request.get("/me")).status, Status.Unauthorized)
    assertEquals(call(routes, Request.get("/me").withHeader("Authorization", "Bearer t")).text0, "me")
  }

  test("a streamed body called directly is every write, not the last") {
    val routes = Routes(GET("/stream") { _ =>
      Response.stream("text/plain") { out => for i <- 1 to 3 do out.write(s"$i,") }
    })
    assertEquals(call(routes, Request.get("/stream")).text0, "1,2,3,")
  }

  test("the timeout middleware answers 503 and cancels a handler that takes too long") {
    var finished = false
    val routes = Routes(GET("/slow") { _ =>
      gears.async.AsyncOperations.sleep(5000)
      finished = true
      "late"
    }).withMiddleware(Middleware.timeout(scala.concurrent.duration.FiniteDuration(50, "ms")))
    assertEquals(call(routes, Request.get("/slow")).status, Status.ServiceUnavailable)
    assert(!finished)
  }

  test("two routes for one method and pattern are refused as they are built") {
    intercept[IllegalArgumentException](Routes(GET("/a") { _ => "1" }, GET("/a") { _ => "2" }))
  }

  test("bodies and codecs: a decoder's failure is a 400; Unit is a 204") {
    final case class Item(name: String)
    given BodyCodec[Item] = BodyCodec.json(
      i => s"""{"name":"${i.name}"}""",
      s => {
        val m = """\{"name":"([^"]*)"\}""".r
        s match {
          case m(name) => Item(name)
          case _       => throw new IllegalArgumentException("not an item")
        }
      }
    )
    val routes = Routes(
      POST("/items") { r => Response.json(r.as[Item].copy(name = r.as[Item].name.toUpperCase), Status.Created) },
      DELETE("/items/{id}") { (_, _) => () }
    )
    val created = call(routes, Request.post("/items").json(Item("lamp")))
    assertEquals(created.status, Status.Created)
    assertEquals(created.header("Content-Type"), Some("application/json"))
    assertEquals(Http.run(created.as[Item]), Item("LAMP"))
    assertEquals(call(routes, Request.post("/items", Body.text("nonsense"))).status, Status.BadRequest)
    assertEquals(call(routes, Request.delete("/items/1")).status, Status.NoContent)
  }

  extension (r: Response) private def text0: String = Http.run(r.text())

  test("a pattern's parameters are a named tuple, each of its segment's type; a computed pattern is route's") {
    val routes = Routes(
      GET("/users/{user:UUID}/posts/{n:Int}/{flag:Boolean}") { (_, p) => s"${p.user} ${p.n + 1} ${!p.flag}" },
      route(Method.Get, "/raw/" + "{id}") { r => r.pathParam[Long]("id").toString }
    )
    val user = java.util.UUID.fromString("123e4567-e89b-12d3-a456-426614174000")
    assertEquals(call(routes, Request.get(s"/users/$user/posts/41/true")).text0, s"$user 42 false")
    assertEquals(call(routes, Request.get("/raw/7")).text0, "7")
    assertEquals(call(routes, Request.get("/users/nope/posts/1/true")).status, Status.BadRequest)
  }

  test("a pattern's mistakes do not compile") {
    assert(compileErrors("""GET("/items/{id}") { (_, p) => p.idd }""").contains("idd"))
    assert(compileErrors("""GET("/items/{id:Float}") { (_, p) => p.id }""").contains("No decoder for {id:Float}"))
    assert(compileErrors("""GET("/a/{x}/{x}") { (_, p) => p.x }""").contains("Two segments named x"))
    assert(compileErrors("""GET("/a/{rest...}/b") { (_, p) => p.rest }""").contains("comes last"))
    assert(compileErrors("""GET("/items/{id}") { r => "one argument" }""").nonEmpty)
    assert(compileErrors("""val p = "/x"; GET(p) { _ => "computed" }""").contains("literal"))
  }

  test("precedence is per method: a route winning for one method does not hide another's") {
    val routes = Routes(
      GET("/a/{x}") { (_, p) => s"get ${p.x}" },
      POST("/{y}/b") { (_, p) => s"post ${p.y}" },
      PUT("/files/{rest...}") { (_, p) => s"put ${p.rest}" },
      GET("/files/{name}") { (_, p) => s"get ${p.name}" }
    )
    assertEquals(call(routes, Request.get("/a/b")).text0, "get b")
    assertEquals(call(routes, Request.post("/a/b")).text0, "post a", "the literal /a takes GET only")
    assertEquals(call(routes, Request.put("/files/x")).text0, "put x", "{name} takes GET only")
    val refused = call(routes, Request.delete("/a/b"))
    assertEquals(refused.status, Status.MethodNotAllowed)
    assertEquals(refused.header("Allow"), Some("GET, HEAD, POST"), "every route that matches the path")
  }

  test("a handler that only throws needs no type") {
    val routes = Routes(GET("/gone") { _ => throw HttpError(Status.Gone) })
    assertEquals(call(routes, Request.get("/gone")).status, Status.Gone)
  }

  test("a segment whose encoded slash would make a dot segment, or that has a NUL, is a 400") {
    val routes = Routes(GET("/static/{path...}") { (_, p) => p.path }, GET("/users/{name}") { (_, p) => p.name })
    for raw <- Seq("/static/..%2F..%2Fetc%2Fpasswd", "/static/a/.%2Fb", "/users/..%2Fadmin", "/users/a%00b") do
      assertEquals(call(routes, Request.get(raw)).status, Status.BadRequest, raw)
    assertEquals(call(routes, Request.get("/users/a%2Fb")).text0, "a/b", "an encoded slash alone is a value's")
    assertEquals(call(routes, Request.get("/users/..a%2Fb..")).text0, "..a/b..")
  }
}

class UrlSuite extends FunSuite {
  test("a header value with a control character, or one that is not one byte, is refused as it is built") {
    for bad <- Seq("a\u010Ab", "a\u010Db", "a\rb", "a\u0001b", "a\u007fb", "\u2028") do
      intercept[IllegalArgumentException](Headers("X-Test" -> bad))
    assertEquals(Headers("X-Test" -> "tab\tand caf\u00e9").get("X-Test"), Some("tab\tand caf\u00e9"))
    intercept[IllegalArgumentException](Response.redirect("/next\u010ASet-Cookie: a=b"))
    intercept[IllegalArgumentException](Wire.head("GET /\u010A HTTP/1.1", Headers.empty))
  }

  test("a URL's path and query go out in ASCII, characters beyond it percent-encoded") {
    val u = Url.parse("http://host/caf\u00e9/\u010A?q=\u00e9")
    assertEquals(u.rawPath, "/caf%C3%A9/%C4%8A")
    assertEquals(u.rawQuery, Some("q=%C3%A9"))
    assertEquals(u.path, "/caf\u00e9/\u010A")
  }

  test("parse: user information, bad ports and other schemes are refused with IllegalArgumentException") {
    intercept[IllegalArgumentException](Url.parse("http://user:pw@host/"))
    intercept[IllegalArgumentException](Url.parse("http://host:0/"))
    intercept[IllegalArgumentException](Url.parse("http://host:70000/"))
    intercept[IllegalArgumentException](Url.parse("ftp://host/"))
    intercept[IllegalArgumentException](Url.parse("/relative"))
    val u = Url.parse("HTTPS://Example.COM/a%20b?q=1#frag")
    assertEquals(
      (u.scheme, u.host, u.port, u.rawPath, u.rawQuery),
      ("https", "example.com", 443, "/a%20b", Some("q=1"))
    )
    assertEquals(u.path, "/a b")
    assertEquals(Url.parse("http://[::1]:8080/").authority, "[::1]:8080")
  }

  test("resolve follows RFC 3986: protocol-relative, absolute paths with URLs in the query, and dot segments") {
    val base = Url.parse("https://api.example.com/v1/items/7?x=1")
    assertEquals(base.resolve("//cdn.example/x").toString, "https://cdn.example/x")
    assertEquals(base.resolve("/login?next=http://x").toString, "https://api.example.com/login?next=http://x")
    assertEquals(base.resolve("../users").toString, "https://api.example.com/v1/users")
    assertEquals(base.resolve("http://other/").toString, "http://other/")
    assertEquals(base.resolve("?page=2").rawQuery, Some("page=2"))
  }

  test("digits of other scripts are not digits: not in an address, nor in a percent-escape") {
    assertEquals(gears.io.net.Tcp.numeric("127.0.0.1", 80).map(_.toString), Some("127.0.0.1:80"))
    assertEquals(gears.io.net.Tcp.numeric("\u0661\u0662\u0667.0.0.1", 80), None)
    assertEquals(gears.io.net.Tcp.numeric("::\u0661", 80), None)
    intercept[IllegalArgumentException](Url.decode("%\u0664\u0661", plusIsSpace = false))
    assertEquals(Url.decode("%4a%4A", plusIsSpace = false), "JJ")
  }

  test("a cookie's attributes cannot carry attributes of their own; a bad one parsed is ignored") {
    intercept[IllegalArgumentException](Cookie("a", "b", path = Some("/; Domain=evil.example")))
    intercept[IllegalArgumentException](Cookie("a", "b", domain = Some("evil.example; Secure")))
    assertEquals(
      Cookie("a", "b", path = Some("/x"), sameSite = Some(Cookie.SameSite.Lax)).rendered,
      "a=b; Path=/x; SameSite=Lax"
    )
    assertEquals(Cookie.parse("a=b; SameSite=strict").flatMap(_.sameSite), Some(Cookie.SameSite.Strict))
    assertEquals(Cookie.parse("a=b; SameSite=Lax; Domain=x").flatMap(_.sameSite), Some(Cookie.SameSite.Lax))
    assertEquals(Cookie.parse("a=b; SameSite=sometimes").flatMap(_.sameSite), scala.None, "an unknown value ignored")
    assertEquals(Cookie.parse("a=b; SameSite=Sideways; Path=/p"), Some(Cookie("a", "b", path = Some("/p"))))
  }

  test("the url interpolator escapes what it interpolates") {
    val name = "a/b c?d"
    assertEquals(url"https://h/users/$name?q=$name".toString, "https://h/users/a%2Fb%20c%3Fd?q=a%2Fb%20c%3Fd")
  }

  test("query parameters decode + and escapes") {
    assertEquals(Url.parseQuery("a=1&b=x+y&c=%C3%A9&d"), Seq("a" -> "1", "b" -> "x y", "c" -> "é", "d" -> ""))
  }

  test("dates are IMF-fixdate") {
    assertEquals(Wire.formatDate(784111777L), "Sun, 06 Nov 1994 08:49:37 GMT")
    assertEquals(Wire.formatDate(0L), "Thu, 01 Jan 1970 00:00:00 GMT")
  }
}
