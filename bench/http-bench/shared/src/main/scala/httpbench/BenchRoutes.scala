package httpbench

import gears.io.http.*

/** The routes the HTTP bench serves, the same as its Go counterpart's: a fixed text, and a path parameter answered with
  * a small JSON document.
  */
object BenchRoutes {
  val routes: Routes = Routes(
    GET("/hello") { _ => "Hello, world" },
    GET("/items/{id:Long}") { (_, p) =>
      Response.text(s"""{"id":${p.id},"name":"item ${p.id}"}""", contentType = "application/json")
    }
  )

  def config(port: Int): ServerConfig = ServerConfig(host = "127.0.0.1", port = port, log = _ => ())
}
