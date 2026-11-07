package httpbench

import gears.io.http.Http

/** Serves the bench's routes on the JVM, over `NioOnCaller`, the default backend: `<port>`. */
object Main {
  def main(args: Array[String]): Unit =
    Http.serve(BenchRoutes.config(args.headOption.fold(8091)(_.toInt)))(BenchRoutes.routes)
}
