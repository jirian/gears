package httpbench

import gears.io.http.Http

object Main {
  def main(args: Array[String]): Unit =
    Http.serve(BenchRoutes.config(args.headOption.fold(8091)(_.toInt)))(BenchRoutes.routes)
}
