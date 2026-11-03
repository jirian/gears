package httpbench

import gears.async.Async
import gears.async.io.Io
import gears.async.io.nio.NioBackend
import gears.io.http.Http

/** Serves the bench's routes on the JVM: `<port> [caller|reactors]`, over `NioOnCaller` (the default backend) or
  * `NioBackend`, whose reactor pool carries the futures (it needs the `--add-opens` flags).
  */
object Main {
  def main(args: Array[String]): Unit = {
    val port = args.headOption.fold(8091)(_.toInt)
    args.lift(1).getOrElse("caller") match {
      case "caller"   => Http.serve(BenchRoutes.config(port))(BenchRoutes.routes)
      case "reactors" =>
        Io.run(using NioBackend) {
          Async.group(Http.server(BenchRoutes.routes, BenchRoutes.config(port)).await())
        }
      case other => sys.error(s"No backend $other: caller or reactors")
    }
  }
}
