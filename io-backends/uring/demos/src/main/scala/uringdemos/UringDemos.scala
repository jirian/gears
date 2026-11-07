package uringdemos

import gears.async.Async
import gears.async.io.Io
import gears.async.io.uring.UringBackend
import gears.io.demos.{DemoBackends, DemoCommands}

import mainargs.ParserForMethods

/** The testkit's gears demos over io_uring, run as `ioUringDemos/run <command>`, and the gears bench over it as
  * `ioUringDemos/run bench` and `bench-serve` (`gearsbench.BenchCommands`). The demos and the bench know only the
  * `gears.async.io` interface: this is the one place that chooses io_uring and starts runs over it.
  */
object UringDemos {

  /** Runs programs over io_uring: with `Io.run`, or on a pool of the workers asked for (`UringBackend.runOn`). */
  private object UringRunner extends gearsbench.BenchRunner[UringBackend.type] {
    val backend: UringBackend.type = UringBackend
    def run[T](program: Io[UringBackend.type] ?=> Async.Spawn ?=> T): T = Io.run(using backend)(program)
    def onPool[T](workers: Int)(program: Io[UringBackend.type] ?=> Async.Spawn ?=> T): T =
      UringBackend.runOn(workers)(program)
  }

  /** The one backend here, `uring`. */
  val backends: DemoBackends = DemoBackends(Seq("uring" -> UringRunner), default = "uring")

  def main(args: Array[String]): Unit =
    if args.headOption.exists(_.startsWith("bench")) then
      ParserForMethods(gearsbench.BenchCommands(backends)).runOrExit(args.toIndexedSeq)
    else DemoCommands(backends).main(args)
}
