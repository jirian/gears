package uringdemos

import gears.async.Async
import gears.async.AsyncOperations
import gears.async.io.Io
import gears.async.io.internal.ReactorRuns
import gears.async.io.internal.TakePolicy
import gears.async.io.uring.UringBackend
import gears.io.demos.{DemoBackends, DemoCommands, DemoRunner}

import mainargs.ParserForMethods

/** The testkit's gears demos over io_uring, run as `ioUringDemos/run <command>` with the demos' commands and flags, and
  * the gears bench over it as `ioUringDemos/run bench` and `bench-serve` (`gearsbench.BenchCommands`).
  *
  * This is the edge: the demos and the bench only know the `gears.async.io` interface, and this is the one place that
  * chooses io_uring and starts runs over it.
  */
object UringDemos {

  /** Runs programs over io_uring: with `Io.run`, or with `ReactorRuns.run` on a pool of the workers asked for. */
  private object UringRunner extends DemoRunner[UringBackend.type] {
    val backend: UringBackend.type = UringBackend

    def run[T](program: Io[UringBackend.type] ?=> (Async.Spawn, AsyncOperations) ?=> T): T =
      Io.run(using backend)(program)

    def onPool[T](workers: Int, policy: TakePolicy)(
        program: Io[UringBackend.type] ?=> (Async.Spawn, AsyncOperations) ?=> T
    ): T =
      ReactorRuns.run(workers, policy)(using backend)(program)
  }

  /** The one backend here, `uring`. */
  val backends: DemoBackends = DemoBackends(Seq("uring" -> UringRunner), default = "uring")

  def main(args: Array[String]): Unit =
    if args.headOption.exists(_.startsWith("bench")) then
      ParserForMethods(gearsbench.BenchCommands(backends)).runOrExit(args.toIndexedSeq)
    else DemoCommands(backends).main(args)
}
