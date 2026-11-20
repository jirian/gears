package uringdemos

import gears.async.Async
import gears.async.io.Io
import gears.async.io.uring.UringBackend
import gears.async.io.uring.UringOps
import gears.io.demos.{DemoBackends, DemoCommands}

import mainargs.ParserForMethods

object UringDemos {

  private object UringRunner extends gearsbench.BenchRunner[UringOps.type] {
    val backend: UringBackend.type = UringBackend
    val ops: UringOps.type = UringOps
    def run[T](program: Io[UringOps.type] ?=> Async.Spawn ?=> T): T = Io.run(using backend)(program)
    def onPool[T](workers: Int)(program: Io[UringOps.type] ?=> Async.Spawn ?=> T): T =
      UringBackend.runOn(workers)(program)
  }

  val backends: DemoBackends = DemoBackends(Seq("uring" -> UringRunner), default = "uring")

  def main(args: Array[String]): Unit =
    if args.headOption.exists(_.startsWith("bench")) then
      ParserForMethods(gearsbench.BenchCommands(backends)).runOrExit(args.toIndexedSeq)
    else DemoCommands(backends).main(args)
}
