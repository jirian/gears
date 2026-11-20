package referencebench

import gears.async.Async
import gears.async.io.Io
import gears.async.io.reference.{ReferenceBackend, ReferenceOps}
import gears.io.demos.DemoBackends

import mainargs.ParserForMethods

object ReferenceBench {

  private object Runner extends gearsbench.BenchRunner[ReferenceOps.type] {
    val ops: ReferenceOps.type = ReferenceOps
    def run[T](program: Io[ReferenceOps.type] ?=> Async.Spawn ?=> T): T = Io.run(using ReferenceBackend)(program)
    def onPool[T](workers: Int)(program: Io[ReferenceOps.type] ?=> Async.Spawn ?=> T): T = run(program)
  }

  val backends: DemoBackends = DemoBackends(Seq("reference" -> Runner), default = "reference")

  def main(args: Array[String]): Unit =
    ParserForMethods(gearsbench.BenchCommands(backends)).runOrExit(args.toIndexedSeq)
}
