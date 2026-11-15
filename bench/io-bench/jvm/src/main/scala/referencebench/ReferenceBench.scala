package referencebench

import gears.async.Async
import gears.async.io.Io
import gears.async.io.reference.{ReferenceBackend, ReferenceOps}
import gears.io.demos.DemoBackends

import mainargs.ParserForMethods

/** The gears bench over the reference backend, run as `ioReferenceBench/run bench` and `bench-serve`
  * (`gearsbench.BenchCommands`): the JVM's column of the bench, beside io_uring's (`uringdemos.UringDemos`).
  */
object ReferenceBench {

  /** Runs programs over the reference backend. Its futures are virtual threads, each performing its own ops, so a pool
    * of any size is `Io.run`.
    */
  private object Runner extends gearsbench.BenchRunner[ReferenceOps.type] {
    val ops: ReferenceOps.type = ReferenceOps
    def run[T](program: Io[ReferenceOps.type] ?=> Async.Spawn ?=> T): T = Io.run(using ReferenceBackend)(program)
    def onPool[T](workers: Int)(program: Io[ReferenceOps.type] ?=> Async.Spawn ?=> T): T = run(program)
  }

  val backends: DemoBackends = DemoBackends(Seq("reference" -> Runner), default = "reference")

  def main(args: Array[String]): Unit = ParserForMethods(gearsbench.BenchCommands(backends)).runOrExit(args.toIndexedSeq)
}
