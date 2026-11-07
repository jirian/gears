package niodemos

import gears.async.Async
import gears.async.io.Io
import gears.async.io.nio.NioOnCaller
import gears.io.demos.{DemoBackends, DemoCommands}

import mainargs.ParserForMethods

/** The testkit's gears demos over NIO (`NioOnCaller`), run as `ioNioDemos/run <command>`, and the gears bench over it
  * as `ioNioDemos/run bench` and `bench-serve` (`gearsbench.BenchCommands`).
  */
object NioDemos {

  /** Runs programs over NIO. Its futures are gears' virtual threads, which the JDK schedules: `--workers` changes
    * nothing (bound them with `-Djdk.virtualThreadScheduler.parallelism`).
    */
  private object NioRunner extends gearsbench.BenchRunner[NioOnCaller.type] {
    val backend: NioOnCaller.type = NioOnCaller
    def run[T](program: Io[NioOnCaller.type] ?=> Async.Spawn ?=> T): T = Io.run(using backend)(program)
    def onPool[T](workers: Int)(program: Io[NioOnCaller.type] ?=> Async.Spawn ?=> T): T = run(program)
  }

  val backends: DemoBackends = DemoBackends(Seq("caller" -> NioRunner), default = "caller")

  def main(args: Array[String]): Unit = {
    val command = if args.headOption.contains("gears") then args.tail else args
    if command.headOption.exists(_.startsWith("bench")) then
      ParserForMethods(gearsbench.BenchCommands(backends)).runOrExit(command.toIndexedSeq)
    else DemoCommands(backends).main(command)
  }
}
