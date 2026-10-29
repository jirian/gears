package gears.async.io.uring

import gears.async.Async
import gears.async.AsyncOperations
import gears.async.io.Io
import gears.async.io.internal.ReactorRuns
import gears.io.higherlevel.HigherLevelChecks

/** The higher level over io_uring's reactors, on a pool of four workers. */
class UringHigherLevelSuite extends HigherLevelChecks {
  val backend: UringBackend.type = UringBackend

  protected def running[T](body: Io[backend.type] ?=> (Async.Spawn, AsyncOperations) ?=> T): T =
    ReactorRuns.run(4)(using backend)(body)
}
