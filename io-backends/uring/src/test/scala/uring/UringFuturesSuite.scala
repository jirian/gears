package gears.async.io
package uring

import gears.async.Async
import gears.async.AsyncOperations
import gears.async.io.Io
import gears.async.io.internal.ReactorRuns
import gears.async.io.testkit.FuturesContract

/** `FuturesContract` over io_uring's reactors, on a pool of `workers` workers. */
abstract class UringFuturesSuite(workers: Int) extends FuturesContract {
  val backend: UringBackend.type = UringBackend

  protected def running[T](body: Io[backend.type] ?=> (Async.Spawn, AsyncOperations) ?=> T): T =
    ReactorRuns.run(workers)(using backend)(body)
}

class UringFuturesOnOneSuite extends UringFuturesSuite(workers = 1)
class UringFuturesOnFourSuite extends UringFuturesSuite(workers = 4)
