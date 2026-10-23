package asyncio.gears

import gears.async.Async
import gears.async.AsyncOperations

import asyncio.Io
import asyncio.uring.UringBackend
import higherlevel.HigherLevelChecks

/** `FuturesContract` over io_uring's reactors, on a pool of `workers` workers. */
abstract class UringFuturesSuite(workers: Int)
    extends FuturesContract
    with ReadinessFuturesChecks
    with HigherLevelChecks {
  val backend: UringBackend.type = UringBackend

  protected def running[T](body: Io[backend.type] => (Async.Spawn, AsyncOperations) ?=> T): T =
    ReactorFutures.run(workers)(using backend)(body)
}

class UringFuturesOnOneSuite extends UringFuturesSuite(workers = 1)
class UringFuturesOnFourSuite extends UringFuturesSuite(workers = 4)
