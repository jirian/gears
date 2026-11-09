package gears.async.io
package uring

import gears.async.io.shareable.DeadlineContract
import gears.async.io.shareable.SchedulingContract
import gears.async.io.shareable.StressContract

/** The shared runtime's contracts (from `io-runtime`'s tests) over io_uring's workers. */
class UringSchedulingSuite extends SchedulingContract {
  type B = UringBackend.type
  val backend: B = UringBackend
  def newPool(workers: Int): UringScheduler = UringScheduler(workers)
}

class UringStressSuite extends StressContract {
  type B = UringBackend.type
  val backend: B = UringBackend
  def newPool(workers: Int): UringScheduler = UringScheduler(workers)
}

class UringDeadlineSuite extends DeadlineContract {
  type B = UringBackend.type
  val backend: B = UringBackend
}

class UringThreadedTimerSuite extends gears.async.io.testkit.ThreadedTimerContract {
  val backend: Backend = UringBackend
}
