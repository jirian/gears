package gears.async.io
package uring

import gears.async.io.shareable.DeadlineContract
import gears.async.io.shareable.SchedulingContract
import gears.async.io.shareable.StressContract

class UringSchedulingSuite extends SchedulingContract {
  type I = UringOps.type
  val backend: UringBackend.type = UringBackend
  def newPool(workers: Int): UringScheduler = UringScheduler(workers)
}

class UringStressSuite extends StressContract {
  type I = UringOps.type
  val backend: UringBackend.type = UringBackend
  def newPool(workers: Int): UringScheduler = UringScheduler(workers)
}

class UringDeadlineSuite extends DeadlineContract {
  type I = UringOps.type
  val backend: UringBackend.type = UringBackend
}

class UringThreadedTimerSuite extends gears.async.io.testkit.ThreadedTimerContract {
  val backend: Backend = UringBackend
}
