package gears.async.io
package shareable

/** The shared runtime's contracts over the fake backend. */
class FakeSchedulingSuite extends SchedulingContract {
  type I = FakeBackend.ops.type
  val backend: FakeBackend.type = FakeBackend
  def newPool(workers: Int): FakeScheduler = FakeScheduler(workers)
}

class FakeStressSuite extends StressContract {
  type I = FakeBackend.ops.type
  val backend: FakeBackend.type = FakeBackend
  def newPool(workers: Int): FakeScheduler = FakeScheduler(workers)
}

class FakeDeadlineSuite extends DeadlineContract {
  type I = FakeBackend.ops.type
  val backend: FakeBackend.type = FakeBackend
}

class FakeThreadedTimerSuite extends gears.async.io.testkit.ThreadedTimerContract {
  val backend: Backend = FakeBackend
}
