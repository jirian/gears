package gears.async.io
package shareable

/** The shared runtime's contracts over the fake backend. */
class FakeSchedulingSuite extends SchedulingContract {
  type B = FakeBackend.type
  val backend: B = FakeBackend
  def newPool(workers: Int): FakeScheduler = FakeScheduler(workers)
}

class FakeStressSuite extends StressContract {
  type B = FakeBackend.type
  val backend: B = FakeBackend
  def newPool(workers: Int): FakeScheduler = FakeScheduler(workers)
}

class FakeDeadlineSuite extends DeadlineContract {
  type B = FakeBackend.type
  val backend: B = FakeBackend
}

class FakeThreadedTimerSuite extends gears.async.io.testkit.ThreadedTimerContract {
  val backend: Backend = FakeBackend
}
