package gears.async.io
package shareable

import gears.async.io.shareable.TimerHeap
import gears.async.io.shareable.TimerHeap.TimerOp

import scala.collection.mutable

class TimerHeapSuite extends munit.FunSuite {
  private class HeapTimer(var deadline: Long) extends TimerOp {
    def deadlineNow: Long = deadline
    def timerWakeup(actAfter: Long): Unit = ()
  }

  private class Heap extends TimerHeap[HeapTimer] {
    override protected def isEmpty(): Boolean = true
  }

  private def drain(heap: Heap, now: Long): List[HeapTimer] =
    Iterator.continually(heap.nextReadyTimer(now)).takeWhile(_ != null).map(_.asInstanceOf[HeapTimer]).toList

  test("an empty heap has no deadline, and nothing ready") {
    val heap = Heap()
    assertEquals(heap.peekNextDeadline(), -1L)
    assert(heap.nextReadyTimer(Long.MaxValue) == null)
  }

  test("only what is due leaves the heap, earliest first, past its first capacity") {
    val heap = Heap()
    val timers = (1 to 100).reverse.map(i => HeapTimer(i.toLong))
    timers.foreach(heap.addTimer)
    assertEquals(heap.peekNextDeadline(), 1L)
    assertEquals(drain(heap, 50).map(_.deadline), (1L to 50L).toList)
    assertEquals(heap.peekNextDeadline(), 51L)
  }

  test("a timer can be removed once") {
    val heap = Heap()
    val timer = HeapTimer(1)
    heap.addTimer(timer)
    assert(heap.removeTimer(timer))
    assert(!heap.removeTimer(timer))
    assert(!heap.removeTimer(HeapTimer(1)))
  }

  test("a timer that fired can't be removed, and can be added again") {
    val heap = Heap()
    val timer = HeapTimer(1)
    heap.addTimer(timer)
    assertEquals(drain(heap, 1), List(timer))
    assert(!heap.removeTimer(timer))
    heap.addTimer(timer)
    assert(heap.removeTimer(timer))
  }

  test("a timer moved later, with no call to the heap, waits for its new deadline") {
    val heap = Heap()
    val moved = HeapTimer(10)
    val other = HeapTimer(20)
    heap.addTimer(moved)
    heap.addTimer(other)
    moved.deadline = 30
    assertEquals(heap.peekNextDeadline(), 10L) // the worker wakes for the old deadline, and finds nothing due
    assert(heap.nextReadyTimer(15) == null)
    assertEquals(heap.peekNextDeadline(), 20L)
    assertEquals(drain(heap, 40), List(other, moved))
  }

  test("a timer moved earlier comes forward") {
    val heap = Heap()
    val other = HeapTimer(10)
    val moved = HeapTimer(20)
    heap.addTimer(other)
    heap.addTimer(moved)
    moved.deadline = 5
    heap.notifyDeadlineChange(moved)
    assertEquals(heap.peekNextDeadline(), 5L)
    assertEquals(drain(heap, 40), List(moved, other))
  }

  test("a change to a timer no longer held is ignored") {
    val heap = Heap()
    val held = HeapTimer(10)
    heap.addTimer(held)
    heap.notifyDeadlineChange(HeapTimer(1))
    assertEquals(heap.peekNextDeadline(), 10L)
  }

  test("random adds, removals and moves match a plain model") {
    for seed <- 1 to 50 do {
      val random = new scala.util.Random(seed)
      val heap = Heap()
      val model = mutable.ArrayBuffer.empty[HeapTimer]
      var now = 0L
      for _ <- 1 to 2000 do
        random.nextInt(6) match {
          case 0 | 1 =>
            val timer = HeapTimer(now + random.nextInt(1000))
            heap.addTimer(timer)
            model += timer
          case 2 if model.nonEmpty =>
            val timer = model.remove(random.nextInt(model.size))
            assert(heap.removeTimer(timer))
          case 3 if model.nonEmpty =>
            val timer = model(random.nextInt(model.size))
            timer.deadline += random.nextInt(1000) // later: a store, and nothing else
          case 4 if model.nonEmpty =>
            val timer = model(random.nextInt(model.size))
            timer.deadline = math.max(now, timer.deadline - random.nextInt(1000))
            heap.notifyDeadlineChange(timer)
          case _ =>
            now += random.nextInt(100)
            val fired = drain(heap, now)
            val due = model.filter(_.deadline <= now)
            assertEquals(fired.toSet, due.toSet, s"seed $seed")
            assertEquals(fired.size, due.size, s"seed $seed")
            model --= due
        }
      assertEquals(drain(heap, Long.MaxValue).toSet, model.toSet, s"seed $seed")
    }
  }

  test("a timer taken off for good is marked so, and its completion fires it; one the heap fired, its completion not") {
    val heap = Heap()
    val taken = HeapTimer(1)
    val fired = HeapTimer(2)
    val held = HeapTimer(3)
    List(taken, fired, held).foreach(heap.addTimer)
    assert(heap.takeOff(taken) && taken.offHeap)
    assert(!heap.takeOff(taken), "it is off already")
    assert(heap.claimFiring(taken), "a completion fires one taken off")
    assertEquals(drain(heap, 2), List(fired))
    assert(!heap.takeOff(fired) && !fired.offHeap, "it left the heap to fire")
    assert(!heap.claimFiring(fired), "the heap fired it")
    assert(heap.claimFiring(held), "its completion takes it out")
    assertEquals(heap.peekNextDeadline(), -1L)
  }

  test("a completion racing a withdrawal's takeOff always finds that one of them is to fire the timer") {
    val heap = Heap()
    val rounds = 200_000
    val start = new java.util.concurrent.CyclicBarrier(2)
    val timers = Array.fill(rounds)(HeapTimer(1))
    val taken = new Array[Boolean](rounds)
    val withdrawer = new Thread(() =>
      for i <- 0 until rounds do {
        start.await()
        taken(i) = heap.takeOff(timers(i))
      }
    )
    withdrawer.start()
    var lost = 0
    for i <- 0 until rounds do {
      heap.addTimer(timers(i))
      start.await()
      // Neither takes it to have fired: the heap never fires it here, so a false claim would wake no one.
      if !heap.claimFiring(timers(i)) then lost += 1
    }
    withdrawer.join()
    assertEquals(lost, 0)
    assert(taken.contains(true) && taken.contains(false), "the race went only one way, so it tested nothing")
  }

  test("a timer with no deadline is refused, as it would fire at once") {
    val heap = Heap()
    val timer = new HeapTimer(-1) {
      override def hasDeadline: Boolean = false
    }
    intercept[IllegalArgumentException](heap.addTimer(timer))
    assertEquals(heap.peekNextDeadline(), -1L)
  }

  test("a deadline past the end of the clock stays there") {
    assertEquals(TimerHeap.deadlineAfter(Long.MaxValue), Long.MaxValue)
    val soon = TimerHeap.deadlineAfter(1000L)
    assert(soon > 0 && soon < Long.MaxValue)
  }
}
