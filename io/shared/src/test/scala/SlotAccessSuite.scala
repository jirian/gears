package callers

import munit.FunSuite

/** Lives outside `gears.io`, where callers of the interface live. */
class SlotAccessSuite extends FunSuite {
  test("callers can take a slot's value but not write to it") {
    assertEquals(compileErrors("def read(slot: gears.io.reactor.Slot[Integer]): Integer = slot.clear()"), "")
    assert(
      compileErrors("def write(slot: gears.io.reactor.Slot[Integer]): Unit = slot.set(Integer.valueOf(1))").nonEmpty
    )
    assertEquals(compileErrors("def read(slot: gears.io.reactor.HandleSlot): Int = slot.clear()"), "")
    assert(compileErrors("def write(slot: gears.io.reactor.HandleSlot): Unit = slot.set(1)").nonEmpty)
  }
}
