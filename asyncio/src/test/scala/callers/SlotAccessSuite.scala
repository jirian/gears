package callers

import munit.FunSuite

/** Lives outside `asyncio`, where callers of the interface live. */
class SlotAccessSuite extends FunSuite {
  test("callers can take a slot's value but not write to it") {
    assertEquals(compileErrors("def read(slot: asyncio.Slot[Integer]): Integer = slot.clear()"), "")
    assert(compileErrors("def write(slot: asyncio.Slot[Integer]): Unit = slot.set(Integer.valueOf(1))").nonEmpty)
  }
}
