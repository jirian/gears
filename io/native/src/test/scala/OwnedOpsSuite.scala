package consumer.uring

import gears.io.uring.UringReactor

class OwnedOpsSuite extends munit.FunSuite:
  test("an op built by one uring reactor cannot be submitted to another"):
    val errors = compileErrors(
      "def cross(a: UringReactor, b: UringReactor): Unit = b.submit(a.ops.timer(1), _ => ())"
    )
    assert(errors.contains("Owned[(a : "), errors)

  test("an op built by one uring reactor cannot be cancelled on another"):
    val errors = compileErrors("def cross(a: UringReactor, b: UringReactor): Boolean = b.cancel(a.ops.timer(1))")
    assert(errors.contains("Owned[(a : "), errors)

  test("an op made without a reactor's ops cannot be submitted"):
    val errors = compileErrors(
      "def forge(a: UringReactor): Unit = a.submit(new gears.io.uring.UringTimer(1L), _ => ())"
    )
    assert(errors.contains("Owned"), errors)

  test("ops cannot be branded or built for a reactor outside the uring package"):
    assert(
      compileErrors(
        "def brand(a: UringReactor) = gears.io.uring.Owned[a.type, gears.io.uring.UringTimer](new gears.io.uring.UringTimer(1L))"
      ).nonEmpty
    )
    assert(
      compileErrors(
        "def builder(a: UringReactor) = new gears.io.uring.UringOps[a.type](gears.io.uring.PosixHostResolver)"
      ).nonEmpty
    )

  test("an op built by a reactor is accepted by that reactor"):
    assertNoDiff(compileErrors("def same(a: UringReactor): Unit = a.submit(a.ops.timer(1), _ => ())"), "")
