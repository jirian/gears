package gears.async.asyncio.uring

import uring._
import uringOps._

import java.util.concurrent.atomic.AtomicBoolean
import scala.scalanative.libc.stdlib
import scala.scalanative.unsafe._
import scala.scalanative.unsigned._

/** Does a completion closure whose ONLY reference anywhere is the raw
  * pointer io_uring_sqe_set_data erased into the kernel's user_data field
  * survive real, concurrent GC pressure while the op is in flight - with
  * nothing in Scala-visible memory (no val, no field, no continuation)
  * holding a reference to it?
  *
  * Deliberately NOT run through Async/submitAwait: the closure below is
  * passed as a bare literal directly to ring.submit and never bound to
  * anything else, so this isolates exactly the reachability question
  * scheduler.scala's `ts` (and every other completion closure in the
  * uring backend) actually depends on.
  */
@main def closureLivenessStressTest(): Unit =
  val ring = UringRing()

  val fired = new AtomicBoolean(false)
  val marker = s"closure-survived-${scala.util.Random.nextInt()}"

  val ts = stdlib.malloc(sizeof[__kernel_timespec]).asInstanceOf[Ptr[__kernel_timespec]]
  ts.tv_sec = 1
  ts.tv_nsec = 0

  ring.submit(sqe => io_uring_prep_timeout(sqe, ts, 0.toULong, 0.toUInt)) { res =>
    stdlib.free(ts.asInstanceOf[Ptr[Byte]])
    println(s"completion fired: res=$res, marker='$marker'")
    fired.set(true)
  }
  // Nothing above is stored anywhere - the closure passed to ring.submit
  // is a bare literal, never bound to a val or field in this frame.

  val until = System.currentTimeMillis() + 1500
  var rounds = 0
  while System.currentTimeMillis() < until do
    var i = 0
    while i < 5 do
      val junk = new Array[Byte](1 << 20)
      java.util.Arrays.fill(junk, 1.toByte)
      i += 1
    System.gc()
    rounds += 1
  println(s"ran $rounds GC rounds while the 1s timeout (and its completion closure) were in flight, unreferenced")

  Thread.sleep(500)
  if fired.get() then
    println("PASSED: completion closure survived being reachable only via the kernel-stored raw pointer")
  else println("FAILED: closure never fired - collected, or something else went wrong")
