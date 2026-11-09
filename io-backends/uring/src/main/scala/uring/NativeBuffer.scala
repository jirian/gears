// From the old shared native runtime (gears.async.io.internal on async-io, 1d7add2), now the io_uring backend's.
package gears.async.io
package uring

import java.nio.ByteBuffer
import scala.scalanative.memory.PointerBuffer
import scala.scalanative.memory.PointerBufferOps.given
import scala.scalanative.runtime.ByteArray
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

/** Pointer-backed byte buffers over zone memory. `ByteBuffer.allocateDirect` on Scala Native is heap-backed, so
  * `PointerBuffer.wrap` is the supported way to get a buffer the kernel can address. The memory lives as long as the
  * zone it was allocated in.
  */
private[uring] object NativeBuffer {

  /** Whether the kernel can be handed `buf` at its own address: it has native memory behind it, as `PointerBuffer.wrap`
    * makes, or is over a heap array, as `ByteBuffer.allocate` and, on Scala Native, `allocateDirect` make, which the
    * collector does not move and the buffer keeps alive. A read-only heap buffer hides its array, and is not.
    */
  def addressable(buf: ByteBuffer): Boolean = buf.hasPointer() || buf.hasArray()

  /** Runs `body` with a buffer the kernel can address standing for `buf`, and returns what it returns: `buf` itself if
    * it is `addressable`; else, a read-only one, a stand-in over memory of its own, holding `buf`'s remaining bytes if
    * `copyIn`, so that an op takes any buffer, at a copy's cost for such a one. What `body` moves the stand-in's
    * position over is then `buf`'s: copied back into it if `copyOut`, its position advanced as far. For a call that is
    * done as it returns: the kernel keeps none of these.
    */
  inline def staged[A](buf: ByteBuffer, inline copyIn: Boolean, inline copyOut: Boolean)(
      inline body: ByteBuffer => A
  ): A =
    if addressable(buf) then body(buf)
    else {
      val zone = Zone.open()
      try {
        val stand = standIn(buf, stagingMemory(buf.remaining())(using zone), copyIn)
        val result = body(stand)
        settle(buf, stand, copyOut)
        result
      } finally zone.close()
    }

  /** As the other `staged`, for buffers taken in order: each `addressable` one stands for itself, the others each by a
    * stand-in of its own.
    */
  inline def staged[A](bufs: Array[ByteBuffer], inline copyIn: Boolean, inline copyOut: Boolean)(
      inline body: Array[ByteBuffer] => A
  ): A =
    if allAddressable(bufs) then body(bufs)
    else {
      val stands = bufs.clone()
      val zone = Zone.open() // the copies' memory, all of it freed as the op is done
      try {
        var i = 0
        while i < bufs.length do {
          if !addressable(bufs(i)) then
            stands(i) = standIn(bufs(i), stagingMemory(bufs(i).remaining())(using zone), copyIn)
          i += 1
        }
        val result = body(stands)
        i = 0
        while i < bufs.length do {
          if !(stands(i) eq bufs(i)) then settle(bufs(i), stands(i), copyOut)
          i += 1
        }
        result
      } finally zone.close()
    }

  def allAddressable(bufs: Array[ByteBuffer]): Boolean = {
    var i = 0
    while i < bufs.length && addressable(bufs(i)) do i += 1
    i == bufs.length
  }

  /** Memory for a stand-in of `size` bytes, in `zone`. */
  def stagingMemory(size: Int)(using zone: Zone): Ptr[Byte] = alloc[Byte](math.max(size, 1))

  /** A stand-in for `buf` over `memory`: its remaining bytes long, holding them if `copyIn`, positioned at 0. */
  def standIn(buf: ByteBuffer, memory: Ptr[Byte], copyIn: Boolean): ByteBuffer = {
    val size = buf.remaining()
    val stand = PointerBuffer.wrap(memory, math.max(size, 1))
    stand.limit(size)
    if copyIn then {
      stand.put(buf.duplicate())
      stand.position(0)
    }
    stand
  }

  /** What an op did to `stand` made `buf`'s: the bytes it moved its position over copied into `buf` if `copyOut`, and
    * `buf`'s position advanced as far.
    */
  def settle(buf: ByteBuffer, stand: ByteBuffer, copyOut: Boolean): Unit = {
    val moved = stand.position()
    if copyOut then {
      val got = stand.duplicate()
      got.flip()
      buf.put(got)
    } else buf.position(buf.position() + moved)
  }

  /** The address of an `addressable` buffer's current position. */
  def atPosition(buf: ByteBuffer): Ptr[Byte] =
    if buf.hasPointer() then buf.pointer() + buf.position()
    else buf.array().asInstanceOf[ByteArray].atUnsafe(buf.arrayOffset() + buf.position())

  /** Moves the position forward after the kernel filled or drained `n` bytes at it. */
  def advance(buf: ByteBuffer, n: Int): Unit = buf.position(buf.position() + n)
}
