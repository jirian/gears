package asyncio.unsafe

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
object NativeBuffer {

  def allocate(capacity: Int)(using Zone): ByteBuffer = {
    require(capacity > 0, "Buffer capacity must be positive")
    PointerBuffer.wrap(alloc[Byte](capacity), capacity)
  }

  /** A buffer holding `bytes`, flipped and ready to drain. Empty input still gets one byte of storage. */
  def of(bytes: Array[Byte])(using Zone): ByteBuffer = {
    val buf = allocate(math.max(bytes.length, 1))
    buf.put(bytes)
    buf.flip()
    buf
  }

  /** The address of the buffer's first byte. */
  def pointer(buf: ByteBuffer): Ptr[Byte] =
    buf.pointer()

  /** The address of the buffer's current position. */
  def atPosition(buf: ByteBuffer): Ptr[Byte] = buf.pointer() + buf.position()

  /** The address of the current position for heap or pointer-backed buffers. */
  def nativeAtPosition(buf: ByteBuffer): Ptr[Byte] =
    if buf.hasArray() then
      val index = buf.arrayOffset() + buf.position()
      if index < buf.array().length then buf.array().asInstanceOf[ByteArray].at(index) else null
    else if buf.hasPointer() then buf.pointer() + buf.position()
    else if buf.remaining() == 0 then null
    else throw new IllegalArgumentException("Buffer must be array-backed or pointer-backed")

  /** Moves the position forward after the kernel filled or drained `n` bytes at it. */
  def advance(buf: ByteBuffer, n: Int): Unit = buf.position(buf.position() + n)
}
