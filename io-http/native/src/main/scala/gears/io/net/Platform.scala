package gears.io.net

import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

/** What Scala Native gives the higher level that differs by platform. */
object Platform {

  /** `n` random bytes from the kernel, for a WebSocket client's masks. */
  def randomBytes(n: Int): Array[Byte] = {
    val out = new Array[Byte](n)
    var got = 0
    while got < n do {
      val r = getrandom(out.asInstanceOf[scala.scalanative.runtime.ByteArray].at(got), (n - got).toUSize, 0)
      if r < 0 then throw new java.io.IOException("getrandom failed")
      got += r.toInt
    }
    out
  }

  @extern private object sys {
    def getrandom(buf: Ptr[Byte], length: CSize, flags: CUnsignedInt): CSSize = extern
  }
  private def getrandom(buf: Ptr[Byte], length: CSize, flags: Int): CSSize = sys.getrandom(buf, length, flags.toUInt)
}
