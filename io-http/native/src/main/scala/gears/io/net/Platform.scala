package gears.io.net

import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

/** Scala Native's default backend: io_uring, on Linux 6.0 or later. */
object Platform {
  val backend: gears.async.io.uring.UringBackend.type = gears.async.io.uring.UringBackend

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

  /** Runs `stop` on a thread of its own once the process is sent SIGTERM or SIGINT, which no longer end it, and returns
    * what to call once the process may end; a second signal ends it at once, as the signal would have. For
    * `Http.serve`. If the signals cannot be caught, `stop` never runs and they end the process as before.
    */
  def onTermination(stop: () => Unit): () => Unit = {
    val fd = sys.gears_http_signal_pipe()
    if fd >= 0 then {
      val reader = new Thread(
        () => {
          val signal = stackalloc[Byte]()
          var signals = 0
          while signals < 2 do {
            if scala.scalanative.posix.unistd.read(fd, signal, 1.toUSize) == 1 then {
              signals += 1
              if signals == 1 then stop()
              else System.exit(128 + (!signal & 0xff))
            }
          }
        },
        "gears-http-signals"
      )
      reader.setDaemon(true)
      reader.start()
    }
    () => ()
  }

  @extern private object sys {
    def getrandom(buf: Ptr[Byte], length: CSize, flags: CUnsignedInt): CSSize = extern
    def gears_http_signal_pipe(): CInt = extern
  }
  private def getrandom(buf: Ptr[Byte], length: CSize, flags: Int): CSSize = sys.getrandom(buf, length, flags.toUInt)
}
