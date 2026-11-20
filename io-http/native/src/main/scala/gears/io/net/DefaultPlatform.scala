package gears.io.net

import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

private[gears] object DefaultPlatform {
  val backend: gears.async.io.uring.UringBackend.type = gears.async.io.uring.UringBackend

  def onTermination(stop: () => Unit): () => Unit =
    if !signals then () => ()
    else {
      waiting.set(stop)
      () => waiting.compareAndSet(stop, null): Unit
    }

  private val waiting = new java.util.concurrent.atomic.AtomicReference[(() => Unit) | Null](null)

  private lazy val signals: Boolean = {
    val fd = sys.gears_http_signal_pipe()
    if fd >= 0 then {
      val reader = new Thread(
        () => {
          val signal = stackalloc[Byte]()
          var reading = true
          while reading do {
            val n = scala.scalanative.posix.unistd.read(fd, signal, 1.toUSize)
            if n == 1 then {
              val stop = waiting.getAndSet(null)
              if stop != null then stop()
              else System.exit(128 + (!signal & 0xff))
            } else if n < 0 && scala.scalanative.posix.errno.errno != scala.scalanative.posix.errno.EINTR then
              reading = false
          }
        },
        "gears-http-signals"
      )
      reader.setDaemon(true)
      reader.start()
    }
    fd >= 0
  }

  @extern private object sys {
    def gears_http_signal_pipe(): CInt = extern
  }
}
