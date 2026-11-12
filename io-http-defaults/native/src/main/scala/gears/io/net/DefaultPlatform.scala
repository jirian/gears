package gears.io.net

import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

/** Scala Native's default backend, io_uring, on Linux 6.0 or later, and how a process here learns it is asked to end.
  */
private[gears] object DefaultPlatform {
  val backend: gears.async.io.uring.UringBackend.type = gears.async.io.uring.UringBackend

  /** Runs `stop` on a thread of its own once the process is sent SIGTERM or SIGINT, which no longer end it, and returns
    * what to call once the process may end; a second signal ends it at once, as the signal would have, and so does one
    * that comes when no `stop` is waiting, before or after. For `Http.serve`: one `stop` waits at a time, the latest.
    * If the signals cannot be caught, `stop` never runs and they end the process as before.
    */
  def onTermination(stop: () => Unit): () => Unit =
    if !signals then () => ()
    else {
      waiting.set(stop)
      () => waiting.compareAndSet(stop, null): Unit
    }

  private val waiting = new java.util.concurrent.atomic.AtomicReference[(() => Unit) | Null](null)

  /** Whether the signals are caught, by one reader for the process, which hands each to the `stop` waiting. */
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
