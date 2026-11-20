package gears.async.io
package uring

import scala.scalanative.libc
import scala.scalanative.posix.signal
import scala.scalanative.posix.signalOps.*
import scala.scalanative.unsafe.*

private[uring] object SigPipe {

  lazy val ignored: Boolean = Zone.acquire { implicit z =>
    val current = alloc[signal.sigaction]()
    if signal.sigaction(signal.SIGPIPE, null, current) != 0 then false
    else if CFuncPtr.toPtr(current.sa_handler) != CFuncPtr.toPtr(libc.signal.SIG_DFL) then
      CFuncPtr.toPtr(current.sa_handler) == CFuncPtr.toPtr(libc.signal.SIG_IGN)
    else {
      val ignore = alloc[signal.sigaction]()
      ignore.sa_handler = libc.signal.SIG_IGN
      signal.sigaction(signal.SIGPIPE, ignore, null) == 0
    }
  }
}
