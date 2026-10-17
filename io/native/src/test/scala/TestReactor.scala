import gears.io.kqueue.KqueueReactor
import gears.io.reactor.Reactor
import gears.io.uring.UringReactor

import scala.scalanative.meta.LinktimeInfo

object TestReactor:
  def open(): Reactor =
    if LinktimeInfo.isLinux then UringReactor.open()
    else KqueueReactor.open()
