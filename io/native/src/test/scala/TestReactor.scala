import gears.asyncio.Reactor
import gears.asyncio.kqueue.KqueueReactor
import gears.asyncio.uring.UringReactor

import scala.scalanative.meta.LinktimeInfo

object TestReactor:
  def open(): Reactor =
    if LinktimeInfo.isLinux then UringReactor.open()
    else KqueueReactor.open()
