import gears.asyncio.Reactor
import gears.asyncio.uring.UringReactor

object TestReactor:
  def open(): Reactor = UringReactor.open()
