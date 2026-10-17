import gears.io.reactor.Reactor
import gears.io.uring.UringReactor

object TestReactor:
  def open(): Reactor = UringReactor.open()
