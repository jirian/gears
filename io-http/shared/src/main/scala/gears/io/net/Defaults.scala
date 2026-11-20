package gears.io.net

import gears.async.Async
import gears.async.io.Io

object Defaults {

  def backend: DefaultPlatform.backend.type = DefaultPlatform.backend

  def run[T](program: Io[DefaultCapabilities] ?=> Async.Spawn ?=> T): T = Io.run(using backend)(program)
}
