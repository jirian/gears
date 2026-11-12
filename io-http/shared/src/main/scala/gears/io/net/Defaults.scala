package gears.io.net

import gears.async.Async
import gears.async.io.Io

/** Runs programs on the platform's default backend. */
object Defaults {

  /** The platform's default backend: the reference backend on the JVM, io_uring on Scala Native (Linux). */
  def backend: DefaultPlatform.backend.type = DefaultPlatform.backend

  /** Runs `program` with an `Io` over the platform's default backend, as `Io.run` does, and returns what it returns. */
  def run[T](program: Io[DefaultCapabilities] ?=> Async.Spawn ?=> T): T = Io.run(using backend)(program)
}
