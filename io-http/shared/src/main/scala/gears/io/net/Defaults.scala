package gears.io.net

import gears.async.Async
import gears.async.AsyncOperations
import gears.async.io.Blocking
import gears.async.io.Io
import gears.async.io.IpDatagrams
import gears.async.io.IpStreams
import gears.async.io.Pipes
import gears.async.io.RegularFiles
import gears.async.io.Resolving
import gears.async.io.UnixStreams

/** What every platform's default backend can do (`Platform.backend`): what a program that runs on any of them may use.
  */
type DefaultCapabilities = IpStreams & IpDatagrams & UnixStreams & Resolving & RegularFiles & Pipes & Blocking

/** Runs programs on the platform's default backend. */
object Defaults {

  /** Runs `program` with an `Io` over the platform's default backend, NIO on the JVM and io_uring on Scala Native
    * (Linux), as `Io.run` does, and returns what it returns.
    */
  def run[T](program: Io[DefaultCapabilities] ?=> (Async.Spawn, AsyncOperations) ?=> T): T = {
    val backend = Platform.backend
    Io.run(using backend)(program)
  }
}
