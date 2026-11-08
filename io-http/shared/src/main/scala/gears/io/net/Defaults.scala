package gears.io.net

import gears.async.Async
import gears.async.io.Blocking
import gears.async.io.Io
import gears.async.io.IpDatagrams
import gears.async.io.IpStreams
import gears.async.io.Pipes
import gears.async.io.RegularFiles
import gears.async.io.Resolving
import gears.async.io.UnixStreams

import scala.util.control.NonFatal

/** What every platform's default backend can do (`Platform.backend`): what a program that runs on any of them may use.
  */
type DefaultCapabilities = IpStreams & IpDatagrams & UnixStreams & Resolving & RegularFiles & Pipes & Blocking

/** Runs programs on the platform's default backend. */
object Defaults {

  /** Runs `program` with an `Io` over the platform's default backend, NIO on the JVM and io_uring on Scala Native
    * (Linux), as `Io.run` does, and returns what it returns. What the run's users registered (`atEnd`) runs as it
    * returns, still in the run.
    */
  def run[T](program: Io[DefaultCapabilities] ?=> Async.Spawn ?=> T): T = {
    val backend = Platform.backend
    Io.run(using backend) { io ?=> spawn ?=>
      endings.synchronized(endings.put(io, Nil))
      try program(using io)(using spawn)
      finally
        for hook <- endings.synchronized(endings.remove(io)).nn.reverse do
          try hook(using spawn)
          catch case NonFatal(_) => ()
    }
  }

  private val endings = new java.util.IdentityHashMap[Io[?], List[Async ?=> Unit]]()

  /** Registers `hook` to run as the run of `io` returns, if `io` is a run's of `run`, and says whether it is: for what
    * lives as long as a run, such as the client `Http`'s functions share.
    */
  private[gears] def atEnd(io: Io[?])(hook: Async ?=> Unit): Boolean =
    endings.synchronized {
      val hooks = endings.get(io)
      if hooks == null then false
      else {
        endings.put(io, hook :: hooks)
        true
      }
    }
}
