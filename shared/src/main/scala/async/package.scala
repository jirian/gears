package gears

/** Asynchronous programming support with direct-style Scala.
  * @see
  *   [[gears.async.Async]] for an introduction to the [[Async]] context and how to create them.
  * @see
  *   [[gears.async.Future]] for a simple interface to spawn concurrent computations.
  * @see
  *   [[gears.async.Channel]] for a simple inter-future communication primitive.
  */
package object async:
  type CancellationException = java.util.concurrent.CancellationException

  /** The [[CancellationException]] gears throws when a computation is cancelled: a new one each time, with no stack
    * trace. A cancel is control flow, not a fault, so its trace says nothing, and filling one in is what a cancel costs
    * most: about 250 µs on Scala Native, which walks the stack, against a few µs without. Not one shared instance,
    * since anything may add suppressed exceptions to one it catches.
    */
  private[async] def cancellation(): CancellationException = StacklessCancellation()

  private final class StacklessCancellation extends CancellationException:
    override def fillInStackTrace(): Throwable = this
