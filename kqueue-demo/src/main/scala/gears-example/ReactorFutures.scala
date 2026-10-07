package gearsexample

import gears.async.Async
import gears.async.AsyncSupport
import gears.async.Cancellable
import gears.async.CompletionGroup
import gears.async.Future
import gears.async.Listener
import gears.async.Scheduler
import gears.async.native.NativeSuspend

import java.util.concurrent.CancellationException
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.FiniteDuration
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import asyncio.Completion
import asyncio.Promise
import asyncio.Reactor

/** A gears scheduler that runs everything on the thread that created it, using that thread's reactor as its event loop:
  * queued tasks, such as starting a future or resuming a suspended one, run from a completion on the reactor. So code
  * running under this scheduler submits ops to the reactor directly.
  *
  * Futures inherit their parent's scheduler, so a program started here, and everything it spawns, runs on this one
  * thread. There is no work stealing or balancing yet.
  */
final class ReactorScheduler private[gearsexample] (val reactor: Reactor) extends Scheduler {
  private val owner = Thread.currentThread()
  private val tasks = new ConcurrentLinkedQueue[Runnable]()
  @volatile private var wakeup: Wakeup | Null = null

  ReactorScheduler.current.set(this)
  arm()

  def isCurrent: Boolean = Thread.currentThread() eq owner

  /** Queues `body` to run on the reactor thread, from any thread. */
  def execute(body: Runnable): Unit = {
    tasks.add(body)
    val w = wakeup
    if w != null then w.fire()
  }

  /** Runs `body` after `delay` on the reactor thread, using a reactor timer. */
  def schedule(delay: FiniteDuration, body: Runnable): Cancellable = {
    val cancelTimer = new AtomicReference[(() => Unit) | Null](null)
    onOwner {
      val timer = reactor.ops.timer(delay.toMillis.toInt)
      reactor.submit(timer, _ => body.run())
      cancelTimer.set(() => reactor.cancel(timer))
    }
    () =>
      onOwner {
        val cancel = cancelTimer.get()
        if cancel != null then cancel()
      }
  }

  /** Runs `body` now if called on the reactor thread, or queues it there otherwise. */
  private[ReactorScheduler] inline def onOwner(body: => Unit): Unit =
    if isCurrent then body else execute(() => body)

  def sync(body: => Unit): Unit = onOwner(body)

  /** One pending promise that wakes the loop for queued tasks. It fires at most once. */
  private final class Wakeup(promise: Promise[Unit]) {
    private val fired = new AtomicBoolean()
    def fire(): Unit = if fired.compareAndSet(false, true) then promise.complete(())
  }

  /** Submits a fresh wakeup and publishes it before draining, so a task queued concurrently is either seen by the drain
    * or wakes the new promise.
    */
  private def arm(): Unit = {
    val promise = reactor.ops.promise[Unit]()
    reactor.submit(
      promise,
      _ => {
        arm()
        drain()
      }
    )
    wakeup = new Wakeup(promise)
  }

  private def drain(): Unit = {
    var task = tasks.poll()
    while task != null do {
      try task.nn.run()
      catch case t: Throwable => t.printStackTrace()
      task = tasks.poll()
    }
  }
}

object ReactorScheduler {
  private val current = new ThreadLocal[ReactorScheduler]()

  /** The scheduler running on this thread, if any. */
  def onThisThread: ReactorScheduler | Null = current.get()
}

/** gears' suspension support on Scala Native, with a reactor scheduler. */
object ReactorSupport extends AsyncSupport with NativeSuspend {
  type Scheduler = ReactorScheduler
}

/** Gears futures over a thread-owned reactor. */
object ReactorFutures {

  /** Submits `op`, which must have been built by `r.ops`, and suspends the caller until the reactor finishes with it:
    * it returns when the op completes, throws its failure, or throws `CancellationException` when it is cancelled. It
    * must be called on the thread that owns `r`, which is where a program started by `run` executes. To run an op
    * alongside other work, call this inside a `Future`.
    *
    * Cancelling the caller asks the reactor to cancel the op, but this keeps waiting until the reactor confirms, so
    * once it returns or throws, the op's buffers and slots belong to the caller again.
    */
  def perform(r: Reactor, op: r.Op)(using ac: Async): Unit = {
    val scheduler = ReactorScheduler.onThisThread
    if scheduler == null || (scheduler.reactor ne r) then
      throw new IllegalStateException("Ops must be submitted on the thread that owns the reactor")
    if ac.group.isCancelled then throw new CancellationException()
    val waiter = new OpWaiter(scheduler, () => r.cancel(op))
    r.submit(op, waiter.completion) // A refused op, such as a one-shot op submitted twice, throws here.
    waiter.link() // Cancelling the caller now cancels the op; if it already was, this cancels it at once.
    // Awaited outside the caller's group, so cancellation cannot resume it before the reactor has confirmed.
    try ac.withGroup(CompletionGroup.Unlinked).await(waiter).get
    finally waiter.unlink()
  }

  private val done = Success(())

  /** One submission of an op, as a source that its completion completes once, and as a member of the caller's
    * cancellation group. It has at most one listener, the suspended caller.
    */
  private final class OpWaiter(scheduler: ReactorScheduler, cancelOp: () => Boolean)
      extends Async.Source[Try[Unit]]
      with Cancellable {
    // Empty, then either the suspended caller's listener or the outcome, and finally the outcome. Each transition is
    // one atomic step, so exactly one side delivers the outcome: `finish` to a listener it swaps out, or `onComplete`
    // to itself when it finds the outcome already there.
    private val state = new AtomicReference[AnyRef](OpWaiter.Empty)

    // Completions are contravariant, so this one fits the op of any reactor.
    val completion: Completion[Any] = new Completion[Any] {
      def onComplete(op: Any): Unit = finish(done)
      override def onFailure(op: Any, failure: Throwable): Unit = finish(Failure(failure))
      override def onCancel(op: Any): Unit = finish(Failure(new CancellationException()))
    }

    private def finish(outcome: Try[Unit]): Unit =
      state.getAndSet(outcome) match {
        case k: Listener[Try[Unit]] @unchecked => k.completeNow(outcome, this)
        case _                                 => ()
      }

    def poll(k: Listener[Try[Unit]]): Boolean =
      state.get() match {
        case outcome: Try[Unit] @unchecked =>
          k.completeNow(outcome, this)
          true
        case _ => false
      }

    def onComplete(k: Listener[Try[Unit]]): Unit =
      if !state.compareAndSet(OpWaiter.Empty, k) then
        state.get() match {
          case outcome: Try[Unit] @unchecked => k.completeNow(outcome, this)
          case _                             => () // Only the one caller ever waits.
        }

    def dropListener(k: Listener[Try[Unit]]): Unit = state.compareAndSet(k, OpWaiter.Empty)

    // Cancellation can come from any thread, so it is carried out on the reactor's own. By then the op may have
    // finished, and a repeatable op may even have been submitted again, which must not be cancelled.
    def cancel(): Unit = scheduler.sync(if !state.get().isInstanceOf[Try[?]] then cancelOp())
  }

  private object OpWaiter {
    private val Empty = new Object
  }

  /** Runs `program` on the calling thread, which becomes the reactor thread: it opens a reactor from the factory,
    * starts the program as a future under a scheduler bound to this thread, and runs the reactor's loop until the
    * program finishes. The reactor is closed afterwards, and a failure of the program is rethrown.
    */
  def run(program: Reactor => Async.Spawn ?=> Unit)(using factory: Reactor.Factory[Reactor]): Unit = {
    val reactor = factory.open()
    try {
      given ReactorScheduler = new ReactorScheduler(reactor)
      given ReactorSupport.type = ReactorSupport
      Async.blocking {
        val main = Future(program(reactor)) // queued: it starts once the loop runs
        main.onComplete(Listener((_, _) => reactor.stop()))
        reactor.run()
        main.await // already complete, so this only rethrows a failure
      }
    } finally reactor.close() // Pending ops are cancelled, rejecting any futures still waiting on them.
  }
}
