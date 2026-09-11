// from https://github.com/natsukagami/gears-io/blob/direct/shared/src/main/scala/util.scala

package gears.async.asyncio

import gears.async.{Async, Future, Listener}
import scala.concurrent.Future as ScalaFuture
import scala.util.Try
import scala.concurrent.CanAwait
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext
import java.util.concurrent.CancellationException

extension [T](scalaFut: ScalaFuture[T])
  /** Convert the given [[ScalaFuture]] into a Gears [[Future]]. */
  def asGears(using Async): Async.Source[Try[T]] =
    given scala.concurrent.ExecutionContext with
      override def execute(runnable: Runnable): Unit =
        Async.current.scheduler.execute(runnable)
      override def reportFailure(cause: Throwable): Unit =
        scala.concurrent.ExecutionContext.defaultReporter(cause)
    Future.withResolver: r =>
      scalaFut.onComplete(r.complete(_))

extension [T](fut: Future[T])
  /** Convert the given [[Future]] into a Scala [[ScalaFuture]].
    *
    * Note that if [[fut]] is cancelled, the resulting [[ScalaFuture]] will also
    * be resolved with a [[CancellationException]].
    */
  def asScala =
    val p = scala.concurrent.Promise[T]()
    fut.onComplete(Listener((v, _) => p.complete(v)))
    p.future
