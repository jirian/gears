package gears.io

import gears.async.{Async, Semaphore}

import java.util.concurrent.atomic.AtomicBoolean

/** A second read was started on a resource while one was pending. */
final class ConcurrentReadException extends IllegalStateException("A read is already pending on this resource")

/** The concurrency rule of every stream: one read at a time, and writes queued and run one at a time, so a read and a
  * write may overlap. A second concurrent read is a programming error, since the two readers would split the incoming
  * bytes unpredictably, so it throws. Writes wait instead, which keeps each write's bytes together.
  */
private[io] final class IoConcurrency:
  private val readPending = AtomicBoolean(false)
  private val writes = Semaphore(1)

  def reading[A](body: => A): A =
    if !readPending.compareAndSet(false, true) then throw ConcurrentReadException()
    try body
    finally readPending.set(false)

  def writing[A](body: => A)(using Async): A =
    val permit = writes.acquire()
    try body
    finally permit.release()
