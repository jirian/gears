package gears.io

import gears.async.{Async, Semaphore}

import java.util.concurrent.atomic.AtomicBoolean

final class ConcurrentReadException extends IllegalStateException("A read is already pending on this resource")

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
