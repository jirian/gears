// from https://github.com/natsukagami/gears-io/blob/direct/shared/src/main/scala/base.scala

package gears.async.asyncio

import gears.async.Async.OriginalSource
import gears.async.Listener
import gears.async.{Async, Future}
import gears.util.either
import gears.util.either._

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ArraySeq
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/** The general buffer type of the traits. */
type Buffer = java.nio.ByteBuffer

/** An asynchronous reader. */
trait Reader:
  def readBuf(buf: Buffer)(using Async): Result[Unit]

  /** Wraps the [[Reader]] inside a new [[BufferedReader]]. */
  final def buffered(bufferSize: Int) = BufferedReader(bufferSize)(this)

/** An asynchronous writer. */
trait Writer:
  def writeBuf(buf: Buffer)(using Async): Result[Unit]

/** Wraps a reader and provides with with buffering capabilities.
  *
  * A [[BufferedReader]] is *not* thread-safe, nor does it allow multiple outstanding reads.
  */
class BufferedReader(bufSize: Int)(reader: Reader) extends Reader:
  require(bufSize > 0, "bufferSize must be positive")
  private inline def errReadPending = Failure(ConcurrentReadException())

  /** Returns the underlying buffer. Panics if there are any pending reads. */
  def toBuffer =
    assert(readPending.get == false)
    buffer

  def isEOF =
    gotEOF && !buffer.hasRemaining()

  /* Parsing */

  /** Read all bytes until EOF to an [[ArrayBuffer]]. */
  def readAll()(using Async) = either:
    mustPending:
      val buf = ArrayBuffer[Byte]()
      while !isEOF do
        buf ++= ArraySeq
          .ofByte(buffer.array())
          .slice(buffer.arrayOffset() + buffer.position(), buffer.arrayOffset() + buffer.limit())
        readToInternal() match
          case Left(Error.EOF) => ()
          case result          => result.?
      buf

  private inline def mustPending[T](inline body: => T) =
    if readPending.compareAndSet(false, true) then
      try
        body
      finally
        readPending.set(false)
    else throw ConcurrentReadException()

  /** Create a race-able read source. */
  def readBufSrc(
      buf: Buffer
  )(using async: Async, spawn: Async.Spawn & async.type) =
    new OriginalSource[Try[Result[Unit]]]:
      src =>
      type Listener = gears.async.Listener[Try[Result[Unit]]]

      var underlyingFuture: (Listener, Future[?]) =
        null

      // Sets the `readPending` flag, returns true if it's successful.
      private inline def setPending(k: Listener): Boolean =
        if !readPending.compareAndSet(false, true) then
          k.completeNow(errReadPending, src)
          false
        else true
      private inline def unsetPending() =
        readPending.set(false)
      private inline def withPending[T](
          k: Listener,
          inline notPending: T
      )(inline body: => T): T =
        if setPending(k) then
          try
            body
          finally
            unsetPending()
        else notPending

      override def poll(k: Listener): Boolean =
        withPending(k, true):
          if buffer.hasRemaining() then copyAndComplete(k)
          else if gotEOF then k.completeNow(Success(Left(Error.EOF)), src) || true
          else false

      override def dropListener(k: Listener): Unit =
        underlyingFuture match
          case (listener, fut) if listener == k => fut.cancel()
          case _                                => ()

      override protected def addListener(k: Listener): Unit =
        if setPending(k) then
          if buffer.hasRemaining() then
            try copyAndComplete(k)
            finally unsetPending()
          else if gotEOF then
            try k.completeNow(Success(Left(Error.EOF)), src)
            finally unsetPending()
          else
            val fut = Future:
              try
                val underlying = readToInternal()
                if underlying.isRight then copyAndComplete(k)
                else k.completeNow(Success(underlying), src)
              catch case e => k.completeNow(Failure(e), src)
              finally
                underlyingFuture = null
                unsetPending()
            underlyingFuture = (k, fut)

      def copyAndComplete(k: Listener): true =
        if k.acquireLock() then
          copyToBuf(buf)
          k.complete(Success(Right(())), src)
        true

  private def readToInternal()(using Async) =
    buffer.clear()
    val result =
      try reader.readBuf(buffer)
      finally buffer.flip()
    if result == Left(Error.EOF) then gotEOF = true
    result

  private inline def copyToBuf(buf: Buffer) =
    if buf.remaining() >= buffer.remaining() then buf.put(buffer)
    else
      val toCopy = buf.remaining()
      val part = buffer.duplicate()
      part.limit(part.position() + toCopy)
      buf.put(part)
      buffer.position(buffer.position() + toCopy)

  override def readBuf(buf: Buffer)(using Async): Result[Unit] = mustPending {
    either {
      if buf.hasRemaining() then
        if buffer.hasRemaining then copyToBuf(buf)
        else if gotEOF then either.error(Error.EOF)
        else
          readToInternal().?
          copyToBuf(buf)
    }
  }

  // private stuff
  private val buffer = java.nio.ByteBuffer.allocate(bufSize).limit(0)
  private var gotEOF = false
  private val readPending = AtomicBoolean(false)
end BufferedReader

/** Possible errors that could occur during IO. */
enum Error:
  /** End of the reader has been reached. */
  case EOF

/** The error type for IO operations. */
type Result[T] = Either[Error, T]
type IOFuture[T] = Future[Either[Error, T]]

private object Result:
  def err(err: Error): Result[Nothing] = Left(err)
  def ok[T](v: T): Result[T] = Right(v)
  def ok(): Result[Unit] = Right(())
