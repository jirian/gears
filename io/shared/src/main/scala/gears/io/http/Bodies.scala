package gears.io.http

import gears.async.{Async, Cancellable, Future}
import gears.io.{Error, IoConcurrency, Reader, Result}

import java.io.{ByteArrayOutputStream, Closeable, EOFException, IOException}
import java.nio.ByteBuffer
import java.util.concurrent.TimeoutException
import scala.concurrent.duration.FiniteDuration
import scala.util.Success

private[http] object Bodies:

  /** Runs `body` like `timed`, but disposes of its value instead of leaking it if the caller is cancelled or times out
    * before receiving it, such as a connection opened just as the timeout fired.
    *
    * @tparam A
    *   the result type
    * @param timeout
    *   the time limit, or `None` for none
    * @param dispose
    *   releases a value that never reached the caller; it closes it by default
    * @param body
    *   produces the value
    * @return
    *   the value of `body`
    */
  def timedOwned[A <: Closeable](
      timeout: Option[FiniteDuration],
      dispose: A => Async ?=> Unit = (value: A) => value.close()
  )(body: Async ?=> A)(using Async): A =
    val owned = new java.util.concurrent.atomic.AtomicReference[A | Null](null)
    try
      val result = timed(timeout):
        val value = body
        owned.set(value)
        value
      // Handed to the caller; only a value that never reached it is disposed of below.
      owned.set(null)
      result
    finally
      val remaining = owned.getAndSet(null)
      if remaining != null then dispose(remaining.nn)

  /** Returns a reader over `data`.
    *
    * @param data
    *   the bytes to read
    */
  def bytes(data: Array[Byte]): Reader = new Reader:
    private val input = ByteBuffer.wrap(data)
    def readBuf(out: ByteBuffer)(using Async): Result[Unit] =
      if !out.hasRemaining then Right(())
      else if !input.hasRemaining then Left(Error.EOF)
      else
        val n = math.min(input.remaining(), out.remaining())
        val part = input.slice()
        part.limit(n)
        out.put(part)
        input.position(input.position() + n)
        Right(())

  /** Reads `reader` to its end.
    *
    * @param reader
    *   the reader to drain
    * @return
    *   everything it read
    */
  def collect(reader: Reader)(using Async): Array[Byte] =
    val out = ByteArrayOutputStream()
    val buf = ByteBuffer.allocate(8192)
    var done = false
    while !done do
      buf.clear()
      reader.readBuf(buf) match
        case Left(Error.EOF) => done = true
        case Right(())       =>
          if buf.position() == 0 then throw IOException("Body reader made no progress")
          out.write(buf.array(), 0, buf.position())
    out.toByteArray

  /** Runs `body`, failing with `TimeoutException` if it takes longer than `timeout`.
    *
    * @tparam A
    *   the result type
    * @param timeout
    *   the time limit, or `None` for none
    * @param armed
    *   receives the timer once it is set, so the caller can cancel it early
    * @param body
    *   what to run
    * @return
    *   the value of `body`
    */
  def timed[A](timeout: Option[FiniteDuration], armed: Cancellable => Unit = _ => ())(
      body: Async ?=> A
  )(using Async): A = timeout match
    case None           => body
    case Some(duration) =>
      Async.group:
        val expired = Future.Promise[Unit]()
        val timer = Async.current.scheduler.schedule(duration, () => expired.complete(Success(())))
        armed(timer)
        try Async.select(Future(body).handle(_.get), expired.handle(_ => throw TimeoutException()))
        finally timer.cancel()

/** Reads one message body with its framing: a fixed `Content-Length`, chunked (with trailers skipped), or until the
  * connection closes. More than `limit` bytes fail with `BodyTooLargeException`.
  */
private[http] final class MessageBody(reader: RequestReader, framing: BodyFraming, limit: Long) extends Reader:
  private val access = IoConcurrency()
  private var remaining = framing match
    case BodyFraming.Fixed(n) => n.toLong
    case _                    => 0L
  private var total = 0L
  private var needChunkEnd = false
  private var done = framing == BodyFraming.Fixed(0)
  def complete: Boolean = done

  def readBuf(out: ByteBuffer)(using Async): Result[Unit] = access.reading:
    if !out.hasRemaining then Right(())
    else if done then Left(Error.EOF)
    else
      if framing == BodyFraming.Chunked && remaining == 0 then
        // Every chunk's data ends with a CRLF of its own, before the next size line.
        if needChunkEnd && reader.readLine(0).nonEmpty then throw IOException("Missing chunk CRLF")
        val size = reader.readLine(1024).takeWhile(_ != ';')
        if size.isEmpty || !size.forall(c => c.isDigit && c <= '9' || "abcdefABCDEF".contains(c)) then
          throw IOException("Invalid chunk length")
        remaining = try java.lang.Long.parseLong(size, 16)
        catch case _: NumberFormatException => throw IOException("Invalid chunk length")
        if remaining < 0 || remaining > limit - total then throw BodyTooLargeException(limit.toInt)
        if remaining == 0 then
          var trailerBytes = 0
          var line = reader.readLine(MaxHeadSize)
          while line.nonEmpty do
            trailerBytes += line.length + 2
            if trailerBytes > MaxHeadSize then throw IOException("Trailers too large")
            line = reader.readLine(MaxHeadSize)
          done = true
        needChunkEnd = true
      if done then Left(Error.EOF)
      else
        // A body without a length may read one byte past the limit, which is how it notices the overflow.
        val n = if framing == BodyFraming.UntilClose then math.min(out.remaining().toLong, limit - total + 1).toInt
        else math.min(out.remaining().toLong, remaining).toInt
        if n <= 0 then throw BodyTooLargeException(limit.toInt)
        val slice = out.slice()
        slice.limit(n)
        reader.readBuf(slice) match
          case Left(Error.EOF) =>
            if framing != BodyFraming.UntilClose then throw EOFException("Connection closed mid-body")
            done = true
            Left(Error.EOF)
          case Right(()) =>
            val received = slice.position()
            if received == 0 then throw IOException("Body read made no progress")
            total += received
            if total > limit then throw BodyTooLargeException(limit.toInt)
            out.position(out.position() + received)
            if framing != BodyFraming.UntilClose then
              remaining -= received
              if remaining == 0 && framing != BodyFraming.Chunked then done = true
            Right(())

/** The body of a streamed client response. Reading it to the end, or `close` after the end, returns the connection to
  * the pool; closing it earlier discards the connection. Each read is bounded by the client's read timeout.
  */
final class ResponseBody private[http] (
    decoder: MessageBody,
    timeout: Option[FiniteDuration],
    finish: Boolean => Unit,
    awaitUpload: Async ?=> Unit
) extends Reader
    with Closeable:
  private val access = IoConcurrency()
  private val released = new java.util.concurrent.atomic.AtomicBoolean(false)
  private def release(reuse: Boolean): Unit = if released.compareAndSet(false, true) then finish(reuse)

  def readBuf(out: ByteBuffer)(using Async): Result[Unit] = access.reading:
    if decoder.complete then
      release(true)
      awaitUpload
      if out.hasRemaining then Left(Error.EOF) else Right(())
    else if released.get() then throw HttpErrors.closedChannel()
    else
      try
        val result = Bodies.timed(timeout)(decoder.readBuf(out))
        if decoder.complete then
          release(true)
          awaitUpload
        result
      catch
        case t: Throwable =>
          release(false)
          awaitUpload
          throw t

  /** Reads the rest of this body.
    *
    * @return
    *   the bytes not read yet
    */
  def readAll()(using Async): Array[Byte] = Bodies.collect(this)

  /** Stops reading. The connection is reused only if this body was complete. */
  def close(): Unit = release(decoder.complete)

  /** Closes this body like `close`, and also stops a request body that is still uploading and waits for it to end. */
  def finish()(using Async): Unit =
    close()
    awaitUpload

/** A client response whose body is read as it arrives. Close or finish it to release the connection. */
final case class StreamingResponse(status: HttpStatus, headers: Headers, body: ResponseBody) extends Closeable:

  /** Closes the body; see [[ResponseBody.close]]. */
  def close(): Unit = body.close()

  /** Finishes the body; see [[ResponseBody.finish]]. */
  def finish()(using Async): Unit = body.finish()

/** A client request whose body is streamed from `body`: with `Content-Length` if `contentLength` is given, chunked
  * otherwise.
  */
final case class StreamingRequest(
    method: HttpMethod,
    url: Url,
    headers: Headers,
    body: Reader,
    contentLength: Option[Long] = None
)
