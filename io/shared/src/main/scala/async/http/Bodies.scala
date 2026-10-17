package gears.async.http

import gears.async.asyncio.{Error, IoConcurrency, Reader, Result}
import gears.async.{Async, Cancellable, Future}

import java.io.{ByteArrayOutputStream, Closeable, EOFException, IOException}
import java.nio.ByteBuffer
import java.util.concurrent.TimeoutException
import scala.concurrent.duration.FiniteDuration
import scala.util.Success

private[http] object Bodies:
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
      owned.set(null)
      result
    finally
      val remaining = owned.getAndSet(null)
      if remaining != null then dispose(remaining.nn)
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

  def readAll()(using Async): Array[Byte] = Bodies.collect(this)

  def close(): Unit = release(decoder.complete)
  def finish()(using Async): Unit =
    close()
    awaitUpload

final case class StreamingResponse(status: HttpStatus, headers: Headers, body: ResponseBody) extends Closeable:
  def close(): Unit = body.close()
  def finish()(using Async): Unit = body.finish()

final case class StreamingRequest(
    method: HttpMethod,
    url: Url,
    headers: Headers,
    body: Reader,
    contentLength: Option[Long] = None
)
