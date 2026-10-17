package gears.async.asyncio.reactor

import gears.async.{Async, Future, uninterruptible}
import gears.asyncio.{Completion, HandleSlot, Reactor}

import java.io.FileNotFoundException
import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousCloseException, ClosedChannelException}
import java.nio.charset.StandardCharsets
import scala.scalanative.memory.PointerBuffer
import scala.scalanative.memory.PointerBufferOps.given
import scala.scalanative.posix.{errno, fcntl}
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

trait ReactorDriver:

  def await(guard: IoGuard | Null)(build: (r: Reactor) => r.Op)(using Async): Unit

  def openFile(path: String, flags: Int, mode: Int)(using Async): Int =
    val owned = new java.util.concurrent.atomic.AtomicInteger(-1)
    try
      await(null)(r => r.ops.blocking(() => { owned.set(ReactorDriver.openNow(path, flags, mode)); () }))
      owned.getAndSet(-1)
    finally
      val fd = owned.getAndSet(-1)
      if fd >= 0 then scala.scalanative.posix.unistd.close(fd)

  final def awaitHandle(guard: IoGuard | Null)(build: (r: Reactor, into: HandleSlot) => r.Op)(using Async): Int =
    val opened = HandleSlot()
    try
      await(guard)(r => build(r, opened))
      opened.clear()
    catch
      case t: Throwable =>
        val fd = try opened.clear()
        catch case _: IllegalStateException => -1
        if fd >= 0 then scala.scalanative.posix.unistd.close(fd)
        throw t

  def acceptsHeapBuffers: Boolean = false

  final def awaitFilling(guard: IoGuard | Null, buf: ByteBuffer)(build: (r: Reactor, into: ByteBuffer) => r.Op)(using
      Async
  ): Unit =
    if acceptsHeapBuffers || buf.hasPointer() then await(guard)(r => build(r, buf))
    else
      Zone.acquire: zone =>
        val n = math.min(buf.remaining(), ReactorDriver.StageSize)
        val staged = PointerBuffer.wrap(alloc[Byte](math.max(n, 1))(using zone), n)
        try await(guard)(r => build(r, staged))
        finally
          staged.flip()
          buf.put(staged)

  final def awaitDraining(guard: IoGuard | Null, buf: ByteBuffer)(build: (r: Reactor, from: ByteBuffer) => r.Op)(using
      Async
  ): Unit =
    if acceptsHeapBuffers || buf.hasPointer() then await(guard)(r => build(r, buf))
    else
      Zone.acquire: zone =>
        val n = math.min(buf.remaining(), ReactorDriver.StageSize)
        val staged = PointerBuffer.wrap(alloc[Byte](math.max(n, 1))(using zone), n)
        val source = buf.duplicate()
        source.limit(source.position() + n)
        staged.put(source)
        staged.flip()
        try await(guard)(r => build(r, staged))
        finally buf.position(buf.position() + staged.position())

  def readFile(guard: IoGuard | Null, fd: Int, buf: ByteBuffer)(using Async): Unit =
    blockingFileIo(guard, fd, buf, -1L, write = false)

  def writeFile(guard: IoGuard | Null, fd: Int, buf: ByteBuffer)(using Async): Unit =
    blockingFileIo(guard, fd, buf, -1L, write = true)

  def readAt(guard: IoGuard, fd: Int, buf: ByteBuffer, offset: Long)(using Async): Unit =
    require(offset >= 0, "offset must be nonnegative")
    blockingFileIo(guard, fd, buf, offset, write = false)

  def writeAt(guard: IoGuard, fd: Int, buf: ByteBuffer, offset: Long)(using Async): Unit =
    require(offset >= 0, "offset must be nonnegative")
    blockingFileIo(guard, fd, buf, offset, write = true)

  private def blockingFileIo(guard: IoGuard | Null, fd: Int, buf: ByteBuffer, offset: Long, write: Boolean)(using
      Async
  ): Unit =
    val copy = if guard == null then ReactorDriver.duplicate(fd) else guard.whileOpen(ReactorDriver.duplicate(fd))
    try
      Zone.acquire: zone =>
        val n = math.min(buf.remaining(), ReactorDriver.StageSize)
        val ptr = alloc[Byte](math.max(n, 1))(using zone)
        if write then
          val source = buf.duplicate()
          source.limit(source.position() + n)
          PointerBuffer.wrap(ptr, n).put(source)
        @volatile var count = 0
        try
          await(guard)(r =>
            r.ops.blocking(() => {
              var done = -1L
              while done < 0 do
                done =
                  if offset < 0 then
                    if write then scala.scalanative.posix.unistd.write(copy, ptr, n.toUSize).toLong
                    else scala.scalanative.posix.unistd.read(copy, ptr, n.toUSize).toLong
                  else if write then scala.scalanative.posix.unistd.pwrite(copy, ptr, n.toUSize, offset.toSize).toLong
                  else scala.scalanative.posix.unistd.pread(copy, ptr, n.toUSize, offset.toSize).toLong
                if done < 0 && errno.errno != errno.EINTR then throw posixError("File I/O failed")
              count = done.toInt
            })
          )
        finally
          if write then buf.position(buf.position() + count)
          else buf.put(PointerBuffer.wrap(ptr, count))
    finally scala.scalanative.posix.unistd.close(copy)

object ReactorDriver:
  private val StageSize = 64 * 1024

  private[reactor] def duplicate(fd: Int): Int =
    val copy = scala.scalanative.posix.unistd.dup(fd)
    if copy < 0 then throw posixError("Failed to duplicate file descriptor")
    if fcntl.fcntl(copy, fcntl.F_SETFD, fcntl.FD_CLOEXEC) < 0 then
      val error = posixError("Failed to mark descriptor close-on-exec")
      scala.scalanative.posix.unistd.close(copy)
      throw error
    copy

  private[asyncio] def waitFor(future: Future[Unit])(using Async): Unit =
    future.link()
    uninterruptible(future.await)

  private[reactor] def openNow(path: String, flags: Int, mode: Int): Int =
    val fd = Zone.acquire { implicit z =>
      fcntl.open(toCString(path, StandardCharsets.UTF_8), flags, mode.toUInt)
    }
    if fd < 0 then
      val err = errno.errno
      if err == errno.ENOENT then throw FileNotFoundException(path)
      throw posixError(s"Failed to open $path", err)
    if fcntl.fcntl(fd, fcntl.F_SETFD, fcntl.FD_CLOEXEC) < 0 then
      val failure = posixError(s"Failed to make $path close-on-exec")
      scala.scalanative.posix.unistd.close(fd)
      throw failure
    fd

  private[asyncio] def submitOn(
      reactor: Reactor,
      onLoop: (=> Unit) => Unit,
      guard: IoGuard | Null,
      build: (r: Reactor) => r.Op,
      resolver: Future.Resolver[Unit]
  ): () => Unit =
    val op = try build(reactor)
    catch
      case t: Throwable =>
        resolver.reject(t)
        return () => ()
    val cancel = () => onLoop(reactor.cancel(op))
    var registration: IoGuard.Registration | Null = null
    def release(): Unit =
      try if registration != null then guard.nn.release(registration.nn)
      catch case t: Throwable => resolver.reject(t)
    val completion = new Completion[reactor.Op]:
      def onComplete(op: reactor.Op): Unit =
        release()
        if guard != null && guard.nn.isClosed then resolver.reject(AsynchronousCloseException())
        else resolver.resolve(())
      override def onFailure(op: reactor.Op, failure: Throwable): Unit =
        release()
        resolver.reject(failure)
      override def onCancel(op: reactor.Op): Unit =
        release()
        if guard != null && guard.nn.isClosed then resolver.reject(AsynchronousCloseException())
        else resolver.rejectAsCancelled()
    try
      if guard == null then reactor.submit(op, completion)
      else
        val accepted = guard.nn.submit(cancel) { registered =>
          registration = registered
          reactor.submit(op, completion)
        }
        if accepted == null then resolver.reject(ClosedChannelException())
    catch case t: Throwable => resolver.reject(t)
    cancel

private[asyncio] final class SharedJob[S](start: S => (() => Unit), whenCancelledFirst: () => Unit):
  private var cancelTaken: (() => Unit) | Null = null
  private var cancelled = false
  private var started = false

  def run(on: S): Unit = synchronized:
    if !cancelled && !started then
      started = true
      cancelTaken = start(on)

  def cancel(): Unit =
    val (first, action) = synchronized:
      val first = !cancelled
      cancelled = true
      (first, cancelTaken)
    if first then if action != null then action() else whenCancelledFirst()
