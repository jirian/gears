package gears.io.reactor

import gears.async.{Async, Future, uninterruptible}
import gears.io.reactor.{Completion, HandleSlot, Reactor}

import java.io.FileNotFoundException
import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousCloseException, ClosedChannelException}
import java.nio.charset.StandardCharsets
import scala.scalanative.memory.PointerBuffer
import scala.scalanative.memory.PointerBufferOps.given
import scala.scalanative.posix.{errno, fcntl}
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

/** Runs reactor ops for fibers: the bridge between a `Reactor`, which has a single thread and completes ops through
  * callbacks, and gears code, which waits for one op at a time. The network, file and DNS supports in this package are
  * written only against this trait, so they work on any driver: `UringPerThreadScheduler`, `SingleReactorDriver`, or
  * one written outside gears.
  *
  * An implementation provides `await`. The other methods have defaults that use only ops every reactor has: file I/O
  * and opening files run as blocking ops on a private `dup` of the descriptor, and heap buffers reach the reactor as
  * pointer views of their arrays. A driver whose reactor does better overrides them, as the uring scheduler does.
  */
trait ReactorDriver:

  /** The kind of reactor this driver runs ops on. A driver for one kind of reactor fixes it, so its own code can build
    * that reactor's specific ops; code written for any driver sees only a `Reactor`.
    */
  type R <: Reactor

  /** Builds an op with `build` on this driver's reactor thread, submits it, and suspends until it completes; a failure
    * is thrown. If the caller is cancelled, the op is cancelled, and the caller still waits until the reactor has
    * finished with the op's buffers before the cancellation is thrown.
    *
    * @param guard
    *   the guard of the handle the op uses, or null. The op is registered with it: closing the guard cancels the op,
    *   and the op fails with `AsynchronousCloseException`.
    * @param build
    *   builds the op on the reactor that will run it
    */
  def await(guard: IoGuard | Null)(build: (r: R) => r.Op)(using Async): Unit

  /** Opens `path` like `open(2)`, close-on-exec, without blocking the reactor.
    *
    * @param path
    *   the file to open
    * @param flags
    *   the `open(2)` flags
    * @param mode
    *   the permissions of a file that `O_CREAT` creates
    * @return
    *   the new descriptor
    */
  def openFile(path: String, flags: Int, mode: Int)(using Async): Int =
    // The open may finish after this fiber was cancelled; whatever descriptor it produced is then closed below.
    val owned = new java.util.concurrent.atomic.AtomicInteger(-1)
    try
      await(null)(r => r.ops.blocking(() => { owned.set(ReactorDriver.openNow(path, flags, mode)); () }))
      owned.getAndSet(-1)
    finally
      val fd = owned.getAndSet(-1)
      if fd >= 0 then scala.scalanative.posix.unistd.close(fd)

  /** Runs `await` for an op that produces a handle. A handle produced for a caller that no longer waits is closed.
    *
    * @param guard
    *   the guard of the handle the op uses, or null
    * @param build
    *   builds the op, which writes its handle into the given slot
    * @return
    *   the handle
    */
  final def awaitHandle(guard: IoGuard | Null)(build: (r: R, into: HandleSlot) => r.Op)(using Async): Int =
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

  /** Returns whether the reactor takes heap `ByteBuffer`s directly. If not, `awaitFilling` and `awaitDraining` pass it
    * a pointer view of the buffer's array instead.
    */
  def acceptsHeapBuffers: Boolean = false

  /** Runs `await` for an op that fills `buf`, such as a read. Bytes that arrived before a cancel are kept.
    *
    * @param guard
    *   the guard of the handle the op uses, or null
    * @param buf
    *   the buffer to fill
    * @param build
    *   builds the op, filling the given buffer, which may be a view of `buf`
    */
  final def awaitFilling(guard: IoGuard | Null, buf: ByteBuffer)(build: (r: R, into: ByteBuffer) => r.Op)(using
      Async
  ): Unit =
    if acceptsHeapBuffers || buf.hasPointer() then await(guard)(r => build(r, buf))
    else
      val view = ReactorDriver.pointerView(buf)
      if view != null then
        try await(guard)(r => build(r, view))
        finally buf.position(buf.position() + view.position())
      else
        Zone.acquire: zone =>
          val n = math.min(buf.remaining(), ReactorDriver.StageSize)
          val staged = PointerBuffer.wrap(alloc[Byte](math.max(n, 1))(using zone), n)
          try await(guard)(r => build(r, staged))
          finally
            staged.flip()
            buf.put(staged)

  /** Runs `await` for an op that consumes `buf`, such as a write. `buf` advances by what was consumed, even on a
    * cancel.
    *
    * @param guard
    *   the guard of the handle the op uses, or null
    * @param buf
    *   the buffer to drain
    * @param build
    *   builds the op, draining the given buffer, which may be a view of `buf`
    */
  final def awaitDraining(guard: IoGuard | Null, buf: ByteBuffer)(build: (r: R, from: ByteBuffer) => r.Op)(using
      Async
  ): Unit =
    if acceptsHeapBuffers || buf.hasPointer() then await(guard)(r => build(r, buf))
    else
      val view = ReactorDriver.pointerView(buf)
      if view != null then
        try await(guard)(r => build(r, view))
        finally buf.position(buf.position() + view.position())
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

  /** Reads from a file at its position, by default with `read(2)` on a blocking-pool thread.
    *
    * @param guard
    *   the file's guard, or null
    * @param fd
    *   the file to read from
    * @param buf
    *   the buffer to fill
    */
  def readFile(guard: IoGuard | Null, fd: Int, buf: ByteBuffer)(using Async): Unit =
    blockingFileIo(guard, fd, buf, -1L, write = false)

  /** Writes to a file at its position, by default with `write(2)` on a blocking-pool thread.
    *
    * @param guard
    *   the file's guard, or null
    * @param fd
    *   the file to write to
    * @param buf
    *   the buffer to drain
    */
  def writeFile(guard: IoGuard | Null, fd: Int, buf: ByteBuffer)(using Async): Unit =
    blockingFileIo(guard, fd, buf, -1L, write = true)

  /** Reads at `offset` without moving the file position, like `pread(2)`.
    *
    * @param guard
    *   the file's guard
    * @param fd
    *   the file to read from
    * @param buf
    *   the buffer to fill
    * @param offset
    *   the position in the file to read at, which must not be negative
    */
  def readAt(guard: IoGuard, fd: Int, buf: ByteBuffer, offset: Long)(using Async): Unit =
    require(offset >= 0, "offset must be nonnegative")
    blockingFileIo(guard, fd, buf, offset, write = false)

  /** Writes at `offset` without moving the file position, like `pwrite(2)`.
    *
    * @param guard
    *   the file's guard
    * @param fd
    *   the file to write to
    * @param buf
    *   the buffer to drain
    * @param offset
    *   the position in the file to write at, which must not be negative
    */
  def writeAt(guard: IoGuard, fd: Int, buf: ByteBuffer, offset: Long)(using Async): Unit =
    require(offset >= 0, "offset must be nonnegative")
    blockingFileIo(guard, fd, buf, offset, write = true)

  private def blockingFileIo(guard: IoGuard | Null, fd: Int, buf: ByteBuffer, offset: Long, write: Boolean)(using
      Async
  ): Unit =
    val copy = if guard == null then ReactorDriver.duplicate(fd) else guard.whileOpen(ReactorDriver.duplicate(fd))
    def transfer(ptr: Ptr[Byte], n: Int)(settle: Int => Unit): Unit =
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
      finally settle(count)
    try
      val direct = ReactorDriver.pointerAt(buf)
      if direct != null then transfer(direct.nn, buf.remaining())(count => buf.position(buf.position() + count))
      else
        Zone.acquire: zone =>
          val n = math.min(buf.remaining(), ReactorDriver.StageSize)
          val ptr = alloc[Byte](math.max(n, 1))(using zone)
          if write then
            val source = buf.duplicate()
            source.limit(source.position() + n)
            PointerBuffer.wrap(ptr, n).put(source)
          transfer(ptr, n): count =>
            if write then buf.position(buf.position() + count)
            else buf.put(PointerBuffer.wrap(ptr, count))
    finally scala.scalanative.posix.unistd.close(copy)

object ReactorDriver:
  private val StageSize = 64 * 1024

  // Heap arrays can be handed out by address because Scala Native's GC never moves objects.
  private[io] def pointerAt(buf: ByteBuffer): Ptr[Byte] | Null =
    if buf.hasPointer() then buf.pointer() + buf.position()
    else if buf.hasArray() && buf.hasRemaining() then buf.array().at(buf.arrayOffset() + buf.position())
    else null

  private[io] def pointerView(buf: ByteBuffer): ByteBuffer | Null =
    val ptr = pointerAt(buf)
    if ptr == null then null else PointerBuffer.wrap(ptr.nn, buf.remaining())

  private[reactor] def duplicate(fd: Int): Int =
    val copy = scala.scalanative.posix.unistd.dup(fd)
    if copy < 0 then throw posixError("Failed to duplicate file descriptor")
    if fcntl.fcntl(copy, fcntl.F_SETFD, fcntl.FD_CLOEXEC) < 0 then
      val error = posixError("Failed to mark descriptor close-on-exec")
      scala.scalanative.posix.unistd.close(copy)
      throw error
    copy

  /** Waits for an op's future. `link` makes cancelling the caller cancel the op; `uninterruptible` makes the caller
    * keep waiting after its own cancellation until the op has settled, because until then the kernel or a pool thread
    * may still use the caller's buffers.
    *
    * @param future
    *   the op's future
    */
  private[io] def waitFor(future: Future[Unit])(using Async): Unit =
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

  /** Builds and submits one op on `reactor`, resolving `resolver` when it settles. It must be called on the reactor's
    * thread.
    *
    * @tparam R
    *   the kind of reactor
    * @param reactor
    *   the reactor to submit to
    * @param onLoop
    *   runs its argument on the reactor's thread
    * @param guard
    *   the guard of the handle the op uses, or null
    * @param build
    *   builds the op
    * @param resolver
    *   settles the caller's future
    * @return
    *   the cancel action, which goes through `onLoop` because cancelling also has to happen on the reactor's thread
    */
  private[io] def submitOn[R <: Reactor](
      reactor: R,
      onLoop: (=> Unit) => Unit,
      guard: IoGuard | Null,
      build: (r: R) => r.Op,
      resolver: Future.Resolver[Unit]
  ): () => Unit =
    val op = try build(reactor)
    catch
      case t: Throwable =>
        resolver.reject(t)
        // Nothing was submitted, so there is nothing to cancel.
        return () => ()
    val cancel = () => onLoop(reactor.cancel(op))
    var registration: IoGuard.Registration | Null = null
    def release(): Unit =
      try if registration != null then guard.nn.release(registration.nn)
      catch case t: Throwable => resolver.reject(t)
    val completion = new Completion[reactor.Op]:
      def onComplete(op: reactor.Op): Unit =
        release()
        // The handle was closed while the op ran; report that rather than a result for a closed stream.
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

/** An op submission that another thread will start, on whichever `S` (a shard, or a driver's loop) picks it up. A
  * cancel before the start skips the start and runs `whenCancelledFirst`; a cancel after it runs the cancel action the
  * start returned. Either way it happens once.
  */
private[io] final class SharedJob[S](start: S => (() => Unit), whenCancelledFirst: () => Unit):
  private var cancelTaken: (() => Unit) | Null = null
  private var cancelled = false
  private var started = false

  /** Starts the submission on `on`, unless it was cancelled or already started.
    *
    * @param on
    *   the shard or loop that starts it
    */
  def run(on: S): Unit = synchronized:
    if !cancelled && !started then
      started = true
      cancelTaken = start(on)

  /** Cancels the submission, the first time it is called. */
  def cancel(): Unit =
    val (first, action) = synchronized:
      val first = !cancelled
      cancelled = true
      (first, cancelTaken)
    if first then if action != null then action() else whenCancelledFirst()
