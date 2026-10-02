package asyncio

import java.nio.ByteBuffer

/** What to do when an op finishes. The op comes back with its outcome, so one completion can own many ops. A completion
  * that submits further ops holds on to its reactor itself, since only a reference to that reactor has the right `Op`
  * type.
  */
@FunctionalInterface
trait Completion[-C] {

  /** The op finished. What finishing means for each kind of op is described in `Ops`; anything it produced is in the
    * caller's buffers and slots.
    */
  def onComplete(op: C): Unit

  /** The op failed with an exception instead of finishing. By default the failure propagates out of `Reactor.run`. */
  def onFailure(op: C, failure: Throwable): Unit = throw failure

  /** The op was cancelled before it finished, by `Reactor.cancel` or because the reactor closed. It runs on the reactor
    * thread, and afterwards neither `onComplete` nor `onFailure` runs for this submission. It must not submit ops while
    * the reactor is closing. By default it does nothing.
    */
  def onCancel(op: C): Unit = ()
}

/** A single-threaded event loop. Every completion runs on the thread that called `run`, and apart from completing a
  * `Promise`, a reactor is only used from that thread.
  */
trait Reactor extends AutoCloseable {

  /** Something submitted to this reactor that later completes. The type belongs to the reactor value, so an op built by
    * one reactor cannot be submitted to another: given `a, b: Reactor`, `a.Op` and `b.Op` are different types.
    */
  type Op

  /** How this reactor names an open file, socket, or other I/O object: a descriptor on POSIX, but a channel, an OS
    * handle, or a registered index elsewhere. Handles belong to the caller; the reactor never closes them.
    */
  type Handle

  type BoxedHandle <: AnyRef

  def unbox(boxed: BoxedHandle): Handle

  /** Builds this reactor's ops. Building an op does not touch the reactor, so `ops` may be used from any thread. */
  def ops: Ops[Op, Handle, BoxedHandle]

  /** Opens, uses without waiting, and closes this reactor's handles. It does not touch the reactor either, so it may be
    * used from any thread.
    */
  def handles: Handles[Handle]

  /** Submits an op with the completion to run when it finishes. An op marked `Repeatable` may be submitted again once
    * its previous submission has completed, failed, or been cancelled; any other op may be submitted only once. No op
    * may be pending twice at the same time.
    */
  def submit[C <: Op](op: C, completion: Completion[C]): Unit

  /** Cancels a submitted op that has not finished. Its completion's `onCancel` runs before this returns, and its
    * `onComplete` and `onFailure` never run. Returns false if the op already finished or was never submitted here.
    */
  def cancel(op: Op): Boolean

  /** Runs completions until `stop` is called. */
  def run(): Unit

  /** Makes `run` return once the current completion finishes. */
  def stop(): Unit

  /** Releases the reactor's own resources. Every pending op is cancelled, so its completion gets `onCancel` and nothing
    * else. Handles named by ops belong to the caller and stay open. Call it when `run` is not running; closing twice
    * does nothing.
    */
  def close(): Unit
}

object Reactor {

  /** A reactor whose handles are POSIX file descriptors, for code that opens sockets and files itself. */
  type Posix = Reactor { type Handle = Int }

  /** Opens reactors of kind `R`. Code written against the interface asks for one of these instead of naming an
    * implementation, and states what it needs of the reactor through `R`, such as `Reactor.Posix`.
    */
  @FunctionalInterface
  trait Factory[+R <: Reactor] {
    def open(): R
  }

  /** Opens a reactor for `body` and closes it afterwards. The reactor's type comes from the given factory. */
  def scoped[R <: Reactor](using factory: Factory[R])[A](body: R => A): A = {
    val reactor = factory.open()
    try body(reactor)
    finally reactor.close()
  }
}

/** The readiness a custom op waits for before it runs. */
enum Interest {
  case Read, Write
}

/** Where a socket lives: a Unix domain path, or a numeric IPv4 or IPv6 address and port. */
enum Address {
  case Unix(path: String)
  case IPv4(host: String, port: Int)
  case IPv6(host: String, port: Int)
}

/** Opening, closing, and waitless use of a reactor's handles. Every operation is synchronous and never blocks; failures
  * throw `java.io.IOException`. Handles belong to the caller, who closes them with `close`.
  */
trait Handles[Handle] {

  /** Opens a file, typically a FIFO, for reading or writing without blocking. */
  def openFile(path: String, write: Boolean): Handle

  /** A stream socket whose connection to `address` has started; submit `ops.connect` to wait for it to finish. */
  def connect(address: Address): Handle

  /** A stream socket bound to `address` and listening for connections. */
  def listen(address: Address): Handle

  /** A datagram socket, bound to `local` and connected to `remote` where given. A Unix datagram client needs a `local`
    * path so that replies can reach it.
    */
  def datagram(local: Address | Null, remote: Address | Null): Handle

  /** A pipe, as its read end and its write end. */
  def pipe(): (Handle, Handle)

  /** Reads what is available now into `buf`: the bytes read, 0 if nothing is, or -1 at end of stream. */
  def readNow(handle: Handle, buf: ByteBuffer): Int

  /** Writes what fits now from `buf`: the bytes written, or -1 if nothing fits. */
  def writeNow(handle: Handle, buf: ByteBuffer): Int

  /** Closes a handle. A Unix socket path that `listen` or `datagram` bound is removed with it. */
  def close(handle: Handle): Unit
}

/** A mutable outcome of an operation. Contract: setting/clearing is atomic; additional set before a clear is forbidden.
  * A `clear()` after the op's completion sees the latest written value.
  */
final class Slot[A <: AnyRef] {
  private val inner: java.util.concurrent.atomic.AtomicReference[A | Null] =
    new java.util.concurrent.atomic.AtomicReference(null)

  def clear(): A = {
    inner.getAndSet(null) match
      case null => throw new IllegalStateException("Nothing has been written to this slot yet")
      case v    => v.asInstanceOf[A]
  }

  private[asyncio] def set(outcome: A): Unit = {
    val value = outcome.nn
    if !inner.compareAndSet(null, value) then throw new IllegalStateException("Slot was already set")
  }
}

/** Marks an op that may be submitted again after its previous submission finished, instead of building a new one each
  * time: an accept loop can resubmit one accept op, and a connection can resubmit one read on its buffer. Anything an
  * op reports, such as `Accepted.accepted`, reflects its latest submission, so read it in that submission's completion.
  */
trait Repeatable

/** What a promise op adds: it completes when whoever holds it calls `complete`, from any thread. */
trait Promise[-A] {

  /** Completes the promise with `value`, from any thread. A promise that carries a value writes it into its slot. */
  def complete(value: A): Unit
}

enum AddressFamily {
  case IPv4, IPv6
}

final case class ResolvedAddress(family: AddressFamily, host: String)

/** The ops a reactor can build, as values of its `Op` type, on I/O objects named by its `Handle` type. Buffers are used
  * between position and limit, and belong to the reactor from submission until the op's completion.
  */
trait Ops[Op, Handle, BoxedHandle <: AnyRef] {

  /** Fills `buf`, which must have room. Completes once at least one byte has been read, or at end of stream, when
    * nothing was added: compare the buffer's position before and after.
    */
  def read(handle: Handle, buf: ByteBuffer): Op & Repeatable

  /** Drains `buf`. Completes once at least one byte has been written; resubmit while `buf` has bytes remaining. */
  def write(handle: Handle, buf: ByteBuffer): Op & Repeatable

  /** Accepts one connection on a listening socket. Completes after writing the new, non-blocking connection into
    * `into`, which must be empty.
    */
  def accept(handle: Handle, into: Slot[BoxedHandle]): Op & Repeatable

  /** Waits for a non-blocking connect on `handle` to finish. Completes once connected, or fails. */
  def connect(handle: Handle): Op

  /** Receives one packet into `buf`, which is left flipped for reading, and writes its sender into `from`, which must
    * be empty. Completes once a packet has arrived; its size is `buf.remaining`, which may be 0.
    */
  def receive(handle: Handle, buf: ByteBuffer, from: Slot[Address]): Op & Repeatable

  /** Sends `buf` as one packet, to `to` or to the connected peer when `to` is null. Completes once the whole packet has
    * been sent, consuming `buf`.
    */
  def send(handle: Handle, buf: ByteBuffer, to: Address | Null): Op & Repeatable

  /** Completes once `milliseconds` have passed. */
  def timer(milliseconds: Int): Op & Repeatable

  /** Waits until `handle` is ready for `interest`, then runs `perform` on the reactor thread. The op completes when
    * `perform` returns true; false means it was not ready after all, and the reactor waits again. For operations the
    * other ops do not cover, such as draining a FIFO.
    */
  def whenReady(handle: Handle, interest: Interest)(perform: () => Boolean): Op & Repeatable

  /** Runs `body` off the reactor thread, completing when it returns. Cancelling it skips `body` if it has not started
    * and interrupts it if it is running; interruption is cooperative.
    */
  def blocking(body: () => Unit): Op

  /** Like the other `blocking`, writing `body`'s value into `into`, which must be empty, before completing. */
  def blocking[A <: AnyRef](body: () => A, into: Slot[A]): Op

  /** A promise for someone else to complete, carrying no value. */
  def promise(): Op & Promise[Unit]

  /** A promise whose completer supplies a value, written into `into`, which must be empty. */
  def promise[A <: AnyRef](into: Slot[A]): Op & Promise[A]

  /** Resolves a host name without blocking the reactor. Completes after writing its addresses, in numeric form, into
    * `into`, which must be empty; fails with an `IOException` if the name cannot be resolved.
    */
  def resolve(host: String, into: Slot[List[ResolvedAddress]]): Op
}
