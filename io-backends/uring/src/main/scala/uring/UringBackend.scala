package gears.async.io
package uring

import gears.async.io.Blocking
import gears.async.io.BufferPool
import gears.async.io.Fifos
import gears.async.io.IpDatagrams
import gears.async.io.IpStreams
import gears.async.io.Pipes
import gears.async.io.RegularFiles
import gears.async.io.Resolving
import gears.async.io.UnixDatagrams
import gears.async.io.UnixStreams
import gears.async.io.internal.Reactor
import gears.async.io.internal.ReactorBackend

/** io_uring's reactors, with every capability a backend may have: IP, Unix stream and Unix datagram sockets, regular
  * files opened, read, written and synced by the kernel, name resolution on the blocker pool, pipes, blocking ops on
  * blocker threads, and FIFOs. io_uring runs operations on POSIX file descriptors, so a handle holds the descriptor.
  * The reactors all run the same ops, which keep no state of one reactor between submissions, so an op may go to any of
  * them in turn.
  */
object UringBackend
    extends ReactorBackend
    with Blocking
    with IpStreams
    with IpDatagrams
    with UnixStreams
    with UnixDatagrams
    with Fifos
    with RegularFiles
    with Resolving
    with Pipes {
  type Op = UringOp
  type Readable = UringKinds.Readable
  type Writable = UringKinds.Writable
  type Connection = UringKinds.Connection
  type Listener = UringKinds.Listener
  type BoundDatagram = UringKinds.BoundDatagram
  type ConnectedDatagram = UringKinds.ConnectedDatagram
  type UnixBoundDatagram = UringKinds.UnixBoundDatagram
  type UnixConnectedDatagram = UringKinds.UnixConnectedDatagram
  type File = UringKinds.File

  /** An open descriptor, as this backend's ops name it: made by the ops that open one, or by `UringHandles.handle` for
    * a descriptor that io_uring-specific code opened itself, so that an op names a descriptor by its handle, never by a
    * bare number. Every use of the descriptor takes the handle for its length (`acquire`), which fails once the handle
    * is closed, so that a stale handle fails its op rather than reaching whatever reuses the descriptor's number. A
    * command's use lasts from the moment its entry is prepared until the kernel has taken it, since the kernel reads
    * the number as the entry is submitted, and holds the file itself from then on, so that closing the number does not
    * reach an entry in the kernel; a system call's, such as `localAddress`'s, lasts for the call. Closing it is Go's:
    * the handle is closed at once, so that no use starts after, and the descriptor itself once no use is in progress,
    * by the closer if none is, and otherwise by the last use to end, on its own thread, so that the number is not
    * reused under a use and nobody waits for one (`close`). Before the descriptor closes, the closing thread cancels
    * the commands in the kernel on it, on every reactor, and waits until the kernel has let go of them, so that the
    * close frees the file, such as a listener's port, as it returns, whether or not their reactors are turning; each
    * reactor fails its own on its next turn.
    */
  final class Handle private[uring] (private[uring] val fd: Int) {
    // The sign bit: closed. The rest: how many uses of the descriptor are in progress.
    private val state = new java.util.concurrent.atomic.AtomicInteger(0)

    // How many commands on it have entries in the kernel, on any reactor: counted as each is prepared, under a use, so
    // that a close, which follows the use, sees it, and uncounted as the kernel lets go of it.
    private val entries = new java.util.concurrent.atomic.AtomicInteger(0)

    // The reactors that have had entries on it, one bit each by `UringReactor.index`, set under the same use: a close
    // tells and cancels on those alone. A reactor with no bit of its own sets them all.
    private val reactors = new java.util.concurrent.atomic.AtomicLong(0L)

    private[uring] def entered(reactorBit: Long): Unit = {
      entries.incrementAndGet(): Unit
      if (reactors.get() & reactorBit) != reactorBit then reactors.getAndAccumulate(reactorBit, _ | _): Unit
    }
    private[uring] def left(): Unit = entries.decrementAndGet(): Unit

    /** Whether a command on it may have an entry in the kernel. */
    private[uring] def inKernel: Boolean = entries.get() > 0

    /** The reactors that may hold entries on it, as bits by `UringReactor.index`. */
    private[uring] def reactorBits: Long = reactors.get()

    // What the descriptor is, for the commands to choose their operation by: found once, by `fstat`, unless its opener
    // said. Racing finders find the same.
    @volatile private var found = Handle.Unknown

    /** Whether the descriptor is a socket, a regular file, or something else, such as a pipe (`Handle.Socket`, ...). */
    private[uring] def kind: Int = {
      var k = found
      if k == Handle.Unknown then {
        k = UringHandles.kindOf(fd)
        found = k
      }
      k
    }

    private[uring] def knownAs(kind: Int): this.type = {
      found = kind
      this
    }

    // How many tries without waiting in a row, for reads and for writes, found nothing to do (`worthTrying`): a guess,
    // so racing updates do no harm.
    private var readMisses = 0
    private var writeMisses = 0

    /** Whether a command in direction `read` is worth trying with a non-blocking system call before it goes to the
      * kernel: not on a regular file, where the call would wait for the disk, and, after two tries in a row that found
      * nothing to do, only one time in eight, so that a connection that is mostly idle costs one system call per read
      * rather than two, and one that becomes busy is found again.
      */
    private[uring] def worthTrying(read: Boolean): Boolean =
      kind != Handle.RegularFile && {
        val misses = if read then readMisses else writeMisses
        misses < 2 || {
          if read then readMisses = misses + 1 else writeMisses = misses + 1
          (misses & 7) == 0
        }
      }

    /** Notes what a try in direction `read` found: something to do (`done`), or nothing. */
    private[uring] def tried(read: Boolean, done: Boolean): Unit = {
      val misses = if done then 0 else math.min((if read then readMisses else writeMisses) + 1, 1 << 20)
      if read then readMisses = misses else writeMisses = misses
    }

    // The turns of the ops pending on it, one reading and one writing at a time, on any submitter: each takes its
    // direction's as it is submitted, or waits for it if it may, and lets it go as its completion is taken (`Turns`).
    private[io] val reader = new gears.async.io.internal.Turns
    private[io] val writer = new gears.async.io.internal.Turns

    // What the last use to end runs, once the handle is closed while uses were in progress.
    @volatile private var finisher: Runnable | Null = null

    /** Whether it has not been closed. */
    private[io] def isOpen: Boolean = state.get() >= 0

    /** Starts a use of the descriptor; false, starting none, if the handle is closed. */
    private[uring] def acquire(): Boolean = {
      var current = state.get()
      while current >= 0 && !state.compareAndSet(current, current + 1) do current = state.get()
      current >= 0
    }

    /** Ends a use `acquire` started: the last of a closed handle's runs what `close` left for it. */
    private[uring] def release(): Unit =
      if state.decrementAndGet() == Int.MinValue then {
        val finish = finisher.nn
        finisher = null
        finish.run()
      }

    /** Marks it closed, so that no use starts after, counting itself a use while it leaves `finish` for the last use:
      * `Handle.Idle` if none was in progress, so that the caller closes the descriptor now, and `finish` never runs;
      * `Handle.InUse` if some were, the last of which runs `finish`; `Handle.AlreadyClosed` if it was.
      */
    private[io] def close(finish: Runnable): Int = {
      var current = state.get()
      while current >= 0 && !state.compareAndSet(current, (current + 1) | Int.MinValue) do current = state.get()
      if current < 0 then Handle.AlreadyClosed
      else {
        finisher = finish // before the closer's own use ends, so that whichever use ends last finds it
        if state.decrementAndGet() == Int.MinValue then {
          finisher = null
          Handle.Idle
        } else Handle.InUse
      }
    }

    override def toString: String = s"Handle($fd)"
  }

  private[io] object Handle {
    inline val Idle = 0
    inline val InUse = 1
    inline val AlreadyClosed = 2

    inline val Unknown = 0
    inline val Socket = 1
    inline val RegularFile = 2
    inline val Other = 3
  }

  def ops: UringOps.type = UringOps
  def pool(bufferSize: Int, buffers: Int): BufferPool = new gears.async.io.internal.NativePool(bufferSize, buffers)

  /** Opens a reactor with `UringReactor.open`'s defaults. */
  def open(): Reactor.Of[Op, Handle] = UringReactor.open()
}
