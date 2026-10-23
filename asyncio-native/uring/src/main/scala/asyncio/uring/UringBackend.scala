package asyncio.uring

import asyncio.Blocking
import asyncio.BufferPool
import asyncio.Fifos
import asyncio.Files
import asyncio.Pipes
import asyncio.Reactor
import asyncio.ReactorBackend
import asyncio.Readiness
import asyncio.Resolving
import asyncio.UnixDatagrams
import asyncio.UnixStreams

/** io_uring's reactors, with every capability a backend may have: IP, Unix stream and Unix datagram sockets, regular
  * files opened, read, written and synced by the kernel, name resolution on the blocker pool, pipes, readiness,
  * blocking ops on blocker threads, and FIFOs. io_uring runs operations on POSIX file descriptors, so a handle is the
  * descriptor itself. The reactors all run the same ops, which keep no state of one reactor between submissions, so an
  * op may go to any of them in turn.
  */
object UringBackend
    extends ReactorBackend
    with Readiness
    with Blocking
    with UnixStreams
    with UnixDatagrams
    with Fifos
    with Files
    with Resolving
    with Pipes {
  type Op = UringOp
  def ops: UringOps.type = UringOps
  def handles: UringHandles.type = UringHandles
  def pool(bufferSize: Int, buffers: Int): BufferPool = new asyncio.unsafe.NativePool(bufferSize, buffers)

  /** Opens a reactor with `UringReactor.open`'s defaults. */
  def open(): Reactor.Of[Op] = UringReactor.open()
}
