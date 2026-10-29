package asyncio.uring

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.scalanative.posix.arpa.inet
import scala.scalanative.posix.fcntl
import scala.scalanative.posix.netinet.in
import scala.scalanative.posix.netinet.inOps.{*, given}
import scala.scalanative.posix.string
import scala.scalanative.posix.sys.socket
import scala.scalanative.posix.sys.socketOps.{*, given}
import scala.scalanative.posix.sys.stat
import scala.scalanative.posix.sys.statOps.*
import scala.scalanative.posix.sys.un
import scala.scalanative.posix.sys.unOps.{*, given}
import scala.scalanative.posix.unistd
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import asyncio.FifoHandles
import asyncio.Handle
import asyncio.IpAddress
import asyncio.PipeHandles
import asyncio.ReadinessHandles
import asyncio.UnixAddress
import asyncio.UnixDatagramHandles
import asyncio.UnixStreamHandles
import asyncio.unsafe.DescriptorTable
import asyncio.unsafe.NonBlocking
import asyncio.unsafe.PosixErr.cError
import asyncio.unsafe.PosixFileOps
import asyncio.unsafe.PosixSockets

/** Either kind of socket address, as the system calls take them. */
private[uring] type SocketAddress = IpAddress | UnixAddress

/** io_uring's handles are POSIX file descriptors, opened non-blocking and close-on-exec, so that a process started
  * meanwhile inherits none of them. Descriptor numbers are process-wide, so the Unix paths bound by `listener` and
  * `datagram` are remembered per descriptor and removed when it is closed. Writes to a socket whose peer has gone fail
  * with `EPIPE` rather than raising `SIGPIPE`, since every send passes `MSG_NOSIGNAL`.
  */
object UringHandles
    extends UnixStreamHandles
    with UnixDatagramHandles
    with FifoHandles
    with ReadinessHandles
    with PipeHandles {

  /** What is kept about an open socket, as its descriptor's note in `DescriptorTable`: the Unix path it bound, to
    * remove when it closes, and for a socket from `listener`, where it is to listen until its `listening` op binds it.
    */
  private final class SocketNote {
    @volatile var boundPath: String | Null = null
    @volatile var toListen: (SocketAddress, Boolean) | Null = null
    @volatile var datagram = false // bound, or connected, when made: its `listening` op has nothing to do
  }

  /** `fd`'s note, made if it has none. Only the thread opening or binding a socket sets one. */
  private def noteOf(fd: Int): SocketNote = DescriptorTable.note(fd) match {
    case note: SocketNote => note
    case _                =>
      val note = new SocketNote
      DescriptorTable.setNote(fd, note)
      note
  }

  /** Forgets a note left under the number of `fd`, which was just opened: one is left only by a descriptor closed
    * without `close`, and its path must not be removed when the new descriptor closes.
    */
  private[uring] def opened(fd: Int): Unit = DescriptorTable.setNote(fd, null)

  /** The handle of descriptor `fd`: for uring-specific code, whose handles are descriptors; the interface's handles
    * come from the openers below.
    */
  def handle(fd: Int): Handle = Handle.wrap(fd)

  /** The descriptor `handle` is, for uring-specific code. */
  def descriptor(handle: Handle): Int = Handle.unwrap(handle)

  private def wrap(fd: Int): Handle = handle(fd)
  private[uring] def fd(handle: Handle): Int = descriptor(handle)

  def listener(address: IpAddress, reuseAddr: Boolean = true): Handle = wrap(listenerOn(address, reuseAddr))
  def listener(address: UnixAddress): Handle = wrap(listenerOn(address, reuseAddr = false))

  private def listenerOn(address: SocketAddress, reuseAddr: Boolean): Int = {
    val fd = socketFor(address, socket.SOCK_STREAM)(_ => ())
    noteOf(fd).toListen = (address, reuseAddr)
    fd
  }

  /** Binds and listens a socket from `listener`, for its `listening` op, or does nothing for a datagram socket, bound
    * when it was made; throws if it is neither, or the bind fails.
    */
  private[uring] def listening(fd: Int): Unit = {
    val note = DescriptorTable.note(fd) match {
      case note: SocketNote => note
      case _                => null
    }
    val pending = if note == null then null else { val pending = note.toListen; note.toListen = null; pending }
    if pending == null then
      if note != null && note.datagram then return
      else throw new IOException(s"Descriptor $fd is not a listener waiting to listen")
    val (address, reuseAddr) = pending
    if reuseAddr then PosixSockets.setReuseAddress(fd)
    bindTo(fd, address)
    PosixSockets.listen(fd, PosixSockets.maxConnections)
  }

  /** Opens a FIFO without blocking; anything else is refused. Opening one for writing fails while it has no reader. */
  def openFifo(path: String, write: Boolean): Handle = {
    val flags = (if write then fcntl.O_WRONLY else fcntl.O_RDONLY) | fcntl.O_NONBLOCK | Linux.O_CLOEXEC
    val fd = Zone.acquire { implicit z => fcntl.open(toCString(path), flags) }
    if fd < 0 then throw new IOException(s"Failed to open $path: ${cError()}")
    opened(fd)
    val info = stackalloc[stat.stat]()
    if stat.fstat(fd, info) < 0 || stat.S_ISFIFO(info.st_mode) == 0 then {
      unistd.close(fd)
      throw new IOException(s"Not a FIFO: $path")
    }
    wrap(fd)
  }

  def connect(address: IpAddress): Handle = wrap(connectStream(address))
  def connect(address: UnixAddress): Handle = wrap(connectStream(address))

  private def connectStream(address: SocketAddress): Int =
    socketFor(address, socket.SOCK_STREAM)(fd => connectTo(fd, address))

  def datagram(local: IpAddress | Null, remote: IpAddress | Null): Handle = {
    require(
      local == null || remote == null || local.getClass == remote.getClass,
      "Local and remote addresses must be of the same family"
    )
    wrap(datagramSocket(local, remote))
  }

  def datagram(local: UnixAddress | Null, remote: UnixAddress | Null): Handle = wrap(datagramSocket(local, remote))

  private def datagramSocket(local: SocketAddress | Null, remote: SocketAddress | Null): Int = {
    val either = if local != null then local else remote
    require(either != null, "A datagram socket needs a local or a remote address")
    val fd = socketFor(either, socket.SOCK_DGRAM) { fd =>
      if local != null then bindTo(fd, local)
      if remote != null then connectTo(fd, remote) // assigns an IP socket an ephemeral local port
    }
    noteOf(fd).datagram = true
    fd
  }

  def pipe(): (Handle, Handle) = {
    val ends = stackalloc[CInt](2)
    if LinuxCalls.pipe2(ends, fcntl.O_NONBLOCK | Linux.O_CLOEXEC) < 0 then
      throw new IOException(s"Failed to create pipe: ${cError()}")
    opened(ends(0))
    opened(ends(1))
    (wrap(ends(0)), wrap(ends(1)))
  }

  def readNow(handle: Handle, buf: ByteBuffer): Int = NonBlocking.read(fd(handle), buf)
  def writeNow(handle: Handle, buf: ByteBuffer): Int = NonBlocking.write(fd(handle), buf)

  /** Closes a descriptor, from any thread: its generation in `DescriptorTable` ends first, while the number is still
    * its, and every open reactor is told afterwards, with the generation that ended, so that the commands pending on it
    * are cancelled and fail (see `Ops.close`). A Unix path it bound is removed before the descriptor is closed, so that
    * the removal cannot hit a socket bound to the same path once the descriptor is gone. A handle must not be used
    * while it is being closed.
    */
  def close(handle: Handle): Unit = closeDescriptor(fd(handle))

  /** `close` on a descriptor number, for the backend's own code. */
  private[uring] def closeDescriptor(handle: Int): Unit = {
    val path = DescriptorTable.note(handle) match { // taken before closing, while the number is still ours
      case note: SocketNote => note.boundPath
      case _                => null
    }
    DescriptorTable.setNote(handle, null)
    val generation = DescriptorTable.generation(DescriptorTable.get(handle))
    try
      if path != null then
        Zone.acquire { implicit z => PosixFileOps.safeUnlink(toCString(path, StandardCharsets.UTF_8)) }
    finally {
      DescriptorTable.closing(handle)
      PosixSockets.close(handle)
      UringReactor.descriptorClosed(handle, generation)
    }
  }

  /** Opens a non-blocking, close-on-exec socket and runs `setup` on it, closing it again if `setup` fails. */
  private def socketFor(address: SocketAddress, kind: Int)(setup: Int => Unit): Int = {
    val family = address match {
      case UnixAddress(_)       => socket.AF_UNIX
      case IpAddress.IPv4(_, _) => socket.AF_INET
      case IpAddress.IPv6(_, _) => socket.AF_INET6
    }
    val fd = socket.socket(family, kind | Linux.SOCK_NONBLOCK | Linux.SOCK_CLOEXEC, 0)
    if fd < 0 then throw new IOException(s"Failed to open socket: ${cError()}")
    opened(fd)
    try {
      setup(fd)
      fd
    } catch {
      case t: Throwable =>
        closeDescriptor(fd)
        throw t
    }
  }

  private def bindTo(fd: Int, address: SocketAddress): Unit = address match {
    case UnixAddress(path) =>
      PosixSockets.bindUnixAddr(fd, path, removeExisting = true)
      noteOf(fd).boundPath = path
    case IpAddress.IPv4(host, port) => PosixSockets.bindIPv4Addr(fd, host, port)
    case IpAddress.IPv6(host, port) => PosixSockets.bindIPv6Addr(fd, host, port)
  }

  private def connectTo(fd: Int, address: SocketAddress): Unit = address match {
    case UnixAddress(path)          => PosixSockets.connectUnixAddr(fd, path)
    case IpAddress.IPv4(host, port) => PosixSockets.connectIPv4Addr(fd, host, port)
    case IpAddress.IPv6(host, port) => PosixSockets.connectIPv6Addr(fd, host, port)
  }

  /** Writes the socket address for `address` into `storage`, which holds `capacity` bytes, and returns its length. */
  private[uring] def encode(address: SocketAddress, storage: Ptr[socket.sockaddr], capacity: Int): Int = {
    def copy(addr: Ptr[socket.sockaddr], length: socket.socklen_t): Int = {
      require(length.toInt <= capacity, s"The address of $address does not fit")
      string.memcpy(storage, addr, length.toLong.toCSize)
      length.toInt
    }
    address match {
      case UnixAddress(path)          => PosixSockets.withUnixSocketAddr(path)(copy)
      case IpAddress.IPv4(host, port) => PosixSockets.withIPv4SocketAddr(host, port)(copy)
      case IpAddress.IPv6(host, port) => PosixSockets.withIPv6SocketAddr(host, port)(copy)
    }
  }

  /** Decodes a socket address filled in by the kernel. An unnamed Unix socket has the empty path. */
  private[uring] def decode(addr: Ptr[socket.sockaddr], length: socket.socklen_t): SocketAddress = {
    val family = addr.sa_family.toInt
    if family == socket.AF_UNIX then {
      val unix = addr.asInstanceOf[Ptr[un.sockaddr_un]]
      val headerLength = sizeOf[un.sockaddr_un] - sizeOf[CArray[CChar, un._108]]
      if length.toInt <= headerLength.toInt then UnixAddress("")
      else UnixAddress(fromCString(unix.sun_path.at(0), StandardCharsets.UTF_8))
    } else {
      val text = stackalloc[Byte](64)
      if family == socket.AF_INET then {
        val v4 = addr.asInstanceOf[Ptr[in.sockaddr_in]]
        inet.inet_ntop(family, v4.at3.asInstanceOf[Ptr[Byte]], text, 64.toUInt)
        IpAddress.IPv4(fromCString(text), inet.ntohs(v4.sin_port).toInt)
      } else if family == socket.AF_INET6 then {
        val v6 = addr.asInstanceOf[Ptr[in.sockaddr_in6]]
        inet.inet_ntop(family, v6.at4.asInstanceOf[Ptr[Byte]], text, 64.toUInt)
        IpAddress.IPv6(fromCString(text), inet.ntohs(v6.sin6_port).toInt)
      } else throw new IOException(s"Unsupported address family $family")
    }
  }
}
