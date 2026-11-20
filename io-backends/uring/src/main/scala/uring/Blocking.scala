package gears.async.io
package uring

import java.io.IOException
import scala.scalanative.libc.string
import scala.scalanative.posix.fcntl
import scala.scalanative.posix.netdb
import scala.scalanative.posix.netdbOps.*
import scala.scalanative.posix.sys.socket
import scala.scalanative.posix.sys.stat
import scala.scalanative.posix.sys.statOps.*
import scala.scalanative.posix.unistd
import scala.scalanative.runtime.ByteArray
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

private[uring] object BlockingPool {

  inline val Lookups = 16

  inline val Others = 64

  val lookups: Pool = new Pool("lookup", Lookups)
  val others: Pool = new Pool("blocking", Others)

  final class Pool(name: String, cap: Int) {
    private val counter = new java.util.concurrent.atomic.AtomicInteger()
    private val executor = {
      val e = new java.util.concurrent.ThreadPoolExecutor(
        cap,
        cap,
        60L,
        java.util.concurrent.TimeUnit.SECONDS,
        new java.util.concurrent.LinkedBlockingQueue[Runnable](),
        { body =>
          val thread = new Thread(body, s"gears-io-uring-$name-${counter.incrementAndGet()}")
          thread.setDaemon(true)
          thread
        }
      )
      e.allowCoreThreadTimeOut(true)
      e
    }

    def run(task: Runnable): Unit = executor.execute(task)

    def threads: Int = executor.getPoolSize()
    def mostThreads: Int = executor.getLargestPoolSize()
  }
}

private[uring] final class OpenFile(path: String, write: Boolean, into: Slot[FileHandle])
    extends BlockingTask[FileHandle](
      () => Files.open(path, write),
      into.set,
      List(into),
      interruptible = false,
      discard = file => unistd.close(file.fd): Unit
    ) {
  override def toString: String = s"open of $path"
}

private[uring] object Files {

  def checkPath(path: String): Unit =
    if path.indexOf('\u0000') >= 0 then throw new IllegalArgumentException(s"A path has no NUL: $path")

  def open(path: String, write: Boolean): FileHandle = Zone.acquire { implicit z =>
    val access = if write then fcntl.O_WRONLY else fcntl.O_RDONLY
    val fd = fcntl.open(toCString(path), access | fcntl.O_NONBLOCK | Linux.O_CLOEXEC)
    if fd < 0 then throw Results.failed(s"Failed to open $path")
    try {
      val info = stackalloc[stat.stat]()
      if stat.fstat(fd, info) < 0 then throw Results.failed(s"Failed to inspect $path")
      if stat.S_ISREG(info.st_mode) == 0 then throw new IOException(s"Not a regular file: $path")
      val flags = fcntl.fcntl(fd, fcntl.F_GETFL, 0)
      if flags < 0 || fcntl.fcntl(fd, fcntl.F_SETFL, flags & ~fcntl.O_NONBLOCK) < 0 then
        throw Results.failed(s"Failed to make $path blocking")
    } catch {
      case e: Throwable =>
        unistd.close(fd)
        throw e
    }
    new FileHandle(fd, path)
  }

  def openFifo(path: String, write: Boolean): FifoSource | FifoSink = Zone.acquire { implicit z =>
    val access = if write then fcntl.O_WRONLY else fcntl.O_RDONLY
    val fd = fcntl.open(toCString(path), access | fcntl.O_NONBLOCK | Linux.O_CLOEXEC)
    if fd < 0 then throw Results.failed(s"Failed to open the FIFO $path")
    try {
      val info = stackalloc[stat.stat]()
      if stat.fstat(fd, info) < 0 then throw Results.failed(s"Failed to inspect $path")
      if stat.S_ISFIFO(info.st_mode) == 0 then throw new IOException(s"Not a FIFO: $path")
    } catch {
      case e: Throwable =>
        unistd.close(fd)
        throw e
    }
    if write then new FifoSink(fd, path) else new FifoSource(fd, path)
  }
}

private[uring] final class OpenFifo[H <: UringHandle](path: String, write: Boolean, into: Slot[H])
    extends BlockingTask[H](
      () => Files.openFifo(path, write).asInstanceOf[H],
      into.set,
      List(into),
      interruptible = false,
      discard = end => unistd.close(end.fd): Unit
    ) {
  override def toString: String = s"open of FIFO $path"
}

private[uring] final class Resolve(host: String, into: Slot[List[IpAddress]])
    extends BlockingTask[List[IpAddress]](
      () => Resolve.lookup(host),
      into.set,
      List(into),
      interruptible = false,
      pool = BlockingPool.lookups
    ) {
  override def toString: String = s"resolve of $host"
}

private[uring] object Resolve {

  def check(host: String): Unit = {
    if host.isEmpty then throw new IllegalArgumentException("An empty host name names no host")
    if host.indexOf('\u0000') >= 0 then throw new IllegalArgumentException(s"A host name has no NUL: $host")
  }

  def lookup(host: String): List[IpAddress] = Zone.acquire { implicit z =>
    val bare = if host.startsWith("[") && host.endsWith("]") then host.substring(1, host.length - 1) else host
    val hints = alloc[netdb.addrinfo]()
    hints.ai_socktype = socket.SOCK_STREAM
    val results = alloc[Ptr[netdb.addrinfo]]()
    val status = netdb.getaddrinfo(toCString(bare), null, hints, results)
    if status != 0 then throw new IOException(s"Failed to resolve $host: ${fromCString(netdb.gai_strerror(status))}")
    val found = scala.collection.mutable.LinkedHashSet.empty[IpAddress]
    try {
      var entry = !results
      while entry != null do {
        val length = entry.ai_addrlen.toInt
        if length <= Sockets.StorageSize then {
          val bytes = new Array[Byte](Sockets.StorageSize)
          string.memcpy(bytes.asInstanceOf[ByteArray].at(0), entry.ai_addr.asInstanceOf[Ptr[Byte]], length.toUSize)
          Sockets.read(bytes, length, unmap = false) match {
            case IpSocketAddress(address, _) => found += address
            case _                           => ()
          }
        }
        entry = entry.ai_next
      }
    } finally netdb.freeaddrinfo(!results)
    if found.isEmpty then throw new IOException(s"Failed to resolve $host: it has no IPv4 or IPv6 address")
    found.toList
  }
}
