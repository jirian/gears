package gears.async.io
package uring

import gears.async.io.shareable.WorkpoolScheduler

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.util.concurrent.CancellationException
import scala.scalanative.libc.string
import scala.scalanative.posix.errno
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
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.control.NonFatal

/** The threads blocking work runs on, off the workers: started as needed, kept a minute once idle, daemons. */
private[uring] object BlockingPool {
  private val counter = new java.util.concurrent.atomic.AtomicInteger()
  private val executor = java.util.concurrent.Executors.newCachedThreadPool { body =>
    val thread = new Thread(body, s"gears-io-uring-blocking-${counter.incrementAndGet()}")
    thread.setDaemon(true)
    thread
  }

  def run(task: Runnable): Unit = executor.execute(task)
}

/** Opens a regular file and writes its handle into `into` (see `FileOps.open`), on the blocking pool: it opens without
  * blocking, so that a FIFO, which it then refuses, cannot keep it waiting for a writer, and turns blocking off again
  * for the regular file, so that io_uring reads uncached data from the disk rather than reporting would-block.
  */
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
  private def cError(): String = fromCString(string.strerror(errno.errno))

  def checkPath(path: String): Unit =
    if path.indexOf('\u0000') >= 0 then throw new IllegalArgumentException(s"A path has no NUL: $path")

  def open(path: String, write: Boolean): FileHandle = Zone.acquire { implicit z =>
    val access = if write then fcntl.O_WRONLY else fcntl.O_RDONLY
    val fd = fcntl.open(toCString(path), access | fcntl.O_NONBLOCK | Linux.O_CLOEXEC)
    if fd < 0 then throw new IOException(s"Failed to open $path: ${cError()}")
    try {
      val info = stackalloc[stat.stat]()
      if stat.fstat(fd, info) < 0 then throw new IOException(s"Failed to inspect $path: ${cError()}")
      if stat.S_ISREG(info.st_mode) == 0 then throw new IOException(s"Not a regular file: $path")
      val flags = fcntl.fcntl(fd, fcntl.F_GETFL, 0)
      if flags < 0 || fcntl.fcntl(fd, fcntl.F_SETFL, flags & ~fcntl.O_NONBLOCK) < 0 then
        throw new IOException(s"Failed to make $path blocking: ${cError()}")
    } catch {
      case e: Throwable =>
        unistd.close(fd)
        throw e
    }
    new FileHandle(fd, path)
  }
}

/** Resolves a host name with `getaddrinfo`, on the blocking pool, which nothing interrupts. */
private[uring] final class Resolve(host: String, into: Slot[List[IpAddress]])
    extends BlockingTask[List[IpAddress]](() => Resolve.lookup(host), into.set, List(into), interruptible = false) {
  override def toString: String = s"resolve of $host"
}

private[uring] object Resolve {

  def check(host: String): Unit = {
    if host.isEmpty then throw new IllegalArgumentException("An empty host name names no host")
    if host.indexOf('\u0000') >= 0 then throw new IllegalArgumentException(s"A host name has no NUL: $host")
  }

  /** Every IPv4 and IPv6 address of `host`, in the resolver's order, without repeats. Blocks. */
  def lookup(host: String): List[IpAddress] = Zone.acquire { implicit z =>
    val bare = if host.startsWith("[") && host.endsWith("]") then host.substring(1, host.length - 1) else host
    val hints = alloc[netdb.addrinfo]()
    hints.ai_socktype = socket.SOCK_STREAM // one entry per address rather than one per socket type
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
