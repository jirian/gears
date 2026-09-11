package gears.async.asyncio.epoll

import gears.async.Async
import gears.async.asyncio.Buffer
import gears.async.asyncio.Error
import gears.async.asyncio.Result
import gears.async.net
import gears.util.either

import java.io.OutputStream
import java.net.SocketAddress
import java.nio.channels.Channels
import scala.scalanative.posix.errno
import scala.scalanative.posix.sys.{socket => posixSocket}
import scala.scalanative.posix.unistd
import scala.scalanative.runtime._
import scala.scalanative.unsafe._
import scala.scalanative.unsigned._
import scala.annotation.tailrec
import java.net.SocketException
import java.io.InputStream
import java.net.SocketTimeoutException
import gears.async.net.SocketOption

class EpollTcpStream private[epoll] (
    socket: java.net.Socket,
    poller: EpollPoller
) extends net.TcpStream:
  private val fd = ??? // SocketFd.ofSocket(socket)
  private val (handle, cancel) =
    poller.registerFd(fd, true, true)

  override def close(): Unit =
    socket.close()
    cancel.cancel()

  inline def debug[T](msg: String)(inline value: => T): T =
    val t = value
    println(s"$msg: $t")
    t

  override def readBuf(buf: Buffer)(using Async): Result[Unit] = either:
    val inputStream = new InputStream {
      val inner = socket.getInputStream()
      override def read(): Int =
        val b = scala.Array(0.toByte)
        if read(b) == -1 then -1 else b(0).toByte
      override def read(b: scala.Array[Byte], off: Int, len: Int): Int =
        @tailrec def loop(current: Int): Int =
          try inner.read(b, off, len)
          catch
            case _: SocketTimeoutException => // either EAGAIN or EWOULDBLOCK
              loop(handle.read.onUpdate(current))
            case e => throw e
          // debug(s"read from $fd (count $len)")(
        loop(handle.read.counter)
        // )
    }
    if Channels.newChannel(inputStream).read(buf) == -1 then
      either.error(Error.EOF)

  override def writeBuf(buf: Buffer)(using Async): Result[Unit] = either:
    val outputStream = new OutputStream:
      override def write(b: Int): Unit = write(scala.Array(b.toByte))
      override def write(b: scala.Array[Byte], off: Int, len: Int): Unit =
        if (off > b.length || off < 0 || len < 0 || len > b.length - off)
          throw new IndexOutOfBoundsException()

        var n = off
        val stop = off + len
        var counter = handle.write.counter
        while (n < stop) {
          val count = posixSocket.send(
            fd,
            b.at(n),
            (stop - n).toUInt,
            posixSocket.MSG_NOSIGNAL
          )
          if count < 0 then
            if errno.errno == errno.EAGAIN then
              // OS tells us to poll, do so
              counter = poll(counter)
            else throw IOException(errno.errno)
          else n += count.toInt
        }

      /* Uses the captured `Async`! */
      def poll(counter: Int): Int =
        handle.write.onUpdate(counter)
    Channels.newChannel(outputStream).write(buf)

  override def localAddress: SocketAddress = socket.getLocalSocketAddress()
  override def remoteAddress: SocketAddress = socket.getRemoteSocketAddress()

class EpollTcpListener private[epoll] (
    socket: java.net.ServerSocket,
    poller: EpollPoller
) extends net.TcpListener:
  type Stream = EpollTcpStream

  val fd = ??? // SocketFd.ofServerSocket(socket)
  val (handle, cancel) = poller.registerFd(fd, true, false)

  override def close(): Unit =
    cancel.cancel()
    socket.close()

  override def accept()(using Async): Result[Stream] =
    @tailrec def loop(current: Int): Result[Stream] =
      try
        val conn = socket.accept()
        either.ok(EpollTcpStream(conn, poller))
      catch
        case _: SocketException
            if errno.errno == errno.EAGAIN || errno.errno == errno.EWOULDBLOCK =>
          loop(handle.read.onUpdate(current))
        case e => throw e
    loop(handle.read.counter)

  override def localAddress: SocketAddress = socket.getLocalSocketAddress()

trait EpollTcpSupport(poller: EpollPoller) extends net.TcpSupport:
  type Stream = EpollTcpStream
  type Listener = EpollTcpListener

  override def connect(address: SocketAddress, options: Seq[SocketOption])(using
      Async
  ): Result[Stream] =
    val socket = java.net.Socket()
    options.foreach(op => socket.setOption(op.key, op.value))
    socket.connect(address)
    either.ok(EpollTcpStream(socket, poller))

  override def listen(address: SocketAddress, options: Seq[SocketOption])(using
      Async
  ): Result[Listener] =
    val socket = java.net.ServerSocket()
    options.foreach(op => socket.setOption(op.key, op.value))
    socket.setReuseAddress(true)
    socket.bind(address)
    either.ok(EpollTcpListener(socket, poller))
