// from https://github.com/natsukagami/gears-io/blob/direct/shared/src/main/scala/net.scala

package gears.async.net

import gears.async.Async
import gears.async.asyncio.{Reader, Writer, Buffer}
import java.io.Closeable
import gears.async.asyncio.Result
import java.net.SocketAddress

// Represents a connected TCP stream.
abstract class TcpStream extends Reader, Writer, Closeable:
  def localAddress: SocketAddress
  def remoteAddress: SocketAddress

// Represents a TCP server/listener.
abstract class TcpListener extends Closeable:
  type Stream <: TcpStream

  def accept()(using Async): Result[Stream]

  def localAddress: SocketAddress

trait TcpSupport:
  type Stream <: TcpStream
  type Listener <: TcpListener { type Stream = TcpSupport.this.Stream }

  def connect(address: SocketAddress, options: Seq[SocketOption])(using
      Async
  ): Result[Stream]
  def listen(address: SocketAddress, options: Seq[SocketOption])(using
      Async
  ): Result[Listener]

object TcpSupport:
  def connect(address: SocketAddress, options: SocketOption*)(using
      tcp: TcpSupport,
      async: Async
  ) =
    tcp.connect(address, options)
  def listen(address: SocketAddress, options: SocketOption*)(using
      tcp: TcpSupport,
      async: Async
  ) =
    tcp.listen(address, options)

// Options for socket creation.
sealed trait SocketOption:
  type Value
  def key: java.net.SocketOption[Value]
  def value: Value

object SocketOption:
  import java.net.{StandardSocketOptions => std}
  sealed abstract class JavaSocketOption[T](
      val key: java.net.SocketOption[T],
      val value: T
  ) extends SocketOption:
    type Value = T

  case class sendBufSize(n: Int) extends JavaSocketOption(std.SO_SNDBUF, n)
  case class recvBufSize(n: Int) extends JavaSocketOption(std.SO_RCVBUF, n)
  case class keepAlive(keep: Boolean)
      extends JavaSocketOption(std.SO_KEEPALIVE, keep)
  case class reuseAddr(reuse: Boolean)
      extends JavaSocketOption(std.SO_REUSEADDR, reuse)
  case class linger(interval: Int)
      extends JavaSocketOption(std.SO_LINGER, interval)
  case class noDelay(noDelay: Boolean)
      extends JavaSocketOption(std.TCP_NODELAY, noDelay)
