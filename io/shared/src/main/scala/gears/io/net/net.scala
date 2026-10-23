// from https://github.com/natsukagami/gears-io/blob/direct/shared/src/main/scala/net.scala

package gears.io.net

import gears.async.Async
import gears.io.Result
import gears.io.{Buffer, Reader, Writer}

import java.io.Closeable
import java.net.SocketAddress

// Represents a connected TCP stream.
abstract class TcpStream extends Reader, Writer, Closeable:
  def localAddress: SocketAddress
  def remoteAddress: SocketAddress

  /** Half-closes the stream: the peer reads end of stream, and this side can still read. */
  def shutdownOutput()(using Async): Unit

  /** Stops reading: a pending or later read returns end of stream. */
  def shutdownInput()(using Async): Unit

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

/** A bound datagram socket, IP or Unix. */
abstract class UdpSocket extends Closeable:
  def localAddress: SocketAddress

  /** Sends the remaining bytes of `buf` as one datagram to `target`. */
  def sendTo(buf: Buffer, target: SocketAddress)(using Async): Result[Unit]

  /** Receives one datagram into `buf`, advancing its position by the datagram's size, and returns its sender. A
    * datagram larger than the room in `buf` is truncated.
    */
  def receiveFrom(buf: Buffer)(using Async): Result[SocketAddress]

/** Opens datagram sockets. */
trait UdpSupport:
  type Socket <: UdpSocket

  def bind(address: SocketAddress, options: Seq[SocketOption])(using
      Async
  ): Result[Socket]

object UdpSupport:
  def bind(address: SocketAddress, options: SocketOption*)(using
      udp: UdpSupport,
      async: Async
  ) =
    udp.bind(address, options)

/** Shared TLS configuration, such as trusted certificates or a server's certificate and key. */
trait TlsContext extends java.io.Closeable

/** TLS over existing TCP streams. */
trait TlsSupport:

  /** A client configuration; `verify` checks the server's certificate against the system store and `caFile`. */
  def clientContext(verify: Boolean = true, caFile: Option[String] = None): TlsContext

  /** A server configuration from PEM files. */
  def serverContext(certFile: String, keyFile: String): TlsContext

  /** Runs a client handshake on `stream` for `serverName` and returns the encrypted stream. */
  def wrapClient(stream: TcpStream, context: TlsContext, serverName: String)(using Async): TcpStream

  /** Runs a server handshake on `stream` and returns the encrypted stream. */
  def wrapServer(stream: TcpStream, context: TlsContext)(using Async): TcpStream

/** Host name lookup that suspends the caller instead of blocking its thread. */
trait DnsSupport:

  /** The addresses of `host`; throws `UnknownHostException` if there are none. */
  def resolve(host: String)(using Async): Array[java.net.InetAddress]

object DnsSupport:
  def resolve(host: String)(using dns: DnsSupport, async: Async): Array[java.net.InetAddress] =
    dns.resolve(host)

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
  case class keepAlive(keep: Boolean) extends JavaSocketOption(std.SO_KEEPALIVE, keep)
  case class reuseAddr(reuse: Boolean) extends JavaSocketOption(std.SO_REUSEADDR, reuse)
  case class linger(interval: Int) extends JavaSocketOption(std.SO_LINGER, interval)
  case class noDelay(noDelay: Boolean) extends JavaSocketOption(std.TCP_NODELAY, noDelay)
