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

  /** Half-closes this stream: the peer reads end of stream, and this side can still read. */
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

  /** Returns the address this socket is bound to. */
  def localAddress: SocketAddress

  /** Sends the remaining bytes of `buf` as one datagram.
    *
    * @param buf
    *   the datagram
    * @param target
    *   where to send it
    */
  def sendTo(buf: Buffer, target: SocketAddress)(using Async): Result[Unit]

  /** Receives one datagram into `buf`, advancing its position by the datagram's size. A datagram larger than the room
    * in `buf` is truncated.
    *
    * @param buf
    *   where to put the datagram, which must be writable
    * @return
    *   the datagram's sender
    */
  def receiveFrom(buf: Buffer)(using Async): Result[SocketAddress]

/** Opens datagram sockets. */
trait UdpSupport:
  type Socket <: UdpSocket

  /** Opens a datagram socket bound to `address`.
    *
    * @param address
    *   the local address; port 0 picks a free port
    * @param options
    *   the options to set before binding
    */
  def bind(address: SocketAddress, options: Seq[SocketOption])(using
      Async
  ): Result[Socket]

object UdpSupport:

  /** Opens a datagram socket bound to `address` with the given `UdpSupport`.
    *
    * @param address
    *   the local address; port 0 picks a free port
    * @param options
    *   the options to set before binding
    */
  def bind(address: SocketAddress, options: SocketOption*)(using
      udp: UdpSupport,
      async: Async
  ) =
    udp.bind(address, options)

/** Shared TLS configuration, such as trusted certificates or a server's certificate and key. */
trait TlsContext extends java.io.Closeable

/** TLS over existing TCP streams. */
trait TlsSupport:

  /** Returns a client configuration.
    *
    * @param verify
    *   whether to check the server's certificate against the system store and `caFile`
    * @param caFile
    *   a PEM file of extra trusted certificates
    */
  def clientContext(verify: Boolean = true, caFile: Option[String] = None): TlsContext

  /** Returns a server configuration from PEM files.
    *
    * @param certFile
    *   the certificate chain
    * @param keyFile
    *   the private key of its first certificate
    */
  def serverContext(certFile: String, keyFile: String): TlsContext

  /** Runs a client handshake on `stream`.
    *
    * @param stream
    *   the connection to secure
    * @param context
    *   a client configuration
    * @param serverName
    *   the name the server's certificate must match
    * @return
    *   the encrypted stream
    */
  def wrapClient(stream: TcpStream, context: TlsContext, serverName: String)(using Async): TcpStream

  /** Runs a server handshake on `stream`.
    *
    * @param stream
    *   the accepted connection to secure
    * @param context
    *   a server configuration
    * @return
    *   the encrypted stream
    */
  def wrapServer(stream: TcpStream, context: TlsContext)(using Async): TcpStream

/** Host name lookup that suspends the caller instead of blocking its thread. */
trait DnsSupport:

  /** Returns the addresses of `host`.
    *
    * @param host
    *   a host name or a numeric address
    * @throws java.net.UnknownHostException
    *   if there are none
    */
  def resolve(host: String)(using Async): Array[java.net.InetAddress]

object DnsSupport:
  /** Returns the addresses of `host`, looked up with the given `DnsSupport`.
    *
    * @param host
    *   a host name or a numeric address
    * @throws java.net.UnknownHostException
    *   if there are none
    */
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
