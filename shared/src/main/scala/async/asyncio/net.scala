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

// Represents a connectionless UDP socket, bound to a local address.
abstract class UdpSocket extends Closeable:
  def localAddress: SocketAddress

  /** Sends one datagram containing all of `buf`'s remaining bytes to
    * `target`. Unlike [[Writer.writeBuf]], a single call always sends the
    * whole of `buf`'s remaining bytes as one datagram - UDP has no partial-
    * send concept the way a stream write does.
    */
  def sendTo(buf: Buffer, target: SocketAddress)(using Async): Result[Unit]

  /** Receives one datagram into `buf`, starting at its current position, up
    * to its remaining capacity. If the datagram is larger than `buf` has
    * room for, the excess is discarded (standard UDP behavior - there is no
    * way to read the rest of an oversized datagram later). Returns the
    * sender's address.
    */
  def receiveFrom(buf: Buffer)(using Async): Result[SocketAddress]

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

/** A reusable TLS configuration - Go's `tls.Config`, built once via
  * [[TlsSupport.clientContext]]/[[TlsSupport.serverContext]] and reused
  * across many connections (loading certificates and building the
  * verification chain is comparatively expensive; the per-connection TLS
  * session objects [[TlsSupport.wrapClient]]/[[TlsSupport.wrapServer]]
  * create from it are cheap). Opaque here - a backend-specific `Context`
  * value only that same backend's [[TlsSupport]] knows how to use.
  */
trait TlsContext

/** A backend-agnostic TLS layer over any [[TcpStream]] - wraps the same
  * `Reader`/`Writer` interface, so callers (like `gears.async.http`) don't
  * need to know whether a given stream is plaintext or encrypted. Native's
  * given instance is backed by OpenSSL, doing the actual encrypt/decrypt
  * work as a pure in-memory transform (an OpenSSL "BIO pair") with all
  * real socket I/O still going through the wrapped [[TcpStream]] - so a
  * suspension point is always at the top of this backend's own call stack,
  * never nested inside a foreign C call - see `gears.async.asyncio.tls`.
  */
trait TlsSupport:
  /** A client-side context. `verify = false` skips certificate/hostname
    * verification entirely (Go's `InsecureSkipVerify`) - for testing
    * against a self-signed certificate only, never for real use. `caFile`,
    * if given, is an additional trusted CA (PEM) beyond the system trust
    * store - what's needed to trust a self-signed certificate without
    * disabling verification altogether.
    */
  def clientContext(verify: Boolean = true, caFile: Option[String] = None): TlsContext

  /** A server-side context, loading its certificate chain and private key
    * from PEM files - Go's `tls.LoadX509KeyPair` +
    * `tls.Config{Certificates: ...}`.
    */
  def serverContext(certFile: String, keyFile: String): TlsContext

  /** Performs a TLS client handshake over `stream` (an already-connected
    * plaintext [[TcpStream]]), verifying the peer as `serverName` (both
    * SNI and certificate hostname checking) unless `context` was built
    * with `verify = false`. The returned stream carries plaintext to and
    * from its callers - encryption is entirely internal.
    */
  def wrapClient(stream: TcpStream, context: TlsContext, serverName: String)(using Async): TcpStream

  /** Performs a TLS server handshake over a freshly `accept()`-ed
    * plaintext `stream`.
    */
  def wrapServer(stream: TcpStream, context: TlsContext)(using Async): TcpStream

/** Asynchronous hostname resolution - Go's `net.Resolver`/`LookupHost`.
  * `java.net.InetSocketAddress(host: String, port: Int)`'s own constructor
  * already resolves `host` internally, but does so *synchronously* - which
  * is exactly the trap `gears.async.http.Transport.dial` used to fall
  * into: calling that constructor from inside a fiber body blocks whatever
  * shard/carrier thread the fiber happens to be running on for the entire
  * lookup, stalling every other fiber that shard owns too, not just the
  * one doing the lookup. [[resolve]] exists to do the same underlying
  * lookup (there's no reason to reimplement DNS - a real resolver is
  * already there, this is purely about *how* it gets called) without ever
  * blocking a shard thread.
  */
trait DnsSupport:
  /** Resolves `host` to all of its addresses. A genuine failure (unknown
    * host, no working resolver, ...) is a thrown exception, matching every
    * other setup-time failure in this package ([[TcpSupport.connect]],
    * `FileSupport`'s `openRead`/`openWrite`) - not a `Result` `Left`.
    */
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
  case class keepAlive(keep: Boolean)
      extends JavaSocketOption(std.SO_KEEPALIVE, keep)
  case class reuseAddr(reuse: Boolean)
      extends JavaSocketOption(std.SO_REUSEADDR, reuse)
  case class linger(interval: Int)
      extends JavaSocketOption(std.SO_LINGER, interval)
  case class noDelay(noDelay: Boolean)
      extends JavaSocketOption(std.TCP_NODELAY, noDelay)
