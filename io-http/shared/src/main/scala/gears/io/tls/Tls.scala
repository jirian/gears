package gears.io.tls

import gears.async.Async
import gears.io.net.Stream

import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt

/** How a client's TLS checks its server: by default, against the system's trusted certificates, and the certificate
  * must name the host connected to (`verify`); `trusted`, a PEM file of certificates, adds to them, as for a private
  * certificate authority. Turning `verify` off accepts any server, which only a test against a known peer should do.
  */
final case class TlsClientConfig(verify: Boolean = true, trusted: Option[String] = None)

/** A server's certificate chain and private key, as PEM files: the key unencrypted, PKCS #8 (`BEGIN PRIVATE KEY`). */
final case class TlsServerConfig(certificateChain: String, privateKey: String)

/** A stream through TLS 1.2 or 1.3. Its end is authenticated: `closeNotified` says whether the peer ended with TLS's
  * close notification, without which an end may be an attacker's truncation (see `Wire.Source`).
  */
abstract class TlsStream extends Stream {
  override def secure: Boolean = true

  /** Whether the peer's end came with a close notification. */
  def closeNotified: Boolean
}

/** A server's loaded keys, made once (`Tls.serverContext`) and shared by its connections. */
final class TlsServerContext private[tls] (private[tls] val native: AnyRef)

object Tls {

  /** Runs a client's handshake over `stream` for `host`, within `timeout`, and returns the TLS stream, which owns
    * `stream`: closing it closes both. Fails, closing `stream`, if the server's certificate does not check out.
    */
  def client(stream: Stream, host: String, config: TlsClientConfig = TlsClientConfig(), timeout: Duration = 10.seconds)(
      using Async
  ): TlsStream =
    TlsPlatform.client(stream, host, clientContext(config), timeout)

  /** Runs a server's handshake over `stream`, within `timeout`, and returns the TLS stream, which owns `stream`. */
  def server(stream: Stream, context: TlsServerContext, timeout: Duration = 10.seconds)(using Async): TlsStream =
    TlsPlatform.server(stream, context.native, timeout)

  /** Loads a server's keys; an `IOException` if they do not load, or do not match. */
  def serverContext(config: TlsServerConfig): TlsServerContext = new TlsServerContext(TlsPlatform.serverContext(config))

  private val clients = new java.util.concurrent.ConcurrentHashMap[TlsClientConfig, AnyRef]()

  /** The client context for `config`, made once: loading the system's certificates is slow. */
  private def clientContext(config: TlsClientConfig): AnyRef =
    clients.computeIfAbsent(config, c => TlsPlatform.clientContext(c))
}
