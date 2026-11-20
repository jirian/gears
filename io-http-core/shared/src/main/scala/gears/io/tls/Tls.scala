package gears.io.tls

import gears.async.Async
import gears.io.net.Stream

import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt

final case class TlsClientConfig(verify: Boolean = true, trusted: Option[String] = None)

final case class TlsServerConfig(certificateChain: String, privateKey: String)

abstract class TlsStream extends Stream {
  override def secure: Boolean = true

  def closeNotified: Boolean
}

final class TlsServerContext private[tls] (private[tls] val native: TlsPlatform.ServerContext)

object Tls {

  def client(stream: Stream, host: String, config: TlsClientConfig = TlsClientConfig(), timeout: Duration = 10.seconds)(
      using Async
  ): TlsStream =
    TlsPlatform.client(stream, host, clientContext(config), timeout)

  def server(stream: Stream, context: TlsServerContext, timeout: Duration = 10.seconds)(using Async): TlsStream =
    TlsPlatform.server(stream, context.native, timeout)

  def serverContext(config: TlsServerConfig): TlsServerContext = new TlsServerContext(TlsPlatform.serverContext(config))

  private val clients = new java.util.concurrent.ConcurrentHashMap[TlsClientConfig, TlsPlatform.ClientContext]()

  private def clientContext(config: TlsClientConfig): TlsPlatform.ClientContext =
    clients.computeIfAbsent(config, c => TlsPlatform.clientContext(c))
}
