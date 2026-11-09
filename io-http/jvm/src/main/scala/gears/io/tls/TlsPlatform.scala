package gears.io.tls

import gears.async.Async
import gears.async.Semaphore
import gears.io.net.Stream

import java.io.FileInputStream
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Paths
import java.security.KeyFactory
import java.security.KeyStore
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate
import java.security.spec.PKCS8EncodedKeySpec
import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.SNIHostName
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLEngine
import javax.net.ssl.SSLEngineResult
import javax.net.ssl.SSLException
import javax.net.ssl.TrustManager
import javax.net.ssl.TrustManagerFactory
import javax.net.ssl.X509TrustManager
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

/** TLS on the JVM: the JDK's `SSLEngine`, its records moved over the stream by the code here. */
private[tls] object TlsPlatform {
  final case class ClientContext(context: SSLContext, verify: Boolean)
  type ServerContext = SSLContext

  def clientContext(config: TlsClientConfig): ClientContext = {
    val managers: Array[TrustManager] =
      if !config.verify then Array(TrustAll)
      else
        config.trusted match {
          case None       => null
          case Some(file) => Array(new EitherTrust(systemTrust(), pemTrust(file)))
        }
    val context = SSLContext.getInstance("TLS")
    context.init(null, managers, null)
    ClientContext(context, config.verify)
  }

  def serverContext(config: TlsServerConfig): ServerContext =
    try {
      val chain = certificates(config.certificateChain)
      if chain.isEmpty then throw new IOException(s"No certificate in ${config.certificateChain}")
      val key = privateKey(config.privateKey)
      val store = KeyStore.getInstance("PKCS12")
      store.load(null, null)
      store.setKeyEntry("server", key, Array.emptyCharArray, chain.toArray)
      val keys = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
      keys.init(store, Array.emptyCharArray)
      val context = SSLContext.getInstance("TLS")
      context.init(keys.getKeyManagers, null, null)
      context
    } catch {
      case e: IOException                            => throw e
      case e: java.security.GeneralSecurityException =>
        throw new IOException(s"Failed to load ${config.certificateChain} and ${config.privateKey}: ${e.getMessage}", e)
    }

  def client(stream: Stream, host: String, context: ClientContext, timeout: Duration)(using Async): TlsStream = {
    val ClientContext(ssl, verify) = context
    val engine = ssl.createSSLEngine(host, 443)
    engine.setUseClientMode(true)
    val params = engine.getSSLParameters
    if verify then params.setEndpointIdentificationAlgorithm("HTTPS") // the certificate must name `host`
    if !gears.io.net.Tcp.numeric(host, 0).isDefined then params.setServerNames(java.util.List.of(new SNIHostName(host)))
    engine.setSSLParameters(params)
    open(stream, engine, timeout)
  }

  def server(stream: Stream, context: ServerContext, timeout: Duration)(using Async): TlsStream = {
    val engine = context.createSSLEngine()
    engine.setUseClientMode(false)
    open(stream, engine, timeout)
  }

  private def open(stream: Stream, engine: SSLEngine, timeout: Duration)(using Async): TlsStream = {
    val tls = new EngineStream(stream, engine)
    try {
      tls.handshake(timeout)
      tls
    } catch {
      case t: Throwable =>
        stream.close()
        t match {
          case e: SSLException => throw new IOException(s"TLS handshake failed: ${e.getMessage}", e)
          case other           => throw other
        }
    }
  }

  private def certificates(file: String): Seq[X509Certificate] = {
    val in = new FileInputStream(file)
    try {
      val all = CertificateFactory.getInstance("X.509").generateCertificates(in)
      scala.jdk.CollectionConverters.CollectionHasAsScala(all).asScala.toSeq.map(_.asInstanceOf[X509Certificate])
    } finally in.close()
  }

  private def privateKey(file: String): java.security.PrivateKey = {
    val text = new String(Files.readAllBytes(Paths.get(file)), java.nio.charset.StandardCharsets.US_ASCII)
    val begin = "-----BEGIN PRIVATE KEY-----"
    val at = text.indexOf(begin)
    if at < 0 then throw new IOException(s"No unencrypted PKCS #8 key (BEGIN PRIVATE KEY) in $file")
    val end = text.indexOf("-----END PRIVATE KEY-----", at)
    val der = java.util.Base64.getMimeDecoder.decode(text.substring(at + begin.length, end))
    val spec = new PKCS8EncodedKeySpec(der)
    val keys = Seq("RSA", "EC", "Ed25519", "Ed448").iterator.flatMap { algorithm =>
      try Some(KeyFactory.getInstance(algorithm).generatePrivate(spec))
      catch case _: java.security.GeneralSecurityException => None
    }
    if keys.hasNext then keys.next() else throw new IOException(s"A key of no supported algorithm in $file")
  }

  private def systemTrust(): X509TrustManager = trustOf(null)

  private def pemTrust(file: String): X509TrustManager = {
    val store = KeyStore.getInstance(KeyStore.getDefaultType)
    store.load(null, null)
    for (cert, i) <- certificates(file).zipWithIndex do store.setCertificateEntry(s"trusted-$i", cert)
    trustOf(store)
  }

  private def trustOf(store: KeyStore | Null): X509TrustManager = {
    val factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    factory.init(store)
    factory.getTrustManagers.collectFirst { case t: X509TrustManager => t }.get
  }

  /** Trusts a server either trusts. */
  private final class EitherTrust(first: X509TrustManager, second: X509TrustManager) extends X509TrustManager {
    def checkClientTrusted(chain: Array[X509Certificate], authType: String): Unit =
      first.checkClientTrusted(chain, authType)
    def checkServerTrusted(chain: Array[X509Certificate], authType: String): Unit =
      try first.checkServerTrusted(chain, authType)
      catch case _: java.security.cert.CertificateException => second.checkServerTrusted(chain, authType)
    def getAcceptedIssuers: Array[X509Certificate] = first.getAcceptedIssuers ++ second.getAcceptedIssuers
  }

  private object TrustAll extends X509TrustManager {
    def checkClientTrusted(chain: Array[X509Certificate], authType: String): Unit = ()
    def checkServerTrusted(chain: Array[X509Certificate], authType: String): Unit = ()
    def getAcceptedIssuers: Array[X509Certificate] = Array.empty
  }
}

/** A TLS stream over `underlying`, through `engine`: records read from it are unwrapped into `appIn`, whose bytes reads
  * take, and writes are wrapped into `netOut` and written. One read and one write may run at once, as `SSLEngine`
  * allows: a read that has the engine send something, such as a key update's answer, takes the write side's turn.
  */
private final class EngineStream(underlying: Stream, engine: SSLEngine) extends TlsStream {
  private val session = engine.getSession
  private var netIn = ByteBuffer.allocate(session.getPacketBufferSize) // records read, not yet unwrapped; write mode
  private var appIn = ByteBuffer.allocate(session.getApplicationBufferSize).flip() // unwrapped, not yet read
  private var netOut = ByteBuffer.allocate(session.getPacketBufferSize).flip() // wrapped, not yet written
  private val writing = Semaphore(1)
  @volatile private var closed = false
  @volatile private var notified = false
  private var inputDone = false

  def closeNotified: Boolean = notified

  /** The handshake, all of it within `timeout`: a peer that sends a byte at a time cannot make it last longer. */
  def handshake(timeout: Duration)(using Async): Unit = {
    val deadline = Stream.limit(timeout) match {
      case null              => -1L
      case d: FiniteDuration => System.nanoTime() + d.toNanos
    }
    def left: Duration =
      if deadline < 0 then timeout
      else {
        val n = deadline - System.nanoTime()
        if n <= 0 then throw new java.util.concurrent.TimeoutException("The TLS handshake took too long")
        Duration.fromNanos(n)
      }
    engine.beginHandshake()
    var status = engine.getHandshakeStatus
    while status != SSLEngineResult.HandshakeStatus.FINISHED &&
      status != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING
    do {
      status match {
        case SSLEngineResult.HandshakeStatus.NEED_WRAP =>
          sendWrapped(ByteBuffer.allocate(0), left)
        case SSLEngineResult.HandshakeStatus.NEED_UNWRAP | SSLEngineResult.HandshakeStatus.NEED_UNWRAP_AGAIN =>
          val result = unwrapSome(left)
          if result == null then throw new IOException("The peer closed the connection during the TLS handshake")
        case SSLEngineResult.HandshakeStatus.NEED_TASK => runTasks()
        case _                                         => ()
      }
      status = engine.getHandshakeStatus
    }
  }

  private def runTasks(): Unit = {
    var task = engine.getDelegatedTask
    while task != null do {
      task.run()
      task = engine.getDelegatedTask
    }
  }

  /** Unwraps one record into `appIn`, reading more of `underlying` as needed, each read within `timeout`, taken anew
    * for each, as a handshake's shrinks: null at the end of the stream.
    */
  private def unwrapSome(timeout: => Duration)(using Async): SSLEngineResult | Null = {
    var result: SSLEngineResult | Null = null
    while result == null do {
      netIn.flip()
      appIn.compact()
      val r =
        try engine.unwrap(netIn, appIn)
        finally {
          netIn.compact()
          appIn.flip()
        }
      r.getStatus match {
        case SSLEngineResult.Status.OK     => result = r
        case SSLEngineResult.Status.CLOSED =>
          notified = true
          result = r
        case SSLEngineResult.Status.BUFFER_OVERFLOW =>
          val bigger = ByteBuffer.allocate(appIn.remaining() + session.getApplicationBufferSize)
          bigger.put(appIn).flip()
          appIn = bigger
        case SSLEngineResult.Status.BUFFER_UNDERFLOW =>
          if netIn.remaining() == 0 then {
            val bigger = ByteBuffer.allocate(netIn.capacity() + session.getPacketBufferSize)
            netIn.flip()
            bigger.put(netIn)
            netIn = bigger
          }
          if underlying.read(netIn, timeout) < 0 then {
            inputDone = true
            try engine.closeInbound() // throws without the peer's close notification
            catch case _: SSLException => ()
            return null
          }
      }
      if r.getHandshakeStatus == SSLEngineResult.HandshakeStatus.NEED_TASK then runTasks()
    }
    result
  }

  /** Wraps all of `src`, or a handshake message for an empty one, and writes the records, holding the write side. What
    * a write that timed out or was cancelled had not written stays in `netOut`, to go first next time, so that no
    * record is cut or lost; `src` is advanced as the engine takes its bytes.
    */
  private def sendWrapped(src: ByteBuffer, timeout: Duration)(using Async): Unit =
    Stream.holding(writing)(wrapHeld(Array(src), timeout))

  /** `sendWrapped`'s work, the write side held, for all of `srcs`, which the engine gathers into each record. */
  private def wrapHeld(srcs: Array[ByteBuffer], timeout: Duration)(using Async): Unit = {
    if netOut.hasRemaining then underlying.write(netOut, timeout)
    var more = true
    while more do {
      netOut.clear()
      val r = engine.wrap(srcs, netOut)
      r.getStatus match {
        case SSLEngineResult.Status.BUFFER_OVERFLOW =>
          netOut = ByteBuffer.allocate(netOut.capacity() + session.getPacketBufferSize)
        case SSLEngineResult.Status.CLOSED | SSLEngineResult.Status.OK =>
          netOut.flip()
          if netOut.hasRemaining then underlying.write(netOut, timeout)
          if r.getHandshakeStatus == SSLEngineResult.HandshakeStatus.NEED_TASK then runTasks()
          more = EngineStream.remains(srcs) && r.getStatus == SSLEngineResult.Status.OK ||
            r.getHandshakeStatus == SSLEngineResult.HandshakeStatus.NEED_WRAP
        case SSLEngineResult.Status.BUFFER_UNDERFLOW =>
          netOut.flip()
          more = false
      }
    }
  }

  def read(buf: ByteBuffer, timeout: Duration)(using Async): Int = {
    require(buf.hasRemaining, "A read needs a buffer with room")
    if closed then throw new java.nio.channels.ClosedChannelException()
    var n = 0
    while n == 0 do
      if appIn.hasRemaining then {
        n = math.min(appIn.remaining(), buf.remaining())
        val slice = appIn.duplicate()
        slice.limit(slice.position() + n)
        buf.put(slice)
        appIn.position(appIn.position() + n)
      } else if inputDone || notified then n = -1
      else {
        val r = unwrapSome(timeout)
        if r == null then n = -1
        else if r.getHandshakeStatus == SSLEngineResult.HandshakeStatus.NEED_WRAP then
          // A post-handshake message's answer, unless a write holds the write side: it sends it with its own records.
          writing.poll() match {
            case Some(guard) =>
              try wrapHeld(Array(ByteBuffer.allocate(0)), timeout)
              finally guard.release()
            case None => ()
          }
      }
    n
  }

  def write(buf: ByteBuffer, timeout: Duration)(using Async): Unit = {
    if closed then throw new java.nio.channels.ClosedChannelException()
    if buf.hasRemaining then sendWrapped(buf, timeout)
  }

  /** Small buffers, such as a response's head and body, go out together, as one record and one write of `underlying`.
    */
  override def write(bufs: Array[ByteBuffer], timeout: Duration)(using Async): Unit = {
    if closed then throw new java.nio.channels.ClosedChannelException()
    if EngineStream.remains(bufs) then Stream.holding(writing)(wrapHeld(bufs, timeout))
  }

  def shutdownOutput()(using Async): Unit = {
    engine.closeOutbound()
    sendWrapped(ByteBuffer.allocate(0), 5.seconds) // the close notification
    underlying.shutdownOutput()
  }

  def close()(using Async): Unit =
    if !closed then {
      closed = true
      Stream.uncancelled {
        // Best effort, the close notification; not behind a write under way, which may be stuck on a peer that does
        // not read: closing the stream fails it.
        try
          if !engine.isOutboundDone then
            writing.poll() match {
              case Some(guard) =>
                try {
                  engine.closeOutbound()
                  wrapHeld(Array(ByteBuffer.allocate(0)), 1.second)
                } finally guard.release()
              case None => ()
            }
        catch case _: Exception => ()
        finally underlying.close()
      }
    }

  def localAddress = underlying.localAddress
  def peerAddress = underlying.peerAddress
}

private object EngineStream {

  /** Whether any of `bufs` has bytes left. */
  def remains(bufs: Array[ByteBuffer]): Boolean = {
    var i = 0
    while i < bufs.length && !bufs(i).hasRemaining do i += 1
    i < bufs.length
  }
}
