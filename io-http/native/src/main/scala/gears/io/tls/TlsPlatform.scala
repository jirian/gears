package gears.io.tls

import gears.async.Async
import gears.async.Semaphore
import gears.io.net.Stream

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.ClosedChannelException
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt
import scala.scalanative.runtime.ByteArray
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import openssl.*

/** TLS on Scala Native: OpenSSL, its records moved over the stream through a BIO pair by the code here. */
private[tls] object TlsPlatform {

  /** An `SSL_CTX`, freed by nobody: contexts live as long as the process, one per configuration (`Tls`). */
  private final class Context(val ctx: Ptr[SSL_CTX], val verify: Boolean)

  def clientContext(config: TlsClientConfig): AnyRef = {
    val ctx = SSL_CTX_new(TLS_client_method())
    if ctx == null then throw new IOException(s"SSL_CTX_new failed: ${lastError()}")
    if config.verify then {
      SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, null)
      if SSL_CTX_set_default_verify_paths(ctx) != 1 then fail(ctx, "Failed to load the system's certificates")
      config.trusted.foreach { file =>
        Zone.acquire { implicit z =>
          if SSL_CTX_load_verify_locations(ctx, toCString(file), null) != 1 then fail(ctx, s"Failed to load $file")
        }
      }
    } else SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, null)
    new Context(ctx, config.verify)
  }

  def serverContext(config: TlsServerConfig): AnyRef = {
    val ctx = SSL_CTX_new(TLS_server_method())
    if ctx == null then throw new IOException(s"SSL_CTX_new failed: ${lastError()}")
    Zone.acquire { implicit z =>
      if SSL_CTX_use_certificate_chain_file(ctx, toCString(config.certificateChain)) != 1 then
        fail(ctx, s"Failed to load the certificate chain ${config.certificateChain}")
      if SSL_CTX_use_PrivateKey_file(ctx, toCString(config.privateKey), SSL_FILETYPE_PEM) != 1 then
        fail(ctx, s"Failed to load the private key ${config.privateKey}")
      if SSL_CTX_check_private_key(ctx) != 1 then fail(ctx, "The private key does not match the certificate")
    }
    new Context(ctx, false)
  }

  private def fail(ctx: Ptr[SSL_CTX], what: String): Nothing = {
    val detail = lastError()
    SSL_CTX_free(ctx)
    throw new IOException(s"$what: $detail")
  }

  def client(stream: Stream, host: String, context: AnyRef, timeout: Duration)(using Async): TlsStream =
    open(stream, context.asInstanceOf[Context], host, timeout)

  def server(stream: Stream, context: AnyRef, timeout: Duration)(using Async): TlsStream =
    open(stream, context.asInstanceOf[Context], null, timeout)

  private def open(stream: Stream, context: Context, host: String | Null, timeout: Duration)(using Async): TlsStream = {
    val tls =
      try OpenSslStream.open(stream, context.ctx, host, context.verify)
      catch {
        case t: Throwable =>
          stream.close()
          throw t
      }
    try {
      tls.handshake(timeout)
      tls
    } catch {
      case t: Throwable =>
        tls.close()
        throw t
    }
  }

  /** The text of the calling thread's oldest OpenSSL error. */
  private[tls] def lastError(): String = {
    val code = ERR_get_error()
    if code == 0.toULong then "no OpenSSL error recorded"
    else {
      val buf = stackalloc[CChar](256)
      ERR_error_string_n(code, buf, 256.toUSize)
      fromCString(buf)
    }
  }
}

/** A TLS stream over `underlying`, through `ssl`, whose records go through a BIO pair: `networkBio` is the network
  * side, which takes what `underlying` reads and gives what is to be written. Every OpenSSL call is made under
  * `nativeLock`, and never suspends, so one read and one write may run at once from different futures, as OpenSSL
  * allows an `SSL` only one call at a time; `inbound` and `outbound` keep one read and one write of `underlying` each.
  */
private final class OpenSslStream private (underlying: Stream, ssl: Ptr[SSL], networkBio: Ptr[BIO]) extends TlsStream {
  private object nativeLock
  private var freed = false
  @volatile private var notified = false

  private val outbound = Semaphore(1)
  private val inbound = Semaphore(1)
  private val netOut = new Array[Byte](OpenSslStream.NetBufSize)
  private val outPending = ByteBuffer.wrap(netOut).limit(0) // taken from the BIO, not yet written
  private val netIn = new Array[Byte](OpenSslStream.NetBufSize)
  private var failureDetail = ""

  def closeNotified: Boolean = notified

  private inline def native[A](inline body: A): A = nativeLock.synchronized {
    if freed then throw new ClosedChannelException()
    body
  }

  /** Calls OpenSSL, returning its result and `SSL_get_error`'s: the error queue is the thread's, and a future moves
    * between threads, so it is cleared first, and a failure's text read before the lock is let go.
    */
  private def call(op: () => Int): (Int, Int) = native {
    ERR_clear_error()
    val r = op()
    val e = if r <= 0 then SSL_get_error(ssl, r) else SSL_ERROR_NONE
    if e == SSL_ERROR_SSL || e == SSL_ERROR_SYSCALL then failureDetail = TlsPlatform.lastError()
    (r, e)
  }

  /** Writes what OpenSSL has for the network. What a write that timed out or was cancelled had not written stays in
    * `outPending`, to go first next time, so that no record is cut or lost.
    */
  private def flushNetworkOut(timeout: Duration)(using Async): Unit =
    Stream.holding(outbound) {
      var done = false
      while !done do {
        if outPending.hasRemaining then underlying.write(outPending, timeout)
        else {
          val pending = native(BIO_ctrl(networkBio, BIO_CTRL_PENDING, 0.toSize, null).toInt)
          val n =
            if pending <= 0 then 0
            else native(BIO_read(networkBio, netOut.asInstanceOf[ByteArray].at(0), math.min(pending, netOut.length)))
          if n > 0 then outPending.position(0).limit(n): Unit else done = true
        }
      }
    }

  /** Reads from the network into OpenSSL: false at the end of the stream. */
  private def fillNetworkIn(timeout: Duration)(using Async): Boolean =
    Stream.holding(inbound) {
      val in = ByteBuffer.wrap(netIn)
      val n = underlying.read(in, timeout)
      if n > 0 then {
        val written = native(BIO_write(networkBio, netIn.asInstanceOf[ByteArray].at(0), n))
        if written != n then throw new IOException(s"The TLS input overflowed its buffer ($written of $n bytes)")
      }
      n > 0
    }

  /** Runs `op` until it is done or fails, moving records as OpenSSL asks. */
  private def pump(op: () => Int, timeout: Duration)(using Async): (Int, Int) = {
    var (result, error) = call(op)
    flushNetworkOut(timeout)
    while result <= 0 && (error == SSL_ERROR_WANT_READ || error == SSL_ERROR_WANT_WRITE) do {
      if error == SSL_ERROR_WANT_READ && !fillNetworkIn(timeout) then {
        error = OpenSslStream.PeerClosed
        return (result, error)
      }
      val (r, e) = call(op)
      result = r
      error = e
      flushNetworkOut(timeout)
    }
    (result, error)
  }

  def handshake(timeout: Duration)(using Async): Unit = {
    val (r, err) = pump(() => SSL_do_handshake(ssl), timeout)
    if r != 1 then
      if err == OpenSslStream.PeerClosed then
        throw new IOException("The peer closed the connection during the TLS handshake")
      else throw new IOException(s"TLS handshake failed (SSL_get_error=$err): $failureDetail")
  }

  def read(buf: ByteBuffer, timeout: Duration)(using Async): Int = {
    require(buf.hasRemaining, "A read needs a buffer with room")
    val cap = buf.remaining()
    val direct = buf.hasArray && !buf.isReadOnly
    val tmp = if direct then buf.array() else new Array[Byte](cap)
    val at = if direct then buf.arrayOffset() + buf.position() else 0
    var got = 0
    val (n, err) =
      try
        pump(
          () => {
            val r = SSL_read(ssl, tmp.asInstanceOf[ByteArray].at(at), cap)
            if r > 0 then {
              got = r
              if direct then buf.position(buf.position() + r) else buf.put(tmp, 0, r)
            }
            r
          },
          timeout
        )
      catch {
        // The bytes are read; what OpenSSL had to send with them, such as a key update's answer, goes next time.
        case _: java.util.concurrent.TimeoutException if got > 0 => (got, SSL_ERROR_NONE)
      }
    if n > 0 then n
    else
      err match {
        case SSL_ERROR_ZERO_RETURN =>
          notified = true
          -1
        case OpenSslStream.PeerClosed    => -1 // without a close notification: `closeNotified` stays false
        case SSL_ERROR_SYSCALL if n == 0 => -1
        case _ => throw new IOException(s"TLS read failed (SSL_get_error=$err): $failureDetail")
      }
  }

  def write(buf: ByteBuffer, timeout: Duration)(using Async): Unit = {
    val n = buf.remaining()
    if n > 0 then {
      val direct = buf.hasArray
      val arr = if direct then buf.array() else new Array[Byte](n)
      val start = if direct then buf.arrayOffset() + buf.position() else 0
      if !direct then buf.duplicate().get(arr)
      var written = 0
      while written < n do {
        // `buf` is advanced as OpenSSL takes its bytes, before they are flushed: a write that times out or is cancelled
        // has taken what its position says, and the records go out first on the next write.
        val (r, err) = pump(
          () => {
            val r = SSL_write(ssl, arr.asInstanceOf[ByteArray].at(start + written), n - written)
            if r > 0 then {
              written += r
              buf.position(buf.position() + r)
            }
            r
          },
          timeout
        )
        if r <= 0 then
          if err == OpenSslStream.PeerClosed then throw new IOException("The peer closed the connection")
          else throw new IOException(s"TLS write failed (SSL_get_error=$err): $failureDetail")
      }
    }
  }

  /** Sends the close notification. */
  private def notifyClose(timeout: Duration)(using Async): Unit = {
    val (result, error) = call(() => SSL_shutdown(ssl))
    if result < 0 && error != SSL_ERROR_WANT_READ && error != SSL_ERROR_WANT_WRITE then
      throw new IOException(s"TLS shutdown failed (SSL_get_error=$error): $failureDetail")
    flushNetworkOut(timeout)
  }

  def shutdownOutput()(using Async): Unit = {
    notifyClose(5.seconds)
    underlying.shutdownOutput()
  }

  def close()(using Async): Unit = {
    val first = nativeLock.synchronized(!freed)
    if first then
      Stream.uncancelled {
        try notifyClose(1.second) // best effort, so that the peer can tell the end from a truncation
        catch case _: Exception => ()
        val free = nativeLock.synchronized {
          if freed then false
          else {
            freed = true
            SSL_free(ssl)
            BIO_free(networkBio)
            true
          }
        }
        if free then underlying.close()
      }
  }

  def localAddress = underlying.localAddress
  def peerAddress = underlying.peerAddress
}

private object OpenSslStream {
  inline val PeerClosed = -1
  inline val NetBufSize = 17 * 1024

  def open(underlying: Stream, ctx: Ptr[SSL_CTX], host: String | Null, verify: Boolean): OpenSslStream = {
    val ssl = SSL_new(ctx)
    if ssl == null then throw new IOException(s"SSL_new failed: ${TlsPlatform.lastError()}")
    val internal = stackalloc[Ptr[BIO]]()
    val network = stackalloc[Ptr[BIO]]()
    if BIO_new_bio_pair(internal, NetBufSize.toUSize, network, NetBufSize.toUSize) != 1 then {
      SSL_free(ssl)
      throw new IOException(s"BIO_new_bio_pair failed: ${TlsPlatform.lastError()}")
    }
    SSL_set_bio(ssl, !internal, !internal)
    // A write that timed out is taken up again with what is left of its buffer, maybe in another array.
    SSL_ctrl(
      ssl,
      SSL_CTRL_MODE,
      (SSL_MODE_ENABLE_PARTIAL_WRITE | SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER).toSize,
      null
    ): Unit
    if host == null then SSL_set_accept_state(ssl)
    else {
      SSL_set_connect_state(ssl)
      val name = host.nn.stripPrefix("[").stripSuffix("]")
      val targetSet = Zone.acquire { implicit z =>
        val c = toCString(name)
        if gears.io.net.Tcp.numeric(name, 0).isDefined then
          !verify || X509_VERIFY_PARAM_set1_ip_asc(SSL_get0_param(ssl), c) == 1
        else {
          SSL_ctrl(ssl, SSL_CTRL_SET_TLSEXT_HOSTNAME, TLSEXT_NAMETYPE_host_name.toSize, c.asInstanceOf[Ptr[Byte]]): Unit
          !verify || SSL_set1_host(ssl, c) == 1
        }
      }
      // Without a target, a verifying context would accept any trusted certificate.
      if !targetSet then {
        val error = new IOException(s"Failed to set the TLS verification target $host: ${TlsPlatform.lastError()}")
        SSL_free(ssl)
        BIO_free(!network)
        throw error
      }
    }
    new OpenSslStream(underlying, ssl, !network)
  }
}
