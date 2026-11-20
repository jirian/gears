package gears.io.tls

import gears.async.Async
import gears.async.Semaphore
import gears.io.net.Stream

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.ClosedChannelException
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import scala.scalanative.runtime.ByteArray
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

import openssl.*

private[tls] object TlsPlatform {

  final class Context(val ctx: Ptr[SSL_CTX], val verify: Boolean)
  type ClientContext = Context
  type ServerContext = Context

  private def atLeastTls12(ctx: Ptr[SSL_CTX]): Unit =
    if SSL_CTX_ctrl(ctx, SSL_CTRL_SET_MIN_PROTO_VERSION, TLS1_2_VERSION.toSize, null) != 1 then
      fail(ctx, "Failed to require TLS 1.2 or later")

  def clientContext(config: TlsClientConfig): ClientContext = {
    val ctx = SSL_CTX_new(TLS_client_method())
    if ctx == null then throw new IOException(s"SSL_CTX_new failed: ${lastError()}")
    atLeastTls12(ctx)
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

  def serverContext(config: TlsServerConfig): ServerContext = {
    val ctx = SSL_CTX_new(TLS_server_method())
    if ctx == null then throw new IOException(s"SSL_CTX_new failed: ${lastError()}")
    atLeastTls12(ctx)
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

  def client(stream: Stream, host: String, context: ClientContext, timeout: Duration)(using Async): TlsStream =
    open(stream, context, host, timeout)

  def server(stream: Stream, context: ServerContext, timeout: Duration)(using Async): TlsStream =
    open(stream, context, null, timeout)

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

private[tls] final class OpenSslStream private (underlying: Stream, ssl: Ptr[SSL], networkBio: Ptr[BIO]) extends TlsStream {
  private object nativeLock
  private var freed = false
  @volatile private var notified = false

  private val outbound = Semaphore(1)
  private val inbound = Semaphore(1)
  private val netOut = new Array[Byte](OpenSslStream.NetBufSize)
  private val outPending = ByteBuffer.wrap(netOut).limit(0)
  private val netIn = new Array[Byte](OpenSslStream.NetBufSize)
  private val netInBuffer = ByteBuffer.wrap(netIn)
  private lazy val gathered = new Array[Byte](OpenSslStream.RecordSize)
  @volatile private[tls] var fills = 0L
  private var failureDetail = ""

  def closeNotified: Boolean = notified

  private inline def native[A](inline body: A): A = nativeLock.synchronized {
    if freed then throw new ClosedChannelException()
    body
  }

  private def call(op: () => Int): (Int, Int) = native {
    ERR_clear_error()
    val r = op()
    val e = if r <= 0 then SSL_get_error(ssl, r) else SSL_ERROR_NONE
    if e == SSL_ERROR_SSL || e == SSL_ERROR_SYSCALL then failureDetail = TlsPlatform.lastError()
    (r, e)
  }

  private def flushNetworkOut(timeout: Duration)(using Async): Unit = Stream.holding(outbound)(flushHeld(timeout))

  private def flushIfFree(timeout: Duration)(using Async): Unit =
    if outPending.hasRemaining || native(BIO_ctrl(networkBio, BIO_CTRL_PENDING, 0.toSize, null).toInt) > 0 then
      outbound.poll() match {
        case Some(guard) =>
          try flushHeld(timeout)
          finally guard.release()
        case None => ()
      }

  private def flushHeld(timeout: Duration)(using Async): Unit = {
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

  // - input another caller added since `seen` may be the record this caller's OpenSSL call wants: retry first
  private[tls] def fillNetworkIn(timeout: Duration, seen: Long)(using Async): Boolean =
    Stream.holding(inbound) {
      if fills != seen then true
      else {
        val n = underlying.read(netInBuffer.clear(), timeout)
        if n > 0 then {
          val written = native(BIO_write(networkBio, netIn.asInstanceOf[ByteArray].at(0), n))
          if written != n then throw new IOException(s"The TLS input overflowed its buffer ($written of $n bytes)")
          fills += 1
        }
        n > 0
      }
    }

  private def pump(op: () => Int, timeout: Duration, reading: Boolean, deadline: Long = -1)(using
      Async
  ): (Int, Int) = {
    def limit: Duration =
      if deadline < 0 then timeout
      else {
        val left = deadline - System.nanoTime()
        if left <= 0 then throw new java.util.concurrent.TimeoutException(s"Not done within $timeout")
        Duration.fromNanos(left)
      }
    def flush(): Unit = if reading then flushIfFree(limit) else flushNetworkOut(limit)
    var seen = fills
    var (result, error) = call(op)
    flush()
    while result <= 0 && (error == SSL_ERROR_WANT_READ || error == SSL_ERROR_WANT_WRITE) do {
      if error == SSL_ERROR_WANT_READ && !fillNetworkIn(limit, seen) then {
        error = OpenSslStream.PeerClosed
        return (result, error)
      }
      seen = fills
      val (r, e) = call(op)
      result = r
      error = e
      flush()
    }
    (result, error)
  }

  def handshake(timeout: Duration)(using Async): Unit = {
    val deadline = Stream.limit(timeout) match {
      case null              => -1L
      case d: FiniteDuration => System.nanoTime() + d.toNanos
    }
    val (r, err) = pump(() => SSL_do_handshake(ssl), timeout, reading = false, deadline)
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
          timeout,
          reading = true
        )
      catch {
        case _: java.util.concurrent.TimeoutException if got > 0 => (got, SSL_ERROR_NONE)
      }
    if n > 0 then n
    else
      err match {
        case SSL_ERROR_ZERO_RETURN =>
          notified = true
          -1
        case OpenSslStream.PeerClosed    => -1
        case SSL_ERROR_SYSCALL if n == 0 => -1
        case _ => throw new IOException(s"TLS read failed (SSL_get_error=$err): $failureDetail")
      }
  }

  private def deadlineOf(timeout: Duration): Long = Stream.limit(timeout) match {
    case null => -1L
    case d    => System.nanoTime() + d.nn.toNanos
  }

  def write(buf: ByteBuffer, timeout: Duration)(using Async): Unit = writeOne(buf, timeout, deadlineOf(timeout))

  private def writeOne(buf: ByteBuffer, timeout: Duration, deadline: Long)(using Async): Unit = {
    val n = buf.remaining()
    if n > 0 then {
      val direct = buf.hasArray
      val arr = if direct then buf.array() else new Array[Byte](n)
      val start = if direct then buf.arrayOffset() + buf.position() else 0
      if !direct then buf.duplicate().get(arr)
      writeArray(arr, start, n, timeout, deadline)(r => buf.position(buf.position() + r): Unit)
    }
  }

  override def write(bufs: Array[ByteBuffer], timeout: Duration)(using Async): Unit = {
    val deadline = deadlineOf(timeout)
    var i = 0
    while i < bufs.length do {
      if !bufs(i).hasRemaining then i += 1
      else if bufs(i).remaining() >= OpenSslStream.RecordSize then {
        writeOne(bufs(i), timeout, deadline)
        i += 1
      } else {
        var n = 0
        var j = i
        while j < bufs.length && n < gathered.length do {
          val b = bufs(j)
          val k = math.min(b.remaining(), gathered.length - n)
          if b.hasArray then System.arraycopy(b.array(), b.arrayOffset() + b.position(), gathered, n, k)
          else b.duplicate().get(gathered, n, k)
          n += k
          j += 1
        }
        writeArray(gathered, 0, n, timeout, deadline) { r =>
          var left = r
          while left > 0 do {
            val b = bufs(i)
            val k = math.min(left, b.remaining())
            b.position(b.position() + k)
            left -= k
            if !b.hasRemaining then i += 1
          }
        }
      }
    }
  }

  private def writeArray(arr: Array[Byte], start: Int, n: Int, timeout: Duration, deadline: Long)(
      taken: Int => Unit
  )(using Async): Unit = {
    var written = 0
    while written < n do {
      val (r, err) = pump(
        () => {
          val r = SSL_write(ssl, arr.asInstanceOf[ByteArray].at(start + written), n - written)
          if r > 0 then {
            written += r
            taken(r)
          }
          r
        },
        timeout,
        reading = false,
        deadline
      )
      if r <= 0 then
        if err == OpenSslStream.PeerClosed then throw new IOException("The peer closed the connection")
        else throw new IOException(s"TLS write failed (SSL_get_error=$err): $failureDetail")
    }
  }

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
        outbound.poll() match {
          case Some(guard) =>
            try {
              val (result, error) = call(() => SSL_shutdown(ssl))
              if result >= 0 || error == SSL_ERROR_WANT_READ || error == SSL_ERROR_WANT_WRITE then flushHeld(1.second)
            } catch case _: Exception => ()
            finally guard.release()
          case None => ()
        }
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

private[tls] object OpenSslStream {
  inline val PeerClosed = -1
  inline val NetBufSize = 17 * 1024
  inline val RecordSize = 16 * 1024

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
