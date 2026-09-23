package gears.async.asyncio.tls

import openssl._

import gears.async.Async
import gears.async.asyncio.{Buffer, Error, Result}
import gears.async.net.{TcpStream, TlsContext, TlsSupport}

import java.io.IOException
import java.net.SocketAddress
import java.nio.ByteBuffer
import java.nio.channels.ClosedChannelException
import scala.scalanative.runtime.ByteArray
import scala.scalanative.unsafe._
import scala.scalanative.unsigned._

private final class OpenSslContext(private[tls] val ctx: Ptr[SSL_CTX]) extends TlsContext

private[tls] def lastOpenSslError(): String =
  val code = ERR_get_error()
  if code == 0.toULong then "no OpenSSL error recorded"
  else
    val buf = stackalloc[CChar](256)
    ERR_error_string_n(code, buf, 256.toUSize)
    fromCString(buf)

/** OpenSSL-backed [[TlsSupport]]. Sits over any backend's [[TcpStream]]
  * (uring, epoll, ...) purely through the `Reader`/`Writer` interface, so
  * it's written once here rather than per-backend.
  */
object NativeTlsSupport extends TlsSupport:
  private def freeAndFail(ctx: Ptr[SSL_CTX], what: String): Nothing =
    val msg = lastOpenSslError()
    SSL_CTX_free(ctx)
    throw new IOException(s"$what: $msg")

  def clientContext(verify: Boolean = true, caFile: Option[String] = None): TlsContext =
    val ctx = SSL_CTX_new(TLS_client_method())
    if ctx == null then throw new IOException(s"SSL_CTX_new failed: ${lastOpenSslError()}")
    if verify then
      SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, null)
      if SSL_CTX_set_default_verify_paths(ctx) != 1 then freeAndFail(ctx, "SSL_CTX_set_default_verify_paths failed")
      caFile.foreach { f =>
        Zone.acquire: zone =>
          if SSL_CTX_load_verify_locations(ctx, toCString(f)(using zone), null) != 1 then
            freeAndFail(ctx, s"failed to load CA file $f")
      }
    else SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, null)
    OpenSslContext(ctx)

  def serverContext(certFile: String, keyFile: String): TlsContext =
    val ctx = SSL_CTX_new(TLS_server_method())
    if ctx == null then throw new IOException(s"SSL_CTX_new failed: ${lastOpenSslError()}")
    Zone.acquire: zone =>
      if SSL_CTX_use_certificate_chain_file(ctx, toCString(certFile)(using zone)) != 1 then
        freeAndFail(ctx, s"failed to load certificate $certFile")
      if SSL_CTX_use_PrivateKey_file(ctx, toCString(keyFile)(using zone), SSL_FILETYPE_PEM) != 1 then
        freeAndFail(ctx, s"failed to load private key $keyFile")
      if SSL_CTX_check_private_key(ctx) != 1 then freeAndFail(ctx, "private key does not match certificate")
    OpenSslContext(ctx)

  def wrapClient(stream: TcpStream, context: TlsContext, serverName: String)(using Async): TcpStream =
    val tls = TlsStream.open(stream, context.asInstanceOf[OpenSslContext].ctx, isServer = false, serverName)
    tls.handshake()
    tls

  def wrapServer(stream: TcpStream, context: TlsContext)(using Async): TcpStream =
    val tls = TlsStream.open(stream, context.asInstanceOf[OpenSslContext].ctx, isServer = true, "")
    tls.handshake()
    tls

/** A [[TcpStream]] wrapping another, plaintext one with TLS, encrypting/
  * decrypting entirely as an in-memory transform via an OpenSSL "BIO pair"
  * (`BIO_new_bio_pair`) - `ssl` reads and writes ciphertext through
  * `internalBio`; `networkBio` is the other end of that same pair, which
  * *this* class reads from / writes to using the wrapped [[TcpStream]]'s
  * own `readBuf`/`writeBuf`.
  *
  * This design exists specifically so that no suspension point is ever
  * nested inside an OpenSSL call: `SSL_read`/`SSL_write`/`SSL_do_handshake`
  * only ever touch in-memory buffers, so they're pure, synchronous, CPU-
  * only C calls from this backend's point of view - every real `await`
  * (the underlying stream's own `readBuf`/`writeBuf`) happens at the top of
  * [[pump]]'s Scala call stack, never inside a foreign C frame. That
  * matters here specifically because of a real, previously-found bug in
  * this backend's own scheduler where two fiber dispatches sharing one
  * native call frame corrupted delimcc's thread-local continuation state
  * (see `UringShard.drainTasks`'s own doc) - letting a suspension happen
  * *inside* `SSL_read` would risk exactly that shape of problem again, one
  * layer further from view. Having OpenSSL do the actual socket I/O
  * itself (the alternative, simpler-looking design) would require exactly
  * that.
  */
final class TlsStream private (
    underlying: TcpStream,
    ssl: Ptr[SSL],
    internalBio: Ptr[BIO],
    networkBio: Ptr[BIO]
) extends TcpStream:
  override def localAddress: SocketAddress = underlying.localAddress
  override def remoteAddress: SocketAddress = underlying.remoteAddress

  private var closed = false
  private def checkOpen(): Unit = if closed then throw new ClosedChannelException()

  private val netOutBuf = new Array[Byte](TlsStream.NetBufSize)
  private val netInBuf = ByteBuffer.allocate(TlsStream.NetBufSize)

  /** Drains whatever ciphertext `ssl` has queued up on `networkBio` (from
    * an `op()` call below) out to the real connection.
    */
  private def flushNetworkOut()(using Async): Unit =
    var pending = BIO_ctrl(networkBio, BIO_CTRL_PENDING, 0.toSize, null).toInt
    while pending > 0 do
      val toRead = math.min(pending, netOutBuf.length)
      val n = BIO_read(networkBio, netOutBuf.asInstanceOf[ByteArray].at(0), toRead)
      if n > 0 then
        underlying.writeBuf(ByteBuffer.wrap(netOutBuf, 0, n)) match
          case Right(())        => ()
          case Left(Error.EOF) => throw new IOException("peer closed the connection while flushing TLS output")
      pending = BIO_ctrl(networkBio, BIO_CTRL_PENDING, 0.toSize, null).toInt

  /** Reads one chunk of ciphertext off the real connection and feeds it to
    * `ssl` via `networkBio`, so a retried `op()` can make progress.
    * `false` means the peer closed the connection (a real EOF, not just
    * "no bytes available right now" - `readBuf` itself suspends until
    * there's something to report).
    */
  private def fillNetworkIn()(using Async): Boolean =
    netInBuf.clear()
    underlying.readBuf(netInBuf) match
      case Right(()) =>
        netInBuf.flip()
        val n = netInBuf.remaining()
        val arr = new Array[Byte](n)
        netInBuf.get(arr)
        val written = BIO_write(networkBio, arr.asInstanceOf[ByteArray].at(0), n)
        if written != n then throw new IOException(s"BIO pair buffer overflowed feeding TLS input ($written/$n bytes written)")
        true
      case Left(Error.EOF) => false

  /** Runs one SSL_* call, pumping ciphertext to/from the real connection
    * and retrying as long as OpenSSL reports it needs more input or has
    * more output to flush, until `op` produces a real (non-retry) result.
    */
  private def pump(op: () => Int)(using Async): Int =
    var result = op()
    flushNetworkOut()
    var retry = result <= 0
    while retry do
      retry = SSL_get_error(ssl, result) match
        case SSL_ERROR_WANT_READ  => fillNetworkIn()
        case SSL_ERROR_WANT_WRITE => true // flushNetworkOut() above already drained the pair buffer
        case _                     => false
      if retry then
        result = op()
        flushNetworkOut()
        retry = result <= 0
    result

  private[tls] def handshake()(using Async): Unit =
    val r = pump(() => SSL_do_handshake(ssl))
    if r != 1 then
      val err = SSL_get_error(ssl, r)
      close()
      throw new IOException(s"TLS handshake failed (SSL_get_error=$err): ${lastOpenSslError()}")

  override def readBuf(buf: Buffer)(using Async): Result[Unit] =
    checkOpen()
    val cap = buf.remaining()
    if cap == 0 then Right(())
    else
      val tmp = new Array[Byte](cap)
      val n = pump(() => SSL_read(ssl, tmp.asInstanceOf[ByteArray].at(0), cap))
      if n > 0 then
        buf.put(tmp, 0, n)
        Right(())
      else
        SSL_get_error(ssl, n) match
          case SSL_ERROR_ZERO_RETURN               => Left(Error.EOF)
          case SSL_ERROR_SYSCALL if n == 0          => Left(Error.EOF) // peer closed without a clean TLS shutdown
          case err                                   => throw new IOException(s"TLS read failed (SSL_get_error=$err): ${lastOpenSslError()}")

  override def writeBuf(buf: Buffer)(using Async): Result[Unit] =
    checkOpen()
    val n = buf.remaining()
    if n == 0 then Right(())
    else
      val arr = new Array[Byte](n)
      buf.get(arr)
      var written = 0
      while written < n do
        val r = pump(() => SSL_write(ssl, arr.asInstanceOf[ByteArray].at(written), n - written))
        if r <= 0 then
          val err = SSL_get_error(ssl, r)
          throw new IOException(s"TLS write failed (SSL_get_error=$err): ${lastOpenSslError()}")
        written += r
      Right(())

  override def close(): Unit =
    if !closed then
      closed = true
      val _ = SSL_shutdown(ssl) // best-effort close_notify; ignore truncation, we're closing regardless
      SSL_free(ssl) // also frees internalBio - SSL_set_bio gave it ownership
      BIO_free(networkBio)
      underlying.close()

private[tls] object TlsStream:
  private[tls] val NetBufSize = 17 * 1024 // matches OpenSSL's own BIO_new_bio_pair default (17*1024)

  private[tls] def open(underlying: TcpStream, ctx: Ptr[SSL_CTX], isServer: Boolean, serverName: String)(using
      Async
  ): TlsStream =
    val ssl = SSL_new(ctx)
    if ssl == null then throw new IOException(s"SSL_new failed: ${lastOpenSslError()}")
    val bio1Slot = stackalloc[Ptr[BIO]]()
    val bio2Slot = stackalloc[Ptr[BIO]]()
    if BIO_new_bio_pair(bio1Slot, NetBufSize.toUSize, bio2Slot, NetBufSize.toUSize) != 1 then
      SSL_free(ssl)
      throw new IOException(s"BIO_new_bio_pair failed: ${lastOpenSslError()}")
    val internalBio = !bio1Slot
    val networkBio = !bio2Slot
    SSL_set_bio(ssl, internalBio, internalBio)
    if isServer then SSL_set_accept_state(ssl)
    else
      SSL_set_connect_state(ssl)
      Zone.acquire: zone =>
        val name = toCString(serverName)(using zone)
        // SNI - see openssl.scala's doc on why this is SSL_ctrl directly.
        val _ = SSL_ctrl(ssl, SSL_CTRL_SET_TLSEXT_HOSTNAME, TLSEXT_NAMETYPE_host_name.toSize, name.asInstanceOf[Ptr[Byte]])
        val _ = SSL_set1_host(ssl, name) // target for certificate hostname verification
    new TlsStream(underlying, ssl, internalBio, networkBio)
