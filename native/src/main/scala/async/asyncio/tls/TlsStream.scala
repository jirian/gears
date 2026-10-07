package gears.async.asyncio.tls

import gears.async.asyncio.{Buffer, Error, Result}
import gears.async.net.{TcpStream, TlsContext, TlsSupport}
import gears.async.{Async, Semaphore}

import java.io.IOException
import java.net.SocketAddress
import java.nio.ByteBuffer
import java.nio.channels.ClosedChannelException
import scala.scalanative.runtime.ByteArray
import scala.scalanative.unsafe._
import scala.scalanative.unsigned._

import openssl._

private final class OpenSslContext(private[tls] val ctx: Ptr[SSL_CTX]) extends TlsContext

private[tls] def lastOpenSslError(): String =
  val code = ERR_get_error()
  if code == 0.toULong then "no OpenSSL error recorded"
  else
    val buf = stackalloc[CChar](256)
    ERR_error_string_n(code, buf, 256.toUSize)
    fromCString(buf)

/** OpenSSL-backed [[TlsSupport]]. Sits over any backend's [[TcpStream]] (uring, epoll, ...) purely through the
  * `Reader`/`Writer` interface, so it's written once here rather than per-backend.
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

/** A [[TcpStream]] wrapping another, plaintext one with TLS, encrypting/ decrypting entirely as an in-memory transform
  * via an OpenSSL "BIO pair" (`BIO_new_bio_pair`) - `ssl` reads and writes ciphertext through `internalBio`;
  * `networkBio` is the other end of that same pair, which *this* class reads from / writes to using the wrapped
  * [[TcpStream]]'s own `readBuf`/`writeBuf`.
  *
  * This design exists specifically so that no suspension point is ever nested inside an OpenSSL call:
  * `SSL_read`/`SSL_write`/`SSL_do_handshake` only ever touch in-memory buffers, so they're pure, synchronous, CPU- only
  * C calls from this backend's point of view - every real `await` (the underlying stream's own `readBuf`/`writeBuf`)
  * happens at the top of [[pump]]'s Scala call stack, never inside a foreign C frame. That matters here specifically
  * because of a real, previously-found bug in this backend's own scheduler where two fiber dispatches sharing one
  * native call frame corrupted delimcc's thread-local continuation state (see `UringShard.drainTasks`'s own doc) -
  * letting a suspension happen *inside* `SSL_read` would risk exactly that shape of problem again, one layer further
  * from view. Having OpenSSL do the actual socket I/O itself (the alternative, simpler-looking design) would require
  * exactly that.
  */
final class TlsStream private (
    underlying: TcpStream,
    ssl: Ptr[SSL],
    internalBio: Ptr[BIO],
    networkBio: Ptr[BIO]
) extends TcpStream:
  override def localAddress: SocketAddress = underlying.localAddress
  override def remoteAddress: SocketAddress = underlying.remoteAddress

  // OpenSSL must never see two calls on one `SSL` at once, and `close` must not free it under a running call: every
  // call into it goes through `native`, under `nativeLock`. Nothing suspends inside, so a monitor is safe here.
  private object nativeLock
  private var freed = false

  private inline def native[A](inline body: A): A = nativeLock.synchronized:
    if freed then throw new ClosedChannelException()
    body

  // Moving ciphertext between the pair and the connection suspends on the connection's I/O, so it takes gears
  // semaphores rather than a monitor. Without them, two fibers flushing at once could put their TLS records on the wire
  // out of order, and two filling at once could feed records to OpenSSL out of order.
  private val outbound = Semaphore(1)
  private val inbound = Semaphore(1)

  override def shutdownOutput()(using Async): Unit =
    val result = native(SSL_shutdown(ssl)) // queues close_notify
    if result < 0 then
      val error = native(SSL_get_error(ssl, result))
      if error != SSL_ERROR_WANT_READ && error != SSL_ERROR_WANT_WRITE then
        throw new IOException(s"TLS shutdown failed (SSL_get_error=$error): ${lastOpenSslError()}")
    flushNetworkOut() // sends it
    underlying.shutdownOutput()

  override def shutdownInput()(using Async): Unit =
    native(())
    underlying.shutdownInput()

  /** Drains whatever ciphertext `ssl` has queued up on `networkBio` out to the real connection. */
  private def flushNetworkOut()(using Async): Unit =
    val guard = outbound.acquire()
    try
      var pending = native(BIO_ctrl(networkBio, BIO_CTRL_PENDING, 0.toSize, null).toInt)
      while pending > 0 do
        val chunk = new Array[Byte](math.min(pending, TlsStream.NetBufSize))
        val n = native(BIO_read(networkBio, chunk.asInstanceOf[ByteArray].at(0), chunk.length))
        if n > 0 then
          underlying.writeBuf(ByteBuffer.wrap(chunk, 0, n)) match
            case Right(())       => ()
            case Left(Error.EOF) => throw new IOException("peer closed the connection while flushing TLS output")
        pending = native(BIO_ctrl(networkBio, BIO_CTRL_PENDING, 0.toSize, null).toInt)
    finally guard.release()

  /** Reads one chunk of ciphertext off the real connection and feeds it to `ssl` via `networkBio`, so a retried `op()`
    * can make progress. `false` means the peer closed the connection (a real EOF, not just "no bytes available right
    * now" - `readBuf` itself suspends until there's something to report).
    */
  private def fillNetworkIn()(using Async): Boolean =
    val guard = inbound.acquire()
    try
      val netIn = ByteBuffer.allocate(TlsStream.NetBufSize)
      underlying.readBuf(netIn) match
        case Right(()) =>
          netIn.flip()
          val n = netIn.remaining()
          val arr = new Array[Byte](n)
          netIn.get(arr)
          val written = native(BIO_write(networkBio, arr.asInstanceOf[ByteArray].at(0), n))
          if written != n then
            throw new IOException(s"BIO pair buffer overflowed feeding TLS input ($written/$n bytes written)")
          true
        case Left(Error.EOF) => false
    finally guard.release()

  /** Runs one SSL_* call, pumping ciphertext to/from the real connection and retrying as long as OpenSSL reports it
    * needs more input or has more output to flush, until `op` produces a real (non-retry) result. Returns the result
    * with its `SSL_get_error` code, read under the same lock as the call it describes.
    */
  private def pump(op: () => Int)(using Async): (Int, Int) =
    var (result, error) = native:
      val r = op()
      (r, if r <= 0 then SSL_get_error(ssl, r) else SSL_ERROR_NONE)
    flushNetworkOut()
    var retry = result <= 0
    while retry do
      retry = error match
        case SSL_ERROR_WANT_READ  => fillNetworkIn()
        case SSL_ERROR_WANT_WRITE => true // flushNetworkOut() above already drained the pair buffer
        case _                    => false
      if retry then
        val (r, e) = native:
          val r = op()
          (r, if r <= 0 then SSL_get_error(ssl, r) else SSL_ERROR_NONE)
        result = r
        error = e
        flushNetworkOut()
        retry = result <= 0
    (result, error)

  private[tls] def handshake()(using Async): Unit =
    val (r, err) = pump(() => SSL_do_handshake(ssl))
    if r != 1 then
      close()
      throw new IOException(s"TLS handshake failed (SSL_get_error=$err): ${lastOpenSslError()}")

  override def readBuf(buf: Buffer)(using Async): Result[Unit] =
    val cap = buf.remaining()
    if cap == 0 then Right(())
    else
      val tmp = new Array[Byte](cap)
      val (n, err) = pump(() => SSL_read(ssl, tmp.asInstanceOf[ByteArray].at(0), cap))
      if n > 0 then
        buf.put(tmp, 0, n)
        Right(())
      else
        err match
          case SSL_ERROR_ZERO_RETURN       => Left(Error.EOF)
          case SSL_ERROR_SYSCALL if n == 0 => Left(Error.EOF) // peer closed without a clean TLS shutdown
          case err => throw new IOException(s"TLS read failed (SSL_get_error=$err): ${lastOpenSslError()}")

  override def writeBuf(buf: Buffer)(using Async): Result[Unit] =
    val n = buf.remaining()
    if n == 0 then Right(())
    else
      val arr = new Array[Byte](n)
      buf.get(arr)
      var written = 0
      while written < n do
        val (r, err) = pump(() => SSL_write(ssl, arr.asInstanceOf[ByteArray].at(written), n - written))
        if r <= 0 then throw new IOException(s"TLS write failed (SSL_get_error=$err): ${lastOpenSslError()}")
        written += r
      Right(())

  /** Closes the connection at once. It cannot send `close_notify`, which needs an asynchronous write, so the peer sees
    * the close as a truncation; call [[shutdownOutput]] first for a clean TLS close. Pending reads and writes fail, and
    * the OpenSSL state is freed only once no call is using it.
    */
  override def close(): Unit =
    val first = nativeLock.synchronized:
      if freed then false
      else
        freed = true
        SSL_free(ssl) // also frees internalBio - SSL_set_bio gave it ownership
        BIO_free(networkBio)
        true
    if first then underlying.close()

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
        val _ =
          SSL_ctrl(ssl, SSL_CTRL_SET_TLSEXT_HOSTNAME, TLSEXT_NAMETYPE_host_name.toSize, name.asInstanceOf[Ptr[Byte]])
        val _ = SSL_set1_host(ssl, name) // target for certificate hostname verification
    new TlsStream(underlying, ssl, internalBio, networkBio)
