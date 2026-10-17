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

private final class OpenSslContext(ctx: Ptr[SSL_CTX]) extends TlsContext:
  private var closed = false
  def session[A](create: Ptr[SSL_CTX] => A): A = synchronized:
    if closed then throw new ClosedChannelException()
    create(ctx)
  def close(): Unit = synchronized:
    if !closed then
      closed = true
      SSL_CTX_free(ctx)

private[tls] def lastOpenSslError(): String =
  val code = ERR_get_error()
  if code == 0.toULong then "no OpenSSL error recorded"
  else
    val buf = stackalloc[CChar](256)
    ERR_error_string_n(code, buf, 256.toUSize)
    fromCString(buf)

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
    val tls =
      context.asInstanceOf[OpenSslContext].session(ctx => TlsStream.open(stream, ctx, isServer = false, serverName))
    try
      tls.handshake()
      tls
    catch
      case t: Throwable =>
        tls.close()
        throw t

  def wrapServer(stream: TcpStream, context: TlsContext)(using Async): TcpStream =
    val tls = context.asInstanceOf[OpenSslContext].session(ctx => TlsStream.open(stream, ctx, isServer = true, ""))
    try
      tls.handshake()
      tls
    catch
      case t: Throwable =>
        tls.close()
        throw t

final class TlsStream private (
    underlying: TcpStream,
    ssl: Ptr[SSL],
    networkBio: Ptr[BIO]
) extends TcpStream:
  override def localAddress: SocketAddress = underlying.localAddress
  override def remoteAddress: SocketAddress = underlying.remoteAddress

  private object nativeLock
  private var freed = false

  private inline def native[A](inline body: A): A = nativeLock.synchronized:
    if freed then throw new ClosedChannelException()
    body

  private val outbound = Semaphore(1)
  private val inbound = Semaphore(1)

  private val netOut = new Array[Byte](TlsStream.NetBufSize)
  private val netIn = new Array[Byte](TlsStream.NetBufSize)
  private var unsentFrom = 0
  private var unsentTo = 0
  private val applicationAccess = gears.async.asyncio.IoConcurrency()

  override def shutdownOutput()(using Async): Unit =
    val (result, error) = call(() => SSL_shutdown(ssl))
    if result < 0 && error != SSL_ERROR_WANT_READ && error != SSL_ERROR_WANT_WRITE then
      throw new IOException(s"TLS shutdown failed (SSL_get_error=$error): $failureDetail")
    flushNetworkOut()
    underlying.shutdownOutput()

  override def shutdownInput()(using Async): Unit =
    native(())
    underlying.shutdownInput()

  private def flushNetworkOut()(using Async): Unit =
    val guard = outbound.acquire()
    try
      var done = false
      while !done do
        if unsentFrom == unsentTo then
          val pending = native(BIO_ctrl(networkBio, BIO_CTRL_PENDING, 0.toSize, null).toInt)
          val n =
            if pending <= 0 then 0
            else native(BIO_read(networkBio, netOut.asInstanceOf[ByteArray].at(0), math.min(pending, netOut.length)))
          if n > 0 then
            unsentFrom = 0
            unsentTo = n
          else done = true
        if !done then
          val out = ByteBuffer.wrap(netOut, unsentFrom, unsentTo - unsentFrom)
          try
            underlying.writeBuf(out) match
              case Right(())       => ()
              case Left(Error.EOF) => throw new IOException("peer closed the connection while flushing TLS output")
          finally unsentFrom = out.position()
    finally guard.release()

  private def fillNetworkIn()(using Async): Boolean =
    val guard = inbound.acquire()
    try
      val in = ByteBuffer.wrap(netIn)
      def feed(): Unit =
        val n = in.position()
        if n > 0 then
          val written = native(BIO_write(networkBio, netIn.asInstanceOf[ByteArray].at(0), n))
          if written != n then
            throw new IOException(s"BIO pair buffer overflowed feeding TLS input ($written/$n bytes written)")
      val result =
        try underlying.readBuf(in)
        catch
          case t: Throwable =>
            feed()
            throw t
      feed()
      result == Right(())
    finally guard.release()

  private var failureDetail = ""

  private def call(op: () => Int): (Int, Int) = native:
    ERR_clear_error()
    val r = op()
    val e = if r <= 0 then SSL_get_error(ssl, r) else SSL_ERROR_NONE
    if e == SSL_ERROR_SSL || e == SSL_ERROR_SYSCALL then failureDetail = lastOpenSslError()
    (r, e)

  private def pump(op: () => Int)(using Async): (Int, Int) =
    var (result, error) = call(op)
    flushNetworkOut()
    var retry = result <= 0
    while retry do
      retry = error match
        case SSL_ERROR_WANT_READ =>
          val more = fillNetworkIn()
          if !more then error = TlsStream.PeerClosed
          more
        case SSL_ERROR_WANT_WRITE => true
        case _                    => false
      if retry then
        val (r, e) = call(op)
        result = r
        error = e
        flushNetworkOut()
        retry = result <= 0
    (result, error)

  private[tls] def handshake()(using Async): Unit =
    val (r, err) = pump(() => SSL_do_handshake(ssl))
    if r != 1 then
      close()
      if err == TlsStream.PeerClosed then throw new IOException("TLS handshake failed: the peer closed the connection")
      throw new IOException(s"TLS handshake failed (SSL_get_error=$err): $failureDetail")

  override def readBuf(buf: Buffer)(using Async): Result[Unit] = applicationAccess.reading {
    val cap = buf.remaining()
    if cap == 0 then Right(())
    else
      val direct = buf.hasArray() && !buf.isReadOnly()
      val tmp = if direct then buf.array() else new Array[Byte](cap)
      val at = if direct then buf.arrayOffset() + buf.position() else 0
      val (n, err) = pump(() => SSL_read(ssl, tmp.asInstanceOf[ByteArray].at(at), cap))
      if n > 0 then
        if direct then buf.position(buf.position() + n) else buf.put(tmp, 0, n)
        Right(())
      else
        err match
          case SSL_ERROR_ZERO_RETURN       => Left(Error.EOF)
          case SSL_ERROR_SYSCALL if n == 0 => Left(Error.EOF)
          case TlsStream.PeerClosed        => Left(Error.EOF)
          case err => throw new IOException(s"TLS read failed (SSL_get_error=$err): $failureDetail")
  }

  override def writeBuf(buf: Buffer)(using Async): Result[Unit] = applicationAccess.writing {
    val n = buf.remaining()
    if n == 0 then Right(())
    else
      val direct = buf.hasArray()
      val arr = if direct then buf.array() else new Array[Byte](n)
      val start = if direct then buf.arrayOffset() + buf.position() else 0
      if !direct then buf.duplicate().get(arr)
      var written = 0
      while written < n do
        val (r, err) = pump(() => SSL_write(ssl, arr.asInstanceOf[ByteArray].at(start + written), n - written))
        if r <= 0 then
          if err == TlsStream.PeerClosed then throw new IOException("TLS write failed: the peer closed the connection")
          throw new IOException(s"TLS write failed (SSL_get_error=$err): $failureDetail")
        written += r
        buf.position(buf.position() + r)
      Right(())
  }

  override def close(): Unit =
    val first = nativeLock.synchronized:
      if freed then false
      else
        freed = true
        SSL_free(ssl)
        BIO_free(networkBio)
        true
    if first then underlying.close()

private[tls] object TlsStream:
  private[tls] final val PeerClosed = -1

  private[tls] val NetBufSize = 17 * 1024

  private[tls] def open(underlying: TcpStream, ctx: Ptr[SSL_CTX], isServer: Boolean, serverName: String): TlsStream =
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
      val host =
        if serverName.startsWith("[") && serverName.endsWith("]") then serverName.drop(1).dropRight(1) else serverName
      val verifyTargetSet = Zone.acquire: zone =>
        val name = toCString(host)(using zone)
        if isIpLiteral(host) then X509_VERIFY_PARAM_set1_ip_asc(SSL_get0_param(ssl), name) == 1
        else
          val _ =
            SSL_ctrl(ssl, SSL_CTRL_SET_TLSEXT_HOSTNAME, TLSEXT_NAMETYPE_host_name.toSize, name.asInstanceOf[Ptr[Byte]])
          SSL_set1_host(ssl, name) == 1
      // Without a verification target, a verifying context accepts any trusted certificate.
      if !verifyTargetSet then
        val error = new IOException(s"Failed to set the TLS verification target $serverName: ${lastOpenSslError()}")
        SSL_free(ssl)
        BIO_free(networkBio)
        throw error
    new TlsStream(underlying, ssl, networkBio)

  private def isIpLiteral(host: String): Boolean =
    Zone.acquire: zone =>
      val text = toCString(host)(using zone)
      val scratch = stackalloc[Byte](16)
      import scala.scalanative.posix.arpa.inet.inet_pton
      import scala.scalanative.posix.sys.socket.{AF_INET, AF_INET6}
      inet_pton(AF_INET, text, scratch) == 1 || inet_pton(AF_INET6, text, scratch) == 1
