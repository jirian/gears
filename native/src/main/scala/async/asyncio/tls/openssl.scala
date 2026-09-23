package gears.async.asyncio.tls

import scala.scalanative.unsafe._

/** Raw `@extern` bindings to libssl/libcrypto (OpenSSL), just enough of the
  * API surface for [[TlsStream]]'s "BIO pair" design: a client or server
  * TLS session driven entirely through two in-memory buffers, never
  * touching a socket itself - see `TlsStream`'s own doc for why.
  *
  * `SSL`/`SSL_CTX`/`BIO`/`SSL_METHOD` are opaque handles here - this
  * backend never looks inside them, only ever holds `Ptr[_]`s to them and
  * passes those back into the functions below, exactly as C code would.
  *
  * A few real OpenSSL functions are C *macros* wrapping `SSL_ctrl`/
  * `BIO_ctrl` (`SSL_set_tlsext_host_name`, `BIO_pending`) rather than
  * linkable symbols - unlike `io_uring_get_sqe`/`io_uring_cq_advance`
  * (which needed a small C shim, see the uring backend's own history),
  * these don't need one: `SSL_ctrl`/`BIO_ctrl` themselves *are* real,
  * linkable functions, so the macros are reproduced here as plain Scala
  * calls to `SSL_ctrl`/`BIO_ctrl` with the same constants the headers
  * `#define` them with (`ssl.h`, `tls1.h`, `bio.h` - values pinned by
  * OpenSSL's own stable public ABI, checked against this machine's
  * `/usr/include/openssl` headers when writing this).
  */
@extern
private[tls] object openssl:
  type SSL_METHOD
  type SSL_CTX
  type SSL
  type BIO

  final val SSL_FILETYPE_PEM = 1

  final val SSL_VERIFY_NONE = 0x00
  final val SSL_VERIFY_PEER = 0x01

  final val SSL_ERROR_NONE = 0
  final val SSL_ERROR_SSL = 1
  final val SSL_ERROR_WANT_READ = 2
  final val SSL_ERROR_WANT_WRITE = 3
  final val SSL_ERROR_SYSCALL = 5
  final val SSL_ERROR_ZERO_RETURN = 6

  // ssl.h / tls1.h: SSL_set_tlsext_host_name(s, name) is
  //   #define SSL_set_tlsext_host_name(s, name) \
  //     SSL_ctrl(s, SSL_CTRL_SET_TLSEXT_HOSTNAME, TLSEXT_NAMETYPE_host_name, (void*)(name))
  final val SSL_CTRL_SET_TLSEXT_HOSTNAME = 55
  final val TLSEXT_NAMETYPE_host_name = 0

  // bio.h: BIO_pending(b) is #define BIO_pending(b) (int)BIO_ctrl(b, BIO_CTRL_PENDING, 0, NULL)
  final val BIO_CTRL_PENDING = 10

  def TLS_client_method(): Ptr[SSL_METHOD] = extern
  def TLS_server_method(): Ptr[SSL_METHOD] = extern

  def SSL_CTX_new(method: Ptr[SSL_METHOD]): Ptr[SSL_CTX] = extern
  def SSL_CTX_free(ctx: Ptr[SSL_CTX]): Unit = extern
  def SSL_CTX_use_certificate_chain_file(ctx: Ptr[SSL_CTX], file: CString): CInt = extern
  def SSL_CTX_use_PrivateKey_file(ctx: Ptr[SSL_CTX], file: CString, tpe: CInt): CInt = extern
  def SSL_CTX_check_private_key(ctx: Ptr[SSL_CTX]): CInt = extern
  def SSL_CTX_load_verify_locations(ctx: Ptr[SSL_CTX], caFile: CString, caPath: CString): CInt = extern
  def SSL_CTX_set_default_verify_paths(ctx: Ptr[SSL_CTX]): CInt = extern
  def SSL_CTX_set_verify(ctx: Ptr[SSL_CTX], mode: CInt, callback: Ptr[Byte]): Unit = extern

  def SSL_new(ctx: Ptr[SSL_CTX]): Ptr[SSL] = extern
  def SSL_free(ssl: Ptr[SSL]): Unit = extern
  def SSL_set_bio(ssl: Ptr[SSL], rbio: Ptr[BIO], wbio: Ptr[BIO]): Unit = extern
  def SSL_set_connect_state(ssl: Ptr[SSL]): Unit = extern
  def SSL_set_accept_state(ssl: Ptr[SSL]): Unit = extern
  def SSL_do_handshake(ssl: Ptr[SSL]): CInt = extern
  def SSL_read(ssl: Ptr[SSL], buf: Ptr[Byte], num: CInt): CInt = extern
  def SSL_write(ssl: Ptr[SSL], buf: Ptr[Byte], num: CInt): CInt = extern
  def SSL_get_error(ssl: Ptr[SSL], ret: CInt): CInt = extern
  def SSL_shutdown(ssl: Ptr[SSL]): CInt = extern
  def SSL_set1_host(ssl: Ptr[SSL], hostname: CString): CInt = extern
  def SSL_get_verify_result(ssl: Ptr[SSL]): CLong = extern
  def SSL_ctrl(ssl: Ptr[SSL], cmd: CInt, larg: CLong, parg: Ptr[Byte]): CLong = extern

  def BIO_new_bio_pair(bio1: Ptr[Ptr[BIO]], writebuf1: CSize, bio2: Ptr[Ptr[BIO]], writebuf2: CSize): CInt = extern
  def BIO_free(bio: Ptr[BIO]): CInt = extern
  def BIO_read(bio: Ptr[BIO], data: Ptr[Byte], len: CInt): CInt = extern
  def BIO_write(bio: Ptr[BIO], data: Ptr[Byte], len: CInt): CInt = extern
  def BIO_ctrl(bio: Ptr[BIO], cmd: CInt, larg: CLong, parg: Ptr[Byte]): CLong = extern

  def ERR_get_error(): CUnsignedLong = extern
  def ERR_error_string_n(e: CUnsignedLong, buf: Ptr[CChar], len: CSize): Unit = extern
