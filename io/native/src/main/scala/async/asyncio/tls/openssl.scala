package gears.async.asyncio.tls

import scala.scalanative.unsafe._

@link("ssl")
@link("crypto")
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

  final val SSL_CTRL_SET_TLSEXT_HOSTNAME = 55
  final val TLSEXT_NAMETYPE_host_name = 0

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
  def SSL_get0_param(ssl: Ptr[SSL]): Ptr[Byte] = extern
  def X509_VERIFY_PARAM_set1_ip_asc(param: Ptr[Byte], ipasc: CString): CInt = extern
  def SSL_ctrl(ssl: Ptr[SSL], cmd: CInt, larg: CLong, parg: Ptr[Byte]): CLong = extern

  def BIO_new_bio_pair(bio1: Ptr[Ptr[BIO]], writebuf1: CSize, bio2: Ptr[Ptr[BIO]], writebuf2: CSize): CInt = extern
  def BIO_free(bio: Ptr[BIO]): CInt = extern
  def BIO_read(bio: Ptr[BIO], data: Ptr[Byte], len: CInt): CInt = extern
  def BIO_write(bio: Ptr[BIO], data: Ptr[Byte], len: CInt): CInt = extern
  def BIO_ctrl(bio: Ptr[BIO], cmd: CInt, larg: CLong, parg: Ptr[Byte]): CLong = extern

  def ERR_get_error(): CUnsignedLong = extern
  def ERR_clear_error(): Unit = extern
  def ERR_error_string_n(e: CUnsignedLong, buf: Ptr[CChar], len: CSize): Unit = extern
