package gears.async.asyncio.examples

import gears.async._
import gears.async.net.TcpSupport
import gears.async.http._
import gears.async.asyncio.uring.UringPerThreadSupport
import gears.async.asyncio.tls.NativeTlsSupport

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import scala.scalanative.libc.{stdio, stdlib}
import scala.scalanative.meta.LinktimeInfo
import scala.scalanative.posix.unistd
import scala.scalanative.unsafe._

/** A runnable demonstration of TLS support: generates a throwaway
  * self-signed certificate (via the `openssl` CLI - there's no guaranteed
  * external network or pre-existing certificate to use instead), starts an
  * HTTPS server with it, then drives two clients against it: one that
  * trusts that certificate specifically and gets a real, fully verified
  * TLS connection through, and one that doesn't (just the system trust
  * store, like a real browser hitting a self-signed site) and correctly
  * fails the handshake - proving verification is actually enforced, not
  * silently skipped.
  */
private def generateSelfSignedCert(certFile: String, keyFile: String): Unit =
  val cmd =
    s"openssl req -x509 -newkey rsa:2048 -nodes -keyout $keyFile -out $certFile -days 1 -subj /CN=localhost >/dev/null 2>&1"
  val status = Zone.acquire(zone => stdlib.system(toCString(cmd)(using zone)))
  if status != 0 then throw new RuntimeException(s"failed to generate a self-signed certificate (is 'openssl' on PATH?), exit=$status")

private def deleteFile(path: String): Unit =
  val _ = Zone.acquire(zone => stdio.remove(toCString(path)(using zone)))

private def runHttpsDemo(port: Int)(using Async, TcpSupport, AsyncOperations): Unit =
  val pid = unistd.getpid()
  val certFile = s"/tmp/gears-https-example-$pid-cert.pem"
  val keyFile = s"/tmp/gears-https-example-$pid-key.pem"
  generateSelfSignedCert(certFile, keyFile)
  println(s"generated a throwaway self-signed certificate at $certFile")

  val listener = TcpSupport.listen(InetSocketAddress("127.0.0.1", port)) match
    case Right(l) => l
    case Left(e)  => throw new RuntimeException(s"listen failed: $e")

  val router = Router().get("/")(_ => HttpResponse.text("Hello over TLS!\n"))
  val serverConfig = Server(
    handler = router,
    tls = Some((NativeTlsSupport, NativeTlsSupport.serverContext(certFile, keyFile)))
  )
  println(s"HTTPS demo server listening on https://localhost:$port")

  Async.group:
    Future(serverConfig.serve(listener))

    // A client that trusts our throwaway certificate specifically (the CA
    // it was self-signed with, in this case the cert itself) - a real,
    // fully verified TLS handshake, not verification disabled.
    val trustingClient = Client(Transport(tls = Some(NativeTlsSupport), tlsContext = _.clientContext(verify = true, caFile = Some(certFile))))
    val resp = trustingClient.get(s"https://localhost:$port/")
    println(s"[trusting client]  GET / -> ${resp.status.code} ${resp.status.reason}: ${new String(resp.body, StandardCharsets.UTF_8).trim}")

    // A client using only the system trust store (the default, and what a
    // real browser would use) - must correctly reject this self-signed
    // certificate, proving verification is genuinely enforced.
    val strictClient = Client(NativeTlsSupport)
    try
      strictClient.get(s"https://localhost:$port/")
      println("[strict client]    UNEXPECTEDLY SUCCEEDED - certificate verification is not being enforced!")
    catch case e: Exception => println(s"[strict client]    correctly rejected the untrusted certificate: ${e.getMessage}")

    listener.close()
    deleteFile(certFile)
    deleteFile(keyFile)
    System.exit(0)

@main def httpsExample(): Unit =
  val port = 8443
  if LinktimeInfo.isLinux then
    given support: UringPerThreadSupport = UringPerThreadSupport()
    given tcp: TcpSupport = support.tcpSupport
    Async.blocking(runHttpsDemo(port))
  else
    throw new UnsupportedOperationException("no TCP backend wired up for this platform")
