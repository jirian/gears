package gears.async.asyncio.examples

import gears.async._
import gears.async.net.TcpSupport
import gears.async.http._
import gears.async.asyncio.uring.UringPerThreadSupport
import gears.async.asyncio.tls.NativeTlsSupport

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import scala.scalanative.libc.stdlib
import scala.scalanative.meta.LinktimeInfo
import scala.scalanative.posix.unistd
import scala.scalanative.unsafe._

private def generateSelfSignedCert(certFile: String, keyFile: String): Unit =
  val cmd =
    s"openssl req -x509 -newkey rsa:2048 -nodes -keyout $keyFile -out $certFile -days 1 -subj /CN=localhost >/dev/null 2>&1"
  val status = Zone.acquire(zone => stdlib.system(toCString(cmd)(using zone)))
  if status != 0 then throw new RuntimeException(s"failed to generate a self-signed certificate (is 'openssl' on PATH?), exit=$status")

private def demoRouter(): Router =
  Router()
    .get("/")(_ => HttpResponse.text("Hello over TLS!\n"))
    .get("/health")(_ => HttpResponse.text("ok\n"))
    .post("/echo")(req => HttpResponse.text(new String(req.body, StandardCharsets.UTF_8)))

private def runServer(port: Int)(using Async, TcpSupport, AsyncOperations): Unit =
  val pid = unistd.getpid()
  val certFile = s"/tmp/gears-https-server-$pid-cert.pem"
  val keyFile = s"/tmp/gears-https-server-$pid-key.pem"
  generateSelfSignedCert(certFile, keyFile)

  val listener = TcpSupport.listen(InetSocketAddress("0.0.0.0", port)) match
    case Right(l) => l
    case Left(e)  => throw new RuntimeException(s"listen failed: $e")

  val serverConfig = Server(
    handler = demoRouter(),
    tls = Some((NativeTlsSupport, NativeTlsSupport.serverContext(certFile, keyFile)))
  )

  println(s"""listening on https://localhost:$port (uring)
              |
              |self-signed certificate: $certFile
              |
              |try it:
              |  curl -k https://localhost:$port/                  # skip verification (simplest)
              |  curl --cacert $certFile https://localhost:$port/  # verify against this cert specifically
              |  curl -k https://localhost:$port/health
              |  curl -k -X POST --data 'hi there' https://localhost:$port/echo
              |
              |a browser will show a "not secure" / self-signed warning for this
              |cert - that's expected, click through it (or import $certFile as a
              |trusted CA if you want it to show as secure).
              |
              |Ctrl-C to stop.
              |""".stripMargin)

  serverConfig.serve(listener)

@main def httpsServer(): Unit =
  val port = 8443
  if LinktimeInfo.isLinux then
    given support: UringPerThreadSupport = UringPerThreadSupport()
    given tcp: TcpSupport = support.tcpSupport
    Async.blocking(runServer(port))
  else
    throw new UnsupportedOperationException("no TCP backend wired up for this platform")
