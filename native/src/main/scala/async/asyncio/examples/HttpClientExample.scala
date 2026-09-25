package gears.async.asyncio.examples

import gears.async._
import gears.async.net.{DnsSupport, TcpSupport}
import gears.async.http._
import gears.async.asyncio.uring.{NativeDnsSupport, UringPerThreadSupport}

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import scala.scalanative.meta.LinktimeInfo

private def clientDemoRouter(): Router =
  Router()
    .get("/")(_ => HttpResponse.text("Hello from gears!\n"))
    .get("/redirect")(_ => HttpResponse.redirect("/", HttpStatus.Found))
    .post("/echo")(req => HttpResponse.text(new String(req.body, StandardCharsets.UTF_8)))
    .get("/set-cookie")(_ => HttpResponse.text("cookie set\n").withHeader("Set-Cookie", "session=abc123; Path=/; HttpOnly"))
    .get("/items/{id}")(req => HttpResponse.text(s"item ${req.pathValue("id").getOrElse("?")}\n"))

private def runClientDemo(port: Int)(using Async, TcpSupport, DnsSupport, AsyncOperations): Unit =
  val listener = TcpSupport.listen(InetSocketAddress("127.0.0.1", port)) match
    case Right(l) => l
    case Left(e)  => throw new RuntimeException(s"listen failed: $e")
  println(s"demo server listening on http://127.0.0.1:$port")

  Async.group:
    Future(serve(listener, clientDemoRouter()))

    val client = Client()
    val base = s"http://127.0.0.1:$port"

    val getResp = client.get(s"$base/")
    println(s"GET / -> ${getResp.status.code} ${getResp.status.reason}: ${new String(getResp.body, StandardCharsets.UTF_8).trim}")

    val postResp = client.post(s"$base/echo", "round trip!".getBytes(StandardCharsets.UTF_8), "text/plain")
    println(s"POST /echo -> ${postResp.status.code}: ${new String(postResp.body, StandardCharsets.UTF_8).trim}")

    val redirectResp = client.get(s"$base/redirect")
    println(s"GET /redirect -> ${redirectResp.status.code} (after following redirect): ${new String(redirectResp.body, StandardCharsets.UTF_8).trim}")

    val cookieResp = client.get(s"$base/set-cookie")
    println(s"GET /set-cookie -> Set-Cookie parsed as: ${cookieResp.cookies}")

    val itemResp = client.get(s"$base/items/42")
    println(s"GET /items/42 -> ${new String(itemResp.body, StandardCharsets.UTF_8).trim}")

    val headResp = client.send(ClientRequest(HttpMethod.HEAD, Url.parse(s"$base/"), Headers.empty, Array.emptyByteArray))
    println(s"HEAD / -> ${headResp.status.code}, body bytes: ${headResp.body.length} (should be 0)")

    val notFoundResp = client.get(s"$base/does-not-exist")
    println(s"GET /does-not-exist -> ${notFoundResp.status.code}")

    listener.close()
    System.exit(0)

@main def httpClientExample(): Unit =
  val port = 8081
  if LinktimeInfo.isLinux then
    given support: UringPerThreadSupport = UringPerThreadSupport()
    given tcp: TcpSupport = support.tcpSupport
    given dns: DnsSupport = NativeDnsSupport
    Async.blocking(runClientDemo(port))
  else
    throw new UnsupportedOperationException("no TCP backend wired up for this platform")
