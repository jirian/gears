# gears-io-http: streams, TLS, HTTP and WebSockets

`ioHttp` (`io-http/`, cross-built for the JVM and Scala Native) is the higher level programs use: byte streams over
connections (`gears.io.net`), TLS (`gears.io.tls`), and an HTTP/1.1 server and client with WebSockets
(`gears.io.http`). It is written against `gears.async.io` alone, and runs on each platform's default backend: NIO's
`NioOnCaller` on the JVM (the reference backend's ops with a faster way of waiting; no JDK flags), and io_uring on Scala
Native (Linux 6.0 or later, with liburing and OpenSSL). A program that wants another backend, such as the reference
backend or kqueue, runs with its own `Io.run` and uses
`Http.server` and `HttpClient`, which take an `Io` over any backend with the default backends' capabilities
(`DefaultCapabilities`, which the reference backend, NIO and io_uring all have): a handler is given that `Io`, so the capabilities are
in its type. Making them a type parameter of `Routes` instead was weighed and left: Scala cannot infer it from what a
handler's body uses, so every route would have to name it, for a gain no backend here needs.

## Getting started

```scala
import gears.io.http.*

@main def hello(): Unit =
  Http.serve(8080)(Routes(GET("/") { _ => "Hello from gears!" }))

@main def fetch(): Unit =
  Http.run { println(Http.get("https://example.com").ensureSuccess().text()) }
```

A fuller server:

```scala
given BodyCodec[Item] = BodyCodec.json(writeToString(_), readFromString[Item](_)) // any JSON library

val api = Routes(
  GET("/items") { req => Response.json(items.list(req.query[Int]("limit").getOrElse(50))) },
  POST("/items") { req => Response.json(items.add(req.as[Item]), Status.Created) },
  DELETE("/items/{id:Long}") { (_, p) => items.remove(p.id) } // Unit: 204
)

val routes = Routes(
  GET("/health") { _ => "ok" },
  "/api" / api.withMiddleware(auth),
  GET("/ws") { req => WebSocket.accept(req) { ws => for m <- ws.messages do ws.send(m) } }
).withMiddleware(Middleware.logging(), Middleware.timeout(30.seconds))

Http.run {
  Async.group {
    val server = Http.server(routes, ServerConfig(port = 8443, tls = Some(TlsServerConfig("cert.pem", "key.pem"))))
    server.await() // or server.shutdown(10.seconds); leaving the group stops it
  }
}
```

Routes are a value: `routes(Request.get("/api/items?limit=2"))` answers without a socket, which is how they are
tested (inside `Http.run`, which gives the `Io` a handler runs with). Against a real socket,
`Http.testServer(routes) { server => Http.get(server.url.resolve("/items")) }` serves them on a free port of
loopback, as Go's `httptest.NewServer` does.

## Decisions, and where they come from

These follow the review of 2026-10-29 (`../gears-jirian-docs/review-2026-10-29.md`, part B).

- **A handler is a function returning a value**, `Request => (Async, Io) ?=> Response` (axum, http4s, Javalin's tests):
  returning a value is what makes routes testable by calling them. Streaming is a body type (`Response.stream`), not a
  second handler shape, and a WebSocket is a response that takes the connection over (`WebSocket.accept`).
- **What a handler needs is a function it calls.** axum declares a handler's needs as typed arguments ("extractors"),
  each a trait implementation, with rules on their order, so that a request missing one is refused before the handler
  runs. In direct style with exceptions that is a call that throws: `def authenticated(req: Request): User =
  req.header("Authorization").flatMap(users.byToken).getOrElse(throw HttpError(Status.Unauthorized))`, then
  `GET("/me") { req => Response.json(authenticated(req)) }`. Path segments are the exception, typed by the pattern
  itself (`GET("/items/{id:Long}") { (req, p) => items.get(p.id) }`), since the route already names them. What
  middleware computes for the handlers it wraps, such as a request id, goes on the request as an `Attribute`.
- **Errors are exceptions.** A handler throws `HttpError(status)` to answer with a status; anything else is a 500 and
  is logged. A client's non-2xx response is a value until `ensureSuccess()` (reqwest, httpx). Reads return `-1` at the
  end; there is no `Either`-of-EOF.
- **Cancellation and timeouts are gears'.** Every call takes an `Async`; a cancelled caller cancels its I/O. Every
  configured timeout is the time limit of the op it bounds (`Io.perform(op, limit)`), which costs no timer or future.
- **A handler whose client went away is cancelled**, as Go cancels a request's context: once a handler has run for
  `ServerConfig.cancelAbandoned` (100 ms) and read the request's body, the server watches the connection, and if it
  ends, cancels the handler's group, and with it the futures the handler started. The watch is one future per
  connection, started lazily and never cancelled when it has nothing to do, so a handler that finishes sooner costs
  nothing; on Scala Native a cancellation costs about 250 µs (stack traces), and a read withdrawn from io_uring a system
  call, which is why the watch does not start with every request. Bytes that arrive meanwhile are the next request
  (pipelining) and stay buffered for it. A handler whose work must not stop halfway runs it with
  `CompletionGroup.Unlinked`, or the server turns cancelling off (`Duration.Inf`).
- **Typed path segments.** A pattern is read when compiling: `GET("/items/{id:Long}") { (req, p) => items.get(p.id) }`
  gives the handler a named tuple, so a misspelt name does not compile and a segment that does not decode is a 400.
  Go's `PathValue`, Ktor and Hummingbird take names as strings checked when running; axum and http4s are typed but
  positional. A pattern computed when running is `route(method, pattern)`'s, read with `Request.pathParam`.
- **Precedence is per method.** A literal segment beats `{name}`, which beats `{name...}`, but a route that wins for
  one method never hides another that matches the path for the request's: `GET /a/{x}` and `POST /{y}/b` each serve
  `/a/b`. Go refuses such a pair as it is registered; here both are allowed, and the answer is always defined.
- **Shutdown on signals.** `Http.serve` stops gracefully on SIGTERM, as a container's runtime stops it, and on SIGINT;
  a second signal ends the process. A server started with `Http.server` is the program's to stop.
- **Finite defaults.** Server: head 10 s from its first byte (slowloris), each body read 30 s, each write 30 s, idle
  60 s, TLS handshake 10 s; head 16 KiB, 100 fields, body 10 MiB, 10 000 connections. Client: connect 10 s, response
  head 30 s, each read 30 s, no total unless a request sets one; redirects followed, never from https to http, and
  `Authorization` and `Cookie` kept from other origins.
- **Structured.** `Http.server` runs in the caller's scope and stops with it; `HttpClient.stream(request)(use)` closes
  or returns the connection when `use` ends, so a pooled connection cannot leak.
- **One seam for JSON**, `BodyCodec[A]`, so that `req.as[A]`, `Response.json(a)` and `res.as[A]` work with whichever
  library the program uses; adapters are one line each.
- **Plain names**: `Request` and `Response` on both sides (fetch), `Status.Ok`, `Method.Get`.
- **Routing**: patterns compiled into a segment tree as `Routes` is built, a literal beating `{name}` beating
  `{name...}`; path segments decoded after matching; a path with dot segments redirected to its clean form, as Go's
  `ServeMux` does, so a prefix cannot be escaped.

## What the wire code refuses

The server parses strictly where leniency is what request smuggling exploits: whitespace before a header's colon,
folded lines, bare CR or LF, a `Transfer-Encoding` beside a `Content-Length`, lengths that disagree or are not digits,
a transfer coding other than `chunked`, more than one `Host`. A head too large is a 431, a body too large a 413; a
connection ended with request bytes unread is shut down for writing and drained for up to a second first, so that the
client reads the answer rather than a reset. `100 Continue` goes out only when the handler first reads the body. A TLS
response delimited by the connection's close that ends without TLS's close notification is reported as a truncation;
a TLS stream sends its close notification when it closes.

## Platforms

| | JVM | Scala Native |
| --- | --- | --- |
| Default backend | `NioOnCaller` (no flags) | `UringBackend` (Linux 6.0+) |
| TLS | the JDK's `SSLEngine` | OpenSSL 1.1 or 3, through a BIO pair |
| Private key format | PKCS #8 PEM (`BEGIN PRIVATE KEY`) | any OpenSSL reads |

Not there yet: HTTP/2, compression, a Unix-socket client, cookies kept by the client, `Http.serve` on macOS Native
(which needs a kqueue default), and the JSON adapter modules.
