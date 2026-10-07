# gears

![GitHub Release](https://img.shields.io/github/v/release/lampepfl/gears)
![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/lampepfl/gears/ci.yml)
[![Homepage](https://img.shields.io/badge/website-homepage-brightgreen)](https://lampepfl.github.io/gears)
[![API Documentation Link](https://img.shields.io/badge/api-documentation-brightgreen)](https://lampepfl.github.io/gears/api)

An Experimental Asynchronous Programming Library for Scala 3. It aims to be:
- **Simple**: enables direct-style programming (suspending with `.await`, calling Async-functions directly) and comes with few simple concepts.
- **Structured**: allows an idiomatic way of structuring concurrent programs minimizing computation leaking (*structured concurrency*), while
  providing a toolbox for dealing with external, unstructured events.
- **Cross-platform**: Works on JVM >= 21, Scala Native and Scala.js with WAsm support.

> [!WARNING]  
> On V8 <14.2.75 (Node.js 24 and 25), there is a bug that causes stack overflows in nested async contexts, which are used extensively by Gears. Use Node.js 26+, or another runtime with Wasm 3.0 and JSPI support.
> See #165 for more details.

## Getting Started

The [Gears Book](https://natsukagami.github.io/gears-book) is a great way to getting started with programming using Gears.
It provides a tutorial, as well as a guided walkthrough of all concepts available within Gears.

### Adding `gears` to your dependencies

With `sbt` 2:
```scala
libraryDependencies += "ch.epfl.lamp" %% "gears" % "<version>"
```

For cross-platform dependencies with sbt 1, use `%%%`.

With `mill`:
```scala
def ivyDeps = Agg(
  // ... other dependencies
  ivy"ch.epfl.lamp::gears:<version>"
)
```

With `scala` (since 3.5.0) or `scala-cli`:
```scala
//> using dep "ch.epfl.lamp::gears:<version>"
```

## Setting up on an unpublished version of Gears

The build uses sbt 2 and Scala 3.9.0. You will need JDK >= 21 and [Scala Native](https://scala-native.org) set up, plus Node.js 26+ to run the Scala.js tests.
```bash
sbt publishLocal
```

## Contributing

We are happy to take **issues**, **pull requests** and **discussions**!

For a quick look at our development environment and workflow, check our the [contributing guide](./CONTRIBUTING.md).

### Related Projects

You might also be interested in:
- [**ox**](https://github.com/softwaremill/ox): Safe direct-style concurrency and resiliency for Scala on the JVM.

## License

Copyright 2024 LAMP, EPFL.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

See [LICENSE](./LICENSE) for more details.

## Scala Native async I/O demos

The cross-built `asyncio` project (JVM and Native) is interface only: `Op`, `Completion`,
the `Reactor` trait with `Reactor.Factory`, and the `Ops` factory that each reactor exposes
as `reactor.ops` for building reads, writes, accepts, connects, timers, custom readiness
ops, blocking tasks, promises, and name resolution. `nativeAsyncioLoop` implements it in
`asyncio.kqueue`: `KqueueReactor`, `KqueueOps`, the concrete op classes, and the blocker
pool. The kqueue demos use those internals directly. The gears demos use only the interface,
including `reactor.handles` for opening files, sockets, and pipes by `Address`, so they
work with any reactor's handle type; they receive a `Reactor.Factory` from their `Main`,
the one place naming kqueue.

The `nativeAsyncioLoop` and `kqueueDemo` sbt projects were ported
from [bishabosha/scala-native-async-io@6e8320d](https://github.com/bishabosha/scala-native-async-io/commit/6e8320d36c71f75c9daf85065f5c89729f0825c0),
and extended with Unix datagrams and IPv4/IPv6 sockets. These low-level experiments
require kqueue and are tested on macOS.

Kqueue operations use `scala.scalanative.bsd.kevent` from Scala Native's `posixlib`,
including its native event size and get/set helpers. The original custom Scala/C
bindings and `nativeAsyncioCore` project are no longer needed. Scala Native 0.5.11
does not export `EVFILT_TIMER`, so the timer helper keeps that constant locally
and uses kqueue's default millisecond units.

```bash
sbt 'kqueueDemo/run timer'
sbt 'kqueueDemo/run target-info'
sbt 'kqueueDemo/run echo --msg hello'
```

For the FIFO demos, create a named pipe with `mkfifo /tmp/gears-demo.fifo`, then
run the reader and writer in separate terminals:

```bash
sbt 'kqueueDemo/run file-read --fifo /tmp/gears-demo.fifo'
sbt 'kqueueDemo/run file-write --fifo /tmp/gears-demo.fifo -m hello'
```

For Unix domain stream sockets, start the server first, then the client in another terminal:

```bash
sbt 'kqueueDemo/run sock-serve --sock /tmp/gears-demo.sock'
sbt 'kqueueDemo/run sock --sock /tmp/gears-demo.sock'
```

Unix domain datagrams preserve message boundaries and echo each packet to its
sender. The client binds its own path so the server can reply:

```bash
sbt 'kqueueDemo/run datagram-serve --sock /tmp/gears-datagram.sock'
sbt 'kqueueDemo/run datagram --sock /tmp/gears-datagram.sock --local-sock /tmp/gears-client.sock -m hello'
```

Use distinct client and server paths, and a different client path for each
concurrent client. Bound Unix paths are removed when their resource scope exits;
force-stopping a process can leave a path behind, which the next bind removes.

IPv4 (`AF_INET`) and IPv6 (`AF_INET6`) demos default to the numeric localhost
addresses `127.0.0.1` and `::1`. Start each server before its corresponding client:

| Transport | Address family | Server | Client |
| --- | --- | --- | --- |
| TCP stream | IPv4 | `sbt 'kqueueDemo/run sock4-serve --port 9999'` | `sbt 'kqueueDemo/run sock4 --port 9999'` |
| TCP stream | IPv6 | `sbt 'kqueueDemo/run sock6-serve --port 9999'` | `sbt 'kqueueDemo/run sock6 --port 9999'` |
| UDP datagram | IPv4 | `sbt 'kqueueDemo/run datagram4-serve --port 9999'` | `sbt 'kqueueDemo/run datagram4 --port 9999 -m hello'` |
| UDP datagram | IPv6 | `sbt 'kqueueDemo/run datagram6-serve --port 9999'` | `sbt 'kqueueDemo/run datagram6 --port 9999 -m hello'` |

`--host` overrides the numeric address; DNS names and IPv6 scope suffixes are not
resolved. Ports must be in `0..65535` (binding port `0` asks the OS to choose one).
UDP clients receive an ephemeral local port automatically. Datagram clients send
one packet, wait up to five seconds for socket readiness or a reply, and exit.
Empty datagrams are valid; there is no stream length header. The receive buffer is
64 KiB and truncation is reported as an error; the OS may impose smaller send limits.
Stream demos use a four-byte big-endian request length (up to 1 MiB) and close the
connection after replying.

The key-value demos are a pipelined request and response service in the style of
Redis, over the same length-framed protocol. The server keeps each connection open,
answers `SET key value`, `GET key`, and `INCR key` in order, and writes replies while
further commands are still arriving. The client pipelines a batch of `--count`
increments without waiting, reading the replies as they come back, so each socket has
a read and a write pending at the same time. `kv4` and `kv6` use IP addresses:

```bash
sbt 'kqueueDemo/run kv-serve --sock /tmp/gears-kv.sock'
sbt 'kqueueDemo/run kv --sock /tmp/gears-kv.sock --count 20000'
```

`whois` pulls a real reply off the internet with the WHOIS protocol on port 43. It
resolves the host, sends one query line, and prints the raw text until the server
closes the connection. Name resolution is the `Resolve` command: there is no
non-blocking `getaddrinfo`, so it runs on a worker thread and signals the reactor
through a pipe, the way libuv does. `resolve` runs just that step:

```bash
sbt 'kqueueDemo/run whois --host whois.iana.org --query example.com'
sbt 'kqueueDemo/run resolve --host localhost'
```

Every reactor demo also has a gears mirror in `kqueue-demo/src/main/scala/gears-example`,
run by putting `gears` before the command, for example `kqueueDemo/run gears kv-serve
--sock /tmp/gears-kv.sock`. Each completion step becomes a call to `ReactorFutures.perform`, which suspends
the caller until the op finishes, so the code reads sequentially; wrapping a call in a
`Future` runs the op alongside other work. Reactors are thread-owned: `ReactorFutures.run` makes the calling
thread the reactor thread, running the program as a future under a scheduler bound to
that thread and the reactor's loop as its event loop, so futures submit to the reactor
directly and everything a program spawns stays on that thread (there is no work stealing
or balancing yet). The test suites run against both versions.

Stop the long-running servers with Ctrl+C. Use `sbt 'show kqueueDemo/nativeLink'`
to build and locate the executable for running directly.

The `kqueueDemoTests` project holds JVM munit suites that link the demo executable
and drive it as a subprocess with Java sockets and FIFOs. They check timer expiry,
FIFO read/write readiness, and all six socket combinations, including fragmented
stream requests, queued datagrams, empty packets, multiple senders, and invalid
addresses. Java has no Unix domain datagram sockets, so that case uses native
clients only. The suites skip themselves on other operating systems.

```bash
sbt kqueueDemoTests/test
```
