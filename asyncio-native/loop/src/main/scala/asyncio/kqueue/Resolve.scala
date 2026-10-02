package asyncio.kqueue

import java.io.IOException

import asyncio.ResolvedAddress
import asyncio.Slot
import asyncio.unsafe.PosixResolver

/** Resolves a host name without blocking the reactor. There is no non-blocking `getaddrinfo`, so the lookup runs on the
  * reactor's blocker pool, the way libuv and tokio do it. Completes after writing the addresses into `into`, or fails
  * with an `IOException` carrying the resolver's error.
  */
final class Resolve(val host: String, into: Slot[List[ResolvedAddress]]) extends KqueueBlocking {
  def block(): Unit = PosixResolver.lookup(host) match {
    case Right(addresses) => into.set(addresses)
    case Left(error)      => throw new IOException(s"Failed to resolve $host: $error")
  }
}
