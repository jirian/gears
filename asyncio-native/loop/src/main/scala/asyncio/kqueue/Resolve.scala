package asyncio.kqueue

import java.io.IOException

import asyncio.ResolvedAddress
import asyncio.HostResolver
import asyncio.Slot

/** Resolves a host name without blocking the reactor. There is no non-blocking `getaddrinfo`, so the lookup runs on the
  * reactor's blocker pool, the way libuv and tokio do it. Completes after writing the addresses into `into`, or fails
  * with an `IOException` carrying the resolver's error.
  */
final class Resolve[Owner](val host: String, into: Slot[List[ResolvedAddress]], resolver: HostResolver)
    extends KqueueBlocking[Owner] {
  def block(): Unit = resolver.resolve(host) match {
    case Right(addresses) => into.set(addresses)
    case Left(error)      => throw new IOException(s"Failed to resolve $host: $error")
  }
}
