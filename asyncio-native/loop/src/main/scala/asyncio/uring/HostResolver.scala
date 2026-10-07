package asyncio.uring

import asyncio.ResolvedAddress
import asyncio.unsafe.PosixResolver

/** How a `UringReactor` resolves host names for `Ops.resolve`. The lookup runs on the reactor's blocker pool, so it may
  * block; an implementation may cache, or use a platform-specific resolver.
  */
trait HostResolver {

  /** Every address of `host`, in numeric form, or the resolver's error message. */
  def resolve(host: String): Either[String, List[ResolvedAddress]]
}

/** Resolves with POSIX `getaddrinfo`, through `asyncio.unsafe.PosixResolver`. */
object PosixHostResolver extends HostResolver {
  def resolve(host: String): Either[String, List[ResolvedAddress]] = PosixResolver.lookup(host)
}
