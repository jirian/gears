package gears.io.uring

import gears.io.reactor.ResolvedAddress
import gears.io.unsafe.PosixResolver

/** Looks up host names for `Ops.resolve`. It is called on a `BlockingPool` thread, so it may block. */
trait HostResolver {

  /** The numeric addresses of `host`, or an error message. */
  def resolve(host: String): Either[String, List[ResolvedAddress]]
}

/** `getaddrinfo(3)`. */
object PosixHostResolver extends HostResolver {
  def resolve(host: String): Either[String, List[ResolvedAddress]] = PosixResolver.lookup(host)
}
