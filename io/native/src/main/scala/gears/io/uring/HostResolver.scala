package gears.io.uring

import gears.io.reactor.ResolvedAddress
import gears.io.unsafe.PosixResolver

trait HostResolver {

  def resolve(host: String): Either[String, List[ResolvedAddress]]
}

object PosixHostResolver extends HostResolver {
  def resolve(host: String): Either[String, List[ResolvedAddress]] = PosixResolver.lookup(host)
}
