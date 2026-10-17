package gears.asyncio.uring

import gears.asyncio.ResolvedAddress
import gears.asyncio.unsafe.PosixResolver

trait HostResolver {

  def resolve(host: String): Either[String, List[ResolvedAddress]]
}

object PosixHostResolver extends HostResolver {
  def resolve(host: String): Either[String, List[ResolvedAddress]] = PosixResolver.lookup(host)
}
