package gears.async.asyncio.reactor

import gears.async.Async
import gears.async.net.DnsSupport
import gears.asyncio.{ResolvedAddress, Slot}

import java.io.IOException
import java.net.{InetAddress, UnknownHostException}

trait ReactorDnsSupport(driver: ReactorDriver) extends DnsSupport:
  override def resolve(host: String)(using Async): Array[InetAddress] =
    val addresses = Slot[List[ResolvedAddress]]()
    try driver.await(null)(r => r.ops.resolve(host, addresses))
    catch
      case e: IOException =>
        throw UnknownHostException(Option(e.getMessage).getOrElse(s"Failed to resolve $host")).initCause(e)
    addresses.clear().map(a => InetAddress.getByAddress(host, InetAddress.getByName(a.host).getAddress)).toArray
