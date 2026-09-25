package gears.async.asyncio.uring

import gears.async.{Async, Future}
import gears.async.net.DnsSupport

import java.net.InetAddress

object NativeDnsSupport extends DnsSupport:
  override def resolve(host: String)(using Async): Array[InetAddress] =
    Future
      .withResolver[Array[InetAddress]]: resolver =>
        val t = new Thread(() =>
          try resolver.resolve(InetAddress.getAllByName(host))
          catch case e: Exception => resolver.reject(e)
        )
        t.setDaemon(true)
        t.start()
      .link()
      .await
