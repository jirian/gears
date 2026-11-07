package gears.async.io
package nio

import gears.async.io.testkit.*
import gears.io.testkit.SimpleIOChecks
import gears.io.testkit.SimpleIOIpStreamChecks
import gears.io.testkit.SimpleIOResolveChecks

import java.net.Inet6Address
import java.net.NetworkInterface
import scala.jdk.CollectionConverters.*

/** Each suite sees only the capabilities required by its contracts. */
class NioContractSuite extends OpsContract {
  val backend: Backend = NioOnCaller
}

class NioThreadedTimerSuite extends ThreadedTimerContract {
  val backend: Backend = NioOnCaller
}

class NioBlockingSuite extends BlockingContract {
  val backend: Blocking = NioOnCaller
}

class NioPipeSuite extends PipeContract {
  val backend: Pipes = NioOnCaller
}

/** Runs the same stream contracts using TCP, without exposing Pipes to the suite. */
class NioSocketStreamSuite extends StreamContract with IpStreamFixture {
  val backend: IpStreams = NioOnCaller
}

class NioIpStreamSuite extends IpStreamContract {
  val backend: IpStreams = NioOnCaller
}

class NioIpDatagramSuite extends IpDatagramContract {
  val backend: IpDatagrams = NioOnCaller
}

class NioUnixStreamSuite extends UnixStreamContract {
  val backend: UnixStreams = NioOnCaller
}

class NioFileSuite extends FileContract {
  val backend: RegularFiles = NioOnCaller
}

class NioResolveSuite extends ResolveContract {
  val backend: Resolving = NioOnCaller

  override protected def namedScopedAddresses: List[(String, IpAddress)] =
    NetworkInterface
      .getNetworkInterfaces()
      .asScala
      .flatMap(interface =>
        interface.getInetAddresses().asScala.collect {
          case address: Inet6Address if address.isLinkLocalAddress() && address.getScopeId() > 0 =>
            (interface.getName(), address)
        }
      )
      .nextOption()
      .toList
      .flatMap { (name, address) =>
        val expected = IpAddress.fromBytes(Protocol.IPv6, address.getAddress(), address.getScopeId())
        val compressed = expected.toString.takeWhile(_ != '%')
        val expanded = address.getHostAddress().takeWhile(_ != '%')
        List(s"$compressed%$name", s"$expanded%$name", s"[$compressed%$name]").map(_ -> expected)
      }
}

class NioSimpleIOSuite extends SimpleIOChecks with PipeFixture {
  val backend: Pipes = NioOnCaller
}

class NioSimpleIOSocketSuite extends SimpleIOChecks with IpStreamFixture {
  val backend: IpStreams = NioOnCaller
}

class NioSimpleIOIpStreamSuite extends SimpleIOIpStreamChecks {
  val backend: IpStreams = NioOnCaller
}

class NioSimpleIOResolveSuite extends SimpleIOResolveChecks {
  val backend: Resolving = NioOnCaller
}
