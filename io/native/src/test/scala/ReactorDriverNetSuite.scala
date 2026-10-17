import gears.async.*
import gears.async.asyncio.reactor.{SingleReactorDriver, dnsSupport, tcpSupport, udpSupport}
import gears.async.default.given
import gears.async.net.{DnsSupport, TcpSupport, UdpSupport}

class ReactorDriverNetSuite extends NetBehavior("driver"):
  private lazy val driver = SingleReactorDriver(() => TestReactor.open())
  protected given tcp: TcpSupport = driver.tcpSupport
  protected given udp: UdpSupport = driver.udpSupport
  protected def dns: DnsSupport = driver.dnsSupport
  protected given operations: AsyncOperations = gears.async.default.DefaultSupport

  protected def run[A](body: Async.Spawn ?=> A): A = Async.blocking(body)

  override def afterAll(): Unit = driver.close()
