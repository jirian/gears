import gears.async.*
import gears.async.asyncio.reactor.{SingleReactorDriver, dnsSupport, tcpSupport}
import gears.async.default.given
import gears.async.net.{DnsSupport, TcpSupport}

class ReactorDriverHttpSuite extends HttpBehavior:
  private lazy val driver = SingleReactorDriver(() => TestReactor.open())
  protected given tcp: TcpSupport = driver.tcpSupport
  protected given dns: DnsSupport = driver.dnsSupport
  protected given operations: AsyncOperations = gears.async.default.DefaultSupport

  protected def run[A](body: Async.Spawn ?=> A): A = Async.blocking(body)

  override def afterAll(): Unit = driver.close()
