import gears.async.*
import gears.async.default.given
import gears.io.net.{DnsSupport, TcpSupport}
import gears.io.reactor.{SingleReactorDriver, dnsSupport, tcpSupport}

class ReactorDriverHttpSuite extends HttpBehavior:
  private lazy val driver = SingleReactorDriver(() => TestReactor.open())
  protected given tcp: TcpSupport = driver.tcpSupport
  protected given dns: DnsSupport = driver.dnsSupport
  protected given operations: AsyncOperations = gears.async.default.DefaultSupport

  protected def run[A](body: Async.Spawn ?=> A): A = Async.blocking(body)

  override def afterAll(): Unit = driver.close()
