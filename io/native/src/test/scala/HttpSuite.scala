import gears.async.*
import gears.async.asyncio.uring.UringPerThreadSupport
import gears.async.net.{DnsSupport, TcpSupport}

class HttpSuite extends HttpBehavior:
  private lazy val support = UringPerThreadSupport(parallelism = 2)
  private given UringPerThreadSupport = support
  protected given tcp: TcpSupport = support.tcpSupport
  protected given dns: DnsSupport = support.dnsSupport
  protected given operations: AsyncOperations = support

  protected def run[A](body: Async.Spawn ?=> A): A = Async.blocking(body)

  override def afterAll(): Unit = support.close()
