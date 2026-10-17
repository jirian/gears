import gears.async.asyncio.reactor.ReactorDriver
import gears.async.asyncio.uring.UringPerThreadSupport

class UringFileSuite extends FileBehavior:
  private lazy val support = UringPerThreadSupport(parallelism = 2)
  protected def driver: ReactorDriver = support
  override def afterAll(): Unit = support.close()
