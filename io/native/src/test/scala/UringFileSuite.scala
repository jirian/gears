import gears.io.reactor.ReactorDriver
import gears.io.uring.UringPerThreadSupport

class UringFileSuite extends FileBehavior:
  private lazy val support = UringPerThreadSupport(parallelism = 2)
  protected def driver: ReactorDriver = support
  override def afterAll(): Unit = support.close()
