package example.tests

/** The same checks against the gears mirrors in `src/main/scala/gears-example`, run as `kqueuedemo gears <command>`. */
class GearsKQueueDemoSuite extends KQueueDemoSuite {
  override def commandPrefix: Seq[String] = Seq("gears")
}

class GearsSocketDemoSuite extends SocketDemoSuite {
  override def commandPrefix: Seq[String] = Seq("gears")
}
