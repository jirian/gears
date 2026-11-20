package gears.io.net

private[gears] object DefaultPlatform {
  val backend: gears.async.io.reference.ReferenceBackend.type = gears.async.io.reference.ReferenceBackend

  def onTermination(stop: () => Unit): () => Unit = {
    val finished = new java.util.concurrent.CountDownLatch(1)
    val hook = new Thread(
      () => {
        stop()
        finished.await(1, java.util.concurrent.TimeUnit.MINUTES): Unit
      },
      "gears-http-shutdown"
    )
    Runtime.getRuntime.addShutdownHook(hook)
    () => {
      finished.countDown()
      try Runtime.getRuntime.removeShutdownHook(hook): Unit
      catch case _: IllegalStateException => ()
    }
  }
}
