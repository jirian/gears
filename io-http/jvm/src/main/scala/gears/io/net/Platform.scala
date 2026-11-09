package gears.io.net

/** The JVM's default backend: the reference backend, each op performed on its future's own virtual thread. */
object Platform {
  val backend: gears.async.io.reference.ReferenceBackend.type = gears.async.io.reference.ReferenceBackend

  /** `n` random bytes, for a WebSocket client's masks. */
  def randomBytes(n: Int): Array[Byte] = {
    val out = new Array[Byte](n)
    random.nextBytes(out)
    out
  }

  private val random = new java.security.SecureRandom()

  /** Runs `stop` once the process is asked to end, by SIGTERM or SIGINT, from a shutdown hook, and returns what to call
    * once the process may end, which the hook waits for, for at most a minute. For `Http.serve`.
    */
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
      catch case _: IllegalStateException => () // the process is ending: the hook is running
    }
  }
}
