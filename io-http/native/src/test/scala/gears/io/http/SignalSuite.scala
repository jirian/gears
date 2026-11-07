package gears.io.http

import gears.async.AsyncOperations
import gears.async.Future

import java.io.IOException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.DurationInt

import munit.FunSuite

/** `Http.serve` stops gracefully on SIGTERM. Native only: on the JVM the signal would end the test's own process. */
class SignalSuite extends FunSuite {
  override val munitTimeout = scala.concurrent.duration.Duration(60, "s")

  test("SIGTERM shuts Http.serve down gracefully: the request under way finishes, and serve returns") {
    val port = 20_000 + scala.util.Random.nextInt(20_000)
    val returned = new CountDownLatch(1)
    val serving = new Thread(() => {
      val routes = Routes(GET("/up") { _ => "up" }, GET("/slow") { _ => AsyncOperations.sleep(500); "done" })
      Http.serve(ServerConfig(host = "127.0.0.1", port = port, log = _ => ()), grace = 5.seconds)(routes)
      returned.countDown()
    })
    serving.setDaemon(true)
    serving.start()
    Http.run {
      var up = false
      while !up do
        try up = Http.get(s"http://127.0.0.1:$port/up").text() == "up"
        catch case _: IOException => AsyncOperations.sleep(20)
      val slow = Future(Http.get(s"http://127.0.0.1:$port/slow"))
      AsyncOperations.sleep(100)
      scala.scalanative.libc.signal.raise(scala.scalanative.libc.signal.SIGTERM)
      assertEquals(slow.await.text(), "done")
    }
    assert(returned.await(10, TimeUnit.SECONDS), "serve returned")
  }
}
