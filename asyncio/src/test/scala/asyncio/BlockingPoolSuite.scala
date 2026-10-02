package asyncio

import java.util.concurrent.CountDownLatch
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeUnit
import munit.FunSuite

class BlockingPoolSuite extends FunSuite {
  test("the blocking pool is bounded, rejects saturation, and drains queued work on close") {
    val pool = new BlockingPool(parallelism = 1, queueCapacity = 1, threadNamePrefix = "test-blocker")
    val started = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val queued = new CountDownLatch(1)
    pool.execute(() => {
      started.countDown()
      release.await()
    })
    assert(started.await(2, TimeUnit.SECONDS))
    pool.execute(() => queued.countDown())
    intercept[RejectedExecutionException](pool.execute(() => ()))
    pool.close()
    intercept[RejectedExecutionException](pool.execute(() => ()))
    release.countDown()
    assert(queued.await(2, TimeUnit.SECONDS))
  }
}
