package gearsbench

import gears.async.Async
import gears.async.Future
import gears.async.Future.awaitAll
import gears.async.io.Blocking
import gears.async.io.Interface
import gears.async.io.Io
import gears.async.io.Received
import gears.async.io.Slot
import gears.async.io.StreamSockets
import gears.async.io.UnixAddress
import gears.async.io.testkit.BufferArena
import gears.io.demos.{DemoRunner, Endpoint, Sockets}

trait BenchRunner[+I <: Interface] extends DemoRunner[I] {
  def ops: I
  def onPool[T](workers: Int)(program: Io[I] ?=> Async.Spawn ?=> T): T
}

import java.io.IOException
import java.nio.file.Files
import java.nio.file.Paths
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicReference

object GearsBench {

  inline val MessageSize = 64

  val LargeMessage: Int = sys.env.get("GEARS_BENCH_LARGE").map(_.toInt).getOrElse(128 * 1024)

  final case class Scenario(
      name: String,
      stresses: String,
      baseCount: Int,
      body: Client,
      runsOn: Interface => Boolean = _ => true
  )

  trait Connect[H] {
    def apply()(using Async): H
  }

  trait Client {
    def apply(io: Io[StreamSockets], connect: Connect[io.ops.Connection], count: Int, connections: Int)(using
        Async.Spawn
    ): Unit
  }

  val scenarios: Seq[Scenario] = Seq(
    Scenario(
      "pingpong",
      "one message in flight: a resume and a readiness wait per message on each side",
      50_000,
      new Client {
        def apply(io: Io[StreamSockets], connect: Connect[io.ops.Connection], count: Int, connections: Int)(using
            Async.Spawn
        ): Unit =
          pingPong(io, connect, count, deadline = false)
      }
    ),
    Scenario(
      "pingpong-fresh-ops",
      "as pingpong, but building the write and read ops anew for every message, as code that does not keep them does",
      50_000,
      new Client {
        def apply(io: Io[StreamSockets], connect: Connect[io.ops.Connection], count: Int, connections: Int)(using
            Async.Spawn
        ): Unit =
          pingPong(io, connect, count, deadline = false, freshOps = true)
      }
    ),
    Scenario(
      "pingpong-pooled",
      "as pingpong, reading each echo into a buffer of the backend's pool, which a backend that receives into it hands "
        + "over without a copy",
      50_000,
      new Client {
        def apply(io: Io[StreamSockets], connect: Connect[io.ops.Connection], count: Int, connections: Int)(using
            Async.Spawn
        ): Unit =
          pingPong(io, connect, count, deadline = false, pooled = true)
      }
    ),
    Scenario(
      "large",
      s"as pipelined, with messages of $LargeMessage bytes and reads into a buffer that fits one: a read's copies, "
        + "at a size where they count",
      2_000,
      new Client {
        def apply(io: Io[StreamSockets], connect: Connect[io.ops.Connection], count: Int, connections: Int)(using
            Async.Spawn
        ): Unit =
          pipelined(io, connect, count, messageSize = LargeMessage)
      }
    ),
    Scenario(
      "large-pooled",
      "as large, reading into the backend's pool, whose buffers fit one message too",
      2_000,
      new Client {
        def apply(io: Io[StreamSockets], connect: Connect[io.ops.Connection], count: Int, connections: Int)(using
            Async.Spawn
        ): Unit =
          pipelined(io, connect, count, messageSize = LargeMessage, pooled = true)
      }
    ),
    Scenario(
      "pingpong-deadline",
      "as pingpong, with a time limit on every op of the round trip, as a request with a timeout has",
      50_000,
      new Client {
        def apply(io: Io[StreamSockets], connect: Connect[io.ops.Connection], count: Int, connections: Int)(using
            Async.Spawn
        ): Unit =
          pingPong(io, connect, count, deadline = true)
      }
    ),
    Scenario(
      "concurrent",
      "many connections ping-ponging at once: many events and registrations per turn",
      100_000,
      new Client {
        def apply(io: Io[StreamSockets], connect: Connect[io.ops.Connection], count: Int, connections: Int)(using
            Async.Spawn
        ): Unit = {
          val each = math.max(1, count / connections)
          (1 to connections).map(_ => Future(pingPong(io, connect, each, deadline = false))).awaitAll
        }
      }
    ),
    Scenario(
      "concurrent-compute",
      "as concurrent, with a slice of computation after each round trip, in the connection's own future",
      100_000,
      new Client {
        def apply(io: Io[StreamSockets], connect: Connect[io.ops.Connection], count: Int, connections: Int)(using
            Async.Spawn
        ): Unit = {
          val each = math.max(1, count / connections)
          (1 to connections)
            .map(_ => Future(pingPong(io, connect, each, deadline = false, work = Work.Inline)))
            .awaitAll
        }
      }
    ),
    Scenario(
      "concurrent-offload",
      "as concurrent-compute, with each slice in a future of its own, awaited before the next round trip",
      100_000,
      new Client {
        def apply(io: Io[StreamSockets], connect: Connect[io.ops.Connection], count: Int, connections: Int)(using
            Async.Spawn
        ): Unit = {
          val each = math.max(1, count / connections)
          (1 to connections)
            .map(_ => Future(pingPong(io, connect, each, deadline = false, work = Work.Offload)))
            .awaitAll
        }
      }
    ),
    Scenario(
      "connections",
      "a new connection for each round trip: connect, one message and its echo, close; the server accepts each",
      20_000,
      new Client {
        def apply(io: Io[StreamSockets], connect: Connect[io.ops.Connection], count: Int, connections: Int)(using
            Async.Spawn
        ): Unit = {
          var i = 0
          while i < count do {
            pingPong(io, connect, 1, deadline = false)
            i += 1
          }
        }
      }
    ),
    Scenario(
      "pipelined",
      "one connection, each message its own write while replies are read at the same time",
      200_000,
      new Client {
        def apply(io: Io[StreamSockets], connect: Connect[io.ops.Connection], count: Int, connections: Int)(using
            Async.Spawn
        ): Unit =
          pipelined(io, connect, count)
      }
    ),
    Scenario(
      "blocking",
      "trivial blocking tasks one after another: a wake-up from a worker thread for each",
      100_000,
      new Client {
        def apply(io: Io[StreamSockets], connect: Connect[io.ops.Connection], count: Int, connections: Int)(using
            Async.Spawn
        ): Unit = {
          val b: Blocking & io.ops.type = blocking(io)
          var i = 0
          while i < count do {
            io.perform(b.blocking(() => ()))
            i += 1
          }
        }
      },
      _.isInstanceOf[Blocking]
    ),
    Scenario(
      "blocking-io",
      "a trivial blocking task and a round trip in turn on one connection: moving between blocking work and I/O",
      50_000,
      new Client {
        def apply(io: Io[StreamSockets], connect: Connect[io.ops.Connection], count: Int, connections: Int)(using
            Async.Spawn
        ): Unit =
          pingPong(io, connect, count, deadline = false, blockingFirst = true)
      },
      _.isInstanceOf[Blocking]
    ),
    Scenario(
      "timers",
      s"timers armed $TimerBatch at a time, each by a future performing it, half firing at once and half cancelled: "
        + "the cost of many pending timers",
      200_000,
      new Client {
        def apply(io: Io[StreamSockets], connect: Connect[io.ops.Connection], count: Int, connections: Int)(using
            Async.Spawn
        ): Unit =
          timerStress(io, count)
      }
    ),
    Scenario(
      "rearm-cancel",
      "one timer re-armed again and again, as QUIC re-arms its loss timer, by cancelling the future performing it and "
        + "performing a new one: how it was done before deadline timers",
      20_000,
      new Client {
        def apply(io: Io[StreamSockets], connect: Connect[io.ops.Connection], count: Int, connections: Int)(using
            Async.Spawn
        ): Unit =
          rearmByCancel(io, count)
      }
    ),
    Scenario(
      "rearm-later",
      "one deadline timer, performed once by one future, its deadline moved later for each re-arm (Deadline.moveTo), "
        + "QUIC's usual case",
      2_000_000,
      new Client {
        def apply(io: Io[StreamSockets], connect: Connect[io.ops.Connection], count: Int, connections: Int)(using
            Async.Spawn
        ): Unit =
          rearmByMove(io, count, later = true)
      }
    ),
    Scenario(
      "rearm-earlier",
      "the same, its deadline moved earlier each time, which files it again in the worker's heap",
      2_000_000,
      new Client {
        def apply(io: Io[StreamSockets], connect: Connect[io.ops.Connection], count: Int, connections: Int)(using
            Async.Spawn
        ): Unit =
          rearmByMove(io, count, later = false)
      }
    ),
    Scenario(
      "fairness",
      "pipelined traffic while a ticker on the same thread performs zero-length timers; also reports fairness-tick, "
        + "the time between ticks, which rises if busy futures stop yielding",
      200_000,
      new Client {
        def apply(io: Io[StreamSockets], connect: Connect[io.ops.Connection], count: Int, connections: Int)(using
            Async.Spawn
        ): Unit =
          fairness(io, connect, count)
      }
    ),
    Scenario(
      "starvation",
      "concurrent's round trips while a compute future per core works in slices, yielding between them, and a checker "
        + "sleeps 1 ms over and over, as cats-effect's CPU starvation check does; also reports starvation-p50, -p99 "
        + "and -max: how late, in µs, the checker woke",
      100_000,
      new Client {
        def apply(io: Io[StreamSockets], connect: Connect[io.ops.Connection], count: Int, connections: Int)(using
            Async.Spawn
        ): Unit =
          starvation(io, connect, count, connections)
      }
    ),
    Scenario(
      "mixed",
      "pingpong while a compute future on the same thread works in slices, yielding between them; also reports "
        + "mixed-compute, the time per slice meanwhile",
      50_000,
      new Client {
        def apply(io: Io[StreamSockets], connect: Connect[io.ops.Connection], count: Int, connections: Int)(using
            Async.Spawn
        ): Unit =
          mixed(io, connect, count)
      }
    )
  )

  private def blocking(io: Io[Interface]): Blocking & io.ops.type = io.ops match {
    case blocking: (Blocking & io.ops.type) => blocking
    case _                                  => throw new IllegalArgumentException(s"$io has no blocking ops")
  }

  inline val TimerBatch = 1000

  private def timerStress(io: Io[Interface], count: Int)(using Async.Spawn): Unit = {
    var done = 0
    while done < count do {
      val batch = math.min(TimerBatch, count - done)
      val firing = scala.collection.mutable.ArrayBuffer.empty[Future[Unit]]
      val cancelled = scala.collection.mutable.ArrayBuffer.empty[Future[Unit]]
      var i = 0
      while i < batch do {
        if i % 2 == 0 then firing += Future(io.perform(io.ops.timer(0)))
        else cancelled += Future(io.perform(io.ops.timer(60_000)))
        i += 1
      }
      firing.foreach(_.await)
      cancelled.foreach(_.cancel())
      cancelled.foreach(_.awaitResult)
      done += batch
    }
  }

  private def rearmByCancel(io: Io[Interface], count: Int)(using Async.Spawn): Unit = {
    var pending = Future(io.perform(io.ops.timer(60_000)))
    io.perform(io.ops.timer(0))
    var i = 0
    while i < count do {
      val next = Future(io.perform(io.ops.timer(60_000)))
      pending.cancel()
      pending.awaitResult
      pending = next
      i += 1
    }
    pending.cancel()
    pending.awaitResult: Unit
  }

  private def rearmByMove(io: Io[Interface], count: Int, later: Boolean)(using Async.Spawn): Unit = {
    val base = System.nanoTime() + 60_000_000_000L
    val timer = io.ops.deadlineTimer(if later then base else base + count)
    val waiting = Future(io.perform(timer))
    io.perform(io.ops.timer(0))
    var i = 0
    while i < count do {
      timer.moveTo(if later then base + i else base + count - i - 1)
      i += 1
    }
    timer.moveTo(System.nanoTime())
    waiting.await
  }

  def report(name: String, count: Long, seconds: Double): Unit =
    println(f"bench: $name%-18s $count%10d $seconds%9.3f ${count / seconds}%12.0f ${seconds * 1e6 / count}%8.2f")

  @volatile private var computeSink = 0L

  private def computeSlice(seed: Long, steps: Int = 10_000): Long = {
    var x = seed | 1L
    var i = 0
    while i < steps do {
      x ^= x << 13
      x ^= x >>> 7
      x ^= x << 17
      i += 1
    }
    x
  }

  private def mixed(io: Io[StreamSockets], connect: Connect[io.ops.Connection], count: Int)(using Async.Spawn): Unit = {
    val pingPongDone =
      new java.util.concurrent.atomic.AtomicBoolean
    var slices = 0L
    val start = System.nanoTime()
    val compute = Future {
      var result = 0L
      while !pingPongDone.get() do {
        result += computeSlice(slices)
        io.`yield`()
        slices += 1
      }
      computeSink = result
    }
    pingPong(io, connect, count, deadline = false)
    pingPongDone.set(true)
    val seconds = (System.nanoTime() - start) / 1e9
    compute.await
    report("mixed-compute", slices, seconds)
  }

  inline val StarvationCheckMillis = 1

  private def starvation(io: Io[StreamSockets], connect: Connect[io.ops.Connection], count: Int, connections: Int)(using
      Async.Spawn
  ): Unit = {
    val done = new java.util.concurrent.atomic.AtomicBoolean
    val interval = StarvationCheckMillis * 1_000_000L
    val late = scala.collection.mutable.ArrayBuffer.empty[Long]
    val checker = Future {
      val timer = io.ops.timer(StarvationCheckMillis)
      while !done.get() do {
        val started = System.nanoTime()
        io.perform(timer)
        late += math.max(0L, System.nanoTime() - started - interval)
      }
    }
    val computes = (1 to Runtime.getRuntime.availableProcessors).map { core =>
      Future {
        var result = 0L
        var slice = core.toLong
        while !done.get() do {
          result += computeSlice(slice)
          io.`yield`()
          slice += 1
        }
        if result == 1L then computeSink = result
      }
    }
    val each = math.max(1, count / connections)
    (1 to connections).map(_ => Future(pingPong(io, connect, each, deadline = false))).awaitAll
    done.set(true)
    checker.await
    computes.awaitAll
    val sorted = late.sorted
    def at(fraction: Double): Long = if sorted.isEmpty then 0L else sorted(((sorted.length - 1) * fraction).toInt)
    report("starvation-p50", 1, at(0.5) / 1e9)
    report("starvation-p99", 1, at(0.99) / 1e9)
    report("starvation-max", 1, at(1.0) / 1e9)
  }

  private def withConnection(io: Io[StreamSockets], connect: Connect[io.ops.Connection])(
      body: io.ops.Connection => Unit
  )(using
      Async
  ): Unit = {
    val fd = connect()
    try {
      body(fd)
    } finally io.perform(io.ops.close(fd))
  }

  enum Work { case NoWork, Inline, Offload }

  inline val WorkSteps = 2_000

  private def pingPong(
      io: Io[StreamSockets],
      connect: Connect[io.ops.Connection],
      count: Int,
      deadline: Boolean,
      freshOps: Boolean = false,
      blockingFirst: Boolean = false,
      work: Work = Work.NoWork,
      pooled: Boolean = false,
      messageSize: Int = MessageSize
  )(using Async.Spawn): Unit = {
    val pool = if pooled then io.backend.pool(messageSize, 2) else null
    try
      withConnection(io, connect) { fd =>
        val arena = BufferArena.open()
        try {
          val buf = arena.allocate(messageSize)
          val write = io.ops.write(fd, buf)
          val read = io.ops.read(fd, buf)
          val into = Slot[Received]()
          val pooledRead = if pooled then io.ops.read(fd, pool.nn, into) else null
          def limited(op: io.Op): Unit =
            if deadline then io.perform(op, scala.concurrent.duration.FiniteDuration(60, "s")) else io.perform(op)
          val blocker: Option[Blocking & io.ops.type] = if blockingFirst then Some(blocking(io)) else None
          var worked = 0L
          var i = 0
          while i < count do {
            blocker match {
              case Some(b) => io.perform(b.blocking(() => ()))
              case None    => ()
            }
            buf.clear()
            while buf.hasRemaining() do limited(if freshOps then io.ops.write(fd, buf) else write)
            if pooled then {
              var got = 0
              while got < messageSize do {
                limited(pooledRead.nn)
                val received = into.clear()
                val n = received.buffer.remaining()
                received.release()
                if n == 0 then throw new IOException("The server closed the connection")
                got += n
              }
            } else {
              buf.clear()
              while buf.hasRemaining() do {
                val before = buf.position()
                limited(if freshOps then io.ops.read(fd, buf) else read)
                if buf.position() == before then throw new IOException("The server closed the connection")
              }
            }
            work match {
              case Work.NoWork  => ()
              case Work.Inline  => worked += computeSlice(i, WorkSteps)
              case Work.Offload => worked += Future(computeSlice(i, WorkSteps)).await
            }
            i += 1
          }
          if worked == 1L then computeSink = worked
        } finally arena.close()
      }
    finally if pool != null then pool.close()
  }

  private def pipelined(
      io: Io[StreamSockets],
      connect: Connect[io.ops.Connection],
      count: Int,
      messageSize: Int = MessageSize,
      pooled: Boolean = false
  )(using Async.Spawn): Unit = {
    val pool = if pooled then io.backend.pool(messageSize, 2) else null
    try
      withConnection(io, connect) { fd =>
        val arena = BufferArena.open()
        try {
          val total = count.toLong * messageSize
          val out = arena.allocate(messageSize)
          val in = arena.allocate(if messageSize > 8192 then messageSize else 8192)
          val writer = Future {
            val write = io.ops.write(fd, out)
            var i = 0
            while i < count do {
              out.clear()
              while out.hasRemaining() do io.perform(write)
              i += 1
            }
          }
          var received = 0L
          if pooled then {
            val into = Slot[Received]()
            val read = io.ops.read(fd, pool.nn, into)
            while received < total do {
              io.perform(read)
              val got = into.clear()
              val n = got.buffer.remaining()
              got.release()
              if n == 0 then throw new IOException("The server closed the connection")
              received += n
            }
          } else {
            val read = io.ops.read(fd, in)
            while received < total do {
              in.clear()
              io.perform(read)
              if in.position() == 0 then throw new IOException("The server closed the connection")
              received += in.position()
            }
          }
          writer.await
        } finally arena.close()
      }
    finally if pool != null then pool.close()
  }

  private def fairness(io: Io[StreamSockets], connect: Connect[io.ops.Connection], count: Int)(using
      Async.Spawn
  ): Unit = {
    val trafficDone = new java.util.concurrent.atomic.AtomicBoolean
    var ticks = 0L
    val start = System.nanoTime()
    val ticker = Future {
      while !trafficDone.get() do {
        io.perform(io.ops.timer(0))
        ticks += 1
      }
    }
    pipelined(io, connect, count)
    trafficDone.set(true)
    val seconds = (System.nanoTime() - start) / 1e9
    ticker.await
    report("fairness-tick", math.max(ticks, 1L), seconds)
  }

  private def echo(io: Io[StreamSockets], client: io.ops.Connection)(using Async): Unit = {
    val arena = BufferArena.open()
    try {
      val buf = arena.allocate(8192)
      val read = io.ops.read(client, buf)
      val write = io.ops.write(client, buf)
      var open = true
      while open do {
        buf.clear()
        io.perform(read)
        if buf.position() == 0 then open = false
        else {
          buf.flip()
          while buf.hasRemaining() do io.perform(write)
        }
      }
    } catch case _: IOException => ()
    finally {
      arena.close()
      io.perform(io.ops.close(client))
    }
  }

  private def startServer[B <: StreamSockets](
      endpoint: Endpoint[B],
      workers: Int
  )(using BenchRunner[B]): Unit = {
    val listening = new CountDownLatch(1)
    val failure = new AtomicReference[Throwable | Null](null)
    val thread = new Thread(() => serve(endpoint, listening, failure, workers), "bench-server")
    thread.setDaemon(true)
    thread.start()
    listening.await()
    val failed = failure.get()
    if failed != null then throw failed
  }

  private def serve[B <: StreamSockets](
      endpoint: Endpoint[B],
      listening: CountDownLatch,
      failure: AtomicReference[Throwable | Null],
      workers: Int
  )(using BenchRunner[B]): Unit =
    try
      summon[BenchRunner[B]].onPool(workers) { io ?=>
        val server =
          try endpoint.listen()
          finally listening.countDown()
        Sockets.serve(server)(client => echo(io, client))
      }
    catch {
      case t: Throwable =>
        failure.set(t)
        listening.countDown()
    }

  def serveForever[B <: StreamSockets](endpoint: Endpoint[B], workers: Int = 1)(using
      BenchRunner[B]
  ): Unit = {
    val listening = new CountDownLatch(1)
    val failure = new AtomicReference[Throwable | Null](null)
    serve(endpoint, listening, failure, workers)
    val failed = failure.get()
    if failed != null then throw failed
  }

  def run[B <: StreamSockets](
      names: Seq[String],
      scale: Double,
      connections: Int,
      endpoint: Endpoint[B],
      workers: Int = 1,
      warmup: Boolean = false,
      server: Boolean = true
  )(using runner: BenchRunner[B]): Unit = {
    val ops = runner.ops
    val selected =
      if names.isEmpty then scenarios
      else
        names.map(name =>
          scenarios
            .find(_.name == name)
            .getOrElse(
              throw new IllegalArgumentException(
                s"Unknown scenario `$name`; try ${scenarios.map(_.name).mkString(", ")}"
              )
            )
        )
    for scenario <- selected if !scenario.runsOn(ops) do
      System.err.println(s"skipping ${scenario.name}: $ops does not have what it needs")
    val runnable = selected.filter(_.runsOn(ops))
    Sockets.removeFile(endpoint.address)
    try {
      if server then startServer(endpoint, workers)
      summon[BenchRunner[B]].onPool(workers) { io ?=>
        val connect = new Connect[io.ops.Connection] {
          def apply()(using Async): io.ops.Connection = endpoint.connect()
        }
        if warmup then
          for scenario <- runnable do
            scenario.body(io, connect, math.max(1, (scenario.baseCount * scale / 5).toInt), connections)
        println(f"bench: ${"scenario"}%-18s ${"ops"}%10s ${"seconds"}%9s ${"ops/s"}%12s ${"us/op"}%8s")
        for scenario <- runnable do {
          val count = math.max(1, (scenario.baseCount * scale).toInt)
          val start = System.nanoTime()
          scenario.body(io, connect, count, connections)
          val seconds = (System.nanoTime() - start) / 1e9
          report(scenario.name, count, seconds)
        }
      }
    } finally Sockets.removeFile(endpoint.address)
  }
}
