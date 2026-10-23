package gears.io.examples

import gears.async.Async
import gears.io.uring.UringPerThreadSupport
import gears.io.{Error, Reader}

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

/** Runs the examples named on the command line, or lists them. Exits 1 if any check failed. */
object Main:
  private val examples: Seq[(String, String, () => Unit)] = Seq(
    ("tcp-echo", "TCP listen, accept, connect, socket options and half-close", tcpEchoExample),
    ("udp", "UDP datagrams between two bound sockets", udpExample),
    ("unix-socket", "stream and datagram sockets on Unix domain paths", unixSocketExample),
    ("dns", "host name resolution that does not block a shard", dnsExample),
    ("file-read", "reading a file, directly and buffered", fileReadExample),
    ("file-write", "writing a file and reading it back", fileWriteExample),
    (
      "file-positioned",
      "positioned reads and writes, seek, metadata, truncate and write options",
      filePositionedExample
    ),
    ("cancellation", "timeouts, cancelled futures and closing a stream under a pending read", cancellationExample),
    ("http-client", "an HTTP client against a local router", httpClientExample),
    ("http-streaming", "streamed uploads, chunked responses and path parameters", httpStreamingExample),
    ("https", "HTTPS with certificate verification", httpsExample),
    ("websocket", "a WebSocket endpoint and client: text, binary, ping and close", webSocketExample),
    ("reactor", "the gears.io.reactor.Reactor interface used directly: ops, runOnce and wakeup", reactorExample),
    ("driver", "the gears network layer on any reactor through SingleReactorDriver", driverExample),
    ("http-server", "a long-running HTTP server on port 8080", httpServer),
    ("https-server", "a long-running HTTPS server on port 8443", httpsServer)
  )

  /** Runs the examples named in `args`, or lists them if `args` is empty or names an unknown one.
    *
    * @param args
    *   the names of the examples to run
    */
  def main(args: Array[String]): Unit =
    if args.isEmpty || !args.forall(name => examples.exists(_._1 == name)) then
      println("usage: nativeExamples/run <example>...")
      examples.foreach((name, about, _) => println(f"  $name%-16s $about"))
      System.exit(2)
    for name <- args do
      println(s"== $name")
      try examples.find(_._1 == name).get._3()
      catch
        case e: Throwable =>
          fail(s"$name threw $e")
    if failures.get() > 0 then
      println(s"${failures.get()} check(s) failed")
      System.exit(1)
    System.exit(0)

private val failures = java.util.concurrent.atomic.AtomicInteger()

/** Prints a check's result and counts a failure.
  *
  * @param ok
  *   whether the check passed
  * @param what
  *   what was checked
  */
private[examples] def check(ok: Boolean, what: String): Unit =
  println(s"  $what: ${if ok then "ok" else "FAILED"}")
  if !ok then failures.incrementAndGet()

/** Prints a failure and counts it.
  *
  * @param message
  *   what went wrong
  */
private[examples] def fail(message: String): Unit =
  println(s"  FAILED: $message")
  failures.incrementAndGet()

/** Runs `body` with a fresh io_uring runtime and closes the runtime afterwards.
  *
  * @param body
  *   the example, given the runtime
  */
private[examples] def withUring(body: UringPerThreadSupport ?=> Unit): Unit =
  val support = UringPerThreadSupport()
  try body(using support)
  finally support.close()

/** Returns the value of `result`, or throws an exception naming `what`.
  *
  * @tparam E
  *   the error type
  * @tparam A
  *   the value type
  * @param what
  *   what produced `result`
  * @param result
  *   the outcome to unwrap
  */
private[examples] def orFail[E, A](what: String)(result: Either[E, A]): A =
  result.fold(e => throw RuntimeException(s"$what failed: $e"), identity)

/** Reads `reader` to its end as UTF-8.
  *
  * @param reader
  *   the reader to drain
  * @return
  *   everything it read, decoded
  */
private[examples] def readToEnd(reader: Reader)(using Async): String =
  val out = java.io.ByteArrayOutputStream()
  val buf = ByteBuffer.allocate(1024)
  while reader.readBuf(buf) != Left(Error.EOF) do
    out.write(buf.array(), 0, buf.position())
    buf.clear()
  out.toString(StandardCharsets.UTF_8)

private[examples] def bytes(text: String): ByteBuffer = ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8))
