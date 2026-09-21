import gears.async.*
import gears.async.asyncio.uring.ForkJoinUringSupport
import gears.async.asyncio.uring.examples.{SkipException, UringNetTestBodies as B}
import gears.async.net.{TcpSupport, UdpSupport}

/** Test suite for the io_uring TCP and UDP backends. The assertions
  * themselves live in UringNetTestBodies (native/src/main/.../examples),
  * shared with UringNetTestRunner - a plain `@main` covering the same
  * checks, useful for a quick manual run without going through sbt's test
  * task. Each check gets its own fresh `ForkJoinUringSupport`, closed
  * before the next starts.
  */
class UringNetSuite extends munit.FunSuite:

  private def check(body: (Async.Spawn, TcpSupport, UdpSupport, AsyncOperations) ?=> Unit): Unit =
    given support: ForkJoinUringSupport = ForkJoinUringSupport()
    given tcp: TcpSupport = support.tcpSupport
    given udp: UdpSupport = support.udpSupport
    try
      Async.blocking:
        body(using summon[Async.Spawn], tcp, udp, support)
    catch case e: SkipException => assume(false, e.getMessage)
    finally support.ring.close()

  test("TCP: echo round-trip")(check(B.tcpEchoRoundTrip()))
  test("TCP: localAddress reflects the kernel-assigned ephemeral port")(check(B.tcpLocalAddressEphemeralPort()))
  test("TCP: SocketOptions apply without error")(check(B.tcpSocketOptionsApply()))
  test("TCP: close() racing a live read defers the syscall until it settles")(check(B.tcpDeferredCloseRace()))
  test("TCP: peer close with nothing sent yields EOF")(check(B.tcpEOFOnPeerClose()))
  test("TCP: a payload spanning many reads/writes arrives intact")(check(B.tcpLargePayloadAcrossMultipleReads()))
  test("TCP: many concurrent clients against one listener don't cross-talk")(check(B.tcpConcurrentClients()))
  test("TCP: cancelling an outstanding read settles cleanly and leaves the fd usable")(
    check(B.tcpCancelOutstandingRead())
  )
  test("TCP: IPv6 loopback round-trip")(check(B.tcpIPv6RoundTrip()))

  test("UDP: send/receive round-trip reports the sender's address")(check(B.udpEchoRoundTrip()))
  test("UDP: back-to-back datagrams keep their boundaries")(check(B.udpDatagramBoundariesPreserved()))
  test("UDP: a datagram larger than the receive buffer is truncated, not an error")(
    check(B.udpTruncatesOversizedDatagram())
  )
  test("UDP: binding port 0 reports a real ephemeral port")(check(B.udpBindEphemeralPort()))
  test("UDP: SocketOptions apply without error")(check(B.udpSocketOptionsApply()))
  test("UDP: close() racing a live receive defers the syscall until it settles")(check(B.udpDeferredCloseRace()))
  test("UDP: IPv6 loopback round-trip")(check(B.udpIPv6RoundTrip()))
