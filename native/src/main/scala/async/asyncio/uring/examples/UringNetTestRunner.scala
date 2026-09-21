package gears.async.asyncio.uring.examples

import gears.async._
import gears.async.net.{TcpSupport, UdpSupport}
import gears.async.asyncio.uring.ForkJoinUringSupport

/** Runs every UringNetTestBodies check as a plain `@main` (`sbt run`) -
  * useful for a quick manual pass without going through sbt's test task, and
  * for isolating a single check by editing this file directly. `UringNetSuite`
  * (`sbt test`) is the real target and runs the same bodies.
  *
  * Each check gets its own fresh `ForkJoinUringSupport`, closed before the
  * next one starts - not just tidiness. Reusing one `ForkJoinUringSupport`
  * (and so one long-lived process's worth of accumulated `Zone`/GC state)
  * across all 16 checks in a row was found to reliably segfault a few
  * checks in, with the crash trace landing inside scala-native's own
  * `java.net.InetAddress`/`SocketHelpers` implementation - not anywhere in
  * this backend's code, and not reproducible when the same check is run on
  * its own. Giving each check its own support (matching what
  * `UringNetSuite` already does per `test(...)` block) avoids it.
  */
@main def uringNetTestRunner(): Unit =
  var passed = 0
  var failed = 0
  var skipped = 0

  def run(name: String)(body: (Async.Spawn, TcpSupport, UdpSupport, AsyncOperations) ?=> Unit): Unit =
    given support: ForkJoinUringSupport = ForkJoinUringSupport()
    given tcp: TcpSupport = support.tcpSupport
    given udp: UdpSupport = support.udpSupport
    try
      Async.blocking:
        body(using summon[Async.Spawn], tcp, udp, support)
      println(s"PASS  $name")
      passed += 1
    catch
      case e: SkipException =>
        println(s"SKIP  $name (${e.getMessage})")
        skipped += 1
      case e: Throwable =>
        println(s"FAIL  $name: $e")
        failed += 1
    finally support.ring.close()

  run("TCP: echo round-trip")(UringNetTestBodies.tcpEchoRoundTrip())
  run("TCP: localAddress reflects the kernel-assigned ephemeral port")(
    UringNetTestBodies.tcpLocalAddressEphemeralPort()
  )
  run("TCP: SocketOptions apply without error")(UringNetTestBodies.tcpSocketOptionsApply())
  run("TCP: close() racing a live read defers the syscall until it settles")(
    UringNetTestBodies.tcpDeferredCloseRace()
  )
  run("TCP: peer close with nothing sent yields EOF")(UringNetTestBodies.tcpEOFOnPeerClose())
  run("TCP: a payload spanning many reads/writes arrives intact")(
    UringNetTestBodies.tcpLargePayloadAcrossMultipleReads()
  )
  run("TCP: many concurrent clients against one listener don't cross-talk")(
    UringNetTestBodies.tcpConcurrentClients()
  )
  run("TCP: cancelling an outstanding read settles cleanly and leaves the fd usable")(
    UringNetTestBodies.tcpCancelOutstandingRead()
  )
  run("TCP: IPv6 loopback round-trip")(UringNetTestBodies.tcpIPv6RoundTrip())

  run("UDP: send/receive round-trip reports the sender's address")(UringNetTestBodies.udpEchoRoundTrip())
  run("UDP: back-to-back datagrams keep their boundaries")(UringNetTestBodies.udpDatagramBoundariesPreserved())
  run("UDP: a datagram larger than the receive buffer is truncated, not an error")(
    UringNetTestBodies.udpTruncatesOversizedDatagram()
  )
  run("UDP: binding port 0 reports a real ephemeral port")(UringNetTestBodies.udpBindEphemeralPort())
  run("UDP: SocketOptions apply without error")(UringNetTestBodies.udpSocketOptionsApply())
  run("UDP: close() racing a live receive defers the syscall until it settles")(
    UringNetTestBodies.udpDeferredCloseRace()
  )
  run("UDP: IPv6 loopback round-trip")(UringNetTestBodies.udpIPv6RoundTrip())

  println(s"\n$passed passed, $failed failed, $skipped skipped")
  if failed > 0 then throw new RuntimeException(s"$failed test(s) failed")
