package gears.async.asyncio.uring

import gears.async._
import uring._
import uringOps._

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import scala.scalanative.posix.sys.{socket => posixSocket}
import scala.scalanative.runtime.ByteArray
import scala.scalanative.unsafe._
import scala.scalanative.unsigned._

/** Track B follow-up: does a real, GC-managed `Array[Byte]` survive being
  * handed directly to io_uring - no malloc'd scratch buffer, no copy - while
  * the kernel genuinely holds a pointer into it and the collector is under
  * real, concurrent pressure? This bypasses `UringTcpStream.readBuf`/
  * `writeBuf` entirely and drives `submitAwait` directly against the raw
  * array pointer, so it's not production code - a standalone check of
  * whether the malloc+memcpy workaround in net.scala is still earning its
  * keep now that we know the GC doesn't move objects and continuation-
  * captured references survive suspension.
  */
@main def directBufferExperiment(): Unit =
  given support: ForkJoinUringSupport = ForkJoinUringSupport()
  val ring = support.ring

  def gcPressureFor(millis: Long): Thread =
    val t = new Thread(() =>
      val until = System.currentTimeMillis() + millis
      var rounds = 0
      while System.currentTimeMillis() < until do
        var i = 0
        while i < 5 do
          val junk = new Array[Byte](1 << 20)
          java.util.Arrays.fill(junk, 0xA5.toByte)
          i += 1
        System.gc()
        rounds += 1
      println(s"  (gc pressure: $rounds rounds during the in-flight window)")
    )
    t.setDaemon(true)
    t

  def rawSocket(): Int =
    val fd = posixSocket.socket(posixSocket.AF_INET, posixSocket.SOCK_STREAM, 0)
    if fd < 0 then throw new RuntimeException("socket() failed")
    fd

  Async.blocking:
    val addr = InetSocketAddress("127.0.0.1", 9200)

    // --- listener, plain synchronous setup, same as net.scala's listen() ---
    val listenFd = rawSocket()
    Zone.acquire: zone =>
      val one = alloc[CInt]()(using zone)
      !one = 1
      posixSocket.setsockopt(
        listenFd,
        posixSocket.SOL_SOCKET,
        posixSocket.SO_REUSEADDR,
        one.asInstanceOf[Ptr[Byte]],
        sizeof[CInt].toUInt
      )
      val sa = alloc[scala.scalanative.posix.netinet.in.sockaddr_in]()(using zone)
      fillSockAddr(sa.asInstanceOf[Ptr[Byte]], addr)
      if posixSocket.bind(listenFd, sa.asInstanceOf[Ptr[posixSocket.sockaddr]], sizeof[
          scala.scalanative.posix.netinet.in.sockaddr_in
        ].toUInt) < 0
      then throw new RuntimeException("bind() failed")
    if posixSocket.listen(listenFd, 16) < 0 then throw new RuntimeException("listen() failed")

    // --- client: connect, then SEND directly from a real Array[Byte]'s raw
    //     pointer - no malloc, no copy - while GC pressure runs concurrently.
    val payload = "the quick brown fox, sent straight from a GC-managed array".getBytes(StandardCharsets.UTF_8)
    val client = Future:
      val fd = submitAwait(ring)(sqe =>
        io_uring_prep_socket(sqe, posixSocket.AF_INET, posixSocket.SOCK_STREAM, 0, 0.toUInt)
      )
      if fd < 0 then throw new RuntimeException(s"client socket() failed: $fd")
      val sa = scala.scalanative.libc.stdlib.malloc(sizeof[scala.scalanative.posix.netinet.in.sockaddr_in])
      fillSockAddr(sa.asInstanceOf[Ptr[Byte]], addr)
      val cres = submitAwait(ring)(sqe =>
        io_uring_prep_connect(
          sqe,
          fd,
          sa.asInstanceOf[Ptr[posixSocket.sockaddr]],
          sizeof[scala.scalanative.posix.netinet.in.sockaddr_in].toUInt
        )
      )
      scala.scalanative.libc.stdlib.free(sa)
      if cres < 0 then throw new RuntimeException(s"connect() failed: $cres")

      val pressure = gcPressureFor(1500)
      pressure.start()
      // Real GC-managed array. Its raw element pointer goes straight into
      // the SQE - the kernel holds this address until the SEND completes.
      val rawPtr = payload.asInstanceOf[ByteArray].at(0)
      var sent = 0
      while sent < payload.length do
        val res = submitAwait(ring)(sqe =>
          io_uring_prep_send(sqe, fd, rawPtr + sent, (payload.length - sent).toUSize, posixSocket.MSG_NOSIGNAL)
        )
        if res < 0 then throw new RuntimeException(s"send() failed: $res")
        sent += res
      pressure.join()
      // If the array had moved, been collected, or been overwritten, this
      // check (after the *send* itself, comparing to the literal we started
      // with) still passing is a first, weaker signal; the real proof is
      // the server independently receiving the exact same bytes below.
      val stillIntact = java.util.Arrays.equals(payload, "the quick brown fox, sent straight from a GC-managed array".getBytes(StandardCharsets.UTF_8))
      println(s"client: sent ${payload.length} bytes directly from Array[Byte], local copy still intact=$stillIntact")
      scala.scalanative.libc.stdlib.free(sa) // no-op double free guard not needed; sa already freed above
      fd

    // --- server: accept, then RECV directly into a real Array[Byte]'s raw
    //     pointer - no malloc, no copy - while GC pressure runs concurrently,
    //     and nothing has been sent yet, so the read is genuinely in flight.
    val addrLen = scala.scalanative.libc.stdlib
      .malloc(sizeof[posixSocket.socklen_t])
      .asInstanceOf[Ptr[posixSocket.socklen_t]]
    !addrLen = sizeof[scala.scalanative.posix.netinet.in.sockaddr_in6].toUInt
    val peerAddr = scala.scalanative.libc.stdlib
      .malloc(sizeof[scala.scalanative.posix.netinet.in.sockaddr_in6])
      .asInstanceOf[Ptr[Byte]]
    val clientFd = submitAwait(ring)(sqe =>
      io_uring_prep_accept(sqe, listenFd, peerAddr.asInstanceOf[Ptr[posixSocket.sockaddr]], addrLen, 0)
    )
    if clientFd < 0 then throw new RuntimeException(s"accept() failed: $clientFd")
    scala.scalanative.libc.stdlib.free(addrLen.asInstanceOf[Ptr[Byte]])
    scala.scalanative.libc.stdlib.free(peerAddr)

    val recvBuf = new Array[Byte](256)
    val pressure2 = gcPressureFor(1500)
    pressure2.start()
    val recvPtr = recvBuf.asInstanceOf[ByteArray].at(0)
    val recvRes = submitAwait(ring)(sqe => io_uring_prep_recv(sqe, clientFd, recvPtr, recvBuf.length.toUSize, 0))
    pressure2.join()
    if recvRes < 0 then throw new RuntimeException(s"recv() failed: $recvRes")

    val received = new String(recvBuf, 0, recvRes, StandardCharsets.UTF_8)
    println(s"server: received $recvRes bytes directly into Array[Byte]: '$received'")

    val clientFdResult = client.await
    assert(received == "the quick brown fox, sent straight from a GC-managed array", s"MISMATCH: got '$received'")
    println("directBufferExperiment PASSED: no copy, no corruption, survived concurrent GC pressure on both ends")
