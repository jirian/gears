package gears.async.asyncio.examples

import gears.async._
import gears.async.asyncio.uring.UringPerThreadSupport
import gears.async.net.UdpSupport

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

def udpExample(): Unit =
  given support: UringPerThreadSupport = UringPerThreadSupport()
  given udp: UdpSupport = support.udpSupport

  Async.blocking:
    val a = UdpSupport.bind(InetSocketAddress("127.0.0.1", 0)) match
      case Right(s) => s
      case Left(e)  => throw new RuntimeException(s"bind A failed: $e")
    val b = UdpSupport.bind(InetSocketAddress("127.0.0.1", 0)) match
      case Right(s) => s
      case Left(e)  => throw new RuntimeException(s"bind B failed: $e")

    println(s"A bound on ${a.localAddress}")
    println(s"B bound on ${b.localAddress}")

    try
      val msg1 = "hello from A".getBytes(StandardCharsets.UTF_8)
      a.sendTo(ByteBuffer.wrap(msg1), b.localAddress) match
        case Right(()) => ()
        case Left(e)   => throw new RuntimeException(s"sendTo A->B failed: $e")

      val recvBuf1 = ByteBuffer.allocate(256)
      val senderAddr = b.receiveFrom(recvBuf1) match
        case Right(addr) => addr
        case Left(e)     => throw new RuntimeException(s"receiveFrom on B failed: $e")
      recvBuf1.flip()
      val received1 = new Array[Byte](recvBuf1.remaining())
      recvBuf1.get(received1)
      val text1 = new String(received1, StandardCharsets.UTF_8)
      println(s"B received '$text1' from $senderAddr")
      check(text1 == "hello from A", "content matches")
      check(senderAddr == a.localAddress, "sender address matches A's bound address")

      val msg2 = "hello back from B".getBytes(StandardCharsets.UTF_8)
      b.sendTo(ByteBuffer.wrap(msg2), senderAddr) match
        case Right(()) => ()
        case Left(e)   => throw new RuntimeException(s"sendTo B->A failed: $e")

      val recvBuf2 = ByteBuffer.allocate(256)
      val replyFrom = a.receiveFrom(recvBuf2) match
        case Right(addr) => addr
        case Left(e)     => throw new RuntimeException(s"receiveFrom on A failed: $e")
      recvBuf2.flip()
      val received2 = new Array[Byte](recvBuf2.remaining())
      recvBuf2.get(received2)
      val text2 = new String(received2, StandardCharsets.UTF_8)
      println(s"A received '$text2' from $replyFrom")
      check(text2 == "hello back from B", "content matches")
      check(replyFrom == b.localAddress, "sender address matches B's bound address")
    finally
      a.close()
      b.close()
