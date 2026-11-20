package gears.io.net

import gears.async.Async
import gears.async.io.Io
import gears.async.io.IpAddress
import gears.async.io.IpSocketAddress
import gears.async.io.IpStreams
import gears.async.io.Protocol
import gears.async.io.Resolving
import gears.async.io.Slot
import gears.async.io.UnixAddress
import gears.async.io.UnixStreams

import java.io.IOException
import java.util.concurrent.TimeoutException
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

object Tcp {

  def connect(host: String, port: Int, timeout: Duration = 10.seconds)(using
      io: Io[IpStreams & Resolving],
      async: Async
  ): SocketStream = {
    require(port >= 0 && port <= 65535, s"A port is 0 to 65535, not $port")
    val deadline = Stream.limit(timeout) match {
      case null              => Long.MaxValue
      case d: FiniteDuration => System.nanoTime() + d.toNanos
    }
    val candidates = addresses(host, port, deadline)
    var failure: Throwable | Null = null
    var result: SocketStream | Null = null
    var left = candidates.length
    val it = candidates.iterator
    while result == null && it.hasNext do {
      val address = it.next()
      val own =
        if deadline == Long.MaxValue || left == 1 then deadline
        else System.nanoTime() + math.max(deadline - System.nanoTime(), 0L) / left
      left -= 1
      try result = connectTo(address, own)
      catch {
        case e: IOException      => failure = e
        case e: TimeoutException =>
          if left == 0 || System.nanoTime() >= deadline then
            throw new TimeoutException(s"Connecting to $host:$port timed out after $timeout")
          failure = e
      }
    }
    if result == null then throw new IOException(s"Failed to connect to $host:$port", failure)
    result.nn
  }

  def connect(address: IpSocketAddress)(using io: Io[IpStreams], async: Async): SocketStream =
    connectTo(address, Long.MaxValue)

  private def connectTo(
      address: IpSocketAddress,
      deadline: Long
  )(using io: Io[IpStreams], async: Async): SocketStream = {
    val into = Slot[io.ops.Connection]()
    val op = io.ops.connect(address, into)
    if deadline == Long.MaxValue then io.perform(op)
    else {
      val left = deadline - System.nanoTime()
      if left <= 0 then throw new TimeoutException(s"Connecting to $address timed out")
      io.perform(op, FiniteDuration(left, java.util.concurrent.TimeUnit.NANOSECONDS))
    }
    new SocketStream(io)(into.clear())
  }

  private def addresses(host: String, port: Int, deadline: Long)(using
      io: Io[IpStreams & Resolving],
      async: Async
  ): List[IpSocketAddress] = {
    val bare = if host.startsWith("[") && host.endsWith("]") then host.substring(1, host.length - 1) else host
    numeric(bare, port) match {
      case Some(address) => List(address)
      case None          =>
        val into = Slot[List[IpAddress]]()
        val op = io.ops.resolve(bare, into)
        val left = deadline - System.nanoTime()
        if deadline == Long.MaxValue then io.perform(op)
        else if left <= 0 then throw new TimeoutException(s"Resolving $host timed out")
        else io.perform(op, FiniteDuration(left, java.util.concurrent.TimeUnit.NANOSECONDS))
        val resolved = into.clear()
        if resolved.isEmpty then throw new IOException(s"$host has no address")
        resolved.sortBy(_.protocol != Protocol.IPv4).map(IpSocketAddress(_, port))
    }
  }

  private[io] def numeric(host: String, port: Int): Option[IpSocketAddress] =
    IpLiteral.parse(host).map(IpSocketAddress(_, port))

  def listen(host: String = "0.0.0.0", port: Int)(using io: Io[IpStreams], async: Async): Listener = {
    val address = numeric(host, port).getOrElse(throw new IllegalArgumentException(s"Not a numeric address: $host"))
    listen(address)
  }

  def listen(address: IpSocketAddress)(using io: Io[IpStreams], async: Async): Listener = {
    val into = Slot[io.ops.Listener]()
    io.perform(io.ops.listen(address, into))
    new Listener(io)(into.clear())
  }

  def port(address: IpSocketAddress | UnixAddress): Int = address match {
    case IpSocketAddress(_, p) => p
    case _: UnixAddress        => throw new IllegalArgumentException(s"$address has no port")
  }
}

object Unix {

  def connect(path: String)(using io: Io[UnixStreams], async: Async): SocketStream = {
    val into = Slot[io.ops.Connection]()
    io.perform(io.ops.connect(UnixAddress(path), into))
    new SocketStream(io)(into.clear())
  }

  def listen(path: String)(using io: Io[UnixStreams], async: Async): Listener = {
    val into = Slot[io.ops.Listener]()
    io.perform(io.ops.listen(UnixAddress(path), into))
    new Listener(io)(into.clear())
  }
}

private[io] object IpLiteral {

  private def isDigit(c: Char): Boolean = c >= '0' && c <= '9'

  def parse(text: String): Option[IpAddress] =
    if text.contains(':') then ipv6(text) else ipv4(text).map(b => IpAddress.fromBytes(Protocol.IPv4, b))

  private def ipv4(text: String): Option[Array[Byte]] = {
    val parts = text.split('.')
    if parts.length == 4 && parts.forall(p => p.nonEmpty && p.length <= 3 && p.forall(isDigit) && p.toInt <= 255)
    then Some(parts.map(_.toInt.toByte))
    else None
  }

  private def ipv6(text: String): Option[IpAddress] = {
    val percent = text.indexOf('%')
    val (body, scope) =
      if percent < 0 then (text, Some(0)) else (text.substring(0, percent), scopeId(text.substring(percent + 1)))
    for {
      scopeId <- scope
      bytes <- groups(body)
    } yield IpAddress.fromBytes(Protocol.IPv6, bytes, scopeId)
  }

  private def scopeId(text: String): Option[Int] =
    if text.nonEmpty && text.length <= 9 && text.forall(isDigit) then Some(text.toInt) else None

  private def groups(text: String): Option[Array[Byte]] = {
    val gap = text.indexOf("::")
    if gap >= 0 && text.indexOf("::", gap + 1) >= 0 then None
    else {
      val (front, back) = if gap < 0 then (text, "") else (text.substring(0, gap), text.substring(gap + 2))
      for {
        f <- side(front, endsAddress = gap < 0)
        b <- side(back, endsAddress = true)
        if (if gap < 0 then f.length == 16 else f.length + b.length <= 14)
      } yield f ++ Array.fill(16 - f.length - b.length)(0.toByte) ++ b
    }
  }

  private def side(text: String, endsAddress: Boolean): Option[Array[Byte]] =
    if text.isEmpty then Some(Array.empty)
    else {
      val parts = text.split(":", -1)
      val last = parts.last
      val tail = if endsAddress && last.contains('.') then ipv4(last) else group(last)
      val init = parts.init.map(group)
      if init.exists(_.isEmpty) then None else tail.map(t => init.flatMap(_.get) ++ t)
    }

  private def group(s: String): Option[Array[Byte]] =
    if s.nonEmpty && s.length <= 4 && s.forall(c => isDigit(c) || (c | 0x20) >= 'a' && (c | 0x20) <= 'f') then {
      val v = Integer.parseInt(s, 16)
      Some(Array((v >>> 8).toByte, v.toByte))
    } else None
}
