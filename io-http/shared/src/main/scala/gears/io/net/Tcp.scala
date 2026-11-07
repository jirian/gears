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

/** TCP: connecting by host name or address, and listening. */
object Tcp {

  /** Connects to `host`, a name or an IPv4 or IPv6 address, on `port`, and returns the stream, with `TCP_NODELAY` on,
    * as every stream socket of the interface has it. A name is resolved, and its addresses tried in turn, IPv4 ones
    * first, until one connects, within `timeout` in all; the last one's failure is thrown if none does.
    */
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
    val it = candidates.iterator
    while result == null && it.hasNext do {
      val address = it.next()
      try result = connectTo(address, deadline)
      catch {
        case e: IOException      => failure = e
        case _: TimeoutException => throw new TimeoutException(s"Connecting to $host:$port timed out after $timeout")
      }
    }
    if result == null then throw new IOException(s"Failed to connect to $host:$port", failure)
    result.nn
  }

  /** Connects to `address` and returns the stream. */
  def connect(address: IpSocketAddress)(using io: Io[IpStreams], async: Async): SocketStream =
    connectTo(address, Long.MaxValue)

  private def connectTo(
      address: IpSocketAddress,
      deadline: Long
  )(using io: Io[IpStreams], async: Async): SocketStream = {
    val into = Slot[io.backend.Connection]()
    val op = io.backend.ops.connect(address, into)
    if deadline == Long.MaxValue then io.perform(op)
    else {
      val left = deadline - System.nanoTime()
      if left <= 0 then throw new TimeoutException(s"Connecting to $address timed out")
      io.perform(op, FiniteDuration(left, java.util.concurrent.TimeUnit.NANOSECONDS))
    }
    new SocketStream(io, into.clear())
  }

  /** `host`'s addresses on `port`: itself if it is numeric, else what resolving it gives, IPv4 first. */
  private def addresses(host: String, port: Int, deadline: Long)(using
      io: Io[IpStreams & Resolving],
      async: Async
  ): List[IpSocketAddress] = {
    val bare = if host.startsWith("[") && host.endsWith("]") then host.substring(1, host.length - 1) else host
    numeric(bare, port) match {
      case Some(address) => List(address)
      case None          =>
        val into = Slot[List[IpAddress]]()
        val op = io.backend.ops.resolve(bare, into)
        val left = deadline - System.nanoTime()
        if deadline == Long.MaxValue then io.perform(op)
        else if left <= 0 then throw new TimeoutException(s"Resolving $host timed out")
        else io.perform(op, FiniteDuration(left, java.util.concurrent.TimeUnit.NANOSECONDS))
        val resolved = into.clear()
        if resolved.isEmpty then throw new IOException(s"$host has no address")
        resolved.sortBy(_.protocol != Protocol.IPv4).map(IpSocketAddress(_, port))
    }
  }

  /** `host` as an address on `port`, if it is a numeric IPv4 or IPv6 one. */
  private[io] def numeric(host: String, port: Int): Option[IpSocketAddress] =
    IpLiteral.parse(host).map(IpSocketAddress(_, port))

  /** Listens on `host`, a numeric address, `0.0.0.0` or `::` for every interface, and `port`, 0 for one the system
    * picks (`Listener.localAddress` says which).
    */
  def listen(host: String = "0.0.0.0", port: Int)(using io: Io[IpStreams], async: Async): Listener = {
    val address = numeric(host, port).getOrElse(throw new IllegalArgumentException(s"Not a numeric address: $host"))
    listen(address)
  }

  def listen(address: IpSocketAddress)(using io: Io[IpStreams], async: Async): Listener = {
    val into = Slot[io.backend.Listener]()
    io.perform(io.backend.ops.listen(address, into))
    new Listener(io, into.clear())
  }

  /** The port of a listener's or a stream's address. */
  def port(address: IpSocketAddress | UnixAddress): Int = address match {
    case IpSocketAddress(_, p) => p
    case _: UnixAddress        => throw new IllegalArgumentException(s"$address has no port")
  }
}

/** Unix domain stream sockets. */
object Unix {

  def connect(path: String)(using io: Io[UnixStreams], async: Async): SocketStream = {
    val into = Slot[io.backend.Connection]()
    io.perform(io.backend.ops.connect(UnixAddress(path), into))
    new SocketStream(io, into.clear())
  }

  def listen(path: String)(using io: Io[UnixStreams], async: Async): Listener = {
    val into = Slot[io.backend.Listener]()
    io.perform(io.backend.ops.listen(UnixAddress(path), into))
    new Listener(io, into.clear())
  }
}

/** Numeric IP addresses written as text: dotted IPv4, and IPv6 with `::` and an optional dotted IPv4 tail and numeric
  * `%scope`. Nothing is resolved, and anything else, a host name, is `None`.
  */
private[io] object IpLiteral {

  def parse(text: String): Option[IpAddress] =
    if text.contains(':') then ipv6(text) else ipv4(text).map(b => IpAddress.fromBytes(Protocol.IPv4, b))

  private def ipv4(text: String): Option[Array[Byte]] = {
    val parts = text.split('.')
    if parts.length == 4 && parts.forall(p => p.nonEmpty && p.length <= 3 && p.forall(_.isDigit) && p.toInt <= 255)
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
    if text.nonEmpty && text.length <= 9 && text.forall(_.isDigit) then Some(text.toInt) else None

  /** The 16 bytes of `text`, without its scope: hex groups, at most one `::` for a run of zero groups, and a dotted
    * IPv4 tail only at the very end.
    */
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
    if s.nonEmpty && s.length <= 4 && s.forall(c => Character.digit(c, 16) >= 0) then {
      val v = Integer.parseInt(s, 16)
      Some(Array((v >>> 8).toByte, v.toByte))
    } else None
}
