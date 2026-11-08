package gears.io.http

import gears.async.Async
import gears.async.BufferedChannel
import gears.async.Future
import gears.async.Semaphore
import gears.async.io.Io
import gears.async.io.IpStreams
import gears.async.io.Resolving
import gears.io.net.BufferedInput
import gears.io.net.Stream
import gears.io.net.Tcp
import gears.io.tls.Tls

import java.io.EOFException
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.charset.CodingErrorAction
import java.nio.charset.StandardCharsets
import java.util.concurrent.CancellationException
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

/** A WebSocket (RFC 6455), scoped to its session: `WebSocket.accept` answers a request with one, and
  * `WebSocket.connect` opens one, each running the session function with it and closing it when the function returns,
  * with the closing handshake.
  *
  * A future of the session's reads the peer's frames as they come, so that pings are answered and the peer's close is
  * seen while the session is busy: it hands messages over one at a time, and while one waits for the session to take it
  * reads nothing more, which holds the peer back. `receive` takes the next, and `messages` iterates them until the peer
  * closes. `send` may be called from many futures at once: frames go out whole, one after another, each within
  * `writeTimeout`; one that is not, or that is cancelled once part of it has gone, closes the socket, since the peer
  * would read the next frame as the rest of it. With `pingEvery`, the socket pings the peer that often, and closes if
  * nothing comes from it for twice as long while it is read, which is how a peer that vanished without closing is
  * found.
  */
final class WebSocket private (
    stream: Stream,
    input: BufferedInput,
    client: Boolean,
    val protocol: Option[String],
    maxMessage: Int,
    writeTimeout: FiniteDuration,
    timers: gears.async.AsyncOperations
) {
  import WebSocket.*

  private val writing = Semaphore(1)
  private val inbox = BufferedChannel[AnyRef](1)
  @volatile private var closeSent = false
  @volatile private var ended = false
  @volatile private var lastHeard = System.nanoTime()
  @volatile private var handing = false // a message waits for the session, so the peer is not read
  @volatile private var received: Option[Close] = None

  /** The peer's close, once it has come. */
  def peerClose: Option[Close] = received

  def send(text: String)(using Async): Unit = send(Message.Text(text))
  def send(bytes: Array[Byte])(using Async): Unit = send(Message.Binary(bytes))

  def send(message: Message)(using Async): Unit = message match {
    case Message.Text(text)    => writeFrame(1, encode(text))
    case Message.Binary(bytes) => writeFrame(2, bytes)
  }

  def ping(data: Array[Byte] = Array.emptyByteArray)(using Async): Unit = {
    require(data.length <= 125, "A ping carries at most 125 bytes")
    writeFrame(9, data)
  }

  /** The next message, or None once the peer has closed, or the socket was closed; the reading's failure if it ended
    * with one, such as a protocol error, which closed the socket.
    */
  def receive()(using Async): Option[Message] =
    if ended then None
    else
      inbox.read() match {
        case Right(m: Message)   => Some(m)
        case Right(Ended(error)) =>
          ended = true
          if error != null then throw error.nn
          None
        case _ => None
      }

  /** The messages, as they come, until the peer closes. */
  def messages(using Async): Iterator[Message] = new Iterator[Message] {
    private var ahead: Option[Message] | Null = null
    def hasNext: Boolean = {
      if ahead == null then ahead = receive()
      ahead.nn.isDefined
    }
    def next(): Message = {
      if !hasNext then throw new NoSuchElementException("The WebSocket is closed")
      val m = ahead.nn.get
      ahead = null
      m
    }
  }

  /** Starts the closing handshake: sends a close with `code` and `reason`, once; later sends fail. */
  def close(code: Int = 1000, reason: String = "")(using Async): Unit =
    if !closeSent then {
      require(validCloseCode(code), s"Not a close code a socket may send: $code")
      val text = encode(reason)
      require(text.length <= 123, "A close reason is at most 123 bytes of UTF-8")
      writeFrame(8, ByteBuffer.allocate(text.length + 2).putShort(code.toShort).put(text).array())
    }

  private def writeFrame(opcode: Int, payload: Array[Byte])(using Async): Unit =
    Stream.holding(writing) {
      if closeSent then throw new IOException("The WebSocket is closing")
      if opcode == 8 then closeSent = true
      if opcode < 8 && payload.length > maxMessage then
        throw new IllegalArgumentException(s"A message larger than $maxMessage bytes")
      val extended = if payload.length < 126 then 0 else if payload.length <= 65535 then 2 else 8
      val head = ByteBuffer.allocate(2 + extended + (if client then 4 else 0))
      head.put((0x80 | opcode).toByte)
      val size = if extended == 0 then payload.length else if extended == 2 then 126 else 127
      head.put((size | (if client then 0x80 else 0)).toByte)
      if extended == 2 then head.putShort(payload.length.toShort)
      else if extended == 8 then head.putLong(payload.length.toLong)
      val body =
        if !client then payload
        else {
          val mask = gears.io.net.Platform.randomBytes(4)
          head.put(mask)
          val masked = new Array[Byte](payload.length)
          var i = 0
          while i < payload.length do {
            masked(i) = (payload(i) ^ mask(i & 3)).toByte
            i += 1
          }
          masked
        }
      head.flip()
      try stream.write(Array(head, ByteBuffer.wrap(body)), writeTimeout)
      catch {
        case e: java.util.concurrent.TimeoutException =>
          stream.close()
          throw new IOException(s"A frame was not written within $writeTimeout: the WebSocket is closed", e)
        case e: CancellationException if head.position() > 0 =>
          stream.close() // part of the frame went: the stream is no longer framed
          throw e
      }
    }

  /** Reads frames until the peer closes, or the stream ends or fails, handing messages to the session. */
  private def readLoop()(using Async): Unit = {
    var failure: Throwable | Null = null
    try {
      val message = new java.io.ByteArrayOutputStream()
      var kind = 0
      var open = true
      while open do {
        val frame = readFrame(maxMessage - message.size())
        lastHeard = System.nanoTime()
        frame.opcode match {
          case 8 =>
            received = Some(parseClose(frame.payload))
            if !closeSent then
              try writeFrame(8, frame.payload.take(2))
              catch case _: IOException => ()
            open = false
          case 9      => if !closeSent then writeFrame(10, frame.payload)
          case 10     => ()
          case opcode =>
            if opcode == 0 && kind == 0 then protocolError("A continuation with no message")
            if opcode != 0 && kind != 0 then protocolError("A message before the last one ended")
            if opcode != 0 then kind = opcode
            message.write(frame.payload)
            if frame.fin then {
              val bytes = message.toByteArray
              message.reset()
              val value =
                if kind == 1 then
                  Message.Text(
                    try utf8(bytes)
                    catch case _: IOException => protocolError("A text message that is not UTF-8", 1007)
                  )
                else Message.Binary(bytes)
              kind = 0
              if !closeSent then { // after our close, data is dropped until the peer's
                handing = true
                try inbox.send(value)
                finally {
                  handing = false
                  lastHeard = System.nanoTime()
                }
              }
            }
        }
      }
    } catch {
      case e: CancellationException => throw e
      case e: ProtocolException     =>
        failure = e
        try if !closeSent then writeFrame(8, ByteBuffer.allocate(2).putShort(e.closeCode.toShort).array())
        catch case _: Exception => ()
      case _: EOFException if closeSent => () // the peer closed the connection rather than answering our close
      case e: Exception                 => failure = e
    } finally
      try inbox.send(Ended(failure))
      catch case _: Exception => ()
  }

  private final case class Frame(fin: Boolean, opcode: Int, payload: Array[Byte])

  private def readFrame(room: Int)(using Async): Frame = {
    val head = input.readExactly(2)
    val first = head(0) & 255
    val second = head(1) & 255
    val fin = (first & 0x80) != 0
    val opcode = first & 15
    if (first & 0x70) != 0 then protocolError("Reserved bits set, with no extension agreed")
    if !(opcode <= 2 || (opcode >= 8 && opcode <= 10)) then protocolError(s"An unknown opcode: $opcode")
    if ((second & 0x80) != 0) == client then
      protocolError(if client then "A masked frame from a server" else "An unmasked frame from a client")
    val short = second & 127
    if opcode >= 8 && (!fin || short > 125) then protocolError("A fragmented or long control frame")
    val length = short match {
      case 126 =>
        val n = ByteBuffer.wrap(input.readExactly(2)).getShort() & 65535
        if n < 126 then protocolError("A length not in its shortest form")
        n.toLong
      case 127 =>
        val n = ByteBuffer.wrap(input.readExactly(8)).getLong()
        if n < 65536 then protocolError("A length not in its shortest form")
        n
      case n => n.toLong
    }
    if opcode < 8 && length > room then protocolError(s"A message larger than $maxMessage bytes", 1009)
    val mask = if !client then input.readExactly(4) else Array.emptyByteArray
    val payload = readPayload(length.toInt)
    if !client then {
      var i = 0
      while i < payload.length do {
        payload(i) = (payload(i) ^ mask(i & 3)).toByte
        i += 1
      }
    }
    Frame(fin, opcode, payload)
  }

  /** `n` bytes, held as they arrive rather than all at once, so that a length announced but not sent costs nothing. */
  private def readPayload(n: Int)(using Async): Array[Byte] =
    if n <= PayloadChunk then input.readExactly(n)
    else {
      val out = new java.io.ByteArrayOutputStream(PayloadChunk)
      var left = n
      while left > 0 do {
        val chunk = input.readExactly(math.min(left, PayloadChunk))
        out.write(chunk)
        left -= chunk.length
      }
      out.toByteArray
    }

  private def parseClose(payload: Array[Byte]): Close =
    if payload.isEmpty then Close(None, "")
    else {
      if payload.length == 1 then protocolError("A close of one byte")
      val code = ByteBuffer.wrap(payload).getShort() & 65535
      if !validCloseCode(code) then protocolError(s"A close code a peer may not send: $code")
      val reason =
        try utf8(payload.drop(2))
        catch case _: IOException => protocolError("A close reason that is not UTF-8", 1007)
      Close(Some(code), reason)
    }

  /** Runs `session` with this socket: the reader, and the pinger if `pingEvery`, beside it; then the closing handshake,
    * waiting at most two seconds for the peer's close, and the stream's close.
    */
  private def run[A](session: WebSocket => Async ?=> A, pingEvery: Option[FiniteDuration])(using Async): A =
    try
      Async.group {
        val reader = Future(readLoop())
        val pinger = pingEvery.map { every =>
          Future {
            var alive = true
            while alive do {
              timers.sleep(every.toMillis)
              if !handing && System.nanoTime() - lastHeard > 2 * every.toNanos then {
                stream.close() // the peer went away without closing: the reader ends
                alive = false
              } else
                try ping()
                catch case _: IOException => ()
            }
          }
        }
        try session(this)
        finally {
          try close()
          catch case _: Exception => ()
          pinger.foreach(_.cancel())
          // The peer's close ends the reader; one that never answers is cut off.
          gears.async.withTimeoutOption(2.seconds)(reader.awaitResult)(using timers, summon[Async]): Unit
          reader.cancel()
        }
      }
    finally stream.close()
}

object WebSocket {

  enum Message {
    case Text(text: String)
    case Binary(bytes: Array[Byte])
  }

  /** A peer's close: its code, if it gave one, and its reason. */
  final case class Close(code: Option[Int], reason: String)

  /** A frame that breaks the protocol, closed with `closeCode`. */
  final class ProtocolException(message: String, val closeCode: Int) extends IOException(message)

  private final case class Ended(failure: Throwable | Null)

  private inline val PayloadChunk = 64 * 1024

  private def protocolError(message: String, code: Int = 1002): Nothing = throw new ProtocolException(message, code)

  /** Answers `request` by upgrading it to a WebSocket, running `session` with it, and closing it as `session` returns:
    * {{{
    * GET("/echo") { req => WebSocket.accept(req) { ws => for m <- ws.messages do ws.send(m) } }
    * }}}
    * Refuses, with the response a client expects, a request that is not a valid version 13 handshake, and one whose
    * `Origin` is not allowed: by default only the server's own, so that another site's page cannot open a socket with
    * its visitor's cookies. `protocols` are the subprotocols offered, in preference order, of which the first the
    * client also offers is chosen (`WebSocket.protocol`).
    */
  def accept(
      request: Request,
      protocols: Seq[String] = Nil,
      maxMessage: Int = 1024 * 1024,
      pingEvery: Option[FiniteDuration] = Some(30.seconds),
      origin: Request => Boolean = sameOrigin,
      writeTimeout: FiniteDuration = 30.seconds
  )(session: WebSocket => Async ?=> Unit)(using ops: gears.async.AsyncOperations): Response = {
    require(maxMessage > 0, "maxMessage is positive")
    val h = request.headers
    val keys = h.getAll("Sec-WebSocket-Key")
    val offered = h.tokens("Sec-WebSocket-Protocol")
    val valid = request.method == Method.Get && request.version == "HTTP/1.1" &&
      h.hasToken("Connection", "upgrade") && h.hasToken("Upgrade", "websocket") && keys.size == 1 &&
      validKey(keys.head) && request.body.length.forall(_ == 0) && offered.forall(Headers.isToken)
    if !valid then throw HttpError(Status.BadRequest, "Not a WebSocket handshake")
    if h.getAll("Sec-WebSocket-Version") != Seq("13") then
      throw HttpError(
        Status.UpgradeRequired,
        "WebSocket version 13 is required",
        Headers("Sec-WebSocket-Version" -> "13")
      )
    if !origin(request) then throw HttpError(Status.Forbidden, "The origin is not allowed")
    val chosen = protocols.find(offered.contains)
    var headers =
      Headers("Connection" -> "Upgrade", "Upgrade" -> "websocket", "Sec-WebSocket-Accept" -> acceptKey(keys.head))
    chosen.foreach(p => headers = headers.set("Sec-WebSocket-Protocol", p))
    Response(
      Status.SwitchingProtocols,
      headers,
      Body.Upgrade { (stream, input) =>
        new WebSocket(stream, input, client = false, chosen, maxMessage, writeTimeout, ops).run(session, pingEvery)
      }
    )
  }

  /** Opens a WebSocket to `url`, `ws` or `wss`, runs `session` with it, closes it as `session` returns, and returns
    * what `session` returns. Fails if the server refuses the upgrade, or answers it wrongly.
    */
  def connect[A](
      url: String | Url,
      protocols: Seq[String] = Nil,
      headers: Headers = Headers.empty,
      maxMessage: Int = 16 * 1024 * 1024,
      pingEvery: Option[FiniteDuration] = None,
      config: ClientConfig = ClientConfig()
  )(session: WebSocket => Async ?=> A)(using io: Io[IpStreams & Resolving], async: Async): A = {
    val target = url match {
      case u: Url    => u
      case s: String => Url.parse(s)
    }
    require(target.scheme == "ws" || target.scheme == "wss", s"Not a ws or wss URL: $target")
    require(protocols.forall(Headers.isToken), "A subprotocol is a token")
    val t = config.timeouts
    val began = System.nanoTime()
    val raw = Tcp.connect(target.host, target.port, t.connect)
    val stream =
      try if target.secure then Tls.client(raw, target.host, config.tls, HttpClient.leftOf(t.connect, began)) else raw
      catch {
        case e: Throwable =>
          raw.close()
          throw e
      }
    try {
      val key = java.util.Base64.getEncoder.encodeToString(gears.io.net.Platform.randomBytes(16))
      var request = headers
        .set("Host", target.authority)
        .set("Connection", "Upgrade")
        .set("Upgrade", "websocket")
        .set("Sec-WebSocket-Key", key)
        .set("Sec-WebSocket-Version", "13")
      if protocols.nonEmpty then request = request.set("Sec-WebSocket-Protocol", protocols.mkString(", "))
      config.userAgent.foreach(ua => request = request.setIfAbsent("User-Agent", ua))
      stream.write(Wire.head(s"GET ${target.target} HTTP/1.1", request), t.write)
      val input = new BufferedInput(stream, 8192, 64 * 1024)
      val head = Wire.readResponseHead(input, 64 * 1024, t.responseHead)
      if head.status != Status.SwitchingProtocols then throw new HttpStatusException(head.status, head.headers, "")
      val r = head.headers
      if !r.hasToken("Connection", "upgrade") || !r.hasToken("Upgrade", "websocket") ||
        r.getAll("Sec-WebSocket-Accept") != Seq(acceptKey(key)) || r.contains("Sec-WebSocket-Extensions")
      then throw new IOException("A WebSocket upgrade answered wrongly")
      val chosen = r.getAll("Sec-WebSocket-Protocol")
      if chosen.size > 1 || chosen.exists(!protocols.contains(_)) then
        throw new IOException("The server chose a subprotocol not offered")
      val frames = Stream.limit(t.write) match {
        case null              => 30.seconds
        case d: FiniteDuration => d
      }
      new WebSocket(stream, input, client = true, chosen.headOption, maxMessage, frames, io).run(session, pingEvery)
    } finally stream.close()
  }

  private def sameOrigin(request: Request): Boolean =
    request.headers.getAll("Origin") match {
      case Seq()       => true // not a browser
      case Seq(origin) =>
        try {
          val o = Url.parse(origin)
          request.headers.get("Host").exists(_.equalsIgnoreCase(o.authority))
        } catch case _: IllegalArgumentException => false
      case _ => false
    }

  private val alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

  private def validKey(key: String): Boolean =
    key.length == 24 && key.endsWith("==") && key.take(22).forall(alphabet.contains(_)) &&
      (alphabet.indexOf(key(21)) & 15) == 0

  /** `Sec-WebSocket-Accept` for `key`: the base64 of the SHA-1 of the key and the protocol's GUID. */
  private[http] def acceptKey(key: String): String =
    java.util.Base64.getEncoder
      .encodeToString(sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").getBytes(StandardCharsets.US_ASCII)))

  /** SHA-1, which the handshake needs, and Scala Native's library lacks. */
  private def sha1(input: Array[Byte]): Array[Byte] = {
    val padded = new Array[Byte](((input.length + 9 + 63) / 64) * 64)
    System.arraycopy(input, 0, padded, 0, input.length)
    padded(input.length) = 0x80.toByte
    ByteBuffer.wrap(padded).putLong(padded.length - 8, input.length.toLong * 8)
    val h = Array(0x67452301, 0xefcdab89, 0x98badcfe, 0x10325476, 0xc3d2e1f0)
    val w = new Array[Int](80)
    val data = ByteBuffer.wrap(padded)
    while data.hasRemaining do {
      var i = 0
      while i < 16 do { w(i) = data.getInt(); i += 1 }
      while i < 80 do { w(i) = Integer.rotateLeft(w(i - 3) ^ w(i - 8) ^ w(i - 14) ^ w(i - 16), 1); i += 1 }
      var a = h(0); var b = h(1); var c = h(2); var d = h(3); var e = h(4)
      i = 0
      while i < 80 do {
        val (f, k) =
          if i < 20 then ((b & c) | (~b & d), 0x5a827999)
          else if i < 40 then (b ^ c ^ d, 0x6ed9eba1)
          else if i < 60 then ((b & c) | (b & d) | (c & d), 0x8f1bbcdc)
          else (b ^ c ^ d, 0xca62c1d6)
        val next = Integer.rotateLeft(a, 5) + f + e + k + w(i)
        e = d; d = c; c = Integer.rotateLeft(b, 30); b = a; a = next
        i += 1
      }
      h(0) += a; h(1) += b; h(2) += c; h(3) += d; h(4) += e
    }
    val digest = ByteBuffer.allocate(20)
    h.foreach(digest.putInt(_))
    digest.array()
  }

  private def encode(text: String): Array[Byte] = {
    val buf = StandardCharsets.UTF_8
      .newEncoder()
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT)
      .encode(java.nio.CharBuffer.wrap(text))
    val bytes = new Array[Byte](buf.remaining())
    buf.get(bytes)
    bytes
  }

  private def utf8(bytes: Array[Byte]): String =
    try
      StandardCharsets.UTF_8
        .newDecoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT)
        .decode(ByteBuffer.wrap(bytes))
        .toString
    catch case e: java.nio.charset.CharacterCodingException => throw new IOException("Not UTF-8", e)

  private def validCloseCode(code: Int): Boolean =
    (code >= 1000 && code <= 1014 && code != 1004 && code != 1005 && code != 1006) || (code >= 3000 && code <= 4999)
}
