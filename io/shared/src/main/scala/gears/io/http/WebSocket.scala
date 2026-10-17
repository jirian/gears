package gears.io.http

import gears.async.{Async, Resource, Semaphore}
import gears.io.net.{DnsSupport, TcpStream, TcpSupport, TlsSupport}
import gears.io.{Error, Reader}

import java.io.{ByteArrayOutputStream, Closeable, EOFException, IOException}
import java.nio.ByteBuffer
import java.nio.charset.{CodingErrorAction, StandardCharsets}
import java.util.concurrent.atomic.AtomicBoolean

final class WebSocket private[http] (
    stream: TcpStream,
    input: Reader,
    client: Boolean,
    val subprotocol: Option[String],
    val maxMessageSize: Int
) extends Closeable:
  import WebSocket.*
  import WebSocketProtocol.*

  require(maxMessageSize > 0, "maxMessageSize must be positive")
  private val reading = AtomicBoolean(false)
  private val writing = Semaphore(1)
  private val closed = AtomicBoolean(false)
  @volatile private var closeSent = false
  @volatile private var closeReceived = false
  @volatile private var receivedClose: Option[Close] = None

  def peerClose: Option[Close] = receivedClose

  override def close(): Unit = if closed.compareAndSet(false, true) then stream.close()

  private def encode(text: String): Array[Byte] =
    val buf = StandardCharsets.UTF_8
      .newEncoder()
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT)
      .encode(java.nio.CharBuffer.wrap(text))
    val bytes = new Array[Byte](buf.remaining())
    buf.get(bytes)
    bytes

  def sendText(text: String)(using Async): Unit = send(Message.Text(text))
  def sendBinary(bytes: Array[Byte])(using Async): Unit = send(Message.Binary(bytes))

  def send(message: Message)(using Async): Unit =
    val (opcode, bytes) = message match
      case Message.Text(text)   => (1, encode(text))
      case Message.Binary(data) => (2, data)
    require(bytes.length <= maxMessageSize, "Message exceeds maxMessageSize")
    writeFrame(opcode, bytes)

  def ping(bytes: Array[Byte] = Array.emptyByteArray)(using Async): Unit =
    require(bytes.length <= 125, "A ping may contain at most 125 bytes")
    writeFrame(9, bytes)

  def sendClose(code: Int = 1000, reason: String = "")(using Async): Unit =
    require(validCloseCode(code), s"Invalid close code: $code")
    val text = encode(reason)
    require(text.length <= 123, "A close reason may contain at most 123 UTF-8 bytes")
    writeFrame(8, ByteBuffer.allocate(text.length + 2).putShort(code.toShort).put(text).array())

  private def writeFrame(opcode: Int, payload: Array[Byte])(using Async): Unit =
    val permit = writing.acquire()
    try
      if closed.get() then throw HttpErrors.closedChannel()
      if closeSent then
        if opcode != 8 then throw HttpErrors.closedChannel()
      else
        if opcode == 8 then closeSent = true
        val extended = if payload.length < 126 then 0 else if payload.length <= 65535 then 2 else 8
        val out = ByteBuffer.allocate(2 + extended + (if client then 4 else 0) + payload.length)
        out.put((0x80 | opcode).toByte)
        val size = if extended == 0 then payload.length else if extended == 2 then 126 else 127
        out.put((size | (if client then 0x80 else 0)).toByte)
        if extended == 2 then out.putShort(payload.length.toShort)
        else if extended == 8 then out.putLong(payload.length.toLong)
        if client then
          val mask = WebSocketRandom.bytes(4)
          out.put(mask)
          var i = 0
          while i < payload.length do
            out.put((payload(i) ^ mask(i & 3)).toByte)
            i += 1
        else out.put(payload)
        out.flip()
        stream.writeBuf(out) match
          case Left(Error.EOF) => throw EOFException("WebSocket closed while writing")
          case Right(())       => ()
    catch
      case t: Throwable =>
        close()
        throw t
    finally permit.release()

  def receive()(using Async): Option[Message] =
    if !reading.compareAndSet(false, true) then throw ConcurrentReceiveException()
    try
      if closeReceived then None
      else
        if closed.get() then throw HttpErrors.closedChannel()
        val message = ByteArrayOutputStream()
        var kind = 0
        var result: Option[Message] = None
        var done = false
        while !done do
          val frame = readFrame(maxMessageSize - message.size())
          frame.opcode match
            case 8 =>
              val status = parseClose(frame.payload)
              receivedClose = Some(status)
              closeReceived = true
              if !closeSent then writeFrame(8, frame.payload)
              close()
              done = true
            case 9      => if !closeSent then writeFrame(10, frame.payload)
            case 10     => ()
            case opcode =>
              if opcode == 0 && kind == 0 then protocolError("Unexpected continuation frame")
              if opcode != 0 && kind != 0 then protocolError("New message before the previous message finished")
              if opcode != 0 then kind = opcode
              message.write(frame.payload)
              if frame.fin then
                val bytes = message.toByteArray
                val value =
                  if kind == 1 then
                    val text = try utf8(bytes)
                    catch case _: IOException => protocolError("Invalid text UTF-8", 1007)
                    Message.Text(text)
                  else Message.Binary(bytes)
                if !closeSent then
                  result = Some(value)
                  done = true
                message.reset()
                kind = 0
        result
    catch
      case e: ProtocolException =>
        try if !closeSent && !closed.get() then sendClose(e.closeCode)
        catch case _: Exception => ()
        finally close()
        throw e
      case t: Throwable =>
        close()
        throw t
    finally reading.set(false)

  private def readExactly(size: Int)(using Async): Array[Byte] =
    val out = ByteBuffer.allocate(size)
    while out.hasRemaining do
      val before = out.position()
      input.readBuf(out) match
        case Left(Error.EOF) => throw EOFException("WebSocket ended without a close frame")
        case Right(())       =>
          if out.position() == before then throw IOException("WebSocket read made no progress")
    out.array()

  private case class Frame(fin: Boolean, opcode: Int, payload: Array[Byte])

  private def readFrame(remaining: Int)(using Async): Frame =
    val head = readExactly(2)
    val first = head(0) & 255
    val second = head(1) & 255
    val fin = (first & 0x80) != 0
    val opcode = first & 15
    if (first & 0x70) != 0 then protocolError("Unexpected reserved frame bits")
    if !Set(0, 1, 2, 8, 9, 10).contains(opcode) then protocolError("Unknown frame opcode")
    if ((second & 0x80) != 0) == client then protocolError("Incorrect frame masking")
    val short = second & 127
    if opcode >= 8 && (!fin || short > 125) then protocolError("Invalid control frame")
    val length = short match
      case 126 =>
        val n = ByteBuffer.wrap(readExactly(2)).getShort() & 65535
        if n < 126 then protocolError("Non-minimal frame length")
        n.toLong
      case 127 =>
        val n = ByteBuffer.wrap(readExactly(8)).getLong()
        if n < 65536 then protocolError("Invalid frame length")
        n
      case n => n.toLong
    if opcode < 8 && length > remaining then protocolError("Message exceeds maxMessageSize", 1009)
    val mask = if !client then readExactly(4) else Array.emptyByteArray
    val payload = readExactly(length.toInt)
    if !client then
      var i = 0
      while i < payload.length do
        payload(i) = (payload(i) ^ mask(i & 3)).toByte
        i += 1
    Frame(fin, opcode, payload)

  private def parseClose(payload: Array[Byte]): Close =
    if payload.isEmpty then Close(None, "")
    else
      if payload.length == 1 then protocolError("Invalid close payload")
      val code = ByteBuffer.wrap(payload).getShort() & 65535
      if !validCloseCode(code) then protocolError("Invalid peer close code")
      val reason = try utf8(payload.drop(2))
      catch case _: IOException => protocolError("Invalid close reason UTF-8", 1007)
      Close(Some(code), reason)

  private def protocolError(message: String, code: Int = 1002): Nothing = throw ProtocolException(message, code)

object WebSocket:
  enum Message:
    case Text(value: String)
    case Binary(bytes: Array[Byte])

  final case class Close(code: Option[Int], reason: String)
  final class ProtocolException(message: String, val closeCode: Int) extends IOException(message)
  final class ConcurrentReceiveException extends IllegalStateException("A WebSocket receive is already pending")

  val DefaultMaxMessageSize: Int = 16 * 1024 * 1024

  def connect(
      url: String,
      headers: Headers = Headers.empty,
      subprotocols: Seq[String] = Seq.empty,
      maxMessageSize: Int = DefaultMaxMessageSize,
      tls: Option[TlsSupport] = None
  )(using TcpSupport, DnsSupport, Async): WebSocket =
    val transport = Transport(tls = tls)
    try connectWith(transport, url, headers, subprotocols, maxMessageSize)
    finally transport.close()

  def connectWith(
      transport: Transport,
      url: String,
      headers: Headers = Headers.empty,
      subprotocols: Seq[String] = Seq.empty,
      maxMessageSize: Int = DefaultMaxMessageSize
  )(using Async): WebSocket =
    import WebSocketProtocol.*
    require(maxMessageSize > 0, "maxMessageSize must be positive")
    require(
      subprotocols.forall(validToken) && subprotocols.distinct.size == subprotocols.size,
      "Invalid or duplicate WebSocket subprotocol"
    )
    val httpUrl =
      if url.startsWith("ws://") then "http://" + url.drop(5)
      else if url.startsWith("wss://") then "https://" + url.drop(6)
      else throw IllegalArgumentException("A WebSocket URL must use ws:// or wss://")
    val parsed = Url.parse(httpUrl)
    require(
      parsed.host.nonEmpty && !parsed.host.exists(c => c <= ' ' || "@/#?".contains(c)) &&
        parsed.port > 0 && parsed.port <= 65535 && !url.contains('#') &&
        !parsed.target.exists(c => c <= ' ' || c >= 127),
      "Invalid WebSocket URL"
    )
    val reserved = Set(
      "host",
      "connection",
      "upgrade",
      "content-length",
      "transfer-encoding",
      "sec-websocket-key",
      "sec-websocket-version",
      "sec-websocket-protocol",
      "sec-websocket-extensions"
    )
    require(
      !headers.iterator.exists((name, _) => reserved.exists(_.equalsIgnoreCase(name))),
      "WebSocket handshake headers are managed by the client"
    )
    val key = base64(WebSocketRandom.bytes(16))
    var requestHeaders = headers
      .set("Host", parsed.hostHeader)
      .set("Connection", "Upgrade")
      .set("Upgrade", "websocket")
      .set("Sec-WebSocket-Key", key)
      .set("Sec-WebSocket-Version", "13")
    if subprotocols.nonEmpty then
      requestHeaders = requestHeaders.set("Sec-WebSocket-Protocol", subprotocols.mkString(", "))
    val stream = transport.dial(parsed.scheme, parsed.host, parsed.port)
    try
      val fields = requestHeaders.iterator.map((k, v) => s"$k: $v\r\n").mkString
      stream.writeBuf(
        ByteBuffer.wrap(
          (s"GET ${parsed.target} HTTP/1.1\r\n${fields}\r\n")
            .getBytes(StandardCharsets.ISO_8859_1)
        )
      )
      val reader = RequestReader(stream)
      val head = reader.readHead().getOrElse(throw EOFException("No WebSocket handshake response"))
      val lines = new String(head, StandardCharsets.ISO_8859_1).split("\r\n", -1)
      if !lines.head.matches("HTTP/1\\.1 101(?: .*)?") then throw IOException("WebSocket upgrade was refused")
      var response = Headers.empty
      lines
        .drop(1)
        .foreach: line =>
          val colon = line.indexOf(':')
          if colon <= 0 || !HeaderSyntax.isValidName(line.take(colon)) then
            throw IOException("Malformed upgrade header")
          response = response.add(line.take(colon), line.drop(colon + 1).trim)
      if !hasToken(response, "Connection", "Upgrade") || !hasToken(response, "Upgrade", "websocket") ||
        response.getAll("Sec-WebSocket-Accept") != Seq(accept(key)) || response.contains("Sec-WebSocket-Extensions")
      then throw IOException("Invalid WebSocket handshake response")
      val selected = response.getAll("Sec-WebSocket-Protocol")
      if selected.size > 1 || selected.exists(p => !subprotocols.contains(p)) then
        throw IOException("Server selected an unoffered WebSocket subprotocol")
      new WebSocket(stream, reader, true, selected.headOption, maxMessageSize)
    catch
      case t: Throwable =>
        stream.close()
        throw t

  def resource(
      url: String,
      headers: Headers = Headers.empty,
      subprotocols: Seq[String] = Seq.empty,
      maxMessageSize: Int = DefaultMaxMessageSize,
      tls: Option[TlsSupport] = None
  )(using TcpSupport, DnsSupport): Resource[WebSocket] =
    Resource(connect(url, headers, subprotocols, maxMessageSize, tls), _.close())

  def handler(
      body: WebSocket => Async ?=> Unit,
      subprotocols: Seq[String] = Seq.empty,
      maxMessageSize: Int = DefaultMaxMessageSize,
      originAllowed: HttpRequest => Boolean = sameOrigin
  ): Handler = new Handler:
    def serveHTTP(w: ResponseWriter, r: HttpRequest)(using Async): Unit =
      upgrade(w, r, subprotocols, maxMessageSize, originAllowed) match
        case None         => ()
        case Some(socket) =>
          try body(socket)
          finally socket.close()

  def upgrade(
      w: ResponseWriter,
      request: HttpRequest,
      subprotocols: Seq[String] = Seq.empty,
      maxMessageSize: Int = DefaultMaxMessageSize,
      originAllowed: HttpRequest => Boolean = sameOrigin
  )(using Async): Option[WebSocket] =
    import WebSocketProtocol.*
    require(maxMessageSize > 0, "maxMessageSize must be positive")
    require(
      subprotocols.forall(validToken) && subprotocols.distinct.size == subprotocols.size,
      "Invalid or duplicate WebSocket subprotocol"
    )
    val h = request.headers
    val keys = h.getAll("Sec-WebSocket-Key")
    val offered = tokens(h, "Sec-WebSocket-Protocol")
    val valid = request.method == HttpMethod.GET && request.version == "HTTP/1.1" &&
      h.getAll("Host").size == 1 && hasToken(h, "Connection", "Upgrade") && hasToken(h, "Upgrade", "websocket") &&
      keys.size == 1 && validKey(keys.head) && request.body.isEmpty &&
      !h.contains("Transfer-Encoding") && h.getAll("Content-Length").forall(_ == "0") &&
      offered.forall(validToken) && offered.distinct.size == offered.size
    val error =
      if !valid then Some(HttpResponse.badRequest("invalid WebSocket handshake"))
      else if h.getAll("Sec-WebSocket-Version") != Seq("13") then
        Some(
          HttpResponse(HttpStatus(426, "Upgrade Required"), "WebSocket version 13 required\n")
            .withHeader("Sec-WebSocket-Version", "13")
        )
      else if !originAllowed(request) then Some(HttpResponse(HttpStatus.Forbidden, "WebSocket origin refused\n"))
      else None
    error match
      case Some(response) =>
        Handler.of(_ => response).serveHTTP(w, request)
        None
      case None =>
        val selected = subprotocols.find(offered.contains)
        w.header.set("Connection", "Upgrade")
        w.header.set("Upgrade", "websocket")
        w.header.set("Sec-WebSocket-Accept", accept(keys.head))
        selected.foreach(w.header.set("Sec-WebSocket-Protocol", _))
        val (stream, reader) = w.upgrade()
        Some(new WebSocket(stream, reader, false, selected, maxMessageSize))

  private def sameOrigin(request: HttpRequest): Boolean =
    request.headers.getAll("Origin") match
      case Seq()       => true
      case Seq(origin) =>
        try
          val parsed = Url.parse(origin)
          parsed.path == "/" && parsed.query.isEmpty &&
          request.headers.get("Host").exists(_.equalsIgnoreCase(parsed.hostHeader))
        catch case _: IllegalArgumentException => false
      case _ => false
