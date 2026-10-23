package gears.io.http

/** Random bytes for WebSocket masks and handshake keys, from the platform's secure source. */
private[http] object WebSocketRandom:
  private val random = new java.security.SecureRandom()
  def bytes(size: Int): Array[Byte] =
    val out = new Array[Byte](size)
    random.nextBytes(out)
    out
