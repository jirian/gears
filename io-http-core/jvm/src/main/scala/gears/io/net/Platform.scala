package gears.io.net

/** What the JVM gives the higher level that differs by platform. */
object Platform {

  /** `n` random bytes, for a WebSocket client's masks. */
  def randomBytes(n: Int): Array[Byte] = {
    val out = new Array[Byte](n)
    random.nextBytes(out)
    out
  }

  private val random = new java.security.SecureRandom()
}
