package gears.io.net

object Platform {

  def randomBytes(n: Int): Array[Byte] = {
    val out = new Array[Byte](n)
    random.nextBytes(out)
    out
  }

  private val random = new java.security.SecureRandom()
}
