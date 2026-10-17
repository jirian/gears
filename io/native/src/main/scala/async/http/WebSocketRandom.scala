package gears.async.http

private[http] object WebSocketRandom:
  def bytes(size: Int): Array[Byte] =
    val out = new Array[Byte](size)
    val input = new java.io.FileInputStream("/dev/urandom")
    try
      var offset = 0
      while offset < size do
        val n = input.read(out, offset, size - offset)
        if n <= 0 then throw new java.io.IOException("Could not read WebSocket randomness")
        offset += n
      out
    finally input.close()
