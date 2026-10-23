package gears.io.http

import java.io.IOException

/** The exception for using a closed stream, which differs by platform: Scala.js has no `ClosedChannelException`. */
private[http] object HttpErrors:
  private final class ClosedStream extends IOException("The HTTP stream is closed")
  def closedChannel(): IOException = new ClosedStream()
  def isClosedChannel(error: IOException): Boolean = error.isInstanceOf[ClosedStream]
