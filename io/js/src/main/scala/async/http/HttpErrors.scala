package gears.async.http

import java.io.IOException

private[http] object HttpErrors:
  private final class ClosedStream extends IOException("The HTTP stream is closed")
  def closedChannel(): IOException = new ClosedStream()
  def isClosedChannel(error: IOException): Boolean = error.isInstanceOf[ClosedStream]
