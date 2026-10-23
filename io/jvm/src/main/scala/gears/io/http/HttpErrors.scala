package gears.io.http

import java.io.IOException
import java.nio.channels.ClosedChannelException

/** The exception for using a closed stream, which differs by platform: Scala.js has no `ClosedChannelException`. */
private[http] object HttpErrors:
  def closedChannel(): IOException = new ClosedChannelException()
  def isClosedChannel(error: IOException): Boolean = error.isInstanceOf[ClosedChannelException]
