package gears.async.http

import java.io.IOException
import java.nio.channels.ClosedChannelException

private[http] object HttpErrors:
  def closedChannel(): IOException = new ClosedChannelException()
  def isClosedChannel(error: IOException): Boolean = error.isInstanceOf[ClosedChannelException]
