package gears.io.http

import scala.scalajs.js
import scala.scalajs.js.typedarray.Uint8Array

/** Random bytes for WebSocket masks and handshake keys, from the platform's secure source. */
private[http] object WebSocketRandom:
  def bytes(size: Int): Array[Byte] =
    val out = new Uint8Array(size)
    js.Dynamic.global.globalThis.crypto.getRandomValues(out)
    Array.tabulate(size)(i => out(i).toByte)
