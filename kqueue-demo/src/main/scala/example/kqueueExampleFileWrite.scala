package example

import java.nio.charset.StandardCharsets
import scala.scalanative.unsafe.Zone

import asyncio.Completion
import asyncio.kqueue.KqueueReactor
import asyncio.kqueue.WriteFromBuffer
import asyncio.unsafe.NativeBuffer
import KQueueExampleIO.*

/** actually useless for "real" files, it always blocks, so use with a FIFO for example
  */
object KQueueExampleFileWrite {

  /** Writes the message again after every completed write, until the reader stops draining the FIFO. */
  private final class Writer(r: KqueueReactor, fd: Int, message: String)(using Zone)
      extends Completion[WriteFromBuffer] {
    private val bytes = message.getBytes(StandardCharsets.UTF_8)
    private val buf = NativeBuffer.allocate(math.max(bytes.length, 1))

    /** Loads the whole message and asks for it to be written. */
    def start(): Unit = {
      buf.clear()
      buf.put(bytes)
      buf.flip()
      r.submit(WriteFromBuffer(fd, buf), this)
    }

    def onComplete(command: WriteFromBuffer): Unit = {
      println(s"actually wrote ${buf.position()} bytes") // the buffer was loaded at position 0
      start()
    }
  }

  def run(fifo: String, message: String): Unit = {
    KqueueReactor.scoped(maxEvents = 1) { r =>
      withFile(fifo, write = true) { fd =>
        Zone.acquire { implicit z =>
          val writer = new Writer(r, fd, message)
          writer.start()
          println("Event registered for file write.")
          println("starting to poll...")
          r.run()
        }
      }
    }
  }
}
