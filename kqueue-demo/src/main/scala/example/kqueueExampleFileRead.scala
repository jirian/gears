package example

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import scala.scalanative.unsafe.Zone

import asyncio.Completion
import asyncio.Interest
import asyncio.kqueue.Command
import asyncio.kqueue.KqueueReactor
import asyncio.unsafe.NativeBuffer
import KQueueExampleIO.*

/** actually useless for "real" files, it always blocks, so use with a FIFO for example
  */
object KQueueExampleFileRead {

  /** Reads everything available whenever the FIFO is readable, gathering the contents until the writer closes it. */
  private final class Reader(r: KqueueReactor, fd: Int)(using Zone) extends Completion[Command] {
    private val chunk = NativeBuffer.allocate(1024)
    private val contents = new ByteArrayOutputStream()

    inline val ReadAgain = true
    inline val Stop = false

    /** Drains the FIFO. It is done only once the writer has closed it; until then the reactor waits again. */
    final class Drain extends Command {
      def fd: Int = Reader.this.fd
      def interest: Interest = Interest.Read
      def perform(): Boolean = drain()
    }

    val drainCommand = new Drain

    /** Reads everything currently available; returns true once the writer has closed the FIFO. */
    def drain(): Boolean = {
      var isEOF = false
      while {
        chunk.clear()
        val read = nioReadBytes(fd, chunk)
        if read < 0 then {
          isEOF = true
          Stop
        } else if read == 0 then {
          println("No more data available to read (EAGAIN or EWOULDBLOCK).")
          Stop
        } else {
          println(s"actually read $read bytes")
          chunk.flip()
          drainTo(chunk, contents)
          ReadAgain
        }
      } do ()
      isEOF
    }

    def report(): Unit = {
      println(s"End of file reached. Contents are ${contents.size} long.")
      println(s"File contents: `${contents.toString(StandardCharsets.UTF_8)}`")
      contents.reset() // Start over for the next writer
    }

    def start(): Unit = r.submit(drainCommand, this)

    def onComplete(command: Command): Unit = {
      report()
      r.submit(command, this)
    }
  }

  def run(fifo: String): Unit = {
    KqueueReactor.scoped(maxEvents = 1) { r =>
      withFile(fifo, write = false) { fd =>
        Zone.acquire { implicit z =>
          val reader = new Reader(r, fd)
          reader.start()
          println("Event registered for file read.")
          println("starting to poll...")
          r.run()
        }
      }
    }
  }
}
