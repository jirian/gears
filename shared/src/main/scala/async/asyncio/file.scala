package gears.async.file

import gears.async.Async
import gears.async.asyncio.{Reader, Writer, Result}
import java.io.Closeable

/** An open file, readable asynchronously - mirrors `net.TcpStream`'s own
  * shape (a plain [[Reader]] plus [[Closeable]]), so it composes with the
  * rest of `gears.async.asyncio` (`BufferedReader`, etc.) the same way a
  * socket does.
  */
abstract class FileReader extends Reader, Closeable

/** An open file, writable asynchronously - the [[Writer]] counterpart to
  * [[FileReader]]. Kept as its own type (not `FileReader with FileWriter`)
  * the same way `openRead`/`openWrite` are kept separate rather than one
  * "open for read+write" call: a file's access mode is fixed for real by
  * the OS at `open(2)` time (`O_RDONLY` vs `O_WRONLY`), so a value that's
  * only ever opened one way shouldn't advertise a capability - like
  * reading from a write-only fd - that would just fail at the syscall
  * layer if used.
  */
abstract class FileWriter extends Writer, Closeable

/** Backend capability for opening files - mirrors `net.TcpSupport`. */
trait FileSupport:
  type File <: FileReader
  type FileOut <: FileWriter

  /** Opens `path` for reading. A missing file, a permission error, etc. -
    * anything the OS reports as a genuine open failure - surfaces as a
    * thrown exception, not a [[Result]] `Left`, matching how
    * `TcpSupport.connect`/`listen` already treat their own setup failures:
    * [[Result]]'s `Left` is reserved for the one expected, in-band outcome
    * (end of stream), not for setup-time errors.
    */
  def openRead(path: String)(using Async): Result[File]

  /** Opens `path` for writing - creating it if it doesn't exist and
    * truncating it if it does, matching Go's `os.Create`/Java's
    * `new FileOutputStream(path)` (not `FileOutputStream(path, append =
    * true)`) - the common "start this file fresh" case. Failure modes are
    * the same as [[openRead]]'s: a thrown exception, not a `Left`.
    */
  def openWrite(path: String)(using Async): Result[FileOut]

object FileSupport:
  def openRead(path: String)(using fs: FileSupport, async: Async): Result[fs.File] =
    fs.openRead(path)
  def openWrite(path: String)(using fs: FileSupport, async: Async): Result[fs.FileOut] =
    fs.openWrite(path)
