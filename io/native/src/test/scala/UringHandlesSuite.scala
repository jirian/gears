package gears.io.uring

import gears.io.reactor.Address

import scala.scalanative.posix.fcntl
import scala.scalanative.posix.unistd

class UringHandlesSuite extends munit.FunSuite:
  private def closeOnExec(fd: Int): Boolean = (fcntl.fcntl(fd, fcntl.F_GETFD, 0) & fcntl.FD_CLOEXEC) != 0

  test("every handle is close-on-exec"):
    val (in, out) = UringHandles.pipe()
    val listener = UringHandles.listen(Address.IPv4("127.0.0.1", 0))
    val datagram = UringHandles.datagram(Address.IPv4("127.0.0.1", 0), null)
    val file = UringHandles.openFile("build.sbt", write = false)
    try
      for (name, fd) <- Seq("pipe" -> in, "pipe" -> out, "listener" -> listener, "datagram" -> datagram, "file" -> file)
      do assert(closeOnExec(fd), s"the $name descriptor $fd is inherited by child processes")
    finally Seq(in, out, listener, datagram, file).foreach(UringHandles.close)

  test("a path left behind by a close that bypassed the handles is not removed under a newer descriptor"):
    val path = s"/tmp/gears-handles-stale-${unistd.getpid()}.sock"
    val stale = UringHandles.listen(Address.Unix(path))
    unistd.close(stale)
    try
      // POSIX hands out the lowest free number, so the pipe's read end reuses the closed listener's.
      val (in, out) = UringHandles.pipe()
      assertEquals(in, stale)
      UringHandles.close(in)
      UringHandles.close(out)
      assert(java.io.File(path).exists(), "closing an unrelated descriptor removed the path")
    finally java.io.File(path).delete()
