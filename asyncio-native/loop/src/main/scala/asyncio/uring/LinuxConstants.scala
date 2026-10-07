package asyncio.uring

/** Linux flag and error values that Scala Native's posixlib does not expose, shared by the uring reactor and the code
  * built on it.
  */
object LinuxConstants {
  final val SOCK_NONBLOCK = 0x800
  final val SOCK_CLOEXEC = 0x80000
  final val O_CLOEXEC = 0x80000
  final val POLLIN = 0x001
  final val POLLOUT = 0x004
  final val ETIME = 62
}
