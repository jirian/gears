package gears.io.uring

object LinuxConstants {
  final val SOCK_NONBLOCK = 0x800
  final val SOCK_CLOEXEC = 0x80000
  final val O_CLOEXEC = 0x80000
  final val POLLIN = 0x001
  final val POLLOUT = 0x004
  final val ETIME = 62
}
