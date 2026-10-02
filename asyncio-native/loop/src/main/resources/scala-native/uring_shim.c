// liburing.h relies on glibc declarations (sigset_t, AT_FDCWD, ...) that are
// only visible under GNU/BSD feature-test macros; Scala Native's toolchain
// doesn't enable them by default.
#define _GNU_SOURCE

// This project also builds the kqueue reactor on macOS/BSD, where there is no
// io_uring: compile the shim only where liburing is actually available.
#if defined(__linux__) && __has_include(<liburing.h>)
#include <liburing.h>

struct io_uring_sqe *fs2_io_uring_get_sqe(struct io_uring *ring) {
  return io_uring_get_sqe(ring);
}

void fs2_io_uring_cq_advance(struct io_uring *ring, unsigned nr) {
  io_uring_cq_advance(ring, nr);
}
#endif
