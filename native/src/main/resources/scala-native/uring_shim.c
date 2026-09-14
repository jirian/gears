// liburing.h relies on glibc declarations (sigset_t, AT_FDCWD, ...) that are
// only visible under GNU/BSD feature-test macros; Scala Native's toolchain
// doesn't enable them by default.
#define _GNU_SOURCE
#include <liburing.h>

struct io_uring_sqe *fs2_io_uring_get_sqe(struct io_uring *ring) {
  return io_uring_get_sqe(ring);
}

void fs2_io_uring_cq_advance(struct io_uring *ring, unsigned nr) {
  io_uring_cq_advance(ring, nr);
}
