#define _GNU_SOURCE

#if defined(__linux__) && __has_include(<liburing.h>)
#include <liburing.h>

struct io_uring_sqe *gears_io_uring_get_sqe(struct io_uring *ring) {
  return io_uring_get_sqe(ring);
}

unsigned gears_io_uring_sq_ready(struct io_uring *ring) {
  return io_uring_sq_ready(ring);
}

void gears_io_uring_cq_advance(struct io_uring *ring, unsigned nr) {
  io_uring_cq_advance(ring, nr);
}
#endif
