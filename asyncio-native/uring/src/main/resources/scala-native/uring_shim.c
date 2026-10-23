#define _GNU_SOURCE

#if defined(__linux__) && __has_include(<liburing.h>)
#include <liburing.h>
#include <string.h>

struct io_uring_sqe *asyncio_io_uring_get_sqe(struct io_uring *ring) {
  return io_uring_get_sqe(ring);
}

unsigned asyncio_io_uring_sq_ready(struct io_uring *ring) {
  return io_uring_sq_ready(ring);
}

void asyncio_io_uring_cq_advance(struct io_uring *ring, unsigned nr) {
  io_uring_cq_advance(ring, nr);
}

/* Cancels the request with `user_data` synchronously, without waiting for one that is already running: that one
 * fails with -ETIME, and its completion comes later. Linux 6.0 or later; earlier kernels fail with -EINVAL. */
int asyncio_io_uring_cancel_now(struct io_uring *ring, unsigned long long user_data) {
  struct io_uring_sync_cancel_reg reg;
  memset(&reg, 0, sizeof(reg));
  reg.addr = user_data;
  reg.timeout.tv_sec = 0;
  reg.timeout.tv_nsec = 0;
  return io_uring_register_sync_cancel(ring, &reg);
}
#endif
