#define _GNU_SOURCE

#if defined(__linux__) && __has_include(<liburing.h>)
#include <liburing.h>
#include <string.h>

struct io_uring_sqe *gears_io_uring_get_sqe(struct io_uring *ring) {
  return io_uring_get_sqe(ring);
}

unsigned gears_io_uring_sq_ready(struct io_uring *ring) {
  return io_uring_sq_ready(ring);
}

void gears_io_uring_cq_advance(struct io_uring *ring, unsigned nr) {
  io_uring_cq_advance(ring, nr);
}

/* The size of struct io_uring, which the reactor allocates: its fields are liburing's own, so the Scala side only
 * ever holds a pointer to it. */
size_t gears_io_uring_size(void) {
  return sizeof(struct io_uring);
}

/* The IORING_FEAT_* flags of a ring set up by io_uring_queue_init. */
unsigned gears_io_uring_features(struct io_uring *ring) {
  return ring->features;
}

/* Whether completions wait in the kernel's overflow list, for want of room in the completion queue. */
int gears_io_uring_cq_overflowing(struct io_uring *ring) {
  return io_uring_cq_has_overflow(ring);
}

/* Moves completions from the kernel's overflow list into the completion queue, as far as it has room. */
int gears_io_uring_get_events(struct io_uring *ring) {
  return io_uring_get_events(ring);
}

/* Submits the entries prepared and waits for a completion, in one system call: at most `nanoseconds`, or with no limit
 * if it is negative. 0 once one has arrived, -ETIME if none did in time, else a negated errno. */
int gears_io_uring_submit_and_wait(struct io_uring *ring, long long nanoseconds) {
  struct io_uring_cqe *cqe;
  struct __kernel_timespec ts;
  if (nanoseconds < 0) return io_uring_submit_and_wait_timeout(ring, &cqe, 1, NULL, NULL);
  ts.tv_sec = nanoseconds / 1000000000LL;
  ts.tv_nsec = nanoseconds % 1000000000LL;
  return io_uring_submit_and_wait_timeout(ring, &cqe, 1, &ts, NULL);
}

static int cancel(struct io_uring *ring, struct io_uring_sync_cancel_reg *reg, int wait) {
  // -1/-1 waits until the kernel has let go; 0/0 waits for nothing running in a kernel worker, which is -ETIME.
  reg->timeout.tv_sec = wait ? -1 : 0;
  reg->timeout.tv_nsec = wait ? -1 : 0;
  return io_uring_register_sync_cancel(ring, reg);
}

/* Cancels the request with `user_data`, or with IORING_ASYNC_CANCEL_ANY in `flags` every request. One the kernel had
 * not started, or had armed a poll for, is cancelled at once, and one that finished first was already done: either
 * way its completion is in the ring when this returns on the ring's own thread. One running in a kernel worker, such
 * as a read of a regular file, cannot be stopped: with `wait` this waits for it to finish, and otherwise returns
 * -ETIME (or -EALREADY), leaving it running. -ENOENT if none was found. Linux 6.0 or later; earlier kernels fail with
 * -EINVAL. */
int gears_io_uring_cancel_now(struct io_uring *ring, unsigned long long user_data, int flags, int wait) {
  struct io_uring_sync_cancel_reg reg;
  memset(&reg, 0, sizeof(reg));
  reg.addr = user_data;
  reg.flags = flags;
  return cancel(ring, &reg, wait);
}

/* Cancels every request on descriptor `fd`, as gears_io_uring_cancel_now does one, from any thread: the kernel
 * serializes it with the ring's own submissions and cancels. Never waits for one running in a kernel worker. */
int gears_io_uring_cancel_fd(struct io_uring *ring, int fd) {
  struct io_uring_sync_cancel_reg reg;
  memset(&reg, 0, sizeof(reg));
  reg.fd = fd;
  reg.flags = IORING_ASYNC_CANCEL_FD | IORING_ASYNC_CANCEL_ALL;
  return cancel(ring, &reg, 0);
}
#endif
