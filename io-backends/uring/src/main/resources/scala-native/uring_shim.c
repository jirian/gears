#define _GNU_SOURCE

#if defined(__linux__) && __has_include(<liburing.h>)
#include <liburing.h>
#include <string.h>

/* 1 if completions are in the ring, 2 if the kernel has work to run that posts them, which an enter runs, else 0. */
int gears_io_uring_spin_check(struct io_uring *ring) {
  if (io_uring_cq_ready(ring)) return 1;
  if (IO_URING_READ_ONCE(*ring->sq.kflags) & IORING_SQ_TASKRUN) return 2;
  /* Nothing yet: a pause before the next look, which leaves the core's other hardware thread room to run. */
#if defined(__x86_64__) || defined(__i386__)
  __builtin_ia32_pause();
#elif defined(__aarch64__)
  __asm__ __volatile__("yield");
#endif
  return 0;
}

/* 1 if `sqe` is still in the submission queue, not yet taken by the kernel: a submission refused or cut short (EBUSY,
 * EAGAIN, ENOMEM) leaves it there. The kernel's head moves only in this thread's io_uring_enter (no SQPOLL). */
int gears_io_uring_sqe_pending(struct io_uring *ring, struct io_uring_sqe *sqe) {
  unsigned head = io_uring_smp_load_acquire(ring->sq.khead);
  unsigned index = (unsigned)(sqe - ring->sq.sqes);
  return ((index - head) & ring->sq.ring_mask) < ring->sq.sqe_tail - head;
}

struct io_uring_sqe *gears_io_uring_get_sqe(struct io_uring *ring) {
  return io_uring_get_sqe(ring);
}

unsigned gears_io_uring_sq_ready(struct io_uring *ring) {
  return io_uring_sq_ready(ring);
}

void gears_io_uring_cq_advance(struct io_uring *ring, unsigned nr) {
  io_uring_cq_advance(ring, nr);
}

/* Sets up a ring with `entries` submission entries and `completions` completion entries. The kernel runs completions'
 * work when the worker next enters it, rather than interrupting it for each (COOP_TASKRUN), and flags the ring when
 * there is some, which liburing's reaping checks (TASKRUN_FLAG). */
int gears_io_uring_init(unsigned entries, struct io_uring *ring, unsigned completions) {
  struct io_uring_params params;
  memset(&params, 0, sizeof(params));
  params.flags = IORING_SETUP_CQSIZE | IORING_SETUP_COOP_TASKRUN | IORING_SETUP_TASKRUN_FLAG;
  params.cq_entries = completions;
  return io_uring_queue_init_params(entries, ring, &params);
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

/* Cancels the request with `user_data`. One the kernel had not started, or had armed a poll for, is cancelled at
 * once, and one that finished first was already done: either way its completion is in the ring when this returns on
 * the ring's own thread. One running in a kernel worker, such as a read of a regular file, is interrupted if it waits
 * and otherwise cannot be stopped: with `wait` this waits for it to let go, and otherwise returns -ETIME (or
 * -EALREADY), leaving it running. -ENOENT if none was found. Linux 6.0 or later; earlier kernels fail with -EINVAL. */
int gears_io_uring_cancel_now(struct io_uring *ring, unsigned long long user_data, int wait) {
  struct io_uring_sync_cancel_reg reg;
  memset(&reg, 0, sizeof(reg));
  reg.addr = user_data;
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
