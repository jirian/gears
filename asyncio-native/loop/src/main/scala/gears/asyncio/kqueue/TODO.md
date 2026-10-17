# Kqueue reactor: syscall reduction TODO

Most operations pay for a `kevent` call of their own, or a trip through the wake pipe, even when the work could be done
right away on the reactor thread. A gears `perform(r, read)` that finds data already waiting costs about 7 syscalls
today; with items 1–3 below it would cost 1 (just the `read`), and about 2 when the data isn't there yet.

## Measuring

Measure each item with `kqueuedemo gears bench`: build the executables to compare with `kqueue-demo/bench/build.sh`
(release-fast, against the Scala Native nightly, since 0.5.12's GC can crash multithreaded release builds) and compare
them with `kqueue-demo/bench/compare.py` (see its header). Debug builds overstate the Scala-side cost, so measure release
builds. The scenarios map onto the items below: `pingpong` (items 2, 3 and the resume path), `pingpong-deadline`
(item 5), `concurrent` (item 4), `pipelined` (items 2 and 3 on simultaneous reads and writes), and `blocking` (item 6).
`kqueuedemo bench` runs the same scenarios against `KqueueLoop` directly, as a reference for what a hand-written loop
costs. Medians from the same build differ by about 1% between runs (`blocking` by up to about 6%).

Median microseconds per operation over 3 runs of release-fast builds on the development machine, before and after items
1 and 6, with the hand-written reference:

| scenario | before | after items 1 and 6 | raw `KqueueLoop` |
|---|---|---|---|
| pingpong | 28.7 | 19.1 (−34%) | 6.7 |
| pingpong-deadline | 32.3 | 21.2 (−35%) | 8.3 |
| concurrent | 8.4 | 8.0 (−4%) | 2.3 |
| pipelined | 9.7 | 7.7 (−21%) | 0.9 |
| blocking | 14.3 | 7.9 (−45%) | 5.0 |

## What each operation costs now

| Operation | Syscalls | Where |
|---|---|---|
| Submit a read, write, accept, or connect | 1 `kevent` to register a one-shot filter | `KqueueReactor.arm` |
| Each pass of the loop | 1 `kevent` to wait | `KqueueReactor.run` |
| Perform the op | 1 `read`/`write`/`accept`/… | `Command.perform` |
| Not ready after all (would-block) | 1 more `kevent` to register again | `KqueueReactor.dispatch` |
| Cancel a command or timer | 1 `kevent` (`EV_DELETE`) | `KqueueReactor.cancel` |
| Submit a timer | 1 `kevent` | `KqueueReactor.submit` |
| Complete a promise or blocker | 1 `write` to the wake pipe, then 2+ `read`s to drain it (the last returns `EAGAIN`) | `KqueueReactor.resolved`, `drainWakePipe` |
| Gears task queued with `ReactorScheduler.execute`, even from the reactor thread | the promise path above: `write` + a `kevent` wake + 2 `read`s | `ReactorScheduler.arm`/`execute` |
| Accept a connection | `accept` + `fcntl(F_GETFL)` + `fcntl(F_SETFL)` | `NonBlocking.accept` |

## Proposals, in order of what they save

- [x] **1. Don't use the wake pipe for work queued on the reactor thread.**
  Done, in two steps. First `KqueueReactor.resolved` stopped writing to the pipe for promises resolved on the loop
  thread; on the gears key-value client (200,000 pipelined commands, about 400 resumes) that removed all 406 pipe writes
  and their draining reads, and cut `kevent` waits from 646 to 399. Then the reactor's promise op was removed: gears code
  uses gears promises, and the scheduler drives the loop itself with `Reactor.runOnce`, running its tasks between turns
  and blocking only when it has none. `ReactorScheduler.execute` calls `Reactor.wakeup` only from another thread, so
  resuming a continuation on the reactor thread costs no syscalls and no promise allocation.

- [ ] **2. Try the operation before waiting for readiness.**
  Sockets are usually writable, and a request-response or pipelined peer usually has data waiting already. Go and tokio
  both try first.
  - On `submit`, call `perform()` straight away. If it would block, register and wait as now.
  - If it succeeds, put the completion on the ready queue from item 1, rather than calling it inside `submit`, so
    completions still run from the loop and there's no recursion. Depends on item 1.
  - Saves the registering `kevent`, the wait, and the event for a successful attempt.
  - Cost: a wasted `read` when nothing is waiting. Keep it cheap by not retrying right after a would-block on the same
    descriptor.

- [ ] **3. Persistent edge-triggered registration instead of one-shot filters.**
  `EV_ONESHOT` means one `kevent` per submit, and another after each spurious wake-up.
  - Register each descriptor and direction once with `EV_ADD | EV_CLEAR`, at its first submit or when the handle opens.
  - Edges set a per-slot "ready" bit; a submit checks the bit and performs at once, and a would-block clears it.
  - Closing the descriptor removes its filters automatically.
  - A cancel only clears the slot, so it needs no `EV_DELETE`.
  - Saves every registration and re-registration in steady state.

- [ ] **4. Batch remaining changes into the wait call.**
  `kevent` takes a list of changes and an array for events in one call, but the reactor issues a separate call per change.
  - Append `EV_ADD`/`EV_DELETE` entries (and timers, if kernel timers stay) to a pending change list, and pass it to the
    next waiting `kevent`.
  - Errors for individual changes then arrive as `EV_ERROR` events in the event array. Today `run` throws on any
    `EV_ERROR`: dispatch must instead fail the matching op, and ignore `ENOENT`/`EBADF` from deletes on closed
    descriptors.
  - Saves one syscall per change that items 2 and 3 don't remove (cancels, timers, first registrations).

- [ ] **5. Timers in user space.**
  Each timer submit and cancel is a `kevent` call, and gears creates and cancels timers constantly (`sleep`, timeouts that
  lose an `Async.select`).
  - Keep a deadline heap on the reactor thread, and pass the nearest deadline as the wait's timeout
    (`KqueueLoop.pollEventsTimeout` already exists). libuv and tokio work this way.
  - Saves both syscalls per timer.

- [x] **6. Cheaper wake-ups from other threads.**
  Done: the wake pipe is replaced by an `EVFILT_USER` event, so waking is one `kevent(NOTE_TRIGGER)` call with nothing to
  drain, and the reactor no longer holds two pipe descriptors. Wake-ups are merged with an atomic flag, which the loop
  clears when it sees the event, so only the first `wakeup` since then makes the syscall. `wakeLock` remains, so that a
  trigger cannot reach the kqueue's descriptor number after `close` has released it.

- [ ] **7. Accept with fewer syscalls.**
  - On macOS a socket returned by `accept` inherits `O_NONBLOCK` from the listening socket, so the two `fcntl` calls in
    `NonBlocking.accept` are unnecessary there.
  - Where they are needed, `ioctl(FIONBIO)` sets the mode in one call instead of two; the same applies to
    `PosixSockets.setNonBlocking`.
  - Saves 2 syscalls per accepted connection.

## Related, not syscalls

- [ ] **Read event fields in one call.** `dispatch` reads each event through about five separate
  `scalanative_kevent_get` calls, each with its own `stackalloc`. Fetch all fields at once, or read the struct directly
  (32 bytes on macOS).
- [ ] **SIGPIPE (correctness).** No socket sets `SO_NOSIGPIPE`, and writes don't pass `MSG_NOSIGNAL`, so writing to a peer
  that has closed raises `SIGPIPE` and kills the process. Set `SO_NOSIGPIPE` once per socket, at creation or accept.
- [ ] **Move to Scala Native 0.5.13 once it is released.** On 0.5.12, multithreaded release builds of the gears demos
  crash intermittently in `ReactorFutures.perform` just after a continuation resumes: the GC ignored conservative roots
  that point partway into an object, so an object referenced only by such an interior pointer, as optimised code can
  hold across a suspension, was collected while live ([scala-native#5080](https://github.com/scala-native/scala-native/pull/5080),
  the fix for [#5046](https://github.com/scala-native/scala-native/issues/5046)). Debug builds are unaffected. The
  `gears-release-repro` branch reproduces it with gears alone: two threads awaiting through `Async.withGroup`. Until then,
  `SCALA_NATIVE_NIGHTLY=1` builds against a nightly that includes the fix, and `kqueue-demo/bench/build.sh` uses it for
  benchmark executables. When moving, set the plugin version in `project/plugins.sbt` and remove the nightly switch
  there and in `build.sbt`, along with the nightly check in `build.sh`.
