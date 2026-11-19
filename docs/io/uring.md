# The io_uring backend: a design note

`UringBackend` (`io-backends/uring`, package `gears.async.io.uring`) is the backend for Scala Native on Linux 6.0 or
later, through liburing. It runs on the shared native runtime (`io-runtime`, `gears.async.io.shareable`, based on Jamie
Thompson's `WorkpoolScheduler`, `TimerHeap` and `TimerHeapOp` at 44bb0119 of his `async-io-kqueue-basepoint` branch,
tag `jamie-runtime-base`): a pool of workers, each with an inbox, a ready queue and a heap of timers, which is the
program's gears scheduler and performs its ops. Each worker owns one ring (`Ring`), which is its poller: it prepares
its ops' entries and reaps their completions on its own thread. This note records what an op costs, how an op is taken
back from a kernel that owns its buffers, how a close reaches entries on other rings, and what is open.

## The model

An op is performed on the performer's own worker (`UringScheduler.UringWorker.tryPerform`). A command (`Command`) is
tried at once with a non-blocking system call (`tryAtOnce`); if that would block, it becomes one submission queue entry
on the worker's ring (`prepare`), and the completion that answers it goes to `finish`, which says whether the op is
done or is to be submitted again. Once done the op is fired: its outcome goes to the performer under gears' listener
lock (`AbstractTimerHeapOp.timerWakeup`), which resumes it on the worker.

The kernel owns a command's buffers from the submission of its entry until its completion has been reaped, or until a
cancel has made the kernel let go of it. Everything below follows from that:

- A withdrawn op (cancelled, or past its time limit) cannot just be forgotten, since the kernel may still write into
  its buffer. Its worker takes it back: an entry not yet submitted becomes a no-op; one in the kernel is cancelled
  synchronously (`IORING_REGISTER_SYNC_CANCEL`, Linux 6.0), which returns once the kernel has let go.
- A command holds a use of its handle (`UringHandle.enter`/`leave`) from its first system call on the descriptor until
  the kernel has let go of its entry, so a descriptor's number is never closed, and reused, under an entry that names
  it.
- The kernel reads buffers, a `msghdr` and its `iovec`s and a datagram's address after the submitting call returns, so
  a command keeps them (`Staging`, `MessageHeader`).

## What each operation costs

On the worker's thread:

| Operation | System calls | Where |
| --- | --- | --- |
| Read, write, accept that can proceed | 1 `read`/`write`/`accept4`, tried before the ring (never on a regular file) | `tryAtOnce` |
| One that has to wait | none of its own: its entry goes with the turn's others | `UringWorker.submit` |
| A turn that blocks | 1 `io_uring_enter`, which submits the turn's entries and waits for a completion at once | `Ring.await` |
| A turn that does not block | 1 `io_uring_enter` if entries were prepared, else none | `Ring.await` |
| Set, fire or cancel a timer, or an op's time limit | none | `TimerHeap` |
| Take back an op in the kernel | 1 `io_uring_register` (sync cancel) | `withdrawHere` |
| Close a handle | 1 sync cancel by descriptor on each ring that had entries on it, then `close` once its uses end | `UringHandle.close` |
| Wake a sleeping worker from another thread | 1 `write` to an eventfd the ring always has a read pending on | `Ring.wake` |

Every stream, a socket's too, is read and written with `read` and `write` (and `readv`, `writev`). On a descriptor
that is not ready the kernel waits for readiness itself, for these opcodes on a non-blocking socket or pipe too (seen
on Linux 7.3: no completion comes until there are bytes); only a kernel that answers `EAGAIN` makes the command wait
with a `POLL_ADD` and submit itself again (`Command.finishSubmission`). The backend before used `recv` and `send` on
sockets, with `IORING_RECVSEND_POLL_FIRST` after a try; on this runtime they measured no better (see Measurements),
so they are not used.

A worker wakes another only while that one sleeps: it sets `signalled`, then reads `sleeping`, which the worker set
before it looked for work, so either the worker sees the signal and does not sleep, or the waker sees it asleep and
writes. A wake after the ring is closed does nothing; the write and the close take one lock, so the eventfd's number is
never written once it may name another file.

## Taking an op back

`Io.perform` promises that once it returns or throws, the op is no longer in use, and that a cancelled op has done
nothing. The runtime's protocol (`WorkpoolScheduler.performWithin`) is: await the op; on a cancel ask the worker to
`withdraw` it; if that answers that it will still fire, wait, uncancellably, for it to settle. io_uring's side:

- **On the op's worker** (a cancel from a future on the same worker, and every time limit, which the worker's heap
  fires): an entry still prepared becomes a no-op, its use ended, and `withdraw` answers that nothing will fire. One in the kernel is sync-cancelled: `0` means the kernel let go without doing it; `-ENOENT` means it finished
  first, its completion in the ring, and it fires with that.
- **From another worker** (a performer resumed on the canceller's worker, as the HTTP library's watch for a client
  going away usually makes it): the withdrawal is handed to the op's worker through its inbox, and `withdraw` answers
  that it will fire; the op's worker takes it back as above and fires it cancelled, or its completion fires it. The op's
  time limit is taken off the heap first, and marked so in the same step, under the heap's lock (`TimerHeap.takeOff`):
  a completion that finds the op gone from the heap then knows whether it is to fire it (`claimFiring`).
- **Work already done is delivered.** A completion applied before the cancel reached it (`committed`) is delivered to
  the performer even though it was cancelled, and `perform` returns: the bytes read are in its buffer, and a cancel
  that came too late did not happen first. This is the data-loss race the 2026-10-16 review found, settled on gears'
  listener lock (`AbstractTimerHeapOp.timerWakeup`, `committed`).
- **A time limit** takes the op back first (`limitReached`): only once the kernel has let go does `perform` throw
  `TimeoutException`; if the op finished first, it completes. One whose completion failed and was applied as its time
  ran out (`Command.applied`) is out of use, and fails with `TimeoutException`; before that check it was never fired
  (`UringSuite`, "an op whose time runs out while its failing completion waits in the ring").

The kernel version: `IORING_REGISTER_SYNC_CANCEL` is what makes taking an op back synchronous; before 6.0 a cancel is
itself an entry whose result comes later. Each ring probes for it as it is set up, and for `IORING_FEAT_NODROP`
and `IORING_FEAT_EXT_ARG`, before a run starts (`UringScheduler.apply` makes the rings first), and refuses an older
kernel with an `IOException` naming the requirement.

A command a kernel worker is running (a read or write of a regular file, an `fsync`) cannot be stopped; the sync
cancel waits for it to finish, so a slow disk holds up the worker for that long. The backend before the move to the
shared runtime did not wait, at the cost of `RegularFiles`' wording; this one keeps the wording.

A ring that fails for good (`UringWorker.fail`) cancels each entry still in the kernel the same way, waiting, before it
tears the ring down and fails the ops: tearing it down alone does not wait for a kernel worker still running an entry,
which could then write into a buffer already given back. An entry the kernel does not say it has let go of keeps what
its command staged for good.

## Closing a handle

`Ops.close` promises that once a close completes the descriptor's number may be reused, a listener's port listened on
again, and a peer sees the end of the stream. The close, from
any thread (`UringHandle.close`):

1. marks the handle closing, so that no use of it begins;
2. cancels the entries on the descriptor on every ring that has entries on it (`IORING_ASYNC_CANCEL_FD | ALL`), counted
   per ring by the handle, and wakes each, from its own thread; each completes with `ECANCELED` and fails as closed. A
   ring with none is left alone, so a close costs nothing on the workers a connection's futures merely passed through;
3. closes the descriptor once the last use has ended, which completes the close op.

A close therefore waits for the next turn of each worker with an entry on the handle, since only that worker reaps the
cancelled entry and ends its use: a worker kept busy by a long task holds the close up as long.

An entry prepared but not yet submitted as the close begins is turned into a no-op when its worker submits, and its
command failed as closed; one submitted as the close begins, which the close's cancel may have missed, is cancelled
right after the submission, once the close's wake-up has woken its worker (`UringWorker.Own`, `onSubmit` and
`afterSubmit`). Without that a read on an idle pipe kept its use for ever and the close never completed
(`UringSuite`, "a close between a read's preparation and its submission").

An op whose completion arrives once a close has begun fails as closed even if it finished, as `Ops.close` has it: its
buffer's position and a file's offset left as they were, an accepted connection closed; the close waited for it.

## A ring that fails

`io_uring_enter` failing other than for time, a signal, an overflow or a passing shortage of memory (`EAGAIN`,
`ENOMEM`) breaks the worker's ring. The worker then tears it down, so that the kernel lets go of every entry, fails the
ops it had with an `IOException`, and goes on running its timers and inbox without it, parking between them; every op
that would use the ring fails at once. Before, the worker's thread died and its inbox went on taking work, so every op
sent to it hung.

## Turns

Datagram receives on one socket, its sends, and a listener's accepts are served in the order they began (`Turns`), as
`Ops` says: the kernel does not keep that order among entries waiting together. Only the head is in the kernel; when it
ends, the next is started on its own worker. A withdrawal of one still waiting just takes it out; a close fails those
waiting. A datagram's head is tried at once with one non-blocking `recvmsg` or `sendmsg`, and an accept's with
`accept4`, before it goes to the ring.

## Buffers

A buffer with native memory (a pool's, or `PointerBuffer.wrap`) is handed to the kernel as it is. One over a heap array
is handed over at its array's address (`NativeBuffer.atPosition`), with no stand-in buffer, both to an entry and to a
call tried at once (`NativeBuffer.staged`): sound because Scala Native's collectors never move an object, and the
command, or the call's caller, keeps the array alive. Only a read-only buffer, which hides its array, is copied through a
stand-in (`Staging`). Until 2026-11-09 a heap buffer was wrapped in a `PointerBuffer` over its array for every call. Until 2026-11-08 a call tried at once copied
every heap buffer through zone memory, a `malloc`, a copy and a `free` for each read and write of io-http's.

A read into a pool's buffer takes a buffer as it is submitted, and the kernel receives straight into it, only while more
than half the pool is free; otherwise the kernel waits for readiness with a `POLL_ADD`, and the buffer is taken then.
So reads waiting on idle streams hold at most half the pool between them (`UringSuite`).

## Blocking work

Blocking ops, name resolution (`getaddrinfo`) and opening a file run on threads off the workers (`BlockingPool`), in
two pools with a cap each: up to 16 threads for lookups, and up to 64 for opens and blocking ops, so that lookups a
resolver that does not answer holds up cannot starve an open. Past its cap a pool queues work, first come, first served.
A body of `ops.blocking` that waits for another one past the 64th waits for ever. A cancel or
a time limit skips one not yet started and interrupts a blocking op running, and `perform` returns only once it has
returned. A resolve or an open, which nothing interrupts and which touches nothing of the caller's but its slot, is
abandoned instead, so that the cancel or the limit holds at once, as `Resolving` promises: its value, once it comes, is
not written, and a file it opened is closed. An op's outcome is written before it counts as delivered (`committed`), so
that a time limit landing as the body returns never delivers an empty slot. Its outcome is handed back to the op's worker through
the inbox and fired there: the runtime takes a firing that finds no listener to mean that the performer was cancelled, and the
worker's inbox runs only once the performer has suspended.

## Choices not taken

- **Provided buffer rings** and **multishot accept and receive**, for the reasons in the note before this one (a ring's
  buffers belong to one ring, where a `BufferPool` is shared; a multishot entry produces results with no op to own
  them).
- **The try heuristic** of the backend before (`Handle.worthTrying`: after two misses in a row, try at once only one in
  eight) is not ported; to measure.

## Measurements

**Build release binaries with a Scala Native that has the fix for scala-native#5080** (`SCALA_NATIVE_NIGHTLY=1`, see
`project/plugins.sbt`): on 0.5.12 its GC can collect objects referenced only by interior pointers, and a multithreaded
release build of the HTTP server crashed under 64 connections, three runs in three (a fault in `String.hashCode` on a
request's path segment), and ran with one worker only; with the nightly it ran eight in eight. The tests are not
affected, and pass on both.

The harness, its scripts and every run's results are in [jirian/gears-bench](https://github.com/jirian/gears-bench),
by date; the project report's chapter 5 explains the tables below. Runs before 2026-11-15 were taken on a busy machine
(load 3 to 9), and compare only with the columns beside them.

The bench (`ioUringDemos/run bench`, release-fast, Immix, on the nightly), over a Unix socket, the client's and the
server's pools one worker each, pinned to two cores; µs/op, the median and quartiles of 10 rounds taking turns
(`results/2026-11-16-backends.txt`, load 1 to 2). *Before* is the build before the move to the shared runtime, *no
spin* the commit before the spin (0b6ac649); cats-effect is fs2 on Scala Native, over `epoll`.

| scenario | before (ReactorCore) | no spin | io_uring | cats-effect |
| --- | --- | --- | --- | --- |
| pingpong | 10.7 [10.1–11.6] | 13.7 [13.6–13.8] | 7.4 [7.2–7.4] | 16.7 [16.6–16.8] |
| pingpong-deadline | 12.3 [12.1–12.5] | 14.4 [14.0–14.6] | 7.7 [7.6–7.9] | 25.0 [24.8–25.1] |
| pipelined | 1.40 [1.38–1.42] | 1.02 [0.93–1.03] | 0.98 [0.93–1.03] | 3.67 [3.59–3.69] |
| concurrent | 6.7 [6.6–6.7] | 6.5 [6.4–6.5] | 6.5 [6.5–7.0] | – |
| connections | 42 [42–42] | 36 [35–36] | 31 [30–31] | – |
| timers | 181 [178–186] | 11.1 [10.8–11.4] | 11.3 [10.9–11.6] | 1.9 [1.8–2.0] |

`timers`, half of whose timers are cancelled, went from 272–280 to 17–18 µs on the build before with nothing changed
but gears' stackless cancellation exceptions (2026-10-30); on the shared runtime, which built two per cancel, it had
been 419.

HTTP against Go's `net/http` (`http_bench.py`, 5 rounds, the server on hardware threads 0 and 1, the load generator on
4 to 7, two of which share the server's cores; thousands of requests per second; `results/2026-11-18-http.txt`, at
7f50fe77):

| path, connections | Go net/http | io_uring |
| --- | --- | --- |
| `/hello`, 1 | 29.3 | 35.8 |
| `/hello`, 64 | 72.8 | 75.0 |
| `/items/42`, 1 | 27.9 | 33.2 |
| `/items/42`, 64 | 68.4 | 65.6 |

`recv`/`send` on sockets, as the backend before had them, against `read`/`write`: on a busy machine slower in every
scenario (ping-pong 21.2 against 19.4 µs/op, `results/2026-11-07-uring-recv.txt`); on a quiet one within each other's
quartiles, 6% faster in `concurrent` and 3% slower in `connections` (`results/2026-11-15-uring-recv.txt`). Not used.

## Open

- The spin's cost: what its 20 µs cost a machine busy with other work, and whether the pause added to it (2026-11-18,
  for the core's other hardware thread) changes a round trip; neither is measured yet.
- A close waits for the next turn of each worker with an entry on the handle, since only that worker reaps it: a
  worker kept busy by a long task holds the close up as long.
- The backend's own exceptions on hot paths (`CancellationException` in a withdrawal, `TimeoutException`,
  `HandleClosedException`) are stackless since 2026-11-08, gears' own cancels through this repository's change to
  the core (`gears.async.cancellation()`); a cancel's trace, for debugging, cannot be turned back on.
- A time limit on a file op: the synchronous cancel waits for an io-wq worker's read or write to finish, a slow disk's
  or a network file system's time, and holds the op's worker, and every future on it, meanwhile. Cancelling without
  waiting, the completion then settling the op, would not.
- The try heuristic, if the measurements show the extra `read(2)` on a busy connection.
- A completion-queue overflow test and a kernel < 6.0 refusal test, which this machine cannot run.
- A `perform` in a cancelled group: the reference backend refuses it, so a `close` in a cancelled future's
  `finally` leaks; this runtime lets an op that completes at once through. The contract does not say which.
