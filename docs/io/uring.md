# The io_uring backend: a design note

`UringBackend` (`io-backends/uring`, package `gears.async.io.uring`) is the backend for Scala Native on Linux 6.0 or
later, through liburing. It runs on the shared native runtime (`io-runtime`, `gears.async.io.shareable`, from Jamie
Thompson's `WorkpoolScheduler`): a pool of workers, each with an inbox, a ready queue and a heap of timers, which is the
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

Every stream, a socket's too, is read and written with `read` and `write` (and `readv`, `writev`); an `EAGAIN` makes
the command wait for readiness with a `POLL_ADD`, then submit itself again (`Command.finishSubmission`). The backend
before used `recv` and `send` on sockets, which the kernel waits for itself, with `IORING_RECVSEND_POLL_FIRST` after a
try; on this runtime that measured slower everywhere (see Measurements), so it is not used.

A worker wakes another only while that one sleeps: it sets `signalled`, then reads `sleeping`, which the worker set
before it looked for work, so either the worker sees the signal and does not sleep, or the waker sees it asleep and
writes. A wake after the ring is closed does nothing; the write and the close take one lock, so the eventfd's number is
never written once it may name another file.

## Taking an op back

`Io.perform` promises that once it returns or throws, the op is no longer in use, and that a cancelled op has done
nothing. The runtime's protocol (`WorkpoolScheduler.performWithin`) is: await the op; on a cancel ask the worker to
`withdraw` it; if that answers that it will still fire, wait, uncancellably, for it to settle. io_uring's side:

- **On the op's worker** (the usual case: a cancel from a future on the same worker, and every time limit, which the
  worker's heap fires): an entry still prepared becomes a no-op, its use ended, and `withdraw` answers that nothing will
  fire. One in the kernel is sync-cancelled: `0` means the kernel let go without doing it; `-ENOENT` means it finished
  first, its completion in the ring, and it fires with that.
- **From another worker** (a performer resumed elsewhere): the withdrawal is handed to the op's worker through its
  inbox, and `withdraw` answers that it will fire; the op's worker takes it back as above and fires it cancelled, or
  its completion fires it.
- **Work already done is delivered.** A completion applied before the cancel reached it (`committed`) is delivered to
  the performer even though it was cancelled, and `perform` returns: the bytes read are in its buffer, and a cancel
  that came too late did not happen first. This is the data-loss race the 2026-10-16 review found, settled on gears'
  listener lock (see `io-runtime-on-jamie-base` in the docs repository for the runtime's side).
- **A time limit** takes the op back first (`limitReached`): only once the kernel has let go does `perform` throw
  `TimeoutException`; if the op finished first, it completes. One whose completion failed and was applied as its time
  ran out (`Command.applied`) is out of use, and fails with `TimeoutException`; before that check it was never fired
  (`UringSuite`, "an op whose time runs out while its failing completion waits in the ring").

The kernel version: `IORING_REGISTER_SYNC_CANCEL` is what makes taking an op back synchronous; before 6.0 a cancel is
itself an entry whose result comes later. `Ring.check` probes for it, and for `IORING_FEAT_NODROP` and
`IORING_FEAT_EXT_ARG`, before a run starts, and refuses an older kernel with an `IOException` naming the requirement.

A command a kernel worker is running (a read or write of a regular file, an `fsync`) cannot be stopped; the sync
cancel waits for it to finish, so a slow disk holds up the worker for that long. The backend before the move to the
shared runtime did not wait, at the cost of `FileOps`' wording; this one keeps the wording.

## Closing a handle

`Ops.close` promises that once a close completes the descriptor's number may be reused, a listener's port listened on
again, and a peer sees the end of the stream, even while the worker that holds an entry on it is busy. The close, from
any thread (`UringHandle.close`):

1. marks the handle closing, so that no use of it begins;
2. cancels the entries on the descriptor on every ring that has had entries on it (`IORING_ASYNC_CANCEL_FD | ALL`),
   and wakes each, from its own thread; each completes with `ECANCELED` and fails as closed;
3. closes the descriptor once the last use has ended, which completes the close op.

An entry prepared but not yet submitted as the close begins is turned into a no-op when its worker submits, and its
command failed as closed; one submitted as the close begins, which the close's cancel may have missed, is cancelled
right after the submission, once the close's wake-up has woken its worker (`UringWorker.Own`, `onSubmit` and
`afterSubmit`). Without that a read on an idle pipe kept its use for ever and the close never completed
(`UringSuite`, "a close between a read's preparation and its submission").

## Turns

Datagram receives on one socket, and its sends, are served in the order they began (`Turns`): the kernel does not
keep that order among entries waiting together. Only the head is in the kernel; when it ends, the next is started on
its own worker. A withdrawal of one still waiting just takes it out; a close fails those waiting.

## Buffers

A buffer with native memory (a pool's, or `PointerBuffer.wrap`) is handed to the kernel as it is. One over a heap array
the kernel may write is handed over at its array's address, at no cost: sound because Scala Native's collectors never
move an object, and the command keeps the array alive. Only a read-only buffer is copied through a stand-in
(`Staging`).

A read into a pool's buffer takes a buffer as it is submitted, and the kernel receives straight into it, only while more
than half the pool is free; otherwise the kernel waits for readiness with a `POLL_ADD`, and the buffer is taken then.
So reads waiting on idle streams hold at most half the pool between them (`UringSuite`).

## Blocking work

Blocking ops, name resolution (`getaddrinfo`) and opening a file run on a pool of threads (`BlockingPool`). A cancel or
a time limit skips one not yet started and interrupts one running (not a resolve or an open, which nothing interrupts),
and `perform` returns only once it has returned. Its outcome is handed back to the op's worker through the inbox and
fired there: the runtime takes a firing that finds no listener to mean that the performer was cancelled, and the
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

The bench (`ioUringDemos/run bench`, release-fast, Immix, both builds on the nightly), over a Unix socket, the client's
and the server's pools one worker each, pinned to two cores; µs/op, the median and quartiles of 10 runs taking turns
(`../gears-jirian-validation/results/2026-11-07-uring-nightly.txt`), against the build before the move to the shared
runtime (`bench-builds/iouringdemos`). The machine was not idle (load 3–8), so the columns compare with each other,
not with earlier tables.

| scenario | before (ReactorCore) | shared runtime |
| --- | --- | --- |
| pingpong | 16.51 [16.09–17.22] | 17.20 [16.93–17.59] |
| pingpong-deadline | 17.65 [17.34–17.93] | 16.92 [16.37–17.30] |
| pipelined | 1.93 [1.82–1.98] | 1.26 [1.21–1.33] |
| timers | 243 [242–245] | 419 [417–423] |
| concurrent | 9.63 [9.52–9.76] | 9.64 [9.12–9.82] |
| connections | 60.6 [58.6–61.3] | 53.9 [53.1–55.1] |

So the shared runtime costs about 4% on ping-pong, gains as much with deadlines, is level on `concurrent`, and is
faster on `pipelined` (35%) and `connections` (11%). `timers`, half of whose timers are cancelled, is slower for a
reason outside the backend: counted with gdb, a cancel now builds two stack-traced `CancellationException`s instead of
one (gears' `await`, which the runtime's `performWithin` goes through, and the future's own), each costing about
250 µs in Scala Native's stack walk on Linux. Stackless cancellation exceptions in gears core (branch
`stackless-cancellation`, proposed separately) take it to 18 µs: the final run below.

The final run (2026-11-08, `results/2026-11-08-final-backends.txt`, load 6–9, same setup), with stackless cancels,
against the same build before:

| scenario | before (ReactorCore) | shared runtime, stackless cancels |
| --- | --- | --- |
| pingpong | 23.77 [20.05–32.47] | 22.76 [20.82–27.40] |
| pingpong-deadline | 24.58 [20.95–27.75] | 26.85 [20.66–32.94] |
| pipelined | 2.22 [2.12–2.49] | 1.71 [1.54–2.32] |
| timers | 283 [266–307] | 18.4 [17.1–21.0] |
| concurrent | 13.98 [13.14–15.99] | 13.32 [11.63–14.19] |
| connections | 85.4 [75.5–106.4] | 69.4 [63.6–77.5] |

HTTP in the same session (`results/2026-11-08-final-http.txt`, 5 rounds): 11.9k against 9.8k requests per second on
`/hello` with one connection, 23.8k against 19.6k on 64; Go 17.2k and 54.0k.

HTTP (`http_bench.py`, 3 rounds, the server pinned to two cores and the load generator to four; requests per second,
median; `results/2026-11-07-http-nightly.txt`):

| path, connections | Go net/http | before | shared runtime |
| --- | --- | --- | --- |
| `/hello`, 1 | 20,474 | 12,212 | 14,506 |
| `/hello`, 64 | 58,590 | 24,254 | 27,750 |
| `/items/42`, 1 | 19,788 | 12,538 | 15,197 |
| `/items/42`, 64 | 57,087 | 25,989 | 28,286 |

The shared runtime serves 9–21% more requests than the build before, at lower latency (p99 at 64 connections 3.6–3.8
against 4.6–5.0 ms), with no errors; Go is still ahead.

`recv`/`send` on sockets, as the backend before had them, against `read`/`write`, measured on 0.5.12 builds (8 runs,
`results/2026-11-07-uring-recv.txt`): slower in every scenario (ping-pong 21.2 against 19.4 µs/op), so not used.

## Open

- Measuring on a quiet machine, against the build before the move (`bench-builds/iouringdemos`).
- The backend's own exceptions on hot paths (`CancellationException` in a withdrawal, `TimeoutException`,
  `HandleClosedException`) still have stack traces: `gears.async.cancellation()` once the core change is merged.
- The try heuristic, if the measurements show the extra `read(2)` on a busy connection.
- A completion-queue overflow test and a kernel < 6.0 refusal test, which this machine cannot run.
- A `perform` in a cancelled group: the reference and NIO backends refuse it, so a `close` in a cancelled future's
  `finally` leaks; this runtime lets an op that completes at once through. The contract does not say which.
