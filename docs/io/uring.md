# The io_uring backend: a design note

`UringBackend` (`io-backends/uring`, package `gears.async.io.uring`) is the reactor backend for Scala Native on Linux
6.0 or later, through liburing. Each worker of the pool owns one `UringReactor`, a ring it submits to and reaps on its
own thread (see [`runtime.md`](runtime.md) for the pool). This note records what an op costs, how an op is withdrawn
from a kernel that owns its buffers, how a close reaches entries on other rings, what the choices below rest on, and
what is open. It is the counterpart of [`kqueue.md`](kqueue.md), whose structure it follows; the two backends share
`ReactorCore`, which keeps the turn, the timers and the promises, and differ in what a submission is.

## The model

A command (`Command`, in `UringOps.scala`) is one submission queue entry per submission: `prepare` fills it in, and
the completion queue entry that answers it goes to `finish`, which says whether the op is done or is to be submitted
again. The kernel owns a command's buffers from the submission of its entry until its completion arrives. Everything
below follows from that one fact:

- A withdrawn op (cancelled, timed out, or on a handle that closed) cannot just be forgotten, as kqueue's can, since
  the kernel may still write into its buffer. The reactor asks the kernel to cancel it synchronously
  (`IORING_REGISTER_SYNC_CANCEL`, Linux 6.0) and waits until the kernel has let go: one that finished first counts as
  finished, and any other is cancelled, its `onCancel` run before `cancel` returns, as `Submitter` requires.
- A command keeps a use of its handle (`UringBackend.Handle`) from the moment its entry is prepared until the kernel
  has taken it, since the kernel reads the descriptor's number as the entry is submitted. From then on the kernel
  holds the file itself, so closing the number cannot reach an entry in the kernel, and a number is never reused under
  an entry that names it.
- The kernel reads buffers, a `msghdr` and its `iovec`s, a datagram's address and a path after the submitting call
  returns, so a command keeps all of them (`Staging`, `MessageHeader`).

## What each operation costs

On the reactor's thread:

| Operation | System calls | Where |
| --- | --- | --- |
| Read, write, accept that can proceed, through `trySubmit` | 1 `read`/`write`/`accept4`, tried before the ring; skipped after two tries in a row that found nothing to do, but one in eight | `Handle.worthTrying` |
| One that has to wait | none of its own: its entry goes with the turn's others | `enqueue` |
| A turn that blocks | 1 `io_uring_enter`, which submits the turn's entries and waits for a completion at once | `poll`, `io_uring_submit_and_wait` |
| A turn that does not block | none if completions have arrived (reaped from the shared ring); else 1 `io_uring_enter` to submit | `deliverOwn` |
| Set, fire or cancel a timer, or an op's time limit | none | `TimerHeap` |
| Cancel an op in the kernel | 1 `io_uring_register` (sync cancel) | `letGo` |
| Close a handle with entries in the kernel | 1 `shutdown` for a socket, and 1 sync cancel on each reactor that had entries on it | `UringReactor.cancelClosed` |
| Close a handle with none | the `close` alone | `Handle.inKernel` |
| Wake the loop from another thread | 1 `write` to an eventfd the ring always has a read pending on | `wakeup` |

On a socket a read is a `recv` and a write a `send` (`IORING_OP_RECV`, `SEND`, `RECVMSG`, `SENDMSG`), for which the
kernel waits for readiness itself, arming its internal poll, rather than coming back with `EAGAIN` as a `read` of a
non-blocking descriptor may; one just tried without waiting is sent with `IORING_RECVSEND_POLL_FIRST`, so that the
kernel does not try it again before waiting. A pipe or a FIFO uses `read` and `write`, and an `EAGAIN` makes the
command wait for readiness with a `POLL_ADD`, then submit itself again (`Command.finishSubmission`).

## Withdrawing an op, and why the kernel version is 6.0

`withdrawOwn` decides "finished first" against "cancelled" from the completion entry itself: it hands every prepared
entry to the kernel (a cancel finds only what the kernel has), looks for the command's completion among those copied
out, in the ring, and in the kernel's overflow list (`arrivedResult`, `drain`), and if there is none, cancels it
synchronously and looks again. A result of `-ECANCELED` is a cancel; anything else is what the op did, and the op
completes with it. Ids are never reused, so the completion that the turn later dispatches for a withdrawn command is
dropped.

`IORING_REGISTER_SYNC_CANCEL` is what makes this synchronous; before 6.0 a cancel is itself an entry whose result comes
later, and `cancel` could not return knowing the buffer was free. A reactor probes for it as it opens, and refuses to
open on an older kernel with an `IOException` naming the requirement, rather than failing on the first cancel. It also
requires `IORING_FEAT_NODROP` (completions are never dropped, but overflow into a kernel list when the ring is full)
and `IORING_FEAT_EXT_ARG` (a wait with a time limit in one call).

**A command a kernel worker is running** (a read or write of a regular file, an `fsync`, an `openat`) cannot be
stopped: io-wq does not interrupt it. Waiting for it would hold up the loop for as long as the disk takes, so the
reactor does not: it cancels without waiting (`runsInWorker`), and a command still running counts as committed,
`cancel` returning false and the op completing as it finishes. This matches the timing of `FileOps`' contract
(`perform` returns as the disk answers, as on kqueue, whose file ops run on blocker threads) but not its wording, which
expects `onCancel` once the op stops; giving it that needs `ReactorCore` to let a backend defer an `onCancel`, as it
does for its own blockers. A stream read of a regular file's handle runs the same way, and is never tried on the loop
thread first, where a `read(2)` would wait for the disk.

## Closing a handle

`Ops.close` promises that once a close completes the descriptor's number may be reused, a listener's port listened on
again, and a peer sees the end of the stream: even while the reactor that holds an entry on it is blocked elsewhere and
not turning. On io_uring a cancelled entry's hold on the file is dropped on its own ring's thread, as task work, the
next time that thread enters the kernel, so cancelling alone does not free the port in time. The closing thread
therefore:

1. shuts a socket down (`shutdown(SHUT_RDWR)`), which frees a TCP listener's port at once and sends the peer its end;
2. cancels the entries on the descriptor on every reactor that has had entries on the handle
   (`IORING_ASYNC_CANCEL_FD | ALL`), from its own thread, which the kernel serializes with each ring's own submissions
   under the ring's lock, so a foreign cancel is safe; `ringLock` keeps a ring from being unmapped under one;
3. closes the descriptor;
4. tells those reactors, which fail their commands on the handle on their next turn (`deliverClosed`).

A handle counts its entries in the kernel (`Handle.entries`) and the reactors that held them (`Handle.reactorBits`,
one bit per reactor, up to 64, more counting as all), so a close with nothing in the kernel, nearly every close, costs
no system call beyond `close`, and one with entries reaches only the reactors that had them. Each reactor keeps its
commands by descriptor number (`onFd`, a list through the commands themselves), so failing a closed handle's commands
costs what they number, not what the reactor has in flight.

Measured on Linux 7.2 over 200 rounds: a TCP listener's port is free as the close returns, every round; a UDP socket's
port and a pipe's `EPIPE` can lag by up to about 10 ms and 3 ms, until the owner's thread next enters the kernel,
because `shutdown` does not apply to them.

## Buffers

A buffer with native memory (a pool's, or `PointerBuffer.wrap`) is handed to the kernel as it is. One over a heap array
that the kernel may write (`ByteBuffer.allocate`, and on Scala Native `allocateDirect`) is handed to the kernel at its
array's address, also at no cost: sound because Scala Native's collectors never move an object, and the command, which
keeps its buffers, keeps the array alive. The reactor's own arrays (the ring, the completion batch, a `MessageHeader`,
a path being opened) rely on the same. Only a read-only buffer, whose array is hidden, is copied through a stand-in of
the command's (`Staging`).

A read into a pool's buffer (`Ops.read(handle, pool, into)`) takes a buffer as it is submitted, and the kernel
receives straight into it, in one round trip, only while more than half the pool is free. Otherwise the kernel waits
for readiness with a `POLL_ADD`, and the reactor takes the buffer then and reads without waiting, as kqueue does. So
reads waiting on idle connections hold at most half the pool between them, and a pool of 256 serves any number of
them, where taking a buffer for every pending read would fail the 257th; and a busy connection, with the pool mostly
free, pays no extra round trip. Waiting for readiness first for every read cost pooled ping-pong about 16%
(22.9 against 26.6 µs, medians of 10 pinned runs). From a regular file, which is always readable, the kernel always
reads straight into a buffer.

## The completion queue

Completion entries are copied out a batch at a time, then dispatched, so dispatching may submit again without the ring
filling, and a completion that cancels a command of the same batch still finds that command's entry. When the ring is
full the kernel keeps further completions in its overflow list (`IORING_FEAT_NODROP`); a command looked for and not
found has the whole queue and that list copied out (`drain`) before the reactor concludes it is still in the kernel.
The kernel refuses new entries while completions overflow (`EBUSY`), and taking them in is what lets it go on, so the
loop that hands entries to the kernel for a cancel drains between its tries.

## Choices not taken

- **Provided buffer rings** (`io_uring_register_buf_ring` with `IOSQE_BUFFER_SELECT`), with which the kernel picks a
  pool buffer as data arrives. They would save the readiness wait's round trip for a pooled read, but a ring's buffers
  belong to one ring, while a `BufferPool` is shared by every reactor and by code that releases its buffers on any
  thread; partitioning the pool among rings, and refilling each ring from the thread that releases, is a design of its
  own. Waiting for readiness and then taking the buffer gives the property that matters, that idle connections hold
  nothing, at the cost of one more completion per read that has to wait.
- **Multishot accept and receive** (`IORING_ACCEPT_MULTISHOT`, `IORING_RECV_MULTISHOT`). A multishot entry stays in the
  kernel between ops and produces results whether or not an op is pending, so connections or bytes would arrive with no
  op to own them, and would need a home, and a close, outside the op model, in which an op is in the kernel only while
  it is pending. `acceptAll` already amortizes a burst: one poll, then `accept4` until none is waiting.
- **`signalsWhileAwake`**, which kqueue turns on by measurement of a macOS-specific cost: left off here, so a wake-up
  while the loop is awake costs no eventfd write. Not measured on Linux yet.

## Measurements

The bench runs on io_uring as `ioUringDemos/run bench` (see [`bench.md`](bench.md)). On the machine this was written on
(Linux 7.2, 8 cores, shared with other work) wall-clock runs vary by up to 50%, so comparisons below pin the client's
and server's workers to two cores and take the median and quartiles of 10 runs, against the build before the changes
above. With pooled reads as they are now, no scenario differs beyond the quartiles (µs/op, before against after):
ping-pong 22.7 / 24.5 over Unix sockets and 48.1 / 49.6 over TCP, pooled ping-pong 24.1 / 23.6 and 46.9 / 48.8,
`concurrent` 12.6 / 12.9 and 21.6 / 22.0, `pipelined` 2.34 / 2.39 and 8.00 / 7.82. So the single system call per
blocking turn, the try heuristic and the socket opcodes cost nothing measurable, and gain nothing measurable on
loopback either; what they change is where a busier host spends its time, which needs a quiet machine, or counting
system calls, to show. Reverting to two calls per blocking turn made ping-pong 27.4 against 23.1 µs in one round of 6
runs, and no different in the next round of 10: within the noise. The one change that did cost something, a readiness
wait before every pooled read, is now made only when the pool is more than half held (see Buffers).

The `timers` and `rearm-cancel` scenarios cost about 275 and 540 µs per op here, against about 1 µs on NIO, for a
reason outside the backend: each cancelled future builds `CancellationException`s with stack traces
(`gears.async.Future`), and Scala Native's stack walk through libunwind's DWARF tables costs about 250 µs on Linux.
With them stackless, 18 and 35 µs; the rest is the throw itself, unwound twice through the same tables.

## Open

- The `FileOps` cancel wording above, which needs a `ReactorCore` hook.
- `trySubmit` is required to issue an op at once (`Submitter`), so a read that is not ready costs a `read(2)` and an
  `io_uring_enter`; if the contract let a backend issue it with the turn's others, as "a turn issues everything before
  it blocks" already guarantees progress, the second would go too. The try heuristic removes most of the first.
- Measuring the items above on a quiet machine.
