# NIO on the calling thread: a design note

`NioOnCaller` (`io-backends/nio`, package `gears.async.io.nio`) is the JVM's default backend, and the one
`gears-io-http` runs on there. Its ops and handles are the reference backend's (`io-reference`, by Jamie Thompson),
copied; what it adds is its way of waiting, from his earlier `NioOnCaller`. Each op is performed by the virtual thread
performing it, with no poller thread:

- tried at once on its non-blocking channel;
- if that would block, tried again after a yield, up to `Retries` times (8, `-Dgears.io.nio.retries`), since in
  request and response traffic the reply often arrives within microseconds;
- then it waits on its handle's own selector for that direction (`Ctx.ready`, `Wait.select`), which the JDK's poller
  parks a virtual thread in, and is tried again when woken;
- a run of ops done at once yields every `HereBudget` (32), so that it stays fair.

It needs no JDK flags.

## Changes to the reference backend's ops

An op is linked to its caller's group (`cancellationScope`) only while it waits, in `Ctx.ready`'s select and in
`Ctx.park` and `Ctx.parkUntil`, rather than for the whole of `NioOp.perform` as in the reference backend. Linking takes
the group's lock twice, to add the op and to drop it. An op done at once, the usual case on a busy connection, now
takes no lock, and `Ctx.checkStop` sees a cancel after every try instead. A group cancelled already cancels the wait as
it is linked (`CompletionGroup.add`), so no cancel is missed. The contracts pass with `Retries` at 8 and at 0 (177,
1 skipped); io-http's JVM tests pass (44).

`AcceptAllOp` no longer drops the connections it has accepted when a later accept fails (the descriptor limit, say):
it delivers them, and the next accept meets the failure. The same in the io_uring backend's `AcceptAll`.

## Measurements

The bench (`ioNioDemos/run bench --warmup`), over a Unix socket, pinned to two cores; µs/op, the median and quartiles
of 10 runs taking turns (`../gears-jirian-validation/results/2026-11-08-nio-linked.txt`), against the `NioOnCaller`
before the port (`bench-builds/nio-caller`). The machine was not idle (load 4–6).

| scenario | before | ported | linked while waiting |
| --- | --- | --- | --- |
| pingpong | 4.51 [4.42–5.48] | 5.46 [5.19–5.94] | 5.08 [4.81–5.19] |
| pingpong-deadline | 4.70 [4.48–4.94] | 5.27 [5.00–5.74] | 4.97 [4.79–5.38] |
| pipelined | 1.32 [1.27–1.48] | 1.71 [1.57–1.80] | 1.43 [1.40–1.52] |
| timers | 6.58 [6.12–6.76] | 6.50 [6.20–6.72] | 6.30 [6.20–6.99] |
| concurrent | 3.85 [3.83–4.03] | 4.65 [4.36–4.83] | 3.98 [3.91–4.11] |
| connections | 166 [162–167] | 161 [159–163] | 167 [163–172] |

Linking only while waiting takes back most of what the port cost. What is left, 6–13% on ping-pong and pipelined
and 3% on concurrent, is the rest of the reference ops' bookkeeping:
- a `Wait` and a `Ctx` for every op;
- the handle's lock, taken to enter and to leave, with a set of the waits on it for a close to fail.

The backend before the port did none of this.

HTTP (`http_bench.py`, 3 rounds, the server pinned to two cores and the load generator to four; requests per second,
median; `results/2026-11-08-http-nio-linked.txt`):

| path, connections | Go net/http | before | after |
| --- | --- | --- | --- |
| `/hello`, 1 | 19,164 | 19,826 | 19,749 |
| `/hello`, 64 | 58,399 | 61,561 | 62,046 |
| `/items/42`, 1 | 19,291 | 19,219 | 20,417 |
| `/items/42`, 64 | 56,207 | 62,387 | 64,722 |

The server is level with the build before or ahead, by up to 6%, with no errors. The ported ops, before the change
above, measured 4–7% ahead too (`results/2026-11-08-http-nio.txt`): at this size the server's own work outweighs an
op's.

## Open

- The handle's lock on every op: a counter of ops in progress, with the close waiting for it to reach zero, would do
  without it, if the remaining gap matters.
- Measuring on a quiet machine.
