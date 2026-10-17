#!/usr/bin/env python3
"""Compares `kqueuedemo gears bench` across builds of the demo executable.

Build the executables to compare with kqueue-demo/bench/build.sh, which links a commit in release-fast mode against the
Scala Native nightly (0.5.12's GC can crash multithreaded release builds), then compare them:

    kqueue-demo/bench/build.sh HEAD~ /tmp/kqueuedemo.before
    kqueue-demo/bench/build.sh HEAD /tmp/kqueuedemo.after
    kqueue-demo/bench/compare.py before=/tmp/kqueuedemo.before after=/tmp/kqueuedemo.after raw=/tmp/kqueuedemo.after@raw

A build runs the gears benchmark, `gears bench`; suffix its path with `@raw` to run the same scenarios written against
`KqueueLoop` directly (`bench`), as a reference: `raw=<path>@raw`.

Each build runs `--runs` times, alternating between builds so drift in machine load affects them alike. The table
shows each scenario's median microseconds per operation, and each later build's change against the first.
Arguments after `--` go to `bench`, for example `-- --scenarios pingpong,pipelined --scale 0.5`.
"""

import argparse
import statistics
import subprocess
import sys


def run_once(path, bench_args, retries, crashes):
    command = [path[: -len("@raw")], "bench"] if path.endswith("@raw") else [path, "gears", "bench"]
    for attempt in range(retries + 1):
        result = subprocess.run([*command, *bench_args], capture_output=True, text=True, timeout=600)
        if result.returncode == 0:
            break
        crashes.append(f"{' '.join(command)} exited with {result.returncode}: {result.stderr.strip()[-300:]}")
        if attempt == retries:
            sys.exit(f"{' '.join(command)} failed {retries + 1} times; last output:\n{result.stdout}{result.stderr}")
        print(f"  failed with exit code {result.returncode}; retrying", file=sys.stderr)
    output = result.stdout
    results = {}
    for line in output.splitlines():
        fields = line.split()
        # bench: <scenario> <ops> <seconds> <ops/s> <us/op>
        if len(fields) == 6 and fields[0] == "bench:" and fields[1] != "scenario":
            results[fields[1]] = float(fields[5])
    if not results:
        sys.exit(f"{path} printed no bench results:\n{output}")
    return results


def main():
    argv = sys.argv[1:]
    bench_args = []
    if "--" in argv:
        split = argv.index("--")
        argv, bench_args = argv[:split], argv[split + 1 :]
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "builds", nargs="+", metavar="label=path[@raw]", help="a demo executable and a label for it"
    )
    parser.add_argument("--runs", type=int, default=3, help="runs per build (default 3)")
    parser.add_argument(
        "--retries", type=int, default=0, help="times to rerun a run that fails before giving up (default 0)"
    )
    args = parser.parse_args(argv)

    builds = []
    for build in args.builds:
        label, sep, path = build.partition("=")
        if not sep:
            label, path = build, build
        builds.append((label, path))

    samples = {label: {} for label, _ in builds}
    crashes = []
    for run in range(args.runs):
        for label, path in builds:
            print(f"run {run + 1}/{args.runs}: {label}", file=sys.stderr)
            for scenario, us in run_once(path, bench_args, args.retries, crashes).items():
                samples[label].setdefault(scenario, []).append(us)

    scenarios = list(samples[builds[0][0]])
    base = builds[0][0]
    header = f"{'us/op (median)':<18}" + "".join(f"{label:>22}" for label, _ in builds)
    print(header)
    print("-" * len(header))
    for scenario in scenarios:
        base_median = statistics.median(samples[base][scenario])
        row = f"{scenario:<18}"
        for label, _ in builds:
            median = statistics.median(samples[label][scenario])
            if label == base:
                row += f"{median:>22.2f}"
            else:
                change = (median - base_median) / base_median * 100
                row += f"{median:>13.2f} ({change:+6.1f}%)"
        print(row)

    if crashes:
        print(f"\n{len(crashes)} run(s) failed and were retried:")
        for crash in crashes:
            print(f"  {crash}")

if __name__ == "__main__":
    main()
