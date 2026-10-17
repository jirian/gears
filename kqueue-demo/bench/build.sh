#!/bin/bash
# Builds a kqueue demo executable to benchmark: a commit linked in release-fast mode against the Scala Native nightly
# (see project/plugins.sbt), in a temporary worktree with its own sbt, so the working tree and its sbt server are left
# alone.
#
#   kqueue-demo/bench/build.sh <commit> <output>
#
# Then compare executables with kqueue-demo/bench/compare.py. Only committed code is built.
set -euo pipefail
commit=${1:?usage: build.sh <commit> <output>}
output=${2:?usage: build.sh <commit> <output>}
case "$output" in /*) ;; *) output="$PWD/$output" ;; esac

repo=$(git rev-parse --show-toplevel)
worktree=$(mktemp -d "${TMPDIR:-/tmp}/kqueue-bench-build.XXXXXX")
cleanup() {
  (cd "$worktree" && sbt --client shutdown >/dev/null 2>&1) || true
  git -C "$repo" worktree remove --force "$worktree" >/dev/null 2>&1 || true
}
trap cleanup EXIT

git -C "$repo" worktree add --quiet --detach "$worktree" "$commit"
cd "$worktree"
# Commits from before the nightly switch existed get it from the working tree.
cp "$repo/project/plugins.sbt" project/plugins.sbt
grep -q SCALA_NATIVE_NIGHTLY build.sbt || printf '\nThisBuild / resolvers += Resolver.sonatypeCentralSnapshots\n' >>build.sbt
echo "building $(git log --oneline -1) in release-fast against the Scala Native nightly..." >&2
log="$worktree/build.log"
if ! SCALA_NATIVE_NIGHTLY=1 sbt -batch \
  'set kqueueDemo/nativeConfig ~= (_.withMode(scala.scalanative.build.Mode.releaseFast)); show kqueueDemo/libraryDependencies; kqueueDemo/nativeLink' \
  >"$log" 2>&1; then
  tail -40 "$log" >&2
  exit 1
fi
grep -q "nativelib:0.5.13-" "$log" || { echo "the build did not use the Scala Native nightly" >&2; exit 1; }
binary=target/out/native0.5/scala-3.9.0/kqueuedemo/kqueuedemo
"$binary" target-info | grep -q "release: true" || { echo "the executable is not a release build" >&2; exit 1; }
mkdir -p "$(dirname "$output")"
cp "$binary" "$output"
echo "$output" >&2
