#!/usr/bin/env bash
# Copyright: lituus-io, all rights reserved.
# Author: terekete <spicyzhug@gmail.com>
#
# Run the CI gates locally, in the same order. Needs Docker for the GCS
# emulator (skipped with a warning if absent) and optionally cargo-deny,
# cargo-audit and cargo-fuzz (each skipped with a warning if not installed).
#
#   scripts/ci-local.sh            # everything
#   FUZZ_SECS=120 scripts/ci-local.sh
#
# Never touches real cloud resources: the live GCS test runs only when the
# caller exports OBJSTORE_LIVE_BUCKET themselves.
set -euo pipefail
cd "$(dirname "$0")/.."
export RUSTFLAGS="${RUSTFLAGS:--D warnings}"
FUZZ_SECS="${FUZZ_SECS:-20}"
step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
warn() { printf '\033[33mskip: %s\033[0m\n' "$*"; }
have() { command -v "$1" >/dev/null 2>&1 || cargo "$1" --version >/dev/null 2>&1; }

step fmt;    cargo fmt --check
step clippy; cargo clippy --all-features --all-targets; cargo clippy --no-default-features --all-targets
step "feature matrix"
for f in "" gcs testing gcs,testing; do
  if [ -z "$f" ]; then cargo check -q --no-default-features --all-targets; else cargo check -q --no-default-features --features "$f" --all-targets; fi
done
if cargo tree --no-default-features -e normal | grep -qE "reqwest|hyper|rustls"; then echo "HTTP stack leaked into default features"; exit 1; fi
step docs; RUSTDOCFLAGS="-D warnings" cargo doc -q --all-features --no-deps

started=""
if [ -z "${OBJSTORE_FAKE_GCS:-}" ] && command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  step "start fake-gcs-server"
  docker rm -f objstore-ci-fakegcs >/dev/null 2>&1 || true
  docker run -d --name objstore-ci-fakegcs -p 4444:4444 fsouza/fake-gcs-server \
    -scheme http -port 4444 -public-host 127.0.0.1:4444 -external-url http://127.0.0.1:4444 >/dev/null
  started=1
  for _ in $(seq 1 30); do curl -sf "http://127.0.0.1:4444/storage/v1/b?project=test" >/dev/null && break; sleep 1; done
  export OBJSTORE_FAKE_GCS=http://127.0.0.1:4444
fi
trap '[ -n "$started" ] && docker rm -f objstore-ci-fakegcs >/dev/null 2>&1 || true' EXIT
[ -n "${OBJSTORE_FAKE_GCS:-}" ] || warn "no Docker: GCS emulator conformance will be skipped"

step "tests (unit, conformance, property, security, doc)"
PROPTEST_CASES="${PROPTEST_CASES:-512}" cargo test --all-features
step "benches compile and run once"; cargo bench --all-features -- --test >/dev/null

step security
if have deny; then cargo deny --all-features check; else warn "cargo-deny not installed"; fi
if have audit; then cargo audit; else warn "cargo-audit not installed"; fi
grep -q '#!\[forbid(unsafe_code)\]' src/lib.rs
if grep -rnE --include='*.rs' '\bdyn\b' src/ | grep -vE '^[^:]+:[0-9]+:\s*//'; then echo "dyn in library code"; exit 1; fi

step "fuzz (${FUZZ_SECS}s per target)"
if have fuzz; then
  for t in fuzz_key fuzz_gcs_wire; do (cd fuzz && cargo fuzz run "$t" -- -max_total_time="$FUZZ_SECS" 2>&1 | tail -1); done
else warn "cargo-fuzz not installed"; fi

step "all gates passed"
