#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
expected_rev=da22fa893ac183c5d9df972a7e67215c0d92b383
manifest="$repo_root/crates/speech-native-host/Cargo.toml"
lockfile="$repo_root/Cargo.lock"

fail() {
  printf 'W1 contract pin violation: %s\n' "$1" >&2
  exit 1
}

if grep -R -n -E --include='Cargo.toml' \
  'w1-platform-contracts.*(branch|tag)[[:space:]]*=' "$repo_root"; then
  fail "moving branch or tag dependency found"
fi

manifest_pins=$(grep -c "rev = \"$expected_rev\"" "$manifest")
[[ "$manifest_pins" -eq 2 ]] \
  || fail "host manifest must contain exactly two dependencies at the approved revision"

all_pin_lines=$(grep -R -h -E --include='Cargo.toml' \
  'w1-platform-contracts.*rev[[:space:]]*=' "$repo_root" | wc -l | tr -d ' ')
[[ "$all_pin_lines" -eq 2 ]] \
  || fail "duplicate or competing W1 contract revisions found"

if grep -R -n -E --include='Cargo.toml' \
  'w1-platform-contracts.*rev[[:space:]]*=[[:space:]]*"[^\"]+"' "$repo_root" \
  | grep -v "$expected_rev"; then
  fail "unapproved W1 contract revision found"
fi

lock_sources=$(grep -c \
  "source = \"git+https://github.com/delysis/w1-platform-contracts?rev=$expected_rev#$expected_rev\"" \
  "$lockfile")
[[ "$lock_sources" -eq 2 ]] \
  || fail "lockfile must resolve both contract crates to the approved immutable revision"

grep -q '^unstable-w1-contract-tests = \[' "$manifest" \
  || fail "contract dependencies must remain behind the unstable test feature"
grep -q '#\[cfg(feature = "unstable-w1-contract-tests")\]' \
  "$repo_root/crates/speech-native-host/src/lib.rs" \
  || fail "contract adapter source must remain feature-gated"

printf 'W1 contract dependency is uniquely pinned to %s\n' "$expected_rev"
