#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
lifecycle_rev=cbab33555ab9355a6ac453d659c55ec9e0666821
vertical_rev=fc24ffff08c52690390b4460f44617d5d9732563
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

manifest_pins=$(grep -c "rev = \"$lifecycle_rev\"" "$manifest")
[[ "$manifest_pins" -eq 2 ]] \
  || fail "host manifest must contain exactly two dependencies at the approved revision"

vertical_pins=$(grep -c "rev = \"$vertical_rev\"" "$manifest")
[[ "$vertical_pins" -eq 1 ]] \
  || fail "host manifest must contain exactly one corrected vertical-protocol dependency"

all_pin_lines=$(grep -R -h -E --include='Cargo.toml' \
  'w1-platform-contracts.*rev[[:space:]]*=' "$repo_root" | wc -l | tr -d ' ')
[[ "$all_pin_lines" -eq 3 ]] \
  || fail "duplicate or competing W1 contract revisions found"

if grep -R -n -E --include='Cargo.toml' \
  'w1-platform-contracts.*rev[[:space:]]*=[[:space:]]*"[^\"]+"' "$repo_root" \
  | grep -v -E "$lifecycle_rev|$vertical_rev"; then
  fail "unapproved W1 contract revision found"
fi

lock_sources=$(grep -c \
  "source = \"git+https://github.com/delysis/w1-platform-contracts?rev=$lifecycle_rev#$lifecycle_rev\"" \
  "$lockfile")
[[ "$lock_sources" -eq 2 ]] \
  || fail "lockfile must resolve both contract crates to the approved immutable revision"

vertical_lock_sources=$(grep -c \
  "source = \"git+https://github.com/delysis/w1-platform-contracts?rev=$vertical_rev#$vertical_rev\"" \
  "$lockfile")
[[ "$vertical_lock_sources" -eq 2 ]] \
  || fail "lockfile must resolve the corrected vertical crate and its exact contract dependency"

grep -q '^unstable-w1-contract-tests = \[' "$manifest" \
  || fail "contract dependencies must remain behind the unstable test feature"
grep -q '^unstable-w1-vertical-tests = \[' "$manifest" \
  || fail "vertical protocol dependency must remain behind the unstable test feature"
grep -q '#\[cfg(feature = "unstable-w1-contract-tests")\]' \
  "$repo_root/crates/speech-native-host/src/lib.rs" \
  || fail "contract adapter source must remain feature-gated"

printf 'W1 lifecycle dependency is uniquely pinned to %s\n' "$lifecycle_rev"
printf 'W1 vertical dependency is uniquely pinned to %s\n' "$vertical_rev"
