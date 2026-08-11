#!/usr/bin/env bash
set -euo pipefail

workflow_dir="${1:-.github/workflows}"

if grep -R -nE 'uses:[[:space:]]+[^[:space:]@]+@(main|master|stable|v[0-9]+)([[:space:]]|$)' "${workflow_dir}"; then
  echo "GitHub Actions must be pinned to immutable commit SHAs" >&2
  exit 1
fi

echo "speech-native-kit workflow policy verified"
