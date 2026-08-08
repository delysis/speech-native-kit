#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

fail() {
  printf 'speech boundary violation: %s\n' "$1" >&2
  exit 1
}

if rg -n 'fte[-_](types|router|providers|store|protocols|loopback|backend)|free_token_energy' \
  "$repo_root/crates"; then
  fail "speech-native-kit depends on the Free Token Energy text/provider gateway"
fi

if rg -n '\b(axum|reqwest|hyper|tower_http)\b' \
  "$repo_root/crates" -g 'Cargo.toml' -g '*.rs'; then
  fail "speech-native-kit contains network or loopback authority"
fi

rg -q '^name = "tauri-plugin-speech-native"$' \
  "$repo_root/crates/tauri-plugin-speech-native/Cargo.toml" \
  || fail "the Tauri plugin package name changed"

rg -q 'PluginBuilder::new\("speech-native"\)' \
  "$repo_root/crates/tauri-plugin-speech-native/src/lib.rs" \
  || fail "the Tauri runtime namespace changed"

default_permissions="$repo_root/crates/tauri-plugin-speech-native/permissions/default.toml"
rg -q 'allow-speech-status' "$default_permissions" \
  || fail "the default permission no longer allows status inspection"

if rg -n 'allow-speech-(synthesize|transcribe|transcription-audio|cancel)' \
  "$default_permissions"; then
  fail "the default Tauri permission grants speech execution authority"
fi

printf 'speech-native-kit boundaries verified\n'
