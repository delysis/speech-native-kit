# speech-native-kit contributor guidance

Write safe, direct, idiomatic Rust. Prefer small explicit state machines and
typed failures over abstraction layers that conceal ownership or authority.

## Ownership

- This repository owns speech-domain types, routing, local/platform backends,
  bounded streams, cancellation, and lifecycle.
- Product applications own microphone capture, permission prompts, playback,
  transcript insertion, and user-facing controls.
- Provider gateways own hosted credentials, quotas, retries, accounting, and
  public HTTP compatibility endpoints.
- `llama-native-kit` owns llama.cpp. Generative audio bridges belong downstream
  of both native kits.

## Hard boundaries

- No HTTP server, loopback listener, provider SDK, credential store, telemetry,
  microphone activation, or automatic playback in core crates.
- Discovery is noninteractive: it must not request permission, download assets,
  speak, record, or open a network connection.
- Local-only routing admits only runtime-proven `network = never` capabilities.
- Every request has one terminal result; cancellation affects only its request.
- Backends own their runtime objects and join or stop workers on shutdown.
- Do not merge text-generation and speech request models merely because fields
  such as request IDs or deadlines look similar.
- The Tauri plugin default permission remains status-only. High-authority
  operations require explicit permission sets.

## Required verification

```sh
cargo fmt --all --check
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets --all-features -- -D warnings
./scripts/check-boundaries.sh
```
