# R2 Speech self-reaping and Apple acceptance receipt — 2026-08-11

This receipt covers the uncommitted `R2-SPEECH-REAP` candidate worktree based
exactly on `speech-native-kit@ac1a15047b9eb8f3e845f27b03b0eae70d70cc90`.
It does not claim that the candidate is committed, pushed, or released.

## Bounded lifecycle evidence

The host, Parakeet, and Apple lifecycle paths use a shared task supervisor.
Completed task handles are not retained. The supervisor retains an active
count, one full failure record, and an aggregate additional-failure count.
Shutdown closes task admission, waits for active task count zero, and reports
the retained failure summary. Async and blocking panic fixtures reach shutdown
evidence, and request nonce exhaustion rejects admission before wraparound.

`ten_thousand_fixture_operations_self_reap_task_state` ran 10,000 complete
host requests and asserted:

- every request produced a final response;
- active request count returned to zero;
- active task count returned to zero;
- retained task failure records remained zero.

The test completed in 0.36 seconds in the full workspace run. No process RSS
claim is made: this receipt proves bounded lifecycle records, not allocator or
OS memory reclamation within a fixed tolerance.

## Real Parakeet rerun

The existing real integration test was rerun with the installed model snapshot
`altunenes/parakeet-rs@a61d2818df4659c956b9661a9447f46e98c15126` and a freshly
generated, non-played Samantha WAV. The input SHA-256 was
`326d6723b8bcd7ae63cdff4a2c3e536a29a9d3a44e30f9dca7b65e58a9b4aa34`.

```sh
SPEECH_NATIVE_TEST_WAV=/tmp/speech-r2-real.ABGDyD/parakeet-smoke.wav \
SPEECH_NATIVE_PARAKEET_MODEL_DIR=/Users/george/.cache/huggingface/hub/models--altunenes--parakeet-rs/snapshots/a61d2818df4659c956b9661a9447f46e98c15126/realtime_eou_120m-v1-onnx \
cargo test -p speech-native-backend-parakeet --test real_parakeet -- --ignored --nocapture
```

Result: 1 passed in 24.49 seconds. Complete and streaming inference both
returned “hardly beneath the old stone bridge while morning light reached the
valley”. The test also cancelled one of two peer requests, preserved the peer
completion, and completed joined shutdown. This remains lifecycle evidence,
not transcription-accuracy acceptance; the first words were decoded
incorrectly.

## Launched Tauri Apple TTS

The repository does not contain a standalone Tauri consumer. A disposable,
hidden Tauri 2 consumer at `/tmp/speech-r2-tauri-smoke` used path dependencies
to this exact worktree, registered `AppleSpeechBackend` through
`tauri-plugin-speech-native`, synthesized into a buffer without playback, and
exited through the normal Tauri lifecycle.

The successful launch used an explicit deployment floor so the safe Swift
bridge linked against the host runtime:

```sh
MACOSX_DEPLOYMENT_TARGET=13.0 \
CARGO_TARGET_DIR=/tmp/speech-r2-tauri-target13 \
cargo run
```

The launched process reported:

```text
APPLE_TAURI_SMOKE_OK request_id=apple-launched-tauri-r2 backend=apple.av-speech voice=com.apple.eloquence.en-US.Eddy wav_bytes=181216 terminal_events=1 network=never real_local_inference=true fake_fixture=false
```

The runtime inventory independently reported the Apple backend ready and
enumerated 191 installed voices without speaking or requesting permission.

Negative launch evidence is retained. The first built binary exited 134 because
the default deployment target left `libswift_Concurrency.dylib` unresolved. A
manual `DYLD_LIBRARY_PATH` attempt exited 133 because the older Command Line
Tools library duplicated Swift runtime classes. Rebuilding with
`MACOSX_DEPLOYMENT_TARGET=13.0` resolved the runtime boundary and produced the
successful launched-app receipt above.

## Repository gates

- `cargo fmt --all --check`: passed.
- `cargo test --workspace --all-targets`: passed; 56 passed, 0 failed,
  2 environment-gated tests ignored.
- Rust 1.88 workspace clippy with all targets/features and `-D warnings`:
  passed.
- `./scripts/check-boundaries.sh`: passed.

Host Rust 1.95 clippy additionally surfaced an untouched pre-existing
`manual_is_multiple_of` lint in `speech-native-types`; the declared Rust 1.88
gate passed without changing unrelated code.
