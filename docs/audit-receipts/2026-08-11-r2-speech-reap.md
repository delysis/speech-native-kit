# R2 Speech self-reaping and Apple acceptance receipt — 2026-08-11

This receipt is anchored to the immutable R2 implementation commit
`speech-native-kit@1150d1cc5b76af537ae9aae7a57dc5a6d6adc300` and its focused
Apple lifecycle follow-up commit
`speech-native-kit@34bc0276c41ba5a8f1a4d53619db63ba51a82cb6`. Neither commit is
claimed as pushed or released.

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

The original candidate run passed in 24.49 seconds. The independent reaudit
reran the same command against exact commit
`1150d1cc5b76af537ae9aae7a57dc5a6d6adc300`; it passed in 7.31 seconds. Complete
and streaming inference both returned “hardly beneath the old stone bridge
while morning light reached the valley”. The test also cancelled one of two
peer requests, preserved the peer completion, and completed joined shutdown.
This remains lifecycle evidence, not transcription-accuracy acceptance; the
first words were decoded incorrectly.

## Historical precommit launched Tauri Apple TTS

The repository does not contain a standalone Tauri consumer. Before commit
`1150d1cc5b76af537ae9aae7a57dc5a6d6adc300` was created, a disposable, hidden
Tauri 2 consumer at `/tmp/speech-r2-tauri-smoke` used path dependencies to the
then-uncommitted worktree, registered `AppleSpeechBackend` through
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

This is real launched-completion evidence for the precommit candidate, not
immutable current-source acceptance. The receipt did not record an executable
hash, and no executable identity is inferred after the fact. The smoke covered
a completed synthesis followed by normal exit; it did not exercise shutdown
while an Apple operation remained active.

Negative launch evidence is retained. The first built binary exited 134 because
the default deployment target left `libswift_Concurrency.dylib` unresolved. A
manual `DYLD_LIBRARY_PATH` attempt exited 133 because the older Command Line
Tools library duplicated Swift runtime classes. Rebuilding with
`MACOSX_DEPLOYMENT_TARGET=13.0` resolved the runtime boundary and produced the
successful launched-app receipt above.

## Current-source Apple lifecycle evidence

The independent reaudit checked the production implementation and committed
deterministic portable tests at
`34bc0276c41ba5a8f1a4d53619db63ba51a82cb6`. One fixture holds an owned Apple
worker active, verifies shutdown sets its cancellation flag, verifies shutdown
does not complete before the worker exits, releases the worker, and verifies
joined shutdown, an empty active-operation map, and zero active task records.
A second fixture drives the deterministic `apple_tts_audio_empty` domain error
and verifies the operation lease self-reaps while the supervisor retains no
task failure. These are worker-lifecycle tests; they do not synthesize speech.

## Current-source launched Tauri Apple WAV

At `2026-08-11T22:52:13Z`, the same disposable hidden Tauri 2 consumer was
rebuilt in release mode with path dependencies resolving to exact Rust source
commit `34bc0276c41ba5a8f1a4d53619db63ba51a82cb6`:

```sh
MACOSX_DEPLOYMENT_TARGET=13.0 cargo build --release
./target/release/speech-r2-tauri-smoke
```

The arm64 executable was 12,261,344 bytes with SHA-256
`9dab95f43a7172ff431af118622c29798168895f4b8c7de163cad1ff9ebb3075`.
It exited 0 and reported one real local Apple terminal with no fixture:

```text
APPLE_TAURI_SMOKE_OK request_id=apple-launched-tauri-r2 backend=apple.av-speech voice=com.apple.eloquence.en-US.Eddy wav_bytes=181216 terminal_events=1 network=never real_local_inference=true fake_fixture=false
```

This proves launched current-source completion and a real Apple WAV. Active
operation cancellation and joined shutdown remain separately established by
the deterministic owned-worker test above; the launched smoke completed its
synthesis before normal application exit.

## Repository gates

- `cargo fmt --all --check`: passed.
- `cargo test --workspace --all-targets`: passed; 58 passed, 0 failed,
  2 environment-gated tests ignored.
- Rust 1.88 workspace clippy with all targets/features and `-D warnings`:
  passed.
- `./scripts/check-boundaries.sh`: passed.

Host Rust 1.95 clippy additionally surfaced an untouched pre-existing
`manual_is_multiple_of` lint in `speech-native-types`; the declared Rust 1.88
gate passed without changing unrelated code.
