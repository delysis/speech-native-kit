# W1 Speech fake-owner quit and relaunch — 2026-08-12

This model-free row-8 fixture is a test-only descendant of
`2c427e39ee07c944e0ef51d471729fb676e2f62a`. Its source descriptor binds the
exact observed Git trees for `crates/speech-native-host/src` and
`crates/speech-native-types/src`; both remain unchanged by this candidate. The
vertical protocol is pinned to `fc24ffff08c52690390b4460f44617d5d9732563`
(`w1-vertical-protocol-v0-2026-08-12-r2`) and the lifecycle contract remains
pinned to `cbab33555ab9355a6ac453d659c55ec9e0666821`.

The fixture drives the real `SpeechHost` coordinator and real
`TaskSupervisor` reapers around a deterministic backend owner. Quit cancels an
admitted operation. The backend fsyncs one cancelled terminal receipt before
publishing its sole terminal event and final result. The fixture waits until
the detached host shutdown coordinator has been accepted, aborts its first
caller, releases backend shutdown, and proves that a follower receives the
retained successful result. The closed host summary and backend task snapshot
must report zero active or retained work and exact expected/joined worker-ID
sets. A separate owner-alive witness must be false.

The feature-gated adapter retains the host supervisor's exact expected and
joined IDs instead of collapsing them to counts. The projection freezes all
four input-derived worker identities in both expected and joined form:
`task-supervisor:task-1:host-final-relay:speech-quit-operation`,
`w1-speech-quit-owner:task-1:request:speech-quit-operation`,
`task-supervisor:task-1:host-final-relay:speech-relaunch-operation`, and
`w1-speech-relaunch-owner:task-1:request:speech-relaunch-operation`.

The store is then dropped and reopened at the same path. A fresh `SpeechHost`
and backend use a distinct worker identity, complete nonempty deterministic
post-relaunch work, and repeat the joined shutdown checks. The four-record JSONL
store is sequence/schema-validated on reopen. The post-quit store is 670 bytes
with SHA-256 `0b6dc3371daa9c459ee2dce20bced7f0aecf23e0962141bb2942503e93a58f80`;
the final store is 1,380 bytes with SHA-256
`b5421de134c7649dfde284b621014630f922ba87b06b11152aea30da5fd307cd`.
`fixtures/w1/MANIFEST.sha256` authenticates all fourteen W1 fixture files and
itself has SHA-256
`2fdf00f591c2389041cd254172994c6d104bb3572e5cbd4106297f9186387b07`.

Local Rust 1.92 verification passed the focused row-8 replay, the combined
host lifecycle/vertical suite (28 passed, 1 external Parakeet test ignored),
the complete all-feature workspace suite (85 passed, 4 external-runtime tests
ignored), combined host Clippy with warnings denied, rustfmt, exact pin and
boundary checks, exact source binding, and the fourteen-file SHA-256 ledger.

The repository-wide all-feature Clippy command additionally reports a
pre-existing `collapsible_if` warning in
`crates/speech-native-platform/src/apple.rs`; this row does not change or bind
that Apple production path. No row-8 Clippy warning remains.

This is reproducible fixture evidence. It does not prove an operating-system
process relaunch, real model loading or inference, Apple platform synthesis, or
Parakeet transcription.
