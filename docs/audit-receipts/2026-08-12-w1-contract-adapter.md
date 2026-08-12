# W1 Speech contract adapter receipt — 2026-08-12

This candidate starts from exact `speech-native-kit` commit
`c78f59fd9ee2c5a27baf89aea96946c6e3a79b97` and consumes the temporary W1
contract repository only at immutable revision
`da22fa893ac183c5d9df972a7e67215c0d92b383`.

The `unstable-w1-contract-tests` feature projects the real registered backend
descriptors, host active-operation registry, retained shutdown result, backend
shutdown outcomes, and task-supervisor admitted/completed counters into the v0
capability and closed-summary envelopes. It creates no second operation map and
grants no credential, path, network, or runtime authority.

The contract-enabled tests prove that the capability projection validates,
shutdown attempts every registered backend exactly once, multiple backend
failures remain distinct, the active operation and retained task counts are
zero after shutdown, and expected/completed host relay counts agree. Existing
host tests separately prove exact request nonce ownership, consumer-drop
cancellation, duplicate rejection, nonce exhaustion, self-reaping of 10,000
operations, panic retention, shutdown cancellation, repeated shutdown, and
waiting for backend final completion.

The generic synchronous lifecycle suite is intentionally not claimed. Speech
admission and terminal delivery are asynchronous and do not expose separate
Reserved/Queued transitions. Adapting those tests would require a shadow state
machine rather than exercising the host. Backend resources report that their
shutdown future completed or failed; native Apple and Parakeet worker join
proof remains in their explicitly named backend lifecycle tests and real-smoke
receipts because the host does not own those private worker registries.

Real Apple and Parakeet inference are not fixture contract tests. Their current
evidence remains separately named in `2026-08-11-r2-speech-reap.md` and
`2026-08-10-real-parakeet.md`.
