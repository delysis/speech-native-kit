# W1 Speech contract adapter receipt — 2026-08-12

This candidate starts from exact `speech-native-kit` commit
`c78f59fd9ee2c5a27baf89aea96946c6e3a79b97` and consumes the canonical W1
contract repository only at immutable revision
`cbab33555ab9355a6ac453d659c55ec9e0666821` (`w1-contracts-v0-2026-08-12-r3`).

The declared lifecycle implementation is `speech-native-kit/speech-host-v1`.
Ordinary transcription and synthesis use its production operation registry for
generation-safe reservation, queue/start, one backend attempt, cancellation,
terminal classification, and executor release. Setup rollback and finalization
are checked transactions. Registry or host-state failure is observable in the
consumer result and shutdown result; mutex poison never becomes empty success.

The complete manifest accepts exactly eleven suite results and all eighteen
normative invariants. Component suites exercise the production registry.
Bridge suites cross real `SpeechHost` admission, ticket-drop control, bounded
Tokio event channels, host final relays, backend-owned `TaskSupervisor` tasks,
and host shutdown. Expected worker IDs come from actual supervisor admission;
joined IDs are recorded only after a retained join task has awaited the worker
handle; when active reaches zero, the retained join-handle map must also be
empty or the supervisor fails closed. No adapter-owned active map, completion
counter, worker identity, or lifecycle phase is used as evidence.

Attempt hierarchy is exercised through the production registry API; ordinary
Speech traffic currently starts one backend attempt per public request.
Progress conformance publishes to both the admitted production lease and a real
bounded backend event channel; ordinary host traffic forwards backend events
but does not itself call `publish_progress` on the registry.

Capability fixtures remain deterministic, local, and network-never. They make
no Apple, Parakeet, hosted-provider, credential, or real-inference claim. Real
Apple and Parakeet evidence remains separately named in
`2026-08-11-r2-speech-reap.md` and `2026-08-10-real-parakeet.md`.

Shutdown envelope resources contain no dummy operation worker. Host relay
counts and IDs come from the production task supervisor. Generic backend
resources truthfully report zero known worker IDs because the backend trait
exposes only the awaited shutdown result, not a backend-private worker
registry; an awaited future is not counted as a worker. The full bridge
manifest separately uses controlled backends whose real task supervisors
expose exact worker IDs.

Verification is run under the repository's Rust 1.88 default lane and the
contract lane's Rust 1.92 toolchain. The real Parakeet and launched Apple UI
tests remain explicitly ignored unless their documented hardware/assets are
available; they are not promoted by contract-fixture success.
