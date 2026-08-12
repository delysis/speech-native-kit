# Speech Gateway Boundary

The speech module is a sibling to the text-generation gateway, not another
shape of `GatewayRequest`. Speech-to-text and text-to-speech have separate
typed requests, streams, responses, capabilities, and terminal events in
`speech-native-types`.

Live transcription returns a ticket with a bounded event receiver and a typed,
backpressured `TranscriptionAudioSink`. Complete-file transcription omits that
sink. Dropping either ticket invokes cancellation; terminal results remain
authoritative in Rust. Ticket drop never releases the host-wide request ID:
the host retains a private operation lease until the selected backend's final
response resolves, even when the consumer abandons its event or final channel.
Request IDs are unique across every registered backend for their complete
executor lifetime, not merely for the lifetime of a client handle.

This slice establishes five reusable crates:

- `speech-native-types`: protocol-neutral STT/TTS contracts, privacy and routing
  policy, model and voice descriptors, event tickets, cancellation, usage,
  typed errors, and the backend trait.
- `speech-native-platform`: deterministic platform adapter discovery and bounded
  aggregation of evidence from native or embedded runtime probes.
- `speech-native-router`: hard capability/privacy gates followed by deterministic
  policy ordering, exact model/voice routing, and complete rejection receipts.
- `speech-native-host`: a Tauri-independent registry and execution service that
  plans only across actually registered backends, pins the resolved
  backend/model/voice into each request, owns cancellation and shutdown, and
  relays backend finals through host-owned monitor tasks.
- `speech-native-backend-parakeet`: an executable, embedded Parakeet Realtime EOU 120M
  backend over `parakeet-rs` and ONNX Runtime. It loads one shared model handle
  from the Hugging Face cache and creates independent decoder state per
  request.

On macOS, `AppleCapabilitySource` now performs a real, noninteractive runtime
inventory through safe Speech and AVSpeechSynthesizer bindings. It does not ask
for permission, activate a microphone, speak, or download voices. It reports
authorization and unavailable assets as readiness states for a later explicit
user action.

`AppleSpeechBackend` is the first executable adapter. It renders through
AVSpeechSynthesizer's buffer API, packages the native PCM as WAV, never plays
audio, and does not request permission. The current safe Rust wrapper requires
a live AppKit event loop for offline synthesis: its command-line test is
therefore explicitly ignored, while an opt-in launched-Tauri smoke is the real
acceptance boundary. The launch receipt must record real platform synthesis,
`network = never`, a real installed voice, a valid WAV, and
`fake_fixture = false` before this adapter is considered product-proven.

The executable Apple path currently advertises non-streaming WAV output. Its
upstream bridge collects buffers synchronously and has an internal timeout, so
it would be dishonest to claim live audio streaming or pre-emptive native
cancellation. Those capabilities remain false until a truly asynchronous
bridge is implemented and tested.

The reusable speech crates have no Tauri dependency. The Rust-only,
speech-specific `tauri-plugin-speech-native` owns an injected
`SpeechHost` and exposes scoped status, route-plan, synthesize, transcribe,
stream, and cancel commands plus the
`SpeechNativeExt::speech_native` Rust extension method.
The provider/text gateway contains no speech state, dependency, command,
permission, or shutdown path. Ordinary Rust consumers use the same service and
backend traits without Tauri.

The host lifecycle is one-way: `running -> quiescing -> closed`. Route
selection and global request-ID reservation occur under the same state lock.
The shutdown leader closes admission, cancels every exact active route, asks
every backend to stop, waits for all backend finals, and waits for supervised
relay activity to reach zero. Relay and blocking-worker task records self-reap
at completion; retained task state is one failure record plus an aggregate
failure count, rather than one join record per historical request. Async and
blocking panics are converted into shutdown evidence. The Parakeet and Apple
adapters use the same active-count contract, so neither adapter can report
shutdown while its native worker remains alive. Host and backend request nonces
use checked allocation and fail closed before wraparound.
The Tauri plugin performs this joined shutdown at `RunEvent::Exit`, reports a
failure rather than discarding it, and repeats the retained operation as a
drop-time fallback.

Live Tauri transcription has an explicit input half as well as an output event
half. `speech_transcribe_stream` opens the bounded request, while
`speech_transcription_audio_push` and `speech_transcription_audio_finish` feed
ordered PCM chunks through the ticket's typed audio sink. Closing the event
channel or ticket cancels only that request.

### Tauri migration

The speech request and event types are unchanged. Applications moving from the
former combined plugin must:

1. install `tauri-plugin-speech-native` separately;
2. grant `speech-native:default` instead of receiving speech through
   `free-token-energy:default`;
3. invoke `plugin:speech-native|speech_*` commands; and
4. import `SpeechNativeExt` for Rust-side access.

Applications that do not use speech need no speech crate, plugin, permission,
backend registration, or shutdown path.

## Default Selection Policy

The default profile is `private_balanced` under `local_only` privacy:

1. Use a platform-native backend only when it is ready, meets the requested
   operation features, explicitly reports `network = never`, and provides
   confirmed runtime or real-smoke evidence.
2. Otherwise use an installed embedded backend: Parakeet for transcription and
   Kokoro-class synthesis.
3. An already-resident Gemma 4 E2B, E4B, or 12B audio model may be a complete
   audio transcription fallback when that exact model/runtime combination has
   passed a real smoke. It does not inherit streaming, timestamps,
   diarization, or partial-result claims from a general audio-input flag.
4. Hosted speech never enters a local-only route. It can be added only by an
   explicit hosted-enabled privacy policy.

“Native” and “local” are deliberately different facts. Merely compiling for an
OS produces adapter candidates, not capabilities. A capability enters a
private route only after runtime evidence proves both availability and
never-network behavior.

## Platform Probe Matrix

| Platform | Native candidates | Private routing rule | Embedded fallback |
|---|---|---|---|
| macOS / iOS | SpeechAnalyzer/SpeechTranscriber, SFSpeechRecognizer, AVSpeechSynthesizer | SpeechAnalyzer assets and on-device operation must be present; older recognition must both support and require on-device operation; voices are inventoried individually | Parakeet STT, Kokoro TTS, eligible resident Gemma audio |
| Windows | Windows speech recognition and synthesis | Free-form dictation is treated as online and package-identity-dependent; it is not a private default. Installed TTS is eligible only after runtime audio/voice evidence | Parakeet STT, Kokoro TTS, eligible resident Gemma audio |
| Android | Explicit on-device SpeechRecognizer and TextToSpeech | Only `createOnDeviceSpeechRecognizer` with runtime availability is local; TTS voices requiring network are excluded | Parakeet STT, Kokoro TTS, eligible resident Gemma audio |
| Linux | Spiel providers | Each provider, voice, returned-audio path, and network behavior is probed; the platform name alone proves nothing | Parakeet STT and Kokoro TTS are the dependable default; eligible resident Gemma audio |

## Native Bridge Contract

Each platform adapter implements `PlatformCapabilitySource` directly or sends
a versioned `fte.speech.capability_report.v1` JSON payload to
`ReportedCapabilitySource::from_json`. The probe:

- enforces unique source, backend, and capability identifiers;
- validates capability ownership and runtime-evidence provenance;
- gives each source an independent deadline;
- preserves failures and timeouts as typed source reports;
- sorts reports and descriptors deterministically;
- never lets one failed source erase another working backend;
- never promotes documentation or build-target evidence to a private route.

Permissions and model downloads remain typed readiness states. Discovery must
not request microphone access, download a model, or trigger a platform consent
dialog. Those state changes happen only after a user action selects the
capability.

## Embedded Parakeet Runtime

`speech-native-backend-parakeet` is the first executable cross-platform fallback. Its
current model is `parakeet-realtime-eou-120m-v1-onnx` from
`altunenes/parakeet-rs`. Discovery checks, in order:

1. `SPEECH_NATIVE_PARAKEET_MODEL_DIR` (with the legacy
   `FTE_PARAKEET_MODEL_DIR` alias retained for the 0.1 line);
2. `HUGGINGFACE_HUB_CACHE`;
3. `HF_HOME/hub`;
4. the standard `~/.cache/huggingface/hub` location.

It follows the cache snapshot/reference structure and never copies weights.
When files are absent, the registered descriptor reports a Hugging Face-managed
`asset_install_required` blocker; discovery does not download anything.

The model is English-only and advertises PCM/WAV input, streaming, and partial
results. It does not claim timestamps, diarization, translation, hotwords, or
generative transcription. Complete and live audio are downmixed and linearly
resampled to the model's exact mono 16 kHz input. One ONNX handle remains
resident, while each request has separate encoder/decoder state and an
independent cancellation flag.

The second embedded lane remains intentional rather than forgotten:

- `parakeet-rs`/ONNX is the currently executable default embedded lane.
- `parakeet.cpp` has a flat C API, in-memory PCM, timestamps, batching,
  streaming variants, and broad prebuilt targets. It remains a planned
  shared-library backend for targets where its GGUF packaging is advantageous;
  it must be dynamically isolated from llama.cpp's ggml symbols and pass the
  same backend equivalence suite before becoming selectable.
- `sherpa-onnx` remains a useful broader fallback direction for Parakeet-family
  and Kokoro models, but is not counted as implemented.

## Evidence and Acceptance

A platform adapter is ready only after it can produce a truthful descriptor
from the running machine. A production backend additionally needs real audio
fixtures covering output accuracy, streaming order, cancellation, no reload
for an identical resident configuration, network denial, and exactly one
terminal event. TTS needs returned-audio and voice-specific network tests; STT
needs exact feature tests for timestamps, partials, translation, and
diarization.

The pre-extraction FTE desktop produced real Apple and Parakeet launched-app
receipts through the same plugin-managed host. Those receipts are historical
provenance, not readiness for this repository's next release. A standalone
Tauri smoke consumer must rerun Apple synthesis and Parakeet transcription
before a release claims launched-app evidence. The real Parakeet integration
test uses `SPEECH_NATIVE_TEST_WAV` (with a legacy alias), streams PCM through
the input sink, and cancels one request without affecting its completing peer.

Primary references used for these rules:

- Apple SpeechAnalyzer and on-device SpeechTranscriber:
  <https://developer.apple.com/documentation/Speech/SpeechAnalyzer>
- Windows speech recognition constraints:
  <https://learn.microsoft.com/windows/apps/develop/input/speech-recognition>
- Android explicit on-device recognizer:
  <https://developer.android.com/reference/android/speech/SpeechRecognizer>
- Android voice network requirement:
  <https://developer.android.com/reference/android/speech/tts/Voice>
- Spiel provider model:
  <https://project-spiel.org/>
- Gemma 4 model card:
  <https://ai.google.dev/gemma/docs/core/model_card_4>
- parakeet.cpp:
  <https://github.com/mudler/parakeet.cpp>
- parakeet-rs:
  <https://github.com/altunenes/parakeet-rs>
- sherpa-onnx Rust examples:
  <https://github.com/k2-fsa/sherpa-onnx/tree/master/rust-api-examples>
