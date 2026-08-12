# W1 Speech vertical baselines — 2026-08-12

This receipt freezes section-16 rows 4, 11, and 12 without changing the
production source roots at `89146595df7ba893ddd704a811377f6cb14856bc`.
The lifecycle adapter remains pinned to `cbab33555ab9355a6ac453d659c55ec9e0666821`;
the vertical-fixture validator is pinned to the corrected protocol merge
`fc24ffff08c52690390b4460f44617d5d9732563`.

## Deterministic peer cancellation

The repository-owned integration replay drives two independent deterministic
backends through the production `SpeechHost`. Both requests are admitted before
one exact request identity is cancelled. The selected request produces one
cancelled terminal and cancellation final; the other backend receives no
cancellation, produces one completed terminal and a nonempty response. Both
routes release before `SpeechHost::shutdown`, and shutdown calls both backends.

```sh
cargo test --locked -p speech-native-host --test w1_vertical \
  w1_deterministic_peer_cancellation_projection \
  --features unstable-w1-vertical-tests -- --exact
```

The replay and central baseline validation passed.

## Exact Parakeet model and audio

The exact input was regenerated with Samantha at rate 155 and converted with
`afconvert -f WAVE -d LEF32@16000 -c 1`. It reproduced the existing identity:

- audio: 305,580 bytes, SHA-256
  `326d6723b8bcd7ae63cdff4a2c3e536a29a9d3a44e30f9dca7b65e58a9b4aa34`;
- ordered model bundle (`encoder.onnx`, `decoder_joint.onnx`,
  `tokenizer.json`): 480,708,981 bytes, SHA-256
  `c710ae82b52aa969f89874e7e7b35ad570fec50cc3d943a4fdde0bb874948756`.

The real test now stream-hashes every exact model file, their ordered combined
bytes, and the exact WAV before inference. Complete and streaming inference
both returned nonempty UTF-8. Two peer requests were then admitted, exactly one
was cancelled, the other completed, and shutdown joined. The run passed in
38.89 seconds. The observed transcript remains “hardly beneath the old stone
bridge while morning light reached the valley”; its incorrect first words are
retained as negative evidence, not relabeled as accuracy success.

The independent central prerequisite replay stream-authenticated both external
artifacts without retaining the model bundle and accepted the invariant
projection in 25.36 seconds.

## Apple installed voice and launched output

The noninteractive current inventory reported macOS 15.6 build `24G84`, Darwin
24.6.0 on arm64, 191 installed voices, and exact voice
`com.apple.eloquence.en-US.Eddy`: `en-US`, normal quality, installed,
`network=never`.

A checked-in hidden Tauri consumer built with `MACOSX_DEPLOYMENT_TARGET=13.0`
and ran against current path dependencies. The 11,499,376-byte arm64 executable
had SHA-256
`492296f764ee943007d34541b3883b0d012b70009bb297b654f3cc66e935349a`.
The exact replay exited 0:

```text
APPLE_W1_OK backend=apple.av-speech voice=com.apple.eloquence.en-US.Eddy language=en-US quality=normal wav_bytes=153540 terminal_events=1 network=never real_local_inference=true
```

The accepted output facts are exact voice/language/quality and OS build,
RIFF/WAVE container, audio length greater than the 44-byte header, positive
duration, one terminal, local-only routing, and real local inference. WAV byte
identity is explicitly not an invariant. The earlier current-source receipt
produced 181,216 bytes; this current run produced 153,540 bytes while satisfying
the same semantic output boundary.

## Frozen identities and limitations

`fixtures/w1/MANIFEST.sha256` authenticates every checked-in W1 fixture byte;
its SHA-256 is
`131c47a896383f25928d55ce1f6cc51652efb8946bb12f48d9678133f4d47708`.
The three manifest identities are:

- peer cancellation: `195fb1aea788c2b93180f853e90acaa19b087544e48773fe4656a0d8bedfe7e9`;
- Parakeet: `b181cc24a624deb1e9ea9348b55bfc58fa920faf571ee6c2ae48e791eb96d103`;
- Apple: `cc6fe0586f95f373025568a071147d2aa27d6d276939acc3600005263a8cbbc7`.

These baselines do not claim transcription accuracy, stable Apple audio bytes,
portable execution without the named external Parakeet artifacts, microphone
permission, or speech-recognition permission.
