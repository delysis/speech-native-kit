# Real Parakeet lifecycle receipt — 2026-08-10

This receipt covers real local model execution on the uncommitted candidate
tree for `codex/host-request-leases`. It is not Apple/Tauri acceptance and does
not promote those separately gated claims.

## Runtime inputs

- Host: Apple arm64, Darwin 24.6.0.
- Rust compiler for the real run: `rustc 1.95.0 (59807616e 2026-04-14)`.
- Model: Hugging Face snapshot
  `altunenes/parakeet-rs@a61d2818df4659c956b9661a9447f46e98c15126`,
  `realtime_eou_120m-v1-onnx`.
- `encoder.onnx`: 459,341,289 bytes,
  SHA-256 `d472887cc38a784a5bfc21c2dbe247639edc3b3f9992388d8ceceaec07256b5b`.
- `decoder_joint.onnx`: 21,347,639 bytes,
  SHA-256 `9d2553ac043c2fc5f69e970769b0fb8ab9103fbfdeb7d26a1ea9729d4bd2dddd`.
- `tokenizer.json`: 20,053 bytes,
  SHA-256 `f6b0ad8690559351fa478116fe0985a203b76f7c040f3a9381f485c99c0325f8`.
- Total model bytes: 480,708,981.
- Fresh speech input: macOS `say -v Samantha -r 155`, converted with
  `afconvert -f WAVE -d LEF32@16000 -c 1`.
- WAV SHA-256:
  `326d6723b8bcd7ae63cdff4a2c3e536a29a9d3a44e30f9dca7b65e58a9b4aa34`.

The spoken source sentence was: “The river moved quietly beneath the old
stone bridge while morning light reached the valley.” This generated WAV was
a disposable `/tmp` input and is not checked into the repository.

## Command

```sh
SPEECH_NATIVE_TEST_WAV=/tmp/speech-native-real.66UGk1/parakeet-smoke.wav \
SPEECH_NATIVE_PARAKEET_MODEL_DIR=/Users/george/.cache/huggingface/hub/models--altunenes--parakeet-rs/snapshots/a61d2818df4659c956b9661a9447f46e98c15126/realtime_eou_120m-v1-onnx \
cargo test -p speech-native-backend-parakeet --test real_parakeet -- --ignored --nocapture
```

## Result

- Result: 1 passed, 0 failed, 16.89 seconds including compilation; test body
  4.32 seconds.
- Complete-file transcript: “hardly beneath the old stone bridge while
  morning light reached the valley”.
- Streaming transcript: identical to complete-file output.
- The real test also started two peer requests, cancelled exactly one through
  `SpeechHost`, observed one `Cancelled` terminal and cancellation final for
  that request, observed one `Completed` terminal and non-empty final for the
  peer, and completed joined host/backend shutdown.
- Both completed responses asserted `real_local_inference = true` and
  `model_load_ms = 0` for the already-resident handle.

This is functional/lifecycle evidence, not transcription-accuracy evaluation:
the first words were decoded incorrectly. The mismatch is retained here rather
than normalized away.
