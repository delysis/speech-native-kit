# Architecture decision: speech is a sibling service

Speech recognition and synthesis are one coherent audio domain, but they are
not text-generation provider protocols. This repository therefore owns the
speech host independently from Free Token Energy and `llama-native-kit`.

```text
                              ┌────────────────────────┐
                              │      product app       │
                              │ capture · playback · UX│
                              └───────────┬────────────┘
                                          │
                         ┌────────────────┴───────────────┐
                         │                                │
               ┌─────────▼──────────┐          ┌──────────▼─────────┐
               │ speech-native-kit │          │ free-token-energy │
               │ local STT / TTS   │◄─────────│ optional hosted   │
               │ host + Tauri IPC  │  bridge  │ speech + /audio/*│
               └───────────────────┘          └────────────────────┘
```

The dependency arrow points from an optional FTE bridge to speech contracts.
Speech never imports FTE's text request model, loopback server, provider store,
or secret resolver.

## Boundary decisions

- `SpeechHost` is in-process and transport-neutral.
- The Tauri plugin is optional and Rust-only.
- Hosted speech adapters may implement `SpeechBackend`, but live with the
  provider gateway that owns their credentials and accounting.
- OpenAI-compatible audio endpoints are codecs at FTE's loopback edge.
- Microphone capture and playback remain product responsibilities.
- Model assets are discovered in standard caches and are never copied merely
  to establish application ownership.

The legacy `fte.speech.*` serialized schema identifiers remain stable in the
0.1 line so stored receipts and fixtures do not become unreadable. Rust package
and Tauri permission namespaces use the new ownership names immediately.
