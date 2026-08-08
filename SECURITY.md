# Security

## Authority model

Core speech crates accept caller-owned audio or text and return caller-owned
transcripts or audio. They do not activate microphones, play audio, persist
content, expose network listeners, or resolve hosted credentials.

Platform discovery must remain noninteractive. A capability is eligible for a
local-only route only when current runtime evidence proves that it is available
and never uses the network.

The Tauri plugin exposes no commands unless the embedding application grants a
matching capability. Its default permission allows status inspection only.

## Reporting

Report vulnerabilities privately to the repository owner. Include the affected
crate, platform, and the narrowest reproducible input. Do not include private
audio or transcripts in a public issue.
