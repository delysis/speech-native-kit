# Extraction provenance

The initial implementation was extracted from
`delysis/free-token-energy` at commit
`b32653ed2e076b37d1c4c5a2d2aab209f88803eb` on 2026-08-08.

The source family was already dependency-independent there. Extraction renamed
Rust packages and the Tauri namespace, narrowed default permissions, and
clarified ownership; the serialized `fte.speech.*` receipt schemas remain
readable for compatibility. The original commits remain permanently available
in the Free Token Energy repository.

Real runtime evidence produced before extraction remains historical evidence,
not an automatic readiness promotion for later builds. Each release must rerun
its platform and real-audio proofs.
