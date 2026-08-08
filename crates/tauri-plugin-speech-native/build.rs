const COMMANDS: &[&str] = &[
    "speech_status",
    "speech_plan_transcription",
    "speech_plan_synthesis",
    "speech_synthesize",
    "speech_synthesize_stream",
    "speech_transcribe",
    "speech_transcribe_stream",
    "speech_transcription_audio_push",
    "speech_transcription_audio_finish",
    "speech_cancel",
];

fn main() {
    if let Err(error) = tauri_plugin::Builder::new(COMMANDS).try_build() {
        panic!("failed to build speech-native Tauri plugin metadata: {error}");
    }
}
