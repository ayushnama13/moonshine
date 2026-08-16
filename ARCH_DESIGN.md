# Streaming TTS Daemon — Architecture Design

Status: design phase, no code written yet.

## Goal

Turn the batch `moonshine-tts` CLI (`tools/moonshine-tts-cli.cpp`: text arg → WAV file → exit)
into a persistent daemon: load the model once, then repeatedly accept text, speak it live
through PipeWire, and broadcast the phonemes as it goes.

## New executable

`core/moonshine-tts/tools/moonshine-tts-streamd.cpp`, new CMake target in
`core/moonshine-tts/CMakeLists.txt` (links `moonshine_tts_ort`, `libpipewire-0.3` via
pkg-config). Separate from the batch CLI — different lifecycle (run-once-and-exit vs
run-forever-and-serve).

## Real API being used (this repo, not any other fork)

- `moonshine_tts::MoonshineG2P::text_to_ipa(text)` — text → IPA phoneme string.
- `moonshine_tts::MoonshineTTS::synthesize_from_phonemes(ipa)` — IPA string → `std::vector<float>`
  PCM samples. Per its header doc, it accepts exactly the format `text_to_ipa` produces, so the
  two chain directly with no glue code.
- `MoonshineTTS::kSampleRateHz == 24000` — fixed constant, known at compile time. No need to
  probe/warm-up-synthesize to discover sample rate before opening the PipeWire stream.

## Decisions

### 1. Text-in transport: FIFO vs Unix domain socket

| | FIFO (named pipe) | Unix socket |
|---|---|---|
| Setup | `mkfifo`, one `read()` loop | `socket()+bind()+listen()+accept()` loop |
| Multi-writer safety | Unsafe above `PIPE_BUF` (4KB on Linux) — concurrent writers can interleave mid-message | Each connection gets its own fd; kernel keeps streams separate |
| Connect/disconnect visibility | None — reader just sees EOF when all writers close | `accept()` gives per-client lifecycle |
| Code cost | Low | Higher (accept loop, per-connection buffer) |

**Chosen: Unix socket.** The daemon is long-lived and may serve requests from different
callers/processes over its lifetime. Socket framing removes the FIFO's multi-writer
interleaving risk, which a persistent server shouldn't be exposed to just to save writing an
`accept()` loop.

### 2. Text-in framing: how one utterance ends

| | Plain newline-delimited | NUL-delimited | JSON-lines |
|---|---|---|---|
| Wire format | `text\n` | `text\0` | `{"text":"..."}\n` |
| Parsing cost | None | None | JSON parse (nlohmann already vendored in repo, used by `json-config.cpp`) |
| Manual testing | `echo "hi" \| nc -U sock` | `printf 'hi\0' \| nc -U sock` (less shell-ergonomic) | `echo '{"text":"hi"}' \| nc -U sock` |
| Embedded literal `\n` in the utterance | **Breaks** — truncates at first newline | Safe | Safe (JSON escapes `\n`) |
| Extensibility (future per-utterance voice/speed params) | None — needs a protocol version bump | None | Free — add JSON keys, ignored by old parsers |

**Chosen: plain newline-delimited.** Simplest possible framing, zero parsing dependency,
trivially testable with `echo`/`nc`. Trade-off accepted: a literal newline inside one utterance
truncates it — acceptable since input is expected to be single-line sentences, not
multi-paragraph text.

### 3. Phoneme-out transport: FIFO vs Unix socket

| | FIFO | Unix socket |
|---|---|---|
| Direction | One writer (daemon) → any number of readers can `cat`/`tail -f` while open (no buffering/replay) | Daemon would need to track a list of subscriber connections and write to each |
| Use case fit | Broadcast/observability only (captions, lip-sync, debug) — no consumer needs to talk back | Only useful for bidirectional interaction, which this channel doesn't need |
| Code cost | Low — one `open()` + `write()` | Higher — mirrors the text-in accept loop for no functional gain |

**Chosen: named pipe.** One-directional broadcast metadata. The asymmetry with text-in
(socket) is intentional: text-in needs connection semantics because it drives work,
phoneme-out doesn't because it's pure observability output.

### 4. PCM format to PipeWire: F32 vs S16

| | F32 mono @ 24kHz | S16 mono @ 24kHz |
|---|---|---|
| Conversion needed | None — `synthesize_from_phonemes()` already returns `std::vector<float>` | float→int16 conversion (scale/clamp/round) on every buffer |
| Precision | Full float precision from the vocoder | Slight quantization loss (inaudible for speech at 16-bit, but still an added step) |
| PipeWire support | Native `SPA_AUDIO_FORMAT_F32` | Native `SPA_AUDIO_FORMAT_S16` |

**Chosen: F32 mono @ 24kHz.** Matches the engine's native output type and
`MoonshineTTS::kSampleRateHz` exactly — zero conversion code, removes an entire class of
clipping/rounding bugs for no downside, since PipeWire accepts float natively.

### 5. Threading model

| Thread | Job | Must NOT do |
|---|---|---|
| Socket reader | blocking `read()` per connection, push lines to a thread-safe queue | any G2P/synth work |
| Synth worker | pop queue → `text_to_ipa()` → write phoneme FIFO → `synthesize_from_phonemes()` → push floats to ring buffer | block on socket I/O |
| PipeWire `on_process` callback (PipeWire's own thread) | drain ring buffer into the buffer PipeWire hands it, emit silence on underrun | call into G2P/synth, or any blocking call — this thread is real-time-sensitive |

Rationale: PipeWire drives its callback on its own schedule, independent of when text arrives
or how long synthesis takes. Decoupling via queue + ring buffer lets "receive text," "compute
audio," and "play audio" proceed at three different paces without one blocking another.

### 6. Utterance granularity

One socket message (one line) = one utterance, synthesized and played whole. No sub-utterance
incremental synthesis exists anywhere in this codebase — `synthesize()` and
`synthesize_from_phonemes()` are always whole-buffer-in, whole-buffer-out. Utterance-level
pipelining (queue + worker thread, above) is the finest granularity available, not a
compromise.

## Per-utterance pipeline

```
text line (socket)
  → MoonshineG2P::text_to_ipa(text)          → IPA string  → write to phoneme FIFO immediately
  → MoonshineTTS::synthesize_from_phonemes()  → vector<float> PCM  → push to ring buffer → PipeWire
```

## Build order

1. CMake target + `libpipewire-0.3-dev` linked via pkg-config, no logic yet.
2. Skeleton: load `MoonshineTTS` + `MoonshineG2P` once, accept socket connections, print
   received lines. No audio.
3. Wire the phoneme FIFO: `text_to_ipa()` → write → test with `cat`.
4. Call `synthesize_from_phonemes()`, dump to numbered debug `.wav` files (reuse
   `write_wav_mono_pcm16` from the batch CLI) to confirm correctness before touching PipeWire.
5. Real PipeWire stream + ring buffer + `on_process` callback — first actual sound out.
6. Split into the three-thread model, handle backpressure/underrun.
7. Shutdown handling (SIGINT/SIGTERM, drain, close stream cleanly) + a small test client
   script that fires several lines with delays to simulate live use.
