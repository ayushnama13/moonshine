# Streaming TTS Daemon — Implementation Plan

Companion to `ARCH_DESIGN.md` (decisions + rationale). This file is the step-by-step build
sequence. Each phase has a concrete deliverable and a manual verification command — do not
start a phase until the previous one's verification passes.

## Phase 0 — Prerequisites

- [ ] Install PipeWire dev headers. Checked on this machine: `libpipewire-0.3-0` (runtime) is
  installed, `libpipewire-0.3-dev` (headers, needed to compile against) is **not**.
  ```
  sudo apt install libpipewire-0.3-dev
  pkg-config --exists libpipewire-0.3 && echo OK
  ```
- [ ] Confirm build still works before touching anything: existing `moonshine_tts` target
  builds clean (baseline, so any later break is attributable to new code).

## Phase 1 — CMake target, no logic

**File:** `core/moonshine-tts/tools/moonshine-tts-streamd.cpp` — `main()` that just prints
"streamd starting" and returns 0.

**Edit:** `core/moonshine-tts/CMakeLists.txt`, inside the `if(MOONSHINE_TTS_BUILD_ONNX)` block,
alongside the existing `moonshine_tts` target:
```cmake
find_package(PkgConfig REQUIRED)
pkg_check_modules(PIPEWIRE REQUIRED IMPORTED_TARGET libpipewire-0.3)

add_executable(moonshine_tts_streamd tools/moonshine-tts-streamd.cpp)
set_target_properties(moonshine_tts_streamd PROPERTIES OUTPUT_NAME "moonshine-tts-streamd")
target_link_libraries(moonshine_tts_streamd PRIVATE moonshine_tts_ort PkgConfig::PIPEWIRE)
moonshine_tts_set_ort_rpath(moonshine_tts_streamd)
```

**Verify:** builds, `./moonshine-tts-streamd` prints and exits 0.

## Phase 2 — Load the model once, socket skeleton (no audio, no phonemes yet)

**Add to streamd.cpp:**
- Parse `--model-root` / reuse `MoonshineTTSOptions::parse_options` (same as the batch CLI) so
  asset resolution matches.
- Construct `moonshine_tts::MoonshineTTS tts(lang, opt);` and
  `moonshine_tts::MoonshineG2P g2p(lang, opt.g2p_options);` once at startup — this is the
  "pay the model-load cost once" property the whole daemon exists for.
- Unix domain socket at a fixed path, e.g. `/tmp/moonshine-tts-streamd.sock`:
  - `socket(AF_UNIX, SOCK_STREAM, 0)`
  - `unlink()` the path first (stale socket from a previous crashed run), then `bind()`
  - `listen()`, loop `accept()`
  - Per connection: blocking `read()` loop, split on `\n`, print each line to stdout.

**Verify:**
```
./moonshine-tts-streamd &
echo "hello world" | nc -U /tmp/moonshine-tts-streamd.sock
# daemon stdout should print: hello world
```

## Phase 3 — Phoneme FIFO

**Add:**
- `mkfifo(phoneme_fifo_path, 0666)` at startup if it doesn't exist (e.g.
  `/tmp/moonshine-tts-streamd.phonemes`).
- Open the FIFO for writing. Note: `open()` on a FIFO for writing **blocks until a reader
  opens it** — decide whether streamd opens it once at startup (blocks startup until someone
  is listening) or opens/closes it per-utterance (never blocks startup, but a `cat` reader
  attached mid-stream misses earlier phonemes and `write()` fails with `EPIPE`/`SIGPIPE` if no
  reader is attached at write time — must `signal(SIGPIPE, SIG_IGN)` and check `write()`'s
  return value regardless of which mode is chosen).
  **Plan choice:** open once at startup in a background thread so the accept loop isn't
  blocked waiting for a phoneme reader to attach; if no reader is attached, phoneme writes are
  simply dropped (best-effort broadcast, matches the "observability channel" framing from
  `ARCH_DESIGN.md` §3).
- On each received line: call `g2p.text_to_ipa(text)`, write the resulting IPA string +
  `\n` to the FIFO.

**Verify:**
```
./moonshine-tts-streamd &
cat /tmp/moonshine-tts-streamd.phonemes &
echo "hello world" | nc -U /tmp/moonshine-tts-streamd.sock
# cat output should show the IPA string
```

## Phase 4 — Synthesis, dumped to debug WAV files (still no PipeWire)

**Add:**
- After computing `ipa`, call `tts.synthesize_from_phonemes(ipa)` → `std::vector<float>`.
- Write it to `/tmp/moonshine-tts-streamd-debug-<N>.wav` using the existing
  `moonshine_tts::write_wav_mono_pcm16` helper (same one the batch CLI uses) — reuse, don't
  reimplement.

**Verify:** send 2-3 different lines over the socket, confirm each produces a playable,
correct WAV file (`aplay debug-0.wav` or similar) before trusting the PipeWire step to come.
This isolates "is synthesis correct" from "is PipeWire wired correctly" — if PipeWire output
sounds wrong later, this phase's WAVs prove whether the bug is upstream or in the PipeWire
code.

## Phase 5 — Real PipeWire output

**Add:**
- `pw_init()` once at startup.
- Create a `pw_thread_loop` (PipeWire's own thread — do not reuse the socket-reader thread for
  this).
- `pw_stream_new_simple()` in playback mode (`PW_DIRECTION_OUTPUT`), format
  `SPA_AUDIO_FORMAT_F32`, mono, 24000 Hz (matches `MoonshineTTS::kSampleRateHz` — assert this
  equality in code, don't hardcode 24000 separately, so a future model sample-rate change
  can't silently desync the two).
- Ring buffer (single-producer single-consumer, e.g. a fixed-size `std::vector<float>` +
  atomic read/write indices) shared between the synth worker and the `on_process` callback.
- `on_process` callback: `pw_stream_dequeue_buffer()`, copy up to the requested frame count
  from the ring buffer into the PipeWire buffer, `pw_stream_queue_buffer()`. If the ring
  buffer has fewer frames available than requested, zero-fill the remainder (underrun —
  silence, not garbage or a stall).
- Replace the Phase 4 WAV-dump with: push `synthesize_from_phonemes()`'s output into the ring
  buffer instead.

**Verify:** `echo "hello world" | nc -U /tmp/moonshine-tts-streamd.sock` produces audible
speech through the system's default PipeWire sink in real time.

## Phase 6 — Three-thread split, formalized

By this point Phases 2-5 likely already produced three de facto threads (socket accept/read,
main-thread synth-on-receive, PipeWire's own thread). This phase is about making the boundary
explicit and correct, not adding new threads:

- **Thread A (socket reader):** only reads lines and pushes onto a thread-safe queue (mutex +
  condvar, or a lock-free SPSC queue). Never touches `tts`/`g2p` directly.
- **Thread B (synth worker):** owns `tts` and `g2p`. Pops the queue (blocks on condvar when
  empty), does `text_to_ipa` → FIFO write → `synthesize_from_phonemes` → ring buffer push, one
  utterance at a time, loops forever.
- **Thread C (PipeWire callback):** already isolated by Phase 5's design — verify by code
  review that `on_process` contains no calls into `moonshine_tts::*`, only ring-buffer reads.

**Verify:** send several lines back-to-back with no delay
(`printf "one\ntwo\nthree\n" | nc -U /tmp/moonshine-tts-streamd.sock`) — confirm they queue and
play in order without the socket thread blocking on synthesis, and without audio glitches at
the boundaries between utterances.

## Phase 7 — Shutdown handling + test client script

**Add:**
- `SIGINT`/`SIGTERM` handler: stop accepting new connections, let the synth worker drain its
  current queue (or drop pending — decide and document which), stop the PipeWire stream
  cleanly (`pw_stream_destroy`, `pw_thread_loop_stop`), close/unlink the socket path and FIFO
  path, exit 0.
- Small test client, e.g. `scripts/tts-streamd-test-client.sh` or a `.py`: opens the socket,
  sends N lines with a configurable delay between them, prints anything it reads from the
  phoneme FIFO concurrently — simulates a real caller for manual QA and for anyone picking
  this up later without re-deriving the wire protocol from the C++ source.

**Verify:** `Ctrl-C` while audio is mid-playback exits cleanly (no hang, no crash, no leaked
socket/FIFO file on disk for the next run to trip over).

## Out of scope (explicitly, per ARCH_DESIGN.md)

- Sub-utterance incremental synthesis — no such primitive exists in the vocoder API, not
  attempted.
- Multiple simultaneous PipeWire output streams / multi-client audio mixing — one daemon, one
  stream, one sink.
- Phoneme-out over a socket instead of FIFO — deferred unless a real consumer needs
  bidirectional semantics (see ARCH_DESIGN.md §3).
