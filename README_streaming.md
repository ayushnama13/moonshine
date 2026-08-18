# moonshine-tts-streamd

Streaming TTS daemon. Loads the model once, then serves utterances over a
Unix socket, playing audio live via PipeWire and emitting phonemes over a
FIFO for observability.

## Build

```bash
source .venv/bin/activate   # repo-root venv, has a working cmake
cmake -S core -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --target moonshine_tts_streamd -j$(nproc)
```

## Run

Voice assets must exist under `core/moonshine-tts/data/`
(`scripts/fetch-voice-assets.sh tts`). Asset paths resolve relative to cwd,
so launch with cwd set there:

```bash
cd core/moonshine-tts/data
../../../build/moonshine-tts/moonshine-tts-streamd
```

## I/O

| Channel | Transport | Path |
|---|---|---|
| Text in | Unix domain socket | `/tmp/moonshine-tts-streamd.sock` |
| Phonemes out | Named pipe (FIFO) | `/tmp/moonshine-tts-streamd.phonemes` |
| Audio out | PipeWire stream | mono F32 @ 24000 Hz, autoconnects to default sink |

One line of text in = one utterance. Text framing is plain
newline-delimited (no JSON).

## Try it

```bash
python3 scripts/tts-streamd-test-client.py --delay 1.0 "hello there" "second line"
```

Sends lines over the socket and tails the phoneme FIFO concurrently.

### Direct commands (no test client)

Send text straight over the socket with `nc -U` (one line = one utterance;
each utterance is split into sentences internally, so audio for the first
sentence starts before the rest finishes synthesizing):

```bash
printf "Hello there.\n" | nc -U /tmp/moonshine-tts-streamd.sock
printf "First sentence. Second sentence. Third one.\n" | nc -U /tmp/moonshine-tts-streamd.sock
```

Watch the phoneme output live in another terminal (blocks until the daemon
attaches a writer, so start this before or right after sending text):

```bash
cat /tmp/moonshine-tts-streamd.phonemes
```

Cancel what's queued/playing by sending a control line instead of text —
`!stop` and `!flush` are equivalent, both clear the utterance queue and any
audio already sitting in the playback ring buffer:

```bash
printf "!stop\n" | nc -U /tmp/moonshine-tts-streamd.sock
```

A held-open connection can send multiple lines back to back (each still
one utterance):

```bash
nc -U /tmp/moonshine-tts-streamd.sock <<'EOF'
This plays first.
!stop
This plays instead, since the line above never has time to finish.
EOF
```

Note: the very first utterance after startup is slow (kokoro cold-starts
on CPU, tens of seconds); subsequent ones are much faster.

## Shutdown

`Ctrl-C` / `SIGTERM` triggers an orderly shutdown: stops accepting new
connections, drops any queued-but-not-yet-synthesized utterances (finishes
the one already in flight), tears down PipeWire, and unlinks the socket
and FIFO.

## More detail

See `PLAN.md` (phased build history) and `ARCH_DESIGN.md` (design
decisions + rationale) at the repo root.
