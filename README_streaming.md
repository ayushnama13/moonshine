# moonshine-tts-streamd

Streaming TTS daemon. Loads the model once at startup, then serves
utterances over a Unix socket — playing audio live through PipeWire and
emitting phonemes on a FIFO for observability.

Companion to the batch `moonshine-tts` CLI, which pays the model-load cost
on every invocation.

## Build

```bash
source .venv/bin/activate   # repo-root venv, has a working cmake
cmake -S core -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --target moonshine_tts_streamd -j$(nproc)
```

## Run

Voice assets must exist under `core/moonshine-tts/data/`
(`scripts/fetch-voice-assets.sh tts`, ~2.4G). Asset paths resolve relative
to the working directory, so launch with cwd set there:

```bash
cd core/moonshine-tts/data
../../../build/moonshine-tts/moonshine-tts-streamd
```

Running from anywhere else fails at startup with
`cannot open .../kokoro/config.json`, even with the assets present.

Startup does a one-time warm-up synthesis before it begins listening, so
expect a few seconds of delay and a log line saying so. This is
deliberate — see [Performance](#performance).

Ready when you see:

```
streamd: warm-up done in 3719 ms (34200 samples discarded)
streamd: listening on /tmp/moonshine-tts-streamd.sock
streamd: PipeWire stream state: paused -> streaming
```

## I/O

| Channel | Transport | Path |
|---|---|---|
| Text in | Unix domain socket | `/tmp/moonshine-tts-streamd.sock` |
| Phonemes out | Named pipe (FIFO) | `/tmp/moonshine-tts-streamd.phonemes` |
| Audio out | PipeWire stream | mono F32 @ 24000 Hz, autoconnects to default sink |

One line of text in = one utterance. Framing is plain newline-delimited
text, no JSON. Lines beginning with `!` are control commands rather than
speech (see below).

The phoneme FIFO is strictly best-effort: with no reader attached, lines
are dropped silently and synthesis proceeds normally. Nothing the daemon
does depends on someone listening to it.

## Try it

```bash
python3 scripts/tts-streamd-test-client.py --delay 1.0 "hello there" "second line"
```

Sends lines over the socket and tails the phoneme FIFO concurrently.

### Direct commands (no test client)

Send text straight over the socket with `nc -U`:

```bash
printf "Hello there.\n" | nc -U /tmp/moonshine-tts-streamd.sock
printf "First sentence. Second sentence. Third one.\n" | nc -U /tmp/moonshine-tts-streamd.sock
```

Watch phonemes live in another terminal (`cat` blocks until the daemon has
something to write, which is normal):

```bash
cat /tmp/moonshine-tts-streamd.phonemes
```

A held-open connection can send many lines back to back:

```bash
nc -U /tmp/moonshine-tts-streamd.sock <<'EOF'
This is the first utterance.
This is the second.
EOF
```

## Barge-in (`!stop` / `!flush`)

Send `!stop` (or `!flush`, identical) to cancel immediately:

```bash
printf "!stop\n" | nc -U /tmp/moonshine-tts-streamd.sock
```

It clears the pending utterance queue, discards audio already buffered for
playback, and abandons the utterance being synthesized — including any of
its sentences not yet reached. Sound stops within one PipeWire callback.

Cancellation is tracked by a generation counter rather than by unwinding
work: `!stop` bumps the generation, and the synth worker checks it before
G2P, before synthesis, and while pushing audio, dropping anything that
belongs to a superseded generation.

```
This is a long sentence... Here is a second. And a third.
  [sentence] This is a long sentence...
  -> ðɪs ɪz ə lˈɔŋ sˈɛntəns
streamd: flush/stop requested (gen=1)
  [cancelled during playback push]
```

Sentences two and three are never synthesized at all.

## Streaming behavior

Each utterance is split into sentences and synthesized one at a time, so
audio begins after the *first* sentence rather than after the whole
paragraph. On a multi-sentence input this is the difference between
speaking almost immediately and waiting for everything to finish.

Internally there are three threads: the accept loop only reads socket lines
and hands them off; a synth worker owns the model and does G2P → phonemes →
synthesis; and PipeWire's own realtime thread drains a lock-free ring
buffer to the sink. The accept loop never blocks on synthesis, so a client
can keep sending (or send `!stop`) while a backlog is being worked through.

## Performance

Measured on an AMD Ryzen 5 2500U laptop, CPU-only, 24 kHz mono:

| | |
|---|---|
| Warm-up at startup | ~3.7 s |
| 2.9 s of audio | ~7.6 s to synthesize |

That's roughly **2.7× slower than realtime**, which is why the daemon
pre-warms and splits by sentence: neither is cosmetic. ONNX Runtime defers
graph optimization and kernel setup to the first inference, so without the
warm-up the first real utterance would be dramatically slower than every
later one and the daemon would look hung. Anything needing true realtime
wants a smaller model or a GPU execution provider.

## Diagnostics

Stream state transitions are logged, so a failed negotiation (no sink, or a
rejected format) is visible rather than presenting as silence:

```
streamd: PipeWire stream state: unconnected -> connecting
streamd: PipeWire stream state: connecting -> paused
streamd: PipeWire stream state: paused -> streaming
```

If it never reaches `streaming`, check that a sink exists with
`wpctl status`. stdout is line-buffered, so logs appear live even when
redirected to a file or pipe.

## Shutdown

`Ctrl-C` / `SIGTERM` shuts down in an orderly way: stops accepting
connections, drops queued utterances (finishing only the one in flight),
joins the synth worker before tearing down the model, closes the PipeWire
stream, and unlinks both the socket and the FIFO. Dropping rather than
draining is deliberate — a signal handler should be prompt and bounded, not
gated on however much work happens to be queued.

## Known limitations

- **One client at a time.** A client that connects and sends nothing blocks
  others until it disconnects.
- **World-accessible endpoints.** The socket and FIFO live in `/tmp` with
  permissive modes, so any local user can inject speech or read the phoneme
  stream. `XDG_RUNTIME_DIR` at mode `0600` would be the fix.
- **No configuration.** Language (`en_us`), both paths, and the asset root
  are hardcoded; there are no command-line flags.
- **Sentence splitting is naive.** Abbreviations like "Dr." or "e.g." split
  a sentence early, audible as an unnatural pause. Decimals are handled.
- **Media role is `Music`**, not `Speech`, so it won't duck or route as
  voice on desktops that distinguish them.

## More detail

See `PLAN.md` (phased build history, including the code-review fix rounds)
and `ARCH_DESIGN.md` (design decisions + rationale) at the repo root.
