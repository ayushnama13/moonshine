// Streaming daemon: reads utterance lines from a Unix domain socket, drives
// MoonshineTTS + MoonshineG2P (loaded once), synthesizes PCM, and plays it
// out through PipeWire in real time. Also writes phonemes to a named pipe
// (FIFO) as a debug/observability side channel.
// Phase 3: phoneme FIFO added — each received line is now run through G2P
// and the resulting IPA string is broadcast to whoever is reading the FIFO.
// Phase 4: each utterance was also synthesized to PCM and dumped to a
// numbered debug WAV file, to verify synthesis correctness in isolation
// before wiring up real audio output.
// Phase 5: real PipeWire output. The Phase 4 WAV dump is replaced with a
// lock-free single-producer/single-consumer ring buffer, drained into the
// audio device by PipeWire's own realtime thread (`on_process`).
// Phase 6: the three-thread split is now explicit and enforced by which
// function touches what — Thread A (accept loop, `main`) only reads lines
// off the socket and pushes them onto a queue; Thread B (`run_synth_worker`)
// owns `tts`/`g2p` exclusively, pops the queue, and does G2P + FIFO write +
// synthesis + ring-buffer push, one utterance at a time; Thread C is
// PipeWire's own thread running `on_process`, unchanged since Phase 5. This
// means Thread A never blocks on synthesis — it can keep accepting/reading
// while Thread B works through a backlog.
// Phase 7: SIGINT/SIGTERM now trigger orderly shutdown instead of an abrupt
// kill. Policy (decided here, per PLAN.md's "decide and document which"):
// DROP pending work, don't drain it. The utterance queue is not waited on —
// on shutdown we stop accepting new connections, tear down PipeWire and the
// socket/FIFO files, and exit. (Phase 7 originally left Thread B detached
// too; fix (1) below makes it joined, which does not change this policy —
// see there.) Draining would mean blocking process exit on however much
// synthesis work happens to be queued, which fights the whole point of a
// signal handler (bounded, prompt shutdown); a caller that cares about
// in-flight utterances should wait for a response before sending more.
// Phase 7a: four correctness fixes on top of the above.
//   (1) Thread B is now *joined*, not detached. It previously held
//       references to `tts`/`g2p`, which are locals in main() — returning
//       from main() destroyed them (tearing down the ONNX sessions) while
//       Thread B could still be mid-synthesis, a use-after-free that would
//       typically crash on Ctrl-C during playback. The DROP-don't-drain
//       policy above is unchanged: the worker abandons whatever is still
//       queued, it just finishes the one utterance already in flight so
//       teardown can't race it.
//   (2) RingBuffer::push now reports how many frames it actually took, and
//       Thread B retries the remainder instead of silently discarding it.
//       Dropping is the right policy for the realtime consumer, but Thread
//       B is not time-critical, and the old behaviour truncated any
//       utterance longer than the ring buffer (10s) with no diagnostic.
//   (3) read_lines() now aborts on EINTR when shutdown was requested.
//       It previously treated every EINTR as "retry", so a signal arriving
//       while a client was connected was swallowed and shutdown was
//       deferred until that client happened to disconnect.
//   (4) The phoneme FIFO fd is switched to non-blocking after the reader
//       attaches. write() to a full pipe (a reader that attached but stopped
//       reading) would otherwise block Thread B indefinitely, which
//       contradicts this channel's stated best-effort contract.

// POSIX headers (raw OS syscalls, not C++ standard library). Unix domain
// sockets use the same API shape as TCP sockets (socket/bind/listen/accept)
// but address family AF_UNIX instead of AF_INET, so they talk over a
// filesystem path instead of an IP+port — local-only, no network stack cost.
#include <fcntl.h>       // open(), O_WRONLY — used to open the phoneme FIFO
#include <sys/socket.h>  // socket(), bind(), listen(), accept()
#include <sys/stat.h>    // mkfifo()
#include <sys/un.h>      // sockaddr_un — the Unix-domain-socket address struct
#include <unistd.h>      // read(), write(), close(), unlink()

#include <algorithm>            // std::min, std::fill — ring buffer + underrun silence-fill
#include <atomic>               // std::atomic — safe cross-thread fd/index handoff
#include <cerrno>               // errno — set by syscalls on failure
#include <chrono>               // std::chrono — backoff while the ring buffer drains
#include <condition_variable>   // std::condition_variable — utterance queue wakeup
#include <csignal>              // signal(), SIGPIPE, SIG_IGN
#include <cstdio>               // std::printf
#include <cstring>              // std::strerror(), std::strncpy()
#include <functional>           // std::function — type-erased callback wrapper
#include <iostream>             // std::cout / std::cerr
#include <mutex>                // std::mutex, std::lock_guard, std::unique_lock
#include <optional>             // std::optional<T> — lets us delay construction
#include <queue>                // std::queue — Thread A -> Thread B utterance handoff
#include <string>
#include <thread>  // std::thread — background FIFO-opening thread, synth worker
#include <vector>

// PipeWire + SPA (Simple Plugin API, PipeWire's underlying format/pod
// negotiation library). spa/param/audio/format-utils.h pulls in the pod
// builder and spa_audio_info_raw used to describe "mono F32 @ N Hz" to
// PipeWire when connecting the stream.
#include <pipewire/pipewire.h>
#include <spa/param/audio/format-utils.h>

#include "moonshine-g2p.h"  // MoonshineG2P: text -> IPA phonemes
#include "moonshine-tts.h"  // MoonshineTTS: text/phonemes -> waveform

namespace {

// Fixed, well-known filesystem paths. Both this daemon and any client (e.g.
// `nc -U` for text-in, `cat` for phoneme-out) hardcode the same paths to
// find each other.
constexpr const char* kSocketPath = "/tmp/moonshine-tts-streamd.sock";
constexpr const char* kPhonemeFifoPath = "/tmp/moonshine-tts-streamd.phonemes";

// Holds the FIFO's write-end file descriptor once a reader has attached, or
// -1 if no reader is attached yet (or ever). Written once by the background
// thread below, read by the main/accept-loop thread on every utterance —
// std::atomic makes that cross-thread handoff race-free without a mutex.
std::atomic<int> g_phoneme_fifo_fd{-1};

// Phase 7 shutdown. `sig_atomic_t` (not `std::atomic<bool>`) is the type
// POSIX actually guarantees is safe to write from inside a signal handler.
// `g_listen_fd` is set once in main() before the signal handler is
// installed, then read (never written) by the handler — safe without
// synchronization since the handler only runs after that one-time set.
volatile sig_atomic_t g_shutdown_requested = 0;
int g_listen_fd = -1;

// Fix (1)/(2)/(3): the same "we are shutting down" fact, in a form the
// worker threads can wait on. `g_shutdown_requested` above is written from
// the signal handler and must stay `sig_atomic_t`; this one is written once
// by main() *after* the accept loop exits, and is what Thread B polls (and
// what the queue condvar's predicate tests) so the worker can wind down and
// be joined rather than being torn down mid-flight by process exit.
std::atomic<bool> g_shutting_down{false};

// Signal handler: async-signal-safe only (no printf/malloc/iostream). Sets
// the flag the accept loop polls, then shutdown()s the listening socket to
// unblock a currently-blocked accept() call — shutdown() is POSIX
// async-signal-safe. Installed with SA_RESTART *not* set (see main()), so
// accept() returns EINTR here rather than transparently retrying.
void handle_shutdown_signal(int) {
  g_shutdown_requested = 1;
  if (g_listen_fd >= 0) {
    shutdown(g_listen_fd, SHUT_RDWR);
  }
}

/// Lock-free single-producer/single-consumer ring buffer of audio frames
/// (mono float samples). Producer is the accept-loop/synth thread (`push`,
/// called once per received utterance); consumer is PipeWire's own realtime
/// thread (`pop`, called from `on_process` every time the audio device wants
/// more frames). `write_`/`read_` are monotonically increasing frame counts
/// (never wrapped) — the actual storage index is always `count % buf_.size()`
/// — so "how much is available" is just subtraction, no explicit wrap-around
/// bookkeeping needed. Safe without a mutex specifically *because* there is
/// exactly one writer and one reader: each side only ever writes its own
/// atomic and only ever reads the other's.
class RingBuffer {
 public:
  explicit RingBuffer(size_t capacity_frames) : buf_(capacity_frames) {}

  // Producer-only. Copies as much of [data, data+count) as currently fits
  // and returns how many frames were actually taken (may be less than
  // *count*, including 0). This call itself never blocks — the audio device
  // must never be able to stall the producer inside this function. Deciding
  // what to do with an unaccepted tail is the caller's job; see
  // push_all_blocking() below, which is what Thread B uses.
  size_t push(const float* data, size_t count) {
    const size_t w = write_.load(std::memory_order_relaxed);
    const size_t r = read_.load(std::memory_order_acquire);
    const size_t free_frames = buf_.size() - (w - r);
    const size_t n = std::min(count, free_frames);
    for (size_t i = 0; i < n; ++i) {
      buf_[(w + i) % buf_.size()] = data[i];
    }
    write_.store(w + n, std::memory_order_release);
    return n;
  }

  // Consumer-only. Copies up to *count* frames into *dst*, returns how many
  // were actually available (may be less than *count*, including 0). Caller
  // is responsible for filling any shortfall — see on_process() below.
  size_t pop(float* dst, size_t count) {
    const size_t w = write_.load(std::memory_order_acquire);
    const size_t r = read_.load(std::memory_order_relaxed);
    const size_t avail = w - r;
    const size_t n = std::min(count, avail);
    for (size_t i = 0; i < n; ++i) {
      dst[i] = buf_[(r + i) % buf_.size()];
    }
    read_.store(r + n, std::memory_order_release);
    return n;
  }

 private:
  std::vector<float> buf_;
  std::atomic<size_t> write_{0};
  std::atomic<size_t> read_{0};
};

// 10 seconds of buffered audio at the TTS engine's native sample rate —
// generous headroom so a burst of several utterances queued back-to-back
// doesn't overflow (which would silently drop audio) before PipeWire's
// realtime thread drains it.
RingBuffer g_ring_buffer(static_cast<size_t>(moonshine_tts::MoonshineTTS::kSampleRateHz) * 10);

// Fix (2). Pushes *all* of [data, data+count) into the ring buffer, waiting
// for the realtime consumer to make room whenever it fills up. Called only
// from Thread B, which is explicitly *not* time-critical — so unlike the
// realtime thread, it can afford to wait rather than discard. Without this,
// any utterance longer than the ring buffer's 10s capacity had its tail
// silently truncated.
//
// The wait is a plain sleep rather than a condvar because the other side is
// PipeWire's realtime callback: signalling a condvar from `on_process` would
// mean taking a lock there, which is exactly what PW_STREAM_FLAG_RT_PROCESS
// forbids. 10ms is far below the buffer's 10s capacity, so the polling costs
// nothing and only ever runs at all for unusually long utterances.
//
// Returns the number of frames actually written, which is < *count* only if
// shutdown was requested partway through — the wait loop always yields to
// shutdown so a stopped/stalled audio device can't wedge the join in main().
size_t push_all_blocking(const float* data, size_t count) {
  size_t written = 0;
  while (written < count) {
    written += g_ring_buffer.push(data + written, count - written);
    if (written == count) {
      break;
    }
    if (g_shutting_down.load()) {
      break;  // abandon the rest; we're dropping pending work anyway
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return written;
}

// The connected playback stream, set once in main() right after creation
// and read by on_process() (which runs on PipeWire's own thread, not
// main()'s) — never reassigned afterward, so no synchronization is needed
// beyond it being set before pw_thread_loop_start() lets on_process() run
// at all.
pw_stream* g_pw_stream = nullptr;

/// PipeWire calls this on its own realtime thread whenever the audio device
/// wants more frames. Must stay allocation-free and non-blocking (per
/// PW_STREAM_FLAG_RT_PROCESS) — it only ever touches the ring buffer and the
/// buffer PipeWire hands it, never `tts`/`g2p`/sockets/FIFO.
void on_process(void*) {
  pw_buffer* b = pw_stream_dequeue_buffer(g_pw_stream);
  if (!b) {
    return;  // no buffer ready right now — nothing to do this callback
  }
  spa_buffer* buf = b->buffer;
  float* dst = static_cast<float*>(buf->datas[0].data);
  if (!dst) {
    pw_stream_queue_buffer(g_pw_stream, b);
    return;
  }

  constexpr uint32_t kStride = sizeof(float);  // mono F32: 1 sample = 1 frame
  const uint32_t n_frames = buf->datas[0].maxsize / kStride;

  const size_t got = g_ring_buffer.pop(dst, n_frames);
  if (got < n_frames) {
    // Underrun — ring buffer didn't have enough queued (e.g. no utterance
    // synthesized recently). Silence, not garbage or a stall.
    std::fill(dst + got, dst + n_frames, 0.0f);
  }

  buf->datas[0].chunk->offset = 0;
  buf->datas[0].chunk->stride = static_cast<int32_t>(kStride);
  buf->datas[0].chunk->size = n_frames * kStride;
  pw_stream_queue_buffer(g_pw_stream, b);
}

// All fields explicit (rather than letting later ones default-initialize)
// because this project builds with -Werror=missing-field-initializers.
constexpr pw_stream_events kStreamEvents = {
    .version = PW_VERSION_STREAM_EVENTS,
    .destroy = nullptr,
    .state_changed = nullptr,
    .control_info = nullptr,
    .io_changed = nullptr,
    .param_changed = nullptr,
    .add_buffer = nullptr,
    .remove_buffer = nullptr,
    .process = on_process,
    .drained = nullptr,
    .command = nullptr,
    .trigger_done = nullptr,
};

/// Runs on a background thread, once, at startup. open()-ing a FIFO for
/// writing BLOCKS until some process opens the other end for reading — if
/// this ran on the main thread, the daemon would hang at startup until a
/// phoneme-reader showed up, which would make the socket (text-in) side
/// unusable in the meantime. Running it on its own thread means the daemon
/// starts accepting text immediately; phoneme output silently begins
/// working the moment a reader attaches, whenever that happens.
void open_phoneme_fifo_in_background() {
  // O_WRONLY: this process only ever writes to the FIFO, never reads.
  // This call blocks here until a reader opens the other end.
  int fd = open(kPhonemeFifoPath, O_WRONLY);
  if (fd < 0) {
    std::cerr << "streamd: failed to open phoneme FIFO for writing: "
              << std::strerror(errno) << '\n';
    return;
  }
  // Fix (4). The open() above is deliberately blocking — that's the whole
  // handshake, and the whole reason this runs on its own thread. Subsequent
  // *writes* must not be: a reader that attaches and then stops draining
  // fills the pipe (64K by default on Linux) and every later write() would
  // block Thread B — i.e. stall synthesis — on what is supposed to be a
  // best-effort debug channel. Flip the fd to non-blocking now that the
  // handshake is done, so a full pipe surfaces as EAGAIN and gets dropped.
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
    std::cerr << "streamd: failed to set phoneme FIFO non-blocking: " << std::strerror(errno)
              << " (continuing; writes may block if the reader stalls)\n";
  }
  g_phoneme_fifo_fd.store(fd);
  std::printf("streamd: phoneme FIFO reader attached\n");
}

/// Writes *ipa* + a trailing newline to the phoneme FIFO, if (and only if)
/// a reader is currently attached. This is deliberately best-effort: the
/// phoneme stream is an observability/debug channel, not a channel synthesis
/// depends on, so a missing reader must never block or fail an utterance —
/// it should just silently drop the write.
void broadcast_phoneme_line(const std::string& ipa) {
  int fd = g_phoneme_fifo_fd.load();
  if (fd < 0) {
    return;  // no reader attached (yet, or ever) — drop silently
  }
  std::string line = ipa + '\n';
  // write() to a FIFO with no reader raises SIGPIPE and/or returns -1 with
  // errno==EPIPE (e.g. if the one-and-only reader disconnected after we
  // opened the fd). SIGPIPE is ignored at startup (see main()), so we only
  // need to handle the EPIPE return value here, not a program-terminating
  // signal.
  ssize_t n = write(fd, line.data(), line.size());
  if (n < 0) {
    // EAGAIN/EWOULDBLOCK: the fd is non-blocking (see
    // open_phoneme_fifo_in_background) and the pipe is full because the
    // reader isn't keeping up. That's a dropped phoneme line, not a broken
    // channel — keep the fd and try again on the next utterance.
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return;
    }
    std::cerr << "streamd: phoneme FIFO write failed: " << std::strerror(errno) << '\n';
    // Reader vanished; stop broadcasting until (if) this daemon restarts.
    // (A more complete implementation would re-open and wait for a new
    // reader; out of scope for this phase per PLAN.md.)
    g_phoneme_fifo_fd.store(-1);
  }
}

// Thread A (accept loop) -> Thread B (synth worker) handoff. A plain
// mutex+condvar queue, not a lock-free structure like RingBuffer: utterance
// lines arrive at human-typing/speaking rates, nowhere near the audio ring
// buffer's per-callback frequency, so lock contention here is a non-issue —
// using a mutex keeps this simple and correct instead of chasing
// performance nothing here needs.
std::mutex g_utterance_queue_mutex;
std::condition_variable g_utterance_queue_cv;
std::queue<std::string> g_utterance_queue;

// Called from Thread A only (the accept-loop's read_lines callback). Thread
// A's entire job is this: get the line off the socket and hand it off —
// nothing here touches tts/g2p.
void enqueue_utterance(std::string line) {
  {
    std::lock_guard<std::mutex> lock(g_utterance_queue_mutex);
    g_utterance_queue.push(std::move(line));
  }
  g_utterance_queue_cv.notify_one();
}

/// Thread B. Started once in main() after tts/g2p are loaded, and — as of
/// fix (1) — *joined* by main() during shutdown rather than detached. Owns
/// tts/g2p exclusively: Thread A never touches them, so no locking is
/// needed around the G2P/synthesis calls themselves, only around the queue
/// that feeds this loop.
///
/// The join matters because `tts`/`g2p` are locals in main(): if this thread
/// were still running when main() returned, their destructors would tear
/// down the ONNX sessions out from under an in-flight
/// `synthesize_from_phonemes()` call. Joining also keeps the DROP-not-drain
/// policy intact — the loop below exits as soon as it sees the shutdown
/// flag, abandoning anything still queued, so the join is bounded by the
/// single utterance already being synthesized, not by the backlog.
void run_synth_worker(moonshine_tts::MoonshineG2P& g2p, moonshine_tts::MoonshineTTS& tts) {
  for (;;) {
    std::string line;
    {
      std::unique_lock<std::mutex> lock(g_utterance_queue_mutex);
      // Wake on either a new utterance or shutdown, so an idle worker
      // parked here doesn't keep main() blocked in join() forever.
      g_utterance_queue_cv.wait(
          lock, [] { return !g_utterance_queue.empty() || g_shutting_down.load(); });
      if (g_shutting_down.load()) {
        return;  // drop whatever is still queued (see policy note in header)
      }
      line = std::move(g_utterance_queue.front());
      g_utterance_queue.pop();
    }

    std::cout << line << '\n';
    const std::string ipa = g2p.text_to_ipa(line);
    std::cout << "  -> " << ipa << '\n';
    broadcast_phoneme_line(ipa);

    const std::vector<float> pcm = tts.synthesize_from_phonemes(ipa);
    // Fix (2): push the whole thing, waiting out the realtime consumer if
    // this utterance is longer than the ring buffer, instead of letting the
    // tail be discarded without a word.
    const size_t queued = push_all_blocking(pcm.data(), pcm.size());
    std::cout << "  -> queued " << queued << " samples ("
              << moonshine_tts::MoonshineTTS::kSampleRateHz << " Hz) for playback\n";
    if (queued < pcm.size()) {
      // Only reachable on shutdown — push_all_blocking otherwise waits.
      std::cerr << "streamd: dropped " << (pcm.size() - queued)
                << " samples of audio (shutting down)\n";
    }
  }
}

/// Reads newline-delimited lines from a connected socket fd, invoking
/// *on_line* for each complete line (delimiter stripped). Returns when the
/// peer closes the connection or a read error occurs.
///
/// Takes the callback as a parameter (rather than hardcoding what happens
/// per line) so this function only knows "how bytes become lines" — the
/// caller decides what a line means.
void read_lines(int conn_fd, const std::function<void(const std::string&)>& on_line) {
  // Accumulates raw bytes across multiple read() calls. Stream sockets give
  // no guarantee that one read() == one line: a line can arrive split across
  // reads, or several lines can land in a single read() — buf absorbs both
  // cases.
  std::string buf;
  char chunk[4096];  // fixed scratch buffer read() fills each call
  for (;;) {
    ssize_t n = read(conn_fd, chunk, sizeof(chunk));
    if (n < 0) {
      // EINTR = the call was interrupted by a signal, not a real error —
      // normally just retry. Anything else is a genuine failure.
      if (errno == EINTR) {
        // Fix (3). If that signal was our shutdown signal, retrying would
        // park us back in a blocking read() and the accept loop would never
        // get to see the shutdown flag — Ctrl-C would appear to hang for as
        // long as this client stayed connected. Bail out instead; the
        // caller closes the fd and the accept loop then notices the flag.
        if (g_shutdown_requested) {
          return;
        }
        continue;
      }
      std::cerr << "streamd: read error: " << std::strerror(errno) << '\n';
      return;
    }
    if (n == 0) {
      return;  // peer closed the connection (EOF) — normal disconnect
    }
    buf.append(chunk, static_cast<size_t>(n));
    // Drain every complete line currently sitting in buf. Handles both
    // "line arrived in pieces" (loop just exits, waits for more bytes on
    // the next read()) and "multiple lines arrived in one read()" (loop
    // fires the callback more than once).
    size_t pos;
    while ((pos = buf.find('\n')) != std::string::npos) {
      std::string line = buf.substr(0, pos);
      buf.erase(0, pos + 1);  // remove the line AND its trailing '\n'
      if (!line.empty()) {
        on_line(line);
      }
    }
  }
}

}  // namespace

int main() {
  using moonshine_tts::MoonshineG2P;
  using moonshine_tts::MoonshineTTS;
  using moonshine_tts::MoonshineTTSOptions;

  std::printf("streamd starting\n");

  // A FIFO write() with no reader attached raises SIGPIPE by default, whose
  // default disposition is to terminate the whole process — not what we
  // want for a best-effort broadcast channel. Ignoring it means the write()
  // syscall instead just returns -1 with errno==EPIPE, which
  // broadcast_phoneme_line() already handles explicitly.
  signal(SIGPIPE, SIG_IGN);

  // Load the model + G2P once at startup — this is the whole point of the
  // daemon vs. the batch CLI, which pays this cost per invocation.
  MoonshineTTSOptions opt;
  const std::string lang = "en_us";
  // MoonshineTTS/MoonshineG2P have no default constructor — they must be
  // built with (lang, opt) immediately. std::optional lets us declare the
  // variable empty first, then .emplace(...) construct it in-place inside
  // the try block below, so a construction failure (e.g. missing voice
  // asset files on disk) throws an exception we can catch cleanly instead
  // of the whole process aborting with an uncaught-exception core dump.
  std::optional<MoonshineTTS> tts;
  std::optional<MoonshineG2P> g2p;
  try {
    tts.emplace(lang, opt);
    g2p.emplace(lang, opt.g2p_options);
  } catch (const std::exception& e) {
    std::cerr << "streamd: model load failed: " << e.what() << '\n';
    return 1;
  }
  std::printf("streamd: model loaded (lang=%s)\n", lang.c_str());

  // Phase 6, Thread B: the synth worker owns tts/g2p from here on. Unlike
  // the FIFO thread this one is kept joinable — fix (1): it borrows `tts`
  // and `g2p`, which are locals in this function, so it must be shut down
  // and joined before this function returns and destroys them.
  std::thread synth_worker(run_synth_worker, std::ref(*g2p), std::ref(*tts));

  // Signals Thread B to stop and waits for it. Idempotent, so the normal
  // shutdown path and the early-failure paths below can both just call it.
  // Every `return` after this point must go through it: destroying a
  // still-joinable std::thread calls std::terminate(), and returning while
  // it still runs is the very use-after-free fix (1) is about.
  auto stop_synth_worker = [&synth_worker] {
    g_shutting_down.store(true);
    g_utterance_queue_cv.notify_all();
    if (synth_worker.joinable()) {
      synth_worker.join();
    }
  };

  // Create the phoneme FIFO on disk (a special file — not a regular file —
  // that connects a writer and a reader like a pipe). mkfifo() fails with
  // EEXIST if a previous run already created it; that's fine, we just want
  // it to exist, so we ignore that specific failure rather than checking
  // the return value strictly.
  if (mkfifo(kPhonemeFifoPath, 0666) < 0 && errno != EEXIST) {
    std::cerr << "streamd: mkfifo() failed: " << std::strerror(errno) << '\n';
    stop_synth_worker();
    return 1;
  }
  // Hand this off to a background thread (see open_phoneme_fifo_in_background
  // for why: opening a FIFO for writing blocks until a reader attaches, and
  // we don't want that to block the socket/accept side of the daemon from
  // starting up). detach() means we never join this thread — it runs for
  // the lifetime of the process and is torn down implicitly on exit.
  std::thread(open_phoneme_fifo_in_background).detach();

  // --- Phase 5: PipeWire playback stream --------------------------------
  // pw_init() must be called once before any other pipewire_*/pw_* call.
  pw_init(nullptr, nullptr);

  // A pw_thread_loop runs PipeWire's event loop (including on_process()) on
  // its own dedicated thread, separate from this accept-loop/synth thread —
  // required, since PipeWire's realtime callback must never share a thread
  // with blocking work like accept()/read_lines().
  pw_thread_loop* pw_loop = pw_thread_loop_new("moonshine-tts-streamd-pw", nullptr);
  if (!pw_loop) {
    std::cerr << "streamd: pw_thread_loop_new() failed\n";
    stop_synth_worker();
    return 1;
  }

  pw_properties* pw_props = pw_properties_new(PW_KEY_MEDIA_TYPE, "Audio", PW_KEY_MEDIA_CATEGORY,
                                               "Playback", PW_KEY_MEDIA_ROLE, "Music", nullptr);
  g_pw_stream = pw_stream_new_simple(pw_thread_loop_get_loop(pw_loop), "moonshine-tts-streamd",
                                      pw_props, &kStreamEvents, nullptr);
  if (!g_pw_stream) {
    std::cerr << "streamd: pw_stream_new_simple() failed\n";
    stop_synth_worker();
    return 1;
  }

  // Describe the one format we offer: mono F32 at the TTS engine's native
  // sample rate. Using MoonshineTTS::kSampleRateHz directly (never a
  // hardcoded 24000) means a future model sample-rate change can't silently
  // desync PipeWire's stream from what synthesize_from_phonemes() produces.
  uint8_t pod_buffer[1024];
  spa_pod_builder pod_builder = SPA_POD_BUILDER_INIT(pod_buffer, sizeof(pod_buffer));
  spa_audio_info_raw audio_info{};
  audio_info.format = SPA_AUDIO_FORMAT_F32;
  audio_info.channels = 1;
  audio_info.rate = static_cast<uint32_t>(MoonshineTTS::kSampleRateHz);
  const spa_pod* pw_params[1];
  pw_params[0] = spa_format_audio_raw_build(&pod_builder, SPA_PARAM_EnumFormat, &audio_info);

  // AUTOCONNECT: attach to the system's default playback sink automatically.
  // MAP_BUFFERS: mmap buffer memory so datas[0].data is directly usable, no
  // extra copy step. RT_PROCESS: call on_process() from the realtime thread
  // directly (lowest latency) — this is why on_process() must stay
  // allocation-free and non-blocking.
  const auto pw_flags = static_cast<pw_stream_flags>(
      PW_STREAM_FLAG_AUTOCONNECT | PW_STREAM_FLAG_MAP_BUFFERS | PW_STREAM_FLAG_RT_PROCESS);
  if (pw_stream_connect(g_pw_stream, PW_DIRECTION_OUTPUT, PW_ID_ANY, pw_flags, pw_params, 1) < 0) {
    std::cerr << "streamd: pw_stream_connect() failed\n";
    stop_synth_worker();
    return 1;
  }

  if (pw_thread_loop_start(pw_loop) < 0) {
    std::cerr << "streamd: pw_thread_loop_start() failed\n";
    stop_synth_worker();
    return 1;
  }
  std::printf("streamd: PipeWire stream connecting (async; audio starts once negotiated)\n");

  // Unix sockets are backed by an actual file on disk at kSocketPath. If a
  // previous run crashed without cleaning up, that stale file is still
  // there and bind() below would fail with "address already in use".
  // unlink() removes it first; harmless if the file doesn't exist.
  unlink(kSocketPath);

  // AF_UNIX = Unix domain (filesystem-path addressed, vs AF_INET for IP).
  // SOCK_STREAM = reliable, ordered, connection-oriented byte stream (like
  // TCP), as opposed to SOCK_DGRAM (unordered packets, like UDP).
  int listen_fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (listen_fd < 0) {
    std::cerr << "streamd: socket() failed: " << std::strerror(errno) << '\n';
    stop_synth_worker();
    return 1;
  }
  g_listen_fd = listen_fd;

  // sockaddr_un is the address struct for Unix sockets: a sun_path fixed-size
  // char array (holding a filesystem path) instead of an IP+port.
  sockaddr_un addr{};
  addr.sun_family = AF_UNIX;
  // sizeof(...)-1 leaves room for the null terminator so this can't overflow
  // sun_path's fixed buffer.
  std::strncpy(addr.sun_path, kSocketPath, sizeof(addr.sun_path) - 1);

  // bind() attaches the socket to that filesystem path — this is what
  // actually creates the .sock file on disk. bind()'s signature takes the
  // generic base `sockaddr*` type; sockaddr_un is a "subclass" via C-style
  // struct-layout convention, hence the cast.
  if (bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    std::cerr << "streamd: bind() failed: " << std::strerror(errno) << '\n';
    close(listen_fd);
    stop_synth_worker();
    return 1;
  }

  // Marks the socket ready to accept connections, with a backlog queue of 4
  // pending connections (protects against a burst of simultaneous clients
  // arriving before we've accept()ed the previous ones).
  if (listen(listen_fd, /*backlog=*/4) < 0) {
    std::cerr << "streamd: listen() failed: " << std::strerror(errno) << '\n';
    close(listen_fd);
    stop_synth_worker();
    return 1;
  }

  // Phase 7: install SIGINT/SIGTERM handlers now, right before the accept
  // loop starts (everything before this point either can't be interrupted
  // mid-syscall in a way that matters, or hasn't created state that needs
  // tearing down yet). sigaction (not signal()) so we can explicitly leave
  // SA_RESTART unset — accept() must return EINTR, not silently retry,
  // otherwise the handler's shutdown() would never actually unblock it.
  struct sigaction shutdown_action {};
  shutdown_action.sa_handler = handle_shutdown_signal;
  sigemptyset(&shutdown_action.sa_mask);
  shutdown_action.sa_flags = 0;
  sigaction(SIGINT, &shutdown_action, nullptr);
  sigaction(SIGTERM, &shutdown_action, nullptr);

  std::printf("streamd: listening on %s\n", kSocketPath);
  std::printf("streamd: phoneme FIFO at %s (waiting for a reader in the background)\n",
              kPhonemeFifoPath);

  // Thread A. Accept one client at a time, fully drain it via read_lines()
  // (blocks until that client disconnects), close it, accept the next.
  // Still one connection at a time — Phase 6 doesn't change that — but this
  // thread no longer blocks on G2P/synthesis while doing it: each line is
  // just handed to Thread B via enqueue_utterance() and this loop moves
  // straight on to reading the next one.
  for (;;) {
    // Blocks until a client connects. Returns a NEW fd (conn_fd) for this
    // specific connection; listen_fd stays open for future connections.
    // nullptr, nullptr = don't bother capturing the peer's address (Unix
    // sockets from an unnamed client don't have a meaningful one anyway).
    int conn_fd = accept(listen_fd, nullptr, nullptr);
    if (conn_fd < 0) {
      if (errno == EINTR) {
        if (g_shutdown_requested) {
          std::printf("streamd: shutdown signal received, stopping\n");
          break;
        }
        continue;  // interrupted by some other/spurious signal — keep going
      }
      std::cerr << "streamd: accept() failed: " << std::strerror(errno) << '\n';
      break;
    }
    read_lines(conn_fd, [](const std::string& line) { enqueue_utterance(line); });
    close(conn_fd);
  }

  // Reached either on shutdown signal or a non-EINTR accept() error.
  //
  // Fix (1): stop and join Thread B *first*, before anything else is torn
  // down. It abandons the queued backlog immediately (pending work is
  // dropped, per the Phase 7 policy) and only has to finish the single
  // utterance it may already be synthesizing, so this is bounded. Joining
  // here is what guarantees it is no longer touching `tts`/`g2p` (destroyed
  // when this function returns) or the ring buffer (about to lose its
  // PipeWire consumer).
  //
  // The FIFO-opener thread is still detached and deliberately not joined:
  // if no reader ever attached it is parked forever inside a blocking
  // open(), so joining it could hang shutdown indefinitely. It touches
  // nothing but the atomic fd, so leaving it to process exit is safe.
  stop_synth_worker();

  close(listen_fd);
  unlink(kSocketPath);
  unlink(kPhonemeFifoPath);

  // Lock while destroying the stream so it can't race with on_process()
  // still running concurrently on pw_loop's own thread; stop the loop
  // (joins its thread) before destroying it, then pw_deinit() last.
  pw_thread_loop_lock(pw_loop);
  pw_stream_destroy(g_pw_stream);
  pw_thread_loop_unlock(pw_loop);
  pw_thread_loop_stop(pw_loop);
  pw_thread_loop_destroy(pw_loop);
  pw_deinit();

  return 0;
}
