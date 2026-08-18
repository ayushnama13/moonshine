// Streaming TTS daemon: text in over a Unix socket, phonemes out over a
// FIFO, PCM audio out over PipeWire. Three threads: socket accept loop,
// synth worker (owns tts/g2p), PipeWire's own realtime callback.

#include <fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <functional>
#include <iostream>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include <pipewire/pipewire.h>
#include <spa/param/audio/format-utils.h>

#include "moonshine-g2p.h"
#include "moonshine-tts.h"

namespace {

constexpr const char* kSocketPath = "/tmp/moonshine-tts-streamd.sock";
constexpr const char* kPhonemeFifoPath = "/tmp/moonshine-tts-streamd.phonemes";

std::atomic<int> g_phoneme_fifo_fd{-1};  // -1 = no reader attached yet

volatile sig_atomic_t g_shutdown_requested = 0;  // set only from the signal handler
int g_listen_fd = -1;

std::atomic<bool> g_shutting_down{false};  // worker-thread-visible shutdown flag
std::atomic<uint64_t> g_generation{0};   // barge-in / cancellation generation counter

// Signal-safe only: no malloc/printf/iostream. shutdown()s the listen socket
// so a blocked accept() wakes up with EINTR instead of just sitting there.
void handle_shutdown_signal(int) {
  g_shutdown_requested = 1;
  if (g_listen_fd >= 0) {
    shutdown(g_listen_fd, SHUT_RDWR);
  }
}

// Lock-free SPSC ring buffer: synth worker pushes, PipeWire's on_process
// pops. Safe without a mutex because there's exactly one writer/reader pair —
// the producer owns write_, the consumer owns read_, and neither ever writes
// the other's index. A flush (clear()) must therefore be *applied* by the
// consumer, never by the producer: read_ having two writers is what would
// break the invariant.
class RingBuffer {
 public:
  explicit RingBuffer(size_t capacity_frames) : buf_(capacity_frames) {}

  // Callable from any thread: only raises a flag. The consumer acts on it on
  // its next pop(), i.e. within one PipeWire callback (sub-10ms) — fast
  // enough for barge-in, and it keeps read_ single-writer.
  void clear() {
    clear_requested_.store(true, std::memory_order_release);
  }

  // Producer-only. Never blocks; returns frames actually accepted (may be
  // < count). Deliberately does NOT consume clear_requested_ — doing so
  // would make this thread write read_ concurrently with the consumer's own
  // read_ store, letting a flush be silently undone (read_ moving backwards)
  // and letting the unsigned `w - r` in pop() underflow into a huge avail.
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

  // Consumer-only, and the sole place a pending clear() is applied.
  size_t pop(float* dst, size_t count) {
    if (clear_requested_.exchange(false, std::memory_order_acq_rel)) {
      const size_t w = write_.load(std::memory_order_acquire);
      read_.store(w, std::memory_order_release);
      std::fill(dst, dst + count, 0.0f);
      return 0;
    }
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
  std::atomic<bool> clear_requested_{false};
};

RingBuffer g_ring_buffer(static_cast<size_t>(moonshine_tts::MoonshineTTS::kSampleRateHz) * 10);

// Retries until all of [data, data+count) lands in the ring buffer, so a
// utterance longer than the buffer's capacity doesn't get its tail dropped.
// Sleep-poll, not a condvar: signalling from on_process would need a lock,
// which PW_STREAM_FLAG_RT_PROCESS forbids on that thread.
size_t push_all_blocking(const float* data, size_t count, uint64_t target_gen) {
  size_t written = 0;
  while (written < count) {
    if (g_generation.load(std::memory_order_relaxed) != target_gen) {
      break;
    }
    written += g_ring_buffer.push(data + written, count - written);
    if (written == count) {
      break;
    }
    if (g_shutting_down.load(std::memory_order_relaxed)) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return written;
}

pw_stream* g_pw_stream = nullptr;

// Called on PipeWire's state changes to ensure errors/negotiation failures are logged.
void on_state_changed(void*, enum pw_stream_state old_state, enum pw_stream_state state, const char* error) {
  std::printf("streamd: PipeWire stream state: %s -> %s%s%s%s\n",
              pw_stream_state_as_string(old_state),
              pw_stream_state_as_string(state),
              error ? " (error: " : "",
              error ? error : "",
              error ? ")" : "");
  if (state == PW_STREAM_STATE_ERROR) {
    std::cerr << "streamd: PipeWire stream error: " << (error ? error : "unknown error") << '\n';
  }
}

// Called on PipeWire's own realtime thread. Must stay allocation-free and
// non-blocking — only ever touches the ring buffer and PipeWire's buffer,
// never tts/g2p/sockets/FIFO.
void on_process(void*) {
  pw_buffer* b = pw_stream_dequeue_buffer(g_pw_stream);
  if (!b) {
    return;
  }
  spa_buffer* buf = b->buffer;
  float* dst = static_cast<float*>(buf->datas[0].data);
  if (!dst) {
    pw_stream_queue_buffer(g_pw_stream, b);
    return;
  }

  constexpr uint32_t kStride = sizeof(float);
  const uint32_t n_frames = buf->datas[0].maxsize / kStride;

  const size_t got = g_ring_buffer.pop(dst, n_frames);
  if (got < n_frames) {
    // Underrun (nothing queued recently) — pad with silence, not garbage.
    std::fill(dst + got, dst + n_frames, 0.0f);
  }

  buf->datas[0].chunk->offset = 0;
  buf->datas[0].chunk->stride = static_cast<int32_t>(kStride);
  buf->datas[0].chunk->size = n_frames * kStride;
  pw_stream_queue_buffer(g_pw_stream, b);
}

constexpr pw_stream_events kStreamEvents = {
    .version = PW_VERSION_STREAM_EVENTS,
    .destroy = nullptr,
    .state_changed = on_state_changed,
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

// Runs on its own thread: opening a FIFO for write blocks until a reader
// attaches, and we don't want that to hold up the socket side at startup.
void open_phoneme_fifo_in_background() {
  int fd = open(kPhonemeFifoPath, O_WRONLY);
  if (fd < 0) {
    std::cerr << "streamd: failed to open phoneme FIFO for writing: "
              << std::strerror(errno) << '\n';
    return;
  }

  int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
    std::cerr << "streamd: failed to set phoneme FIFO non-blocking: " << std::strerror(errno)
              << " (continuing; writes may block if the reader stalls)\n";
  }
  g_phoneme_fifo_fd.store(fd);
  std::printf("streamd: phoneme FIFO reader attached\n");
}

// Best-effort: a missing/stalled reader must never block or fail synthesis.
void broadcast_phoneme_line(const std::string& ipa) {
  int fd = g_phoneme_fifo_fd.load();
  if (fd < 0) {
    return;
  }
  std::string line = ipa + '\n';

  ssize_t n = write(fd, line.data(), line.size());
  if (n < 0) {

    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return;
    }
    std::cerr << "streamd: phoneme FIFO write failed: " << std::strerror(errno) << '\n';

    g_phoneme_fifo_fd.store(-1);
  }
}

// Accept-loop (Thread A) -> synth worker (Thread B) handoff. Plain mutex,
// not lock-free: utterance lines arrive at typing/speaking rates.
std::mutex g_utterance_queue_mutex;
std::condition_variable g_utterance_queue_cv;
std::queue<std::string> g_utterance_queue;

void handle_flush_command() {
  {
    std::lock_guard<std::mutex> lock(g_utterance_queue_mutex);
    std::queue<std::string> empty;
    std::swap(g_utterance_queue, empty);
  }
  const uint64_t new_gen = g_generation.fetch_add(1, std::memory_order_relaxed) + 1;
  g_ring_buffer.clear();
  std::printf("streamd: flush/stop requested (gen=%llu)\n",
              static_cast<unsigned long long>(new_gen));
}

std::string trim(const std::string& str) {
  size_t first = str.find_first_not_of(" \t\r\n");
  if (first == std::string::npos) return std::string();
  size_t last = str.find_last_not_of(" \t\r\n");
  return str.substr(first, last - first + 1);
}

void enqueue_utterance(std::string line) {
  {
    std::lock_guard<std::mutex> lock(g_utterance_queue_mutex);
    g_utterance_queue.push(std::move(line));
  }
  g_utterance_queue_cv.notify_one();
}

void process_input_line(const std::string& line) {
  std::string trimmed = trim(line);
  if (trimmed == "!stop" || trimmed == "!flush") {
    handle_flush_command();
  } else if (!trimmed.empty()) {
    enqueue_utterance(line);
  }
}

std::vector<std::string> split_sentences(const std::string& line) {
  std::vector<std::string> sentences;
  const size_t len = line.size();
  size_t start = 0;

  auto is_sentence_delim = [](char c) {
    return c == '.' || c == '!' || c == '?' || c == ';';
  };

  for (size_t i = 0; i < len; ++i) {
    if (is_sentence_delim(line[i])) {
      if (line[i] == '.' && i > 0 && i + 1 < len &&
          std::isdigit(static_cast<unsigned char>(line[i - 1])) &&
          std::isdigit(static_cast<unsigned char>(line[i + 1]))) {
        continue;
      }

      size_t delim_end = i;
      while (delim_end + 1 < len && is_sentence_delim(line[delim_end + 1])) {
        if (line[delim_end + 1] == '.' && delim_end + 2 < len &&
            std::isdigit(static_cast<unsigned char>(line[delim_end])) &&
            std::isdigit(static_cast<unsigned char>(line[delim_end + 2]))) {
          break;
        }
        delim_end++;
      }

      if (delim_end + 1 == len || std::isspace(static_cast<unsigned char>(line[delim_end + 1]))) {
        std::string s = trim(line.substr(start, delim_end + 1 - start));
        if (!s.empty()) {
          sentences.push_back(s);
        }
        start = delim_end + 1;
        while (start < len && std::isspace(static_cast<unsigned char>(line[start]))) {
          start++;
        }
        i = start - 1;
      }
    }
  }

  if (start < len) {
    std::string s = trim(line.substr(start));
    if (!s.empty()) {
      sentences.push_back(s);
    }
  }

  if (sentences.empty()) {
    std::string s = trim(line);
    if (!s.empty()) {
      sentences.push_back(s);
    }
  }

  return sentences;
}

// Thread B: owns tts/g2p exclusively, joined (not detached) by main() so
// shutdown can't tear down the ONNX sessions out from under an in-flight
// synthesize_from_phonemes() call.
void run_synth_worker(moonshine_tts::MoonshineG2P& g2p, moonshine_tts::MoonshineTTS& tts) {
  for (;;) {
    std::string line;
    {
      std::unique_lock<std::mutex> lock(g_utterance_queue_mutex);

      g_utterance_queue_cv.wait(
          lock, [] { return !g_utterance_queue.empty() || g_shutting_down.load(); });
      if (g_shutting_down.load()) {
        return;
      }
      line = std::move(g_utterance_queue.front());
      g_utterance_queue.pop();
    }

    // Snapshot the generation this utterance belongs to; a !stop/!flush that
    // bumps g_generation after this point cancels it (checked per-sentence
    // below), but nothing can have bumped it *before* this line was even
    // dequeued from the front of the queue.
    const uint64_t start_gen = g_generation.load(std::memory_order_relaxed);

    std::cout << line << '\n';
    std::vector<std::string> sentences = split_sentences(line);

    for (const auto& sentence : sentences) {
      if (g_generation.load(std::memory_order_relaxed) != start_gen || g_shutting_down.load()) {
        std::cout << "  [cancelled]\n";
        break;
      }

      const std::string ipa = g2p.text_to_ipa(sentence);
      std::cout << "  [sentence] " << sentence << "\n  -> " << ipa << '\n';
      broadcast_phoneme_line(ipa);

      if (g_generation.load(std::memory_order_relaxed) != start_gen || g_shutting_down.load()) {
        std::cout << "  [cancelled]\n";
        break;
      }

      const std::vector<float> pcm = tts.synthesize_from_phonemes(ipa);

      const size_t queued = push_all_blocking(pcm.data(), pcm.size(), start_gen);
      if (g_generation.load(std::memory_order_relaxed) != start_gen) {
        std::cout << "  [cancelled during playback push]\n";
        break;
      }

      std::cout << "  -> queued " << queued << " samples ("
                << moonshine_tts::MoonshineTTS::kSampleRateHz << " Hz) for playback\n";
      if (queued < pcm.size() && !g_shutting_down.load()) {
        std::cerr << "streamd: dropped " << (pcm.size() - queued)
                  << " samples of audio\n";
      }
    }
  }
}

// Splits a socket stream into newline-delimited lines; a line may arrive
// split across reads or several may land in one read(), so buf absorbs both.
void read_lines(int conn_fd, const std::function<void(const std::string&)>& on_line) {
  std::string buf;
  char chunk[4096];
  for (;;) {
    ssize_t n = read(conn_fd, chunk, sizeof(chunk));
    if (n < 0) {

      if (errno == EINTR) {

        if (g_shutdown_requested) {
          return;
        }
        continue;
      }
      std::cerr << "streamd: read error: " << std::strerror(errno) << '\n';
      return;
    }
    if (n == 0) {
      return;
    }
    buf.append(chunk, static_cast<size_t>(n));

    size_t pos;
    while ((pos = buf.find('\n')) != std::string::npos) {
      std::string line = buf.substr(0, pos);
      buf.erase(0, pos + 1);
      if (!line.empty()) {
        on_line(line);
      }
    }
  }
}

}

int main() {
  using moonshine_tts::MoonshineG2P;
  using moonshine_tts::MoonshineTTS;
  using moonshine_tts::MoonshineTTSOptions;

  // Line-buffer stdout. A daemon's stdout is usually a pipe or a log file,
  // not a tty, and the C runtime then picks *full* buffering — so progress
  // lines sit in the buffer for minutes and only appear when the process
  // exits, which makes a busy daemon look hung. Must happen before the first
  // write to the stream. std::cout is synced with stdout by default, so this
  // covers the worker thread's logging too.
  setvbuf(stdout, nullptr, _IOLBF, 0);

  std::printf("streamd starting\n");

  signal(SIGPIPE, SIG_IGN);  // FIFO write with no reader must not kill us

  // Load once at startup — the whole point of a daemon vs. the batch CLI.
  MoonshineTTSOptions opt;
  const std::string lang = "en_us";

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

  // Pre-warm. Constructing the model is not the whole cost: ORT defers a lot
  // of work (graph optimization, kernel/arena setup) to the first inference,
  // which made the first real utterance take minutes on CPU while every
  // later one took seconds. Burn that cost here, on a throwaway phrase,
  // *before* the socket is listening — a daemon that takes longer to start
  // but answers its first request promptly is the right trade. The audio is
  // discarded (nothing is pushed to the ring buffer) and the PipeWire stream
  // doesn't exist yet, so nothing is audible.
  //
  // Best-effort: a warm-up failure is not fatal. If it throws, the same call
  // would throw for a real utterance too, but that's the synth worker's
  // problem to report per-utterance — it must not stop the daemon booting.
  std::printf("streamd: warming up (first inference is slow; this is a one-time cost)\n");
  try {
    const auto warmup_start = std::chrono::steady_clock::now();
    const std::string warmup_ipa = g2p->text_to_ipa("warm up");
    const std::vector<float> warmup_pcm = tts->synthesize_from_phonemes(warmup_ipa);
    const auto warmup_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                               std::chrono::steady_clock::now() - warmup_start)
                               .count();
    std::printf("streamd: warm-up done in %lld ms (%zu samples discarded)\n",
                static_cast<long long>(warmup_ms), warmup_pcm.size());
  } catch (const std::exception& e) {
    std::cerr << "streamd: warm-up failed (continuing anyway): " << e.what() << '\n';
  }

  // Thread B. Kept joinable: it borrows tts/g2p, locals here, so it must be
  // stopped and joined before this function returns and destroys them.
  std::thread synth_worker(run_synth_worker, std::ref(*g2p), std::ref(*tts));

  // Idempotent — every return path below (early failures included) calls
  // this so a still-joinable std::thread never gets destroyed while running.
  auto stop_synth_worker = [&synth_worker] {
    g_shutting_down.store(true);
    g_utterance_queue_cv.notify_all();
    if (synth_worker.joinable()) {
      synth_worker.join();
    }
  };

  if (mkfifo(kPhonemeFifoPath, 0666) < 0 && errno != EEXIST) {
    std::cerr << "streamd: mkfifo() failed: " << std::strerror(errno) << '\n';
    stop_synth_worker();
    return 1;
  }

  std::thread(open_phoneme_fifo_in_background).detach();

  // --- PipeWire playback stream ---
  pw_init(nullptr, nullptr);

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

  uint8_t pod_buffer[1024];
  spa_pod_builder pod_builder = SPA_POD_BUILDER_INIT(pod_buffer, sizeof(pod_buffer));
  spa_audio_info_raw audio_info{};
  audio_info.format = SPA_AUDIO_FORMAT_F32;
  audio_info.channels = 1;
  audio_info.rate = static_cast<uint32_t>(MoonshineTTS::kSampleRateHz);
  const spa_pod* pw_params[1];
  pw_params[0] = spa_format_audio_raw_build(&pod_builder, SPA_PARAM_EnumFormat, &audio_info);

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

  // --- Unix domain socket, text in ---
  unlink(kSocketPath);  // clear a stale file from a previous crashed run

  int listen_fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (listen_fd < 0) {
    std::cerr << "streamd: socket() failed: " << std::strerror(errno) << '\n';
    stop_synth_worker();
    return 1;
  }
  g_listen_fd = listen_fd;

  sockaddr_un addr{};
  addr.sun_family = AF_UNIX;

  std::strncpy(addr.sun_path, kSocketPath, sizeof(addr.sun_path) - 1);

  if (bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    std::cerr << "streamd: bind() failed: " << std::strerror(errno) << '\n';
    close(listen_fd);
    stop_synth_worker();
    return 1;
  }

  if (listen(listen_fd, 4) < 0) {
    std::cerr << "streamd: listen() failed: " << std::strerror(errno) << '\n';
    close(listen_fd);
    stop_synth_worker();
    return 1;
  }

  // SA_RESTART deliberately left unset: accept() must return EINTR, not
  // silently retry, or the signal handler's shutdown() never unblocks it.
  struct sigaction shutdown_action {};
  shutdown_action.sa_handler = handle_shutdown_signal;
  sigemptyset(&shutdown_action.sa_mask);
  shutdown_action.sa_flags = 0;
  sigaction(SIGINT, &shutdown_action, nullptr);
  sigaction(SIGTERM, &shutdown_action, nullptr);

  std::printf("streamd: listening on %s\n", kSocketPath);
  std::printf("streamd: phoneme FIFO at %s (waiting for a reader in the background)\n",
              kPhonemeFifoPath);

  // Thread A. One connection at a time; each line just gets handed off to
  // Thread B, so this loop never blocks on G2P/synthesis.
  for (;;) {
    int conn_fd = accept(listen_fd, nullptr, nullptr);
    if (conn_fd < 0) {
      if (errno == EINTR) {
        if (g_shutdown_requested) {
          std::printf("streamd: shutdown signal received, stopping\n");
          break;
        }
        continue;
      }
      std::cerr << "streamd: accept() failed: " << std::strerror(errno) << '\n';
      break;
    }
    read_lines(conn_fd, [](const std::string& line) { process_input_line(line); });
    close(conn_fd);
  }

  // Join Thread B first — guarantees it's done touching tts/g2p/ring buffer
  // before we tear any of that down. Policy is drop-pending, not drain: it
  // abandons the queued backlog and only finishes the utterance in flight.
  stop_synth_worker();

  close(listen_fd);
  unlink(kSocketPath);
  unlink(kPhonemeFifoPath);

  pw_thread_loop_lock(pw_loop);
  pw_stream_destroy(g_pw_stream);
  pw_thread_loop_unlock(pw_loop);
  pw_thread_loop_stop(pw_loop);
  pw_thread_loop_destroy(pw_loop);
  pw_deinit();

  return 0;
}
