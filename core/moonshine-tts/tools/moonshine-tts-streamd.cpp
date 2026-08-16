// Streaming daemon: reads utterance lines from a Unix domain socket, drives
// MoonshineTTS + MoonshineG2P (loaded once), and writes phonemes to a named
// pipe (FIFO). PCM output to a PipeWire sink is not wired up yet (Phase 5).
// Phase 3: phoneme FIFO added — each received line is now run through G2P
// and the resulting IPA string is broadcast to whoever is reading the FIFO.

// POSIX headers (raw OS syscalls, not C++ standard library). Unix domain
// sockets use the same API shape as TCP sockets (socket/bind/listen/accept)
// but address family AF_UNIX instead of AF_INET, so they talk over a
// filesystem path instead of an IP+port — local-only, no network stack cost.
#include <fcntl.h>       // open(), O_WRONLY — used to open the phoneme FIFO
#include <sys/socket.h>  // socket(), bind(), listen(), accept()
#include <sys/stat.h>    // mkfifo()
#include <sys/un.h>      // sockaddr_un — the Unix-domain-socket address struct
#include <unistd.h>      // read(), write(), close(), unlink()

#include <atomic>      // std::atomic<int> — safe cross-thread fd handoff
#include <cerrno>      // errno — set by syscalls on failure
#include <csignal>     // signal(), SIGPIPE, SIG_IGN
#include <cstdio>      // std::printf
#include <cstring>     // std::strerror(), std::strncpy()
#include <functional>  // std::function — type-erased callback wrapper
#include <iostream>    // std::cout / std::cerr
#include <optional>    // std::optional<T> — lets us delay construction
#include <string>
#include <thread>  // std::thread — background FIFO-opening thread
#include <vector>

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
    std::cerr << "streamd: phoneme FIFO write failed: " << std::strerror(errno) << '\n';
    // Reader vanished; stop broadcasting until (if) this daemon restarts.
    // (A more complete implementation would re-open and wait for a new
    // reader; out of scope for this phase per PLAN.md.)
    g_phoneme_fifo_fd.store(-1);
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
      // just retry. Anything else is a genuine failure.
      if (errno == EINTR) {
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

  // Create the phoneme FIFO on disk (a special file — not a regular file —
  // that connects a writer and a reader like a pipe). mkfifo() fails with
  // EEXIST if a previous run already created it; that's fine, we just want
  // it to exist, so we ignore that specific failure rather than checking
  // the return value strictly.
  if (mkfifo(kPhonemeFifoPath, 0666) < 0 && errno != EEXIST) {
    std::cerr << "streamd: mkfifo() failed: " << std::strerror(errno) << '\n';
    return 1;
  }
  // Hand this off to a background thread (see open_phoneme_fifo_in_background
  // for why: opening a FIFO for writing blocks until a reader attaches, and
  // we don't want that to block the socket/accept side of the daemon from
  // starting up). detach() means we never join this thread — it runs for
  // the lifetime of the process and is torn down implicitly on exit.
  std::thread(open_phoneme_fifo_in_background).detach();

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
    return 1;
  }

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
    return 1;
  }

  // Marks the socket ready to accept connections, with a backlog queue of 4
  // pending connections (protects against a burst of simultaneous clients
  // arriving before we've accept()ed the previous ones).
  if (listen(listen_fd, /*backlog=*/4) < 0) {
    std::cerr << "streamd: listen() failed: " << std::strerror(errno) << '\n';
    close(listen_fd);
    return 1;
  }

  std::printf("streamd: listening on %s\n", kSocketPath);
  std::printf("streamd: phoneme FIFO at %s (waiting for a reader in the background)\n",
              kPhonemeFifoPath);

  // Single-threaded, sequential: accept one client, fully drain it via
  // read_lines() (blocks until that client disconnects), close it, then
  // accept the next. Later phases split this into three threads (socket
  // reader / synth worker / PipeWire callback) so synthesis work doesn't
  // block new connections from being accepted — see PLAN.md Phase 6.
  for (;;) {
    // Blocks until a client connects. Returns a NEW fd (conn_fd) for this
    // specific connection; listen_fd stays open for future connections.
    // nullptr, nullptr = don't bother capturing the peer's address (Unix
    // sockets from an unnamed client don't have a meaningful one anyway).
    int conn_fd = accept(listen_fd, nullptr, nullptr);
    if (conn_fd < 0) {
      if (errno == EINTR) {
        continue;
      }
      std::cerr << "streamd: accept() failed: " << std::strerror(errno) << '\n';
      break;
    }
    // For each received line: run it through G2P to get an IPA phoneme
    // string, echo the original text to stdout (so you can watch what
    // arrived), and broadcast the phonemes to the FIFO. No audio synthesis
    // yet — that's Phase 4 (debug WAV dump) and Phase 5 (real PipeWire).
    read_lines(conn_fd, [&](const std::string& line) {
      std::cout << line << '\n';
      const std::string ipa = g2p->text_to_ipa(line);
      std::cout << "  -> " << ipa << '\n';
      broadcast_phoneme_line(ipa);
    });
    close(conn_fd);
  }

  // Only reached if the accept loop above breaks (a non-EINTR accept()
  // error). Note: there's no SIGINT/SIGTERM handler yet (that's Phase 7),
  // so Ctrl-C kills the process abruptly and this cleanup never runs in
  // that case — which is exactly why unlink() at startup exists too, as a
  // belt-and-suspenders cleanup for the next run.
  close(listen_fd);
  unlink(kSocketPath);
  return 0;
}
