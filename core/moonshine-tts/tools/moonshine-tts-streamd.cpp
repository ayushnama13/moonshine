// Streaming daemon: reads utterance lines from a Unix domain socket, drives
// MoonshineTTS + MoonshineG2P (loaded once), and (in later phases) writes
// phonemes to a FIFO and PCM to a PipeWire sink. Phase 2: model load + socket
// skeleton only — no phonemes, no audio yet, just echo received lines.
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <functional>
#include <iostream>
#include <optional>
#include <string>
#include <vector>

#include "moonshine-g2p.h"
#include "moonshine-tts.h"

namespace {

constexpr const char* kSocketPath = "/tmp/moonshine-tts-streamd.sock";

/// Reads newline-delimited lines from a connected socket fd, invoking
/// *on_line* for each complete line (delimiter stripped). Returns when the
/// peer closes the connection or a read error occurs.
void read_lines(int conn_fd, const std::function<void(const std::string&)>& on_line) {
  std::string buf;
  char chunk[4096];
  for (;;) {
    ssize_t n = read(conn_fd, chunk, sizeof(chunk));
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      std::cerr << "streamd: read error: " << std::strerror(errno) << '\n';
      return;
    }
    if (n == 0) {
      return;  // peer closed
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

}  // namespace

int main() {
  using moonshine_tts::MoonshineG2P;
  using moonshine_tts::MoonshineTTS;
  using moonshine_tts::MoonshineTTSOptions;

  std::printf("streamd starting\n");

  // Load the model + G2P once at startup — this is the whole point of the
  // daemon vs. the batch CLI, which pays this cost per invocation.
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

  unlink(kSocketPath);  // remove stale socket from a previous crashed run

  int listen_fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (listen_fd < 0) {
    std::cerr << "streamd: socket() failed: " << std::strerror(errno) << '\n';
    return 1;
  }

  sockaddr_un addr{};
  addr.sun_family = AF_UNIX;
  std::strncpy(addr.sun_path, kSocketPath, sizeof(addr.sun_path) - 1);

  if (bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    std::cerr << "streamd: bind() failed: " << std::strerror(errno) << '\n';
    close(listen_fd);
    return 1;
  }

  if (listen(listen_fd, /*backlog=*/4) < 0) {
    std::cerr << "streamd: listen() failed: " << std::strerror(errno) << '\n';
    close(listen_fd);
    return 1;
  }

  std::printf("streamd: listening on %s\n", kSocketPath);

  for (;;) {
    int conn_fd = accept(listen_fd, nullptr, nullptr);
    if (conn_fd < 0) {
      if (errno == EINTR) {
        continue;
      }
      std::cerr << "streamd: accept() failed: " << std::strerror(errno) << '\n';
      break;
    }
    read_lines(conn_fd, [](const std::string& line) { std::cout << line << '\n'; });
    close(conn_fd);
  }

  close(listen_fd);
  unlink(kSocketPath);
  return 0;
}
