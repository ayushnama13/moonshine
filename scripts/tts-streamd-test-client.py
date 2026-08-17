#!/usr/bin/env python3
"""Manual QA / smoke-test client for moonshine-tts-streamd (Phase 7).

Connects to the daemon's Unix domain socket, sends one or more utterance
lines with a configurable delay between them, and concurrently prints
whatever the daemon writes to the phoneme FIFO — so a caller can watch text
in / phonemes out without re-deriving the wire protocol from the C++ source.

Usage:
    scripts/tts-streamd-test-client.py "hello there" "how are you"
    scripts/tts-streamd-test-client.py --delay 1.0 - < lines.txt
    echo "hello" | scripts/tts-streamd-test-client.py -
"""

import argparse
import socket
import sys
import threading
import time

DEFAULT_SOCKET_PATH = "/tmp/moonshine-tts-streamd.sock"
DEFAULT_FIFO_PATH = "/tmp/moonshine-tts-streamd.phonemes"


def read_phoneme_fifo(fifo_path: str, stop_event: threading.Event) -> None:
    """Reads and prints lines from the phoneme FIFO until stop_event fires.

    open() for reading a FIFO blocks until a writer attaches, which is
    exactly the daemon's own open_phoneme_fifo_in_background() on the other
    end — same reason it runs on its own thread here too.
    """
    try:
        with open(fifo_path, "r") as fifo:
            while not stop_event.is_set():
                line = fifo.readline()
                if not line:
                    break
                print(f"[phonemes] {line.rstrip()}")
    except OSError as e:
        print(f"[phonemes] failed to open FIFO: {e}", file=sys.stderr)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("lines", nargs="+", help="utterance lines to send ('-' = read stdin)")
    parser.add_argument("--delay", type=float, default=0.5, help="seconds between lines (default: 0.5)")
    parser.add_argument("--socket", default=DEFAULT_SOCKET_PATH, help="daemon socket path")
    parser.add_argument("--fifo", default=DEFAULT_FIFO_PATH, help="phoneme FIFO path")
    args = parser.parse_args()

    lines = args.lines
    if lines == ["-"]:
        lines = [line.rstrip("\n") for line in sys.stdin if line.strip()]

    stop_event = threading.Event()
    fifo_thread = threading.Thread(target=read_phoneme_fifo, args=(args.fifo, stop_event), daemon=True)
    fifo_thread.start()

    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.connect(args.socket)
            for i, line in enumerate(lines):
                print(f"[send] {line}")
                sock.sendall((line + "\n").encode("utf-8"))
                if i < len(lines) - 1:
                    time.sleep(args.delay)
            # Give the daemon a moment to process the last line and write its
            # phonemes before we tear down the connection and exit.
            time.sleep(args.delay)
    except OSError as e:
        print(f"[error] socket connect/send failed: {e}", file=sys.stderr)
        return 1
    finally:
        stop_event.set()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
