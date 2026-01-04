#!/usr/bin/env python3
"""
TCP Chat Client for the paired-chat server.

Run:
  python3 client.py --host 127.0.0.1 --port 12345
"""

import socket
import threading
import argparse
import struct


def send_frame(sock: socket.socket, data: str) -> None:
    payload = data.encode("utf-8")
    header = struct.pack("!I", len(payload))
    sock.sendall(header + payload)


def recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("Socket closed")
        buf += chunk
    return buf


def recv_frame(sock: socket.socket) -> str:
    header = recv_exact(sock, 4)
    (length,) = struct.unpack("!I", header)
    payload = recv_exact(sock, length)
    return payload.decode("utf-8", errors="replace")


def receiver(sock: socket.socket, stop_event: threading.Event):
    try:
        while not stop_event.is_set():
            msg = recv_frame(sock)
            print(f"\n[SERVER] {msg}\n> ", end="", flush=True)
    except Exception:
        if not stop_event.is_set():
            print("\n[INFO] Disconnected from server.")
    finally:
        stop_event.set()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=12345)
    args = ap.parse_args()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((args.host, args.port))

    stop_event = threading.Event()
    t = threading.Thread(target=receiver, args=(sock, stop_event), daemon=True)
    t.start()

    username = input("Enter username: ").strip()
    if not username:
        print("Username cannot be empty.")
        sock.close()
        return

    send_frame(sock, f"HELLO {username}")

    print("\nCommands: LIST | CHAT <user> | MSG <text> | QUIT\n")
    while not stop_event.is_set():
        try:
            cmd = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            cmd = "QUIT"

        if not cmd:
            continue

        try:
            send_frame(sock, cmd)
        except Exception:
            print("[INFO] Send failed, connection lost.")
            break

        if cmd.upper() == "QUIT":
            break

    stop_event.set()
    try:
        sock.close()
    except Exception:
        pass


if __name__ == "__main__":
    main()
