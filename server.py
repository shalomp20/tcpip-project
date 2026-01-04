#!/usr/bin/env python3
"""
TCP Chat Server (multi-client, 1:1 pairing via server)

מה זה עושה:
- שרת TCP שמקבל הרבה לקוחות במקביל (Thread לכל לקוח)
- כל לקוח מזדהה בשם ייחודי
- לקוח מבקש CHAT עם שם אחר -> השרת "מזווג" אותם
- הודעות עוברות רק בין זוגות (לא Broadcast)
- משתמשים ב-framing של 4 בתים אורך + payload כדי להימנע מבעיות TCP

פרוטוקול (מסרים טקסטואליים בתוך frame):
Client->Server:
  HELLO <username>
  LIST
  CHAT <target_username>
  MSG <text...>
  QUIT

Server->Client:
  OK <message>
  ERR <message>
  USERS <user1,user2,...>
  PAIRED <partner_username>
  FROM <sender_username> <text...>
  INFO <message>
"""

import socket
import threading
import argparse
import struct
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple


# ===== framing helpers (length-prefixed) =====
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


# ===== server state =====
@dataclass
class ClientSession:
    sock: socket.socket
    addr: Tuple[str, int]
    username: str
    partner: Optional[str] = None
    lock: threading.Lock = field(default_factory=threading.Lock)


class ChatServer:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.server_sock: Optional[socket.socket] = None

        self.clients: Dict[str, ClientSession] = {}
        self.clients_lock = threading.Lock()

        self.running = True

    def start(self):
        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_sock.bind((self.host, self.port))
        self.server_sock.listen()
        print(f"[LISTENING] on {self.host}:{self.port}")

        try:
            while self.running:
                client_sock, addr = self.server_sock.accept()
                t = threading.Thread(
                    target=self.handle_new_connection,
                    args=(client_sock, addr),
                    daemon=True
                )
                t.start()
        except KeyboardInterrupt:
            print("\n[SHUTDOWN] KeyboardInterrupt")
        finally:
            self.running = False
            if self.server_sock:
                self.server_sock.close()

    def handle_new_connection(self, client_sock: socket.socket, addr):
        username = None
        try:
            send_frame(client_sock, "INFO Welcome. Identify: HELLO <username>")
            first = recv_frame(client_sock).strip()

            if not first.startswith("HELLO "):
                send_frame(client_sock, "ERR First command must be: HELLO <username>")
                client_sock.close()
                return

            username = first.split(" ", 1)[1].strip()
            if not username:
                send_frame(client_sock, "ERR Username cannot be empty")
                client_sock.close()
                return

            with self.clients_lock:
                if username in self.clients:
                    send_frame(client_sock, "ERR Username already in use")
                    client_sock.close()
                    return
                self.clients[username] = ClientSession(sock=client_sock, addr=addr, username=username)

            send_frame(client_sock, f"OK Logged in as {username}")
            print(f"[CONNECT] {username} from {addr}")

            while True:
                msg = recv_frame(client_sock).strip()
                if msg:
                    self.handle_command(username, msg)

        except ConnectionError:
            pass
        except Exception as e:
            try:
                send_frame(client_sock, f"ERR Server exception: {e}")
            except Exception:
                pass
        finally:
            if username:
                self.disconnect(username)

    def disconnect(self, username: str):
        with self.clients_lock:
            session = self.clients.get(username)
            if not session:
                return
            partner = session.partner

            try:
                session.sock.close()
            except Exception:
                pass

            del self.clients[username]

        print(f"[DISCONNECT] {username}")

        if partner:
            self.unpair(username, partner, notify=True)

    def get_session(self, username: str) -> Optional[ClientSession]:
        with self.clients_lock:
            return self.clients.get(username)

    def safe_send_to(self, username: str, text: str):
        session = self.get_session(username)
        if not session:
            return
        with session.lock:
            try:
                send_frame(session.sock, text)
            except Exception:
                self.disconnect(username)

    def handle_command(self, username: str, msg: str):
        parts = msg.split(" ", 1)
        cmd = parts[0].upper()
        arg = parts[1] if len(parts) > 1 else ""

        if cmd == "LIST":
            with self.clients_lock:
                users = sorted(self.clients.keys())
            self.safe_send_to(username, "USERS " + ",".join(users))
            return

        if cmd == "CHAT":
            target = arg.strip()
            if not target:
                self.safe_send_to(username, "ERR Usage: CHAT <target_username>")
                return
            if target == username:
                self.safe_send_to(username, "ERR You cannot chat with yourself")
                return
            self.try_pair(username, target)
            return

        if cmd == "MSG":
            self.forward_message(username, arg)
            return

        if cmd == "QUIT":
            self.safe_send_to(username, "OK Bye")
            self.disconnect(username)
            return

        self.safe_send_to(username, f"ERR Unknown command: {cmd}")

    def try_pair(self, a: str, b: str):
        with self.clients_lock:
            sa = self.clients.get(a)
            sb = self.clients.get(b)

            if not sb:
                self.safe_send_to(a, f"ERR User '{b}' is not connected")
                return
            if sa is None:
                return
            if sa.partner is not None:
                self.safe_send_to(a, f"ERR You are already paired with '{sa.partner}'")
                return
            if sb.partner is not None:
                self.safe_send_to(a, f"ERR '{b}' is already paired with '{sb.partner}'")
                return

            sa.partner = b
            sb.partner = a

        self.safe_send_to(a, f"PAIRED {b}")
        self.safe_send_to(b, f"PAIRED {a}")
        self.safe_send_to(a, "OK You can now send: MSG <text>")
        self.safe_send_to(b, "OK You can now send: MSG <text>")
        print(f"[PAIR] {a} <-> {b}")

    def unpair(self, a: str, b: str, notify: bool):
        with self.clients_lock:
            sa = self.clients.get(a)
            sb = self.clients.get(b)

            if sa and sa.partner == b:
                sa.partner = None
            if sb and sb.partner == a:
                sb.partner = None

        if notify:
            self.safe_send_to(b, f"INFO '{a}' disconnected. You are unpaired now.")
            self.safe_send_to(a, f"INFO Unpaired from '{b}'.")

    def forward_message(self, sender: str, text: str):
        session = self.get_session(sender)
        if not session:
            return

        partner = session.partner
        if not partner:
            self.safe_send_to(sender, "ERR You are not paired. Use CHAT <username> first.")
            return
        if not text:
            self.safe_send_to(sender, "ERR MSG cannot be empty")
            return

        self.safe_send_to(partner, f"FROM {sender} {text}")
        self.safe_send_to(sender, "OK Sent")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=12345)
    args = ap.parse_args()

    ChatServer(args.host, args.port).start()


if __name__ == "__main__":
    main()
