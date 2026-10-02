import base64
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest


def fake_jwt(claims):
    enc = lambda v: base64.urlsafe_b64encode(json.dumps(v).encode()).rstrip(b"=").decode()
    return enc({"alg": "HS256"}) + "." + enc(claims) + ".sig"


class FakeACP:
    """In-memory acp-server."""

    def __init__(self):
        self.lock = threading.Lock()
        self.token = fake_jwt({"tenantId": "tenant-42"})
        self.fail_registers = 0
        self.reject_401_once = False
        self.refreshed = 0
        self.registrations, self.vocabulary, self.rules, self.captures = [], [], [], []
        acp = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def reply(self, status, body=None):
                raw = json.dumps(body or {}).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])) or b"null")
                auth = (self.headers.get("Authorization") or "").replace("Bearer ", "")
                with acp.lock:
                    if self.path == "/client/register":
                        if acp.fail_registers:
                            acp.fail_registers -= 1
                            return self.reply(503)
                        acp.registrations.append(body)
                        return self.reply(200, {"token": acp.token, "refreshToken": "r1", "expiresInMs": 60000})
                    if self.path == "/auth/token/refresh":
                        acp.refreshed += 1
                        acp.token = fake_jwt({"tenantId": "tenant-42", "n": acp.refreshed})
                        return self.reply(200, {"token": acp.token, "expiresInMs": 60000})
                    if acp.reject_401_once and self.path == "/api/event":
                        acp.reject_401_once = False
                        return self.reply(401)
                    if auth != acp.token:
                        return self.reply(401)
                    if self.path == "/api/vocabulary":
                        acp.vocabulary.append(body)
                    elif self.path == "/api/vocabulary/rules/process":
                        acp.rules.append(body)
                    elif self.path == "/api/event":
                        acp.captures.extend(body)
                    else:
                        return self.reply(404)
                    return self.reply(200)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def of_type(self, event_type):
        with self.lock:
            return [c for c in self.captures if c["event"]["eventType"] == event_type]


@pytest.fixture
def acp():
    server = FakeACP()
    yield server
    server.server.shutdown()
