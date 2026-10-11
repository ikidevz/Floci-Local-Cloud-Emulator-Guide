import json
import pathlib
import re
import sys
import threading
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import kv  # noqa: E402


class FakeVault(BaseHTTPRequestHandler):
    """Just enough Key Vault to exercise OUR client wiring: the 401 challenge handshake real Key Vault performs, then set/get/versions."""
    store, seen_auth = {}, []

    def log_message(self, *a):
        pass

    def _send(self, code, body=None, headers=None):
        raw = json.dumps(body).encode() if body is not None else b""
        self.send_response(code)
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _bundle(self, name, ver, rec):
        return {"value": rec["value"], "id": f"https://{self.headers['Host']}/secrets/{name}/{ver}",
                "attributes": {"enabled": True, "created": rec["t"], "updated": rec["t"]}}

    def _route(self, method):
        auth = self.headers.get("Authorization")
        n = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(n) if n else b""
        if not auth:                                                   # real Key Vault: unauthenticated first request -> 401 + challenge
            return self._send(401, {"error": {"code": "Unauthorized"}}, {"WWW-Authenticate": 'Bearer authorization="https://login.example/tenant", resource="https://vault.azure.net"'})
        FakeVault.seen_auth.append(auth)
        m = re.match(r"^/devstoreaccount1-keyvault/secrets/([^/?]+)(?:/(versions|[0-9a-f]{32}))?", self.path)
        if not m:
            return self._send(404, {"error": {"code": "NotFound"}})
        name, tail = m.groups()
        vers = FakeVault.store.setdefault(name, [])
        if method == "PUT":
            rec = {"value": json.loads(body)["value"], "t": 1700000000 + len(vers)}
            ver = uuid.uuid4().hex
            vers.append((ver, rec))
            return self._send(200, self._bundle(name, ver, rec))
        if method == "GET" and tail == "versions":
            return self._send(200, {"value": [{"id": f"https://{self.headers['Host']}/secrets/{name}/{v}", "attributes": {"enabled": True, "created": r["t"]}} for v, r in vers]})
        if method == "GET" and vers:
            ver, rec = next(((v, r) for v, r in vers if v == tail), None) if tail else vers[-1]
            return self._send(200, self._bundle(name, ver, rec))
        return self._send(404, {"error": {"code": "SecretNotFound"}})

    def do_PUT(self):
        self._route("PUT")

    def do_GET(self):
        self._route("GET")


@pytest.fixture
def fake_vault_client(monkeypatch):
    monkeypatch.delenv("VAULT_URL", raising=False)
    FakeVault.store, FakeVault.seen_auth = {}, []
    srv = HTTPServer(("127.0.0.1", 0), FakeVault)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield kv.client(endpoint=f"http://127.0.0.1:{srv.server_port}")
    srv.shutdown()


@pytest.fixture(scope="session")
def floci_client():
    """The real thing: auto-skips when Floci AZ is not answering."""
    c = kv.client(retry_total=0, connection_timeout=2)
    try:
        list(c.list_properties_of_secrets())
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"Floci AZ Key Vault not reachable ({type(e).__name__}); run `docker compose up -d`")
    return c
