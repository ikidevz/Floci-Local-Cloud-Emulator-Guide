"""python hook_server.py [port]  -> a tiny local webhook standing in for the gate Function. START IT BEFORE provision.py:
creating the subscription makes Floci AZ call this URL immediately with the validation handshake."""
import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

from gate_events import handle_events

RECEIVED = {"processed": [], "ignored": 0}


def process(name):
    RECEIVED["processed"].append(name)                 # idempotent by construction: a set-like record of file names
    return name


class Hook(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _json(self, code, body, headers=None):
        raw = json.dumps(body).encode()
        self.send_response(code)
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_OPTIONS(self):                              # CloudEvents abuse-protection probe
        self._json(200, {}, {"WebHook-Allowed-Origin": self.headers.get("WebHook-Request-Origin", "*"), "Allow": "POST, OPTIONS"})

    def do_GET(self):
        self._json(200, RECEIVED)

    def do_POST(self):
        try:
            events = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)) or b"[]")
            out = handle_events(events, process)
            RECEIVED["ignored"] += out.get("ignored", 0)
            self._json(200, out)
        except Exception as e:                         # non-2xx => Event Grid retries (and eventually gives up): exactly what we want
            self._json(500, {"error": f"{type(e).__name__}: {e}"})


def make_server(port=8099):
    return HTTPServer(("0.0.0.0", port), Hook)


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8099
    print(f"webhook listening on :{port}  (POST /gate, GET /received)")
    make_server(port).serve_forever()
