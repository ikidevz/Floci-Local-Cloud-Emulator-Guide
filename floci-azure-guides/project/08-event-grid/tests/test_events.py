import json
import pathlib
import sys
import threading
import urllib.error
import urllib.request

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import gate_events as G  # noqa: E402
import grid  # noqa: E402
import hook_server  # noqa: E402


def ev(subject, typ=G.BLOB_CREATED):
    return {"id": "1", "eventType": typ, "subject": subject, "data": {}}


def test_validation_handshake_echoes_the_code():
    assert G.handle_events([{"eventType": G.VALIDATION, "data": {"validationCode": "abc-123"}}], lambda n: 1 / 0) == {"validationResponse": "abc-123"}


def test_only_inbox_blob_created_events_are_processed():
    seen = []
    out = G.handle_events([ev(G.INBOX + "a.csv"), ev("/blobServices/default/containers/accepted/blobs/a.csv"), ev("/blobServices/default/containers/reports/blobs/a.json"),
                           ev(G.INBOX + "b.csv", "Microsoft.Storage.BlobDeleted"), {"eventType": G.BLOB_CREATED}], lambda n: seen.append(n) or n)
    assert seen == ["a.csv"] and out == {"processed": ["a.csv"], "ignored": 4}      # layer 2 of the self-trigger guard


def test_nested_inbox_names_keep_their_path():
    assert G.handle_events([ev(G.INBOX + "BAN/2026/x.csv")], lambda n: n)["processed"] == ["BAN/2026/x.csv"]


def test_subscription_filter_matches_the_handlers_guard():
    f = grid.subscription_body("http://h/gate")["properties"]["filter"]
    assert f["subjectBeginsWith"] == G.INBOX.split("blobs/")[0] and f["includedEventTypes"] == [G.BLOB_CREATED]
    assert grid.subscription_body("http://h/gate")["properties"]["destination"]["endpointType"] == "WebHook"      # the only destination Floci supports


def test_published_event_matches_what_the_handler_expects():
    e = grid.blob_created("inbox", "x.csv")
    assert e["subject"] == G.INBOX + "x.csv" and e["eventType"] == G.BLOB_CREATED


def test_topic_endpoint_is_keyed_on_topic_name():
    assert grid.TOPIC_ID.endswith(f"/topics/{grid.TOPIC}")


@pytest.fixture
def server():
    hook_server.RECEIVED.update(processed=[], ignored=0)
    s = hook_server.make_server(0)
    threading.Thread(target=s.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{s.server_port}"
    s.shutdown()


def post(url, body):
    req = urllib.request.Request(url, method="POST", data=json.dumps(body).encode(), headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def test_server_answers_handshake_events_and_cloudevents_probe(server):
    assert post(server + "/gate", [{"eventType": G.VALIDATION, "data": {"validationCode": "zz"}}]) == (200, {"validationResponse": "zz"})
    assert post(server + "/gate", [ev(G.INBOX + "a.csv"), ev("/blobServices/default/containers/accepted/blobs/x")]) == (200, {"processed": ["a.csv"], "ignored": 1})
    assert json.loads(urllib.request.urlopen(server + "/received").read()) == {"processed": ["a.csv"], "ignored": 1}
    r = urllib.request.urlopen(urllib.request.Request(server + "/gate", method="OPTIONS", headers={"WebHook-Request-Origin": "eventgrid.azure.net"}))
    assert r.status == 200 and r.headers["WebHook-Allowed-Origin"] == "eventgrid.azure.net"


def test_a_failure_is_a_500_so_event_grid_retries(server, monkeypatch):
    monkeypatch.setattr(hook_server, "process", lambda n: (_ for _ in ()).throw(RuntimeError("storage down")))
    code, body = post(server + "/gate", [ev(G.INBOX + "a.csv")])
    assert code == 500 and "storage down" in body["error"]
