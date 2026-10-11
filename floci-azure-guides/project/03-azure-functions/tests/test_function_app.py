import json
import pathlib
import sys

import azure.functions as func

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))


def load(monkeypatch, **env):
    monkeypatch.delenv("ENABLE_TIMER", raising=False)
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    sys.modules.pop("function_app", None)
    import function_app
    return function_app


def call(fa, body=None):
    req = func.HttpRequest("POST", "/api/run", body=json.dumps(body).encode() if body is not None else b"")
    return fa.run.build().get_user_function()(req)


class FakeBlob:
    def __init__(self):
        self.store = {}

    def get_container_client(self, name):
        outer = self

        class C:
            def exists(self):
                return True

            def get_blob_client(self, blob):
                class B:
                    def upload_blob(self, data, overwrite=False):
                        outer.store[(name, blob)] = data
                return B()
        return C()


def test_http_trigger_returns_200_and_writes_deterministic_blob(monkeypatch):
    fa = load(monkeypatch, MY_MARKER="m")
    fake = FakeBlob()
    monkeypatch.setattr(fa, "_blob", lambda: fake)
    r = call(fa, {"name": "x"})
    assert r.status_code == 200 and json.loads(r.get_body())["marker"] == "m"
    assert ("fn-out", "hello-x.json") in fake.store                         # same input -> same blob name -> replay-safe


def test_failure_is_a_loud_500_not_a_silent_success(monkeypatch):
    fa = load(monkeypatch)
    monkeypatch.setattr(fa, "_blob", lambda: FakeBlob())
    r = call(fa, {"fail": True})
    assert r.status_code == 500 and json.loads(r.get_body())["status"] == "FAILED"


def test_missing_setting_is_a_500(monkeypatch):
    fa = load(monkeypatch)
    monkeypatch.delenv("STORAGE_CONNECTION_STRING", raising=False)
    assert call(fa, {"name": "x"}).status_code == 500


def test_timer_is_registered_only_when_enabled(monkeypatch):
    def names(fa):
        return {f.get_function_name() for f in fa.app.get_functions()}
    assert names(load(monkeypatch)) == {"run"}                              # what Floci AZ sees
    assert names(load(monkeypatch, ENABLE_TIMER="1")) == {"run", "nightly"}  # what real Azure sees
