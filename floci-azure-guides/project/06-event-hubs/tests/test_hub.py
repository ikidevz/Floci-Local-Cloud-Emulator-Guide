import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import bridge  # noqa: E402
import hub  # noqa: E402
import producer  # noqa: E402


def test_batches_keep_each_keys_order_and_size_limit():
    events = [(k, f"{k}-{i}") for i in range(250) for k in ("A", "B")]          # interleaved
    batches = producer.plan_batches(events, max_batch=100)
    assert all(len(b) <= 100 for _, b in batches)
    for k in ("A", "B"):
        flat = [x for key, b in batches if key == k for x in b]
        assert flat == [f"{k}-{i}" for i in range(250)]                          # order preserved across chunks
    assert {key for key, _ in batches} == {"A", "B"}                             # a batch never mixes keys (one partition key per batch)


def test_key_of_handles_garbage():
    assert producer.key_of(json.dumps({"vehicle_id": "V-1"})) == "V-1"
    assert producer.key_of('{"vehicle_id": "V-1", "te') == "malformed"          # truncated JSON still gets a key
    assert producer.key_of("{}") == "malformed"


def test_connection_string_uses_development_emulator():
    assert "UseDevelopmentEmulator=true" in hub.conn_str() and "Endpoint=sb://localhost;" in hub.conn_str()


class Ev:
    def __init__(self, b): self.b = b
    def body_as_str(self): return self.b


class PC:
    partition_id = "0"
    def __init__(self): self.checkpointed = None
    def update_checkpoint(self, event=None): self.checkpointed = event


def test_checkpoint_advances_only_after_200():
    pc, evs = PC(), [Ev("a"), Ev("b")]
    bridge.make_handler(lambda bodies: True)(pc, evs)
    assert pc.checkpointed is evs[-1]


def test_no_checkpoint_when_function_keeps_failing_and_it_retries_first():
    pc, calls, logs = PC(), [], []
    def post(bodies):
        calls.append(bodies); return False
    bridge.make_handler(post, attempts=3, sleep=lambda s: None, log=logs.append)(pc, [Ev("a")])
    assert pc.checkpointed is None and len(calls) == 3 and "NOT advanced" in logs[0]


def test_recovers_on_second_attempt():
    pc, n = PC(), {"i": 0}
    def post(bodies):
        n["i"] += 1; return n["i"] >= 2
    bridge.make_handler(post, attempts=3, sleep=lambda s: None)(pc, [Ev("a")])
    assert pc.checkpointed is not None


def test_empty_batch_is_ignored():
    pc = PC()
    bridge.make_handler(lambda b: (_ for _ in ()).throw(AssertionError("must not call")))(pc, [])
    assert pc.checkpointed is None


def test_post_events_treats_unreachable_as_failure():
    assert bridge.post_events(["x"], url="http://127.0.0.1:9/none", timeout=1) is False
