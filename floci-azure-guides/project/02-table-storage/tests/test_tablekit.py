import json

import tablekit as tk


def test_pack_is_compact_json():                                   # pure, needs no endpoint
    assert tk.pack({"a": 1, "b": [1, 2]}) == '{"a":1,"b":[1,2]}' and json.loads(tk.pack({"x": "y"})) == {"x": "y"}


def test_ledger_upsert_replaces_not_duplicates(t):
    tk.ledger_put(t, "f1", status="TRANSFORMED")
    tk.ledger_put(t, "f1", status="LOADED")
    assert tk.ledger_get(t, "f1")["status"] == "LOADED" and len(tk.ledger_list(t, "file")) == 1
    assert tk.ledger_get(t, "missing") is None


def test_ledger_partitions_are_separate(t):
    tk.ledger_put(t, "f1", partition="file", status="LOADED")
    tk.ledger_put(t, "b-1", partition="run", status="SUCCEEDED")
    assert [e["RowKey"] for e in tk.ledger_list(t, "run")] == ["b-1"]


def test_insert_only_when_absent(t):
    assert tk.write_if_unchanged(t, {"PartitionKey": "D", "RowKey": "V1", "n": 1}, None) is True
    assert tk.write_if_unchanged(t, {"PartitionKey": "D", "RowKey": "V1", "n": 2}, None) is False   # already exists
    assert tk.read(t, "D", "V1")[0]["n"] == 1


def test_stale_etag_write_is_refused(t):
    tk.write_if_unchanged(t, {"PartitionKey": "D", "RowKey": "V1", "n": 0}, None)
    (a, ea), (b, eb) = tk.read(t, "D", "V1"), tk.read(t, "D", "V1")
    assert tk.write_if_unchanged(t, {**dict(a), "n": 1}, ea) is True          # first writer wins
    assert tk.write_if_unchanged(t, {**dict(b), "n": 2}, eb) is False         # second is refused: re-read and redo
    assert tk.read(t, "D", "V1")[0]["n"] == 1


def test_fresh_etag_write_succeeds_again(t):
    tk.write_if_unchanged(t, {"PartitionKey": "D", "RowKey": "V1", "n": 0}, None)
    for n in (1, 2, 3):                                                       # the retry loop: re-read, write, repeat
        row, etag = tk.read(t, "D", "V1")
        assert tk.write_if_unchanged(t, {**dict(row), "n": n}, etag)
    assert tk.read(t, "D", "V1")[0]["n"] == 3


def test_claim_first_writer_owns_it(t):
    assert tk.claim(t, "hash", "sha-1", name="a.csv") is True
    assert tk.claim(t, "hash", "sha-1", name="b.csv") is False                # the second copy is a DUPLICATE
    assert tk.read(t, "hash", "sha-1")[0]["name"] == "a.csv"
