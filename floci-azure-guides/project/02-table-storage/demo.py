"""python demo.py -> exercises ledger, ETag race and atomic claim."""
import uuid

import tablekit as tk

t = tk.table(f"demo{uuid.uuid4().hex[:8]}")

tk.ledger_put(t, "claims_20260930.csv.gz", status="TRANSFORMED", etag="abc")
tk.ledger_put(t, "claims_20260930.csv.gz", status="LOADED", etag="abc")           # same key: replaced
print("ledger  ", dict(tk.ledger_get(t, "claims_20260930.csv.gz")), "| rows:", len(tk.ledger_list(t, "file")))

row, etag = tk.read(t, "D-01", "V-0001")
assert row is None and tk.write_if_unchanged(t, {"PartitionKey": "D-01", "RowKey": "V-0001", "status": "OK"}, None)
mine, my_etag = tk.read(t, "D-01", "V-0001")
other, other_etag = tk.read(t, "D-01", "V-0001")                                    # a second consumer reads the same row
assert tk.write_if_unchanged(t, {**dict(other), "status": "EXCURSION"}, other_etag)  # ...and writes first
stale = tk.write_if_unchanged(t, {**dict(mine), "status": "CRITICAL"}, my_etag)      # our write is now stale
print("hot row ", "stale write refused" if not stale else "BUG: stale write accepted", "| status =", tk.read(t, "D-01", "V-0001")[0]["status"])

print("claim   ", tk.claim(t, "hash", "sha256-of-file", name="BAN_20261002_001.csv"), "then", tk.claim(t, "hash", "sha256-of-file", name="copy.csv"))
