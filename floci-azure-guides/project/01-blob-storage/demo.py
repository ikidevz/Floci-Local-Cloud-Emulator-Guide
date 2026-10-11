"""python demo.py  -> lands a gzip CSV, lists the partition, replays it (idempotent), rejects a row."""
import blobzones as bz

svc = bz.service()
bz.ensure_zones(svc)

day, name = "2026-10-01", "claims_20260930.csv.gz"
key = bz.partition("claims", day) + name
payload = bz.to_gz_csv(["claim_id", "billed_total"], [["C-1", "10.00"], ["C-2", "20.00"]])

bz.put(svc, "landing", key, payload)
print("landed  ", bz.list_prefix(svc, "landing", bz.partition("claims", day)))

bz.put(svc, "landing", key, payload)                       # same name again: replaced, not duplicated
files = bz.list_prefix(svc, "landing", bz.partition("claims", day))
assert len(files) == 1, files
print("replayed", files, "(still one file)")

header, rows = bz.from_gz_csv(bz.get(svc, "landing", key))
print("read    ", header, rows)

bz.put(svc, "rejects", "batch=demo/claims_2026-10-01.csv.gz", bz.to_gz_csv(["reason", "raw"], [["BAD_NPI", "C-2,..."]]))
bz.promote(svc, "landing", key, "quarantine", name)
print("OK")
