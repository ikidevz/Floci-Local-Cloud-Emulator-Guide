import blobzones as bz


def test_gz_csv_roundtrip_is_pure():                          # needs no endpoint
    header, rows = bz.from_gz_csv(bz.to_gz_csv(["a", "b"], [["1", "x,y"], ["2", 'q"r']]))
    assert header == ["a", "b"] and rows == [["1", "x,y"], ["2", 'q"r']]


def test_partition_prefix():
    assert bz.partition("claims", "2026-10-01") == "claims/dt=2026-10-01/"


def test_ensure_zones_is_idempotent(svc):
    bz.ensure_zones(svc)
    bz.ensure_zones(svc)                                      # second call must not raise
    assert {c.name for c in svc.list_containers()} >= set(bz.ZONES)


def test_prefix_listing_sees_only_its_partition(svc, box):
    bz.put(svc, box, "claims/dt=2026-10-01/a.csv.gz", b"1")
    bz.put(svc, box, "claims/dt=2026-10-02/b.csv.gz", b"2")
    assert [n for n, _ in bz.list_prefix(svc, box, bz.partition("claims", "2026-10-01"))] == ["claims/dt=2026-10-01/a.csv.gz"]


def test_same_name_replaces_and_etag_changes(svc, box):
    bz.put(svc, box, "x/f.csv.gz", b"v1")
    (_, e1), = bz.list_prefix(svc, box, "x/")
    bz.put(svc, box, "x/f.csv.gz", b"v2")
    listing = bz.list_prefix(svc, box, "x/")
    assert len(listing) == 1 and listing[0][1] != e1          # a changed ETag is how a ledger notices a re-delivered file
    assert bz.get(svc, box, "x/f.csv.gz") == b"v2"


def test_promote_keeps_bytes_exact(svc, box):
    raw = bytes(range(256)) * 4                               # binary garbage, as the DQ gate must keep it
    bz.put(svc, box, "inbox/bad.csv", raw)
    bz.promote(svc, box, "inbox/bad.csv", box, "quarantine/bad.csv")
    assert bz.get(svc, box, "quarantine/bad.csv") == raw
