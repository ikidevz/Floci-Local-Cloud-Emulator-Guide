"""The Blob patterns the three pipelines rely on, and nothing else:
zones (containers), partitioned prefixes (feed/dt=DATE/), deterministic names, gzip CSV, byte-exact promotion."""
import csv
import gzip
import io

from azure.storage.blob import BlobServiceClient

from floci import storage_conn_str

# batch ETL: landing/stage/rejects | streaming: archive/rejects/registry | DQ gate: inbox/accepted/quarantine/rejects/reports/registry
ZONES = ("landing", "stage", "rejects", "archive", "inbox", "accepted", "quarantine", "reports", "registry")


def service(**kw):
    return BlobServiceClient.from_connection_string(storage_conn_str(), **kw)


def ensure_zones(svc, zones=ZONES):
    """Idempotent: containers are created only if missing (Floci creates none for you)."""
    for z in zones:
        c = svc.get_container_client(z)
        if not c.exists():
            c.create_container()


def put(svc, container, name, data: bytes):
    """overwrite=True + a DETERMINISTIC name = a replayed job replaces its own output instead of duplicating it."""
    svc.get_blob_client(container, name).upload_blob(data, overwrite=True)


def get(svc, container, name) -> bytes:
    return svc.get_blob_client(container, name).download_blob().readall()


def partition(feed, day):
    return f"{feed}/dt={day}/"


def list_prefix(svc, container, prefix):
    """-> [(name, etag)] under a prefix. The ETag is what a file ledger compares to spot a re-delivered file."""
    return [(b.name, b.etag.strip('"')) for b in svc.get_container_client(container).list_blobs(name_starts_with=prefix)]


def to_gz_csv(header, rows) -> bytes:
    buf = io.StringIO(newline="")
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(header)
    w.writerows(rows)
    return gzip.compress(buf.getvalue().encode())


def from_gz_csv(raw: bytes):
    r = csv.reader(io.StringIO(gzip.decompress(raw).decode()))
    header = next(r, [])
    return header, list(r)


def promote(svc, src, name, dst, dst_name=None):
    """Copy bytes untouched from one zone to another (quarantine keeps the ORIGINAL bytes as evidence)."""
    put(svc, dst, dst_name or name, get(svc, src, name))
