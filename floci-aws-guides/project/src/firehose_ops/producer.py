import json
import time

from src.common.aws import client

fh = client("firehose")
s3 = client("s3")


def send(stream: str, records: list[dict]) -> int:
    """put_record_batch allows 500 records / 4 MiB. Append \\n so the output is newline-delimited JSON."""
    failed = 0
    for i in range(0, len(records), 500):
        chunk = [{"Data": (json.dumps(r) + "\n").encode()} for r in records[i:i + 500]]
        failed += fh.put_record_batch(DeliveryStreamName=stream, Records=chunk)["FailedPutCount"]
    return failed


def wait_for_objects(bucket: str, prefix: str, timeout: int = 150, interval: float = 3) -> list[str]:
    """Firehose flushes only when the buffer interval/size is hit, so poll S3."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        keys = [o["Key"] for o in s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])]
        if keys:
            return keys
        time.sleep(interval)
    raise TimeoutError("Firehose has not flushed to S3. Lower BufferingHints or set "
                       "FLOCI_SERVICES_FIREHOSE_FLUSH_RECORD_COUNT=1 for local dev.")
