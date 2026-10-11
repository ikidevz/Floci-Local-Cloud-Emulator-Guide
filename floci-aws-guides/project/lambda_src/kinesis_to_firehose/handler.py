"""Bridge: Kinesis stream -> Firehose (Floci has no KinesisStreamAsSource, and this works everywhere)."""
import base64
import os

import boto3

firehose = boto3.client("firehose")


def lambda_handler(event, context):
    stream = os.environ["FIREHOSE_STREAM"]
    records = []
    for rec in event.get("Records", []):
        data = base64.b64decode(rec["kinesis"]["data"])
        if not data.endswith(b"\n"):
            data += b"\n"
        records.append({"Data": data})
    failed = 0
    for i in range(0, len(records), 500):
        failed += firehose.put_record_batch(DeliveryStreamName=stream,
                                            Records=records[i:i + 500])["FailedPutCount"]
    if failed:
        raise RuntimeError(f"{failed} records failed to reach Firehose")  # Lambda/Kinesis will retry
    return {"forwarded": len(records)}
