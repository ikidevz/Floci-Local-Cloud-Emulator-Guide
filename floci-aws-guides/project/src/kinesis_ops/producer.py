import json

from src.common.aws import client

kds = client("kinesis")
MAX_BATCH = 500  # API limit per put_records call


def put(stream: str, record: dict, partition_key: str) -> dict:
    r = kds.put_record(StreamName=stream, Data=json.dumps(record).encode(), PartitionKey=partition_key)
    return {"shard": r["ShardId"], "seq": r["SequenceNumber"]}


def put_many(stream: str, records: list[dict], key_field: str) -> int:
    """Batch put; retries partial failures up to 3 times. Returns the number still failed."""
    failed_total = 0
    for i in range(0, len(records), MAX_BATCH):
        batch = [{"Data": json.dumps(r).encode(), "PartitionKey": str(r[key_field])}
                 for r in records[i:i + MAX_BATCH]]
        for _ in range(3):
            resp = kds.put_records(StreamName=stream, Records=batch)
            if resp["FailedRecordCount"] == 0:
                batch = []
                break
            batch = [b for b, res in zip(batch, resp["Records"]) if "ErrorCode" in res]
        failed_total += len(batch)
    return failed_total
