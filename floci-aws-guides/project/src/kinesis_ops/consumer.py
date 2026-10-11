import json
import time
from typing import Callable

from src.common.aws import client
from src.kinesis_ops.streams import shard_ids

kds = client("kinesis")


def read_shard(stream: str, shard_id: str, handler: Callable[[dict], None],
               from_start: bool = True, idle_stops: int = 2) -> int:
    """Read one shard until `idle_stops` consecutive empty polls. Returns records handled."""
    it = kds.get_shard_iterator(
        StreamName=stream, ShardId=shard_id,
        ShardIteratorType="TRIM_HORIZON" if from_start else "LATEST")["ShardIterator"]
    count = idle = 0
    while it and idle < idle_stops:
        resp = kds.get_records(ShardIterator=it, Limit=100)
        for rec in resp["Records"]:
            handler(json.loads(rec["Data"]))
            count += 1
        idle = idle + 1 if not resp["Records"] else 0
        it = resp.get("NextShardIterator")
        time.sleep(0.5)  # stay under 5 GetRecords calls/sec/shard
    return count


def read_all(stream: str, handler: Callable[[dict], None]) -> int:
    return sum(read_shard(stream, s, handler) for s in shard_ids(stream))
