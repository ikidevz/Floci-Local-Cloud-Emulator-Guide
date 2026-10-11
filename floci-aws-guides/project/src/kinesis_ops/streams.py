from botocore.exceptions import ClientError

from src.common.aws import client
from src.common.waiters import wait_until

kds = client("kinesis")


def create(stream: str, shards: int = 2) -> str:
    """Create the stream (idempotent), wait until ACTIVE, return its ARN."""
    try:
        kds.create_stream(StreamName=stream, ShardCount=shards)
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceInUseException":
            raise
    wait_until(lambda: kds.describe_stream_summary(StreamName=stream)
               ["StreamDescriptionSummary"]["StreamStatus"], done={"ACTIVE"}, timeout=60)
    return kds.describe_stream_summary(StreamName=stream)["StreamDescriptionSummary"]["StreamARN"]


def shard_ids(stream: str) -> list[str]:
    return [s["ShardId"] for p in kds.get_paginator("list_shards").paginate(StreamName=stream)
            for s in p["Shards"]]


def set_retention(stream: str, hours: int = 48) -> None:
    kds.increase_stream_retention_period(StreamName=stream, RetentionPeriodHours=hours)


def delete(stream: str) -> None:
    kds.delete_stream(StreamName=stream, EnforceConsumerDeletion=True)
