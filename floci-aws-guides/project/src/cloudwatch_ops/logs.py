import time

from botocore.exceptions import ClientError

from src.common.aws import client

logs = client("logs")


def ensure_group(group: str, retention_days: int = 14) -> None:
    try:
        logs.create_log_group(logGroupName=group)
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceAlreadyExistsException":
            raise
    logs.put_retention_policy(logGroupName=group, retentionInDays=retention_days)


def ensure_stream(group: str, stream: str) -> None:
    try:
        logs.create_log_stream(logGroupName=group, logStreamName=stream)
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceAlreadyExistsException":
            raise


def put(group: str, stream: str, messages: list[str]) -> None:
    now = int(time.time() * 1000)
    logs.put_log_events(logGroupName=group, logStreamName=stream,
                        logEvents=[{"timestamp": now + i, "message": m} for i, m in enumerate(messages)])


def search(group: str, pattern: str = "", minutes: int = 60) -> list[str]:
    start = int((time.time() - minutes * 60) * 1000)
    out: list[str] = []
    for page in logs.get_paginator("filter_log_events").paginate(
            logGroupName=group, filterPattern=pattern, startTime=start):
        out += [e["message"] for e in page["events"]]
    return out


def put_metric_filter(group: str, filter_name: str, pattern: str, metric: str, namespace: str) -> None:
    """Turn log lines matching `pattern` into a metric (e.g. count of ERROR lines)."""
    logs.put_metric_filter(
        logGroupName=group, filterName=filter_name, filterPattern=pattern,
        metricTransformations=[{"metricName": metric, "metricNamespace": namespace,
                                "metricValue": "1", "defaultValue": 0}])
