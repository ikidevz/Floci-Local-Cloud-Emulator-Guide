# 08 — CloudWatch

> [Index](README.md) · [← 07 — IAM](07-iam.md) · [09 — RDS (Postgres) →](09-rds-postgres.md)

Logs (groups, streams, events and metric filters), custom metrics, and alarms that notify SNS.

---

## 1. Documentation

### Overview

- **Logs**: a log group holds log streams; each stream holds events. Metric filters turn matching lines into metrics.
- **Metrics**: custom data points in a namespace, with optional dimensions.
- **Alarms**: watch a metric against a threshold and notify an SNS topic when the state changes.

### At a glance

| Item | Value |
|------|-------|
| boto3 clients | `logs`, `cloudwatch` |
| Terraform endpoints | `logs`, `cloudwatch` |
| Needs first | an SNS topic ([06](06-sns.md)) for alarm actions |
| Docker socket | not required |

### Key concepts

- **Retention**: set per log group (`put_retention_policy`).
- **Filter pattern**: `ERROR` matches lines containing that word. The same syntax drives `filter_log_events` and metric filters.
- **`TreatMissingData=notBreaching`**: no data does not trigger the alarm.
- **`set_alarm_state`**: forces an alarm into a given state, handy for testing notifications.

### Gotchas

- Lambda logs go to `/aws/lambda/<function-name>`.
- Metric filters count only lines ingested **after** the filter exists.
- Alarms evaluate on a schedule; use `set_alarm_state` to test notification paths.
- Metric timestamps must be recent.

---

## 2. Basic Usage

> Prerequisite: Floci is running and the `AWS_*` environment variables are set ([guide 00](00-setup-floci.md)).

### Logs

```python
import time
import boto3

logs, cw = boto3.client("logs"), boto3.client("cloudwatch")
group, stream = "/demo/ingest", "run-1"

logs.create_log_group(logGroupName=group)
logs.create_log_stream(logGroupName=group, logStreamName=stream)

now = int(time.time() * 1000)
logs.put_log_events(logGroupName=group, logStreamName=stream, logEvents=[
    {"timestamp": now,     "message": "INFO start"},
    {"timestamp": now + 1, "message": "ERROR boom"},
    {"timestamp": now + 2, "message": "INFO end"}])

events = logs.filter_log_events(logGroupName=group, filterPattern="ERROR")["events"]
print([e["message"] for e in events])          # ['ERROR boom']
```

### Metrics and an alarm

```python
NS = "Demo/Pipeline"

cw.put_metric_data(Namespace=NS, MetricData=[
    {"MetricName": "FailedJobs", "Value": 1, "Unit": "Count"}])

cw.put_metric_alarm(
    AlarmName="demo-failed-jobs", Namespace=NS, MetricName="FailedJobs",
    Statistic="Sum", Period=60, EvaluationPeriods=1, Threshold=1,
    ComparisonOperator="GreaterThanOrEqualToThreshold", TreatMissingData="notBreaching")

# Force the state to test the notification path without waiting for evaluation
cw.set_alarm_state(AlarmName="demo-failed-jobs", StateValue="ALARM", StateReason="test")
state = cw.describe_alarms(AlarmNames=["demo-failed-jobs"])["MetricAlarms"][0]["StateValue"]
print(state)                                   # ALARM
```

---

## 3. Examples

### Example 1 — Logs: groups, streams, put, search, metric filter

`src/cloudwatch_ops/logs.py`

```python
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
```

### Example 2 — Custom metrics: put and read statistics

`src/cloudwatch_ops/metrics.py`

```python
from datetime import datetime, timedelta, timezone

from src.common.aws import client

cw = client("cloudwatch")
NAMESPACE = "FlociDemo/Pipeline"


def put(metric: str, value: float, unit: str = "Count", **dims: str) -> None:
    cw.put_metric_data(Namespace=NAMESPACE, MetricData=[{
        "MetricName": metric, "Value": value, "Unit": unit,
        "Timestamp": datetime.now(timezone.utc),
        "Dimensions": [{"Name": k, "Value": v} for k, v in dims.items()]}])


def stats(metric: str, minutes: int = 60, period: int = 60, stat: str = "Sum", **dims: str) -> list[dict]:
    end = datetime.now(timezone.utc)
    resp = cw.get_metric_statistics(
        Namespace=NAMESPACE, MetricName=metric,
        Dimensions=[{"Name": k, "Value": v} for k, v in dims.items()],
        StartTime=end - timedelta(minutes=minutes), EndTime=end + timedelta(minutes=1),
        Period=period, Statistics=[stat])
    return sorted(resp["Datapoints"], key=lambda d: d["Timestamp"])
```

### Example 3 — Alarms that notify SNS

`src/cloudwatch_ops/alarms.py`

```python
from src.common.aws import client

cw = client("cloudwatch")


def failure_alarm(alarm_name: str, metric: str, namespace: str, sns_topic_arn: str,
                  threshold: float = 1, periods: int = 1, period_s: int = 60, **dims: str) -> None:
    cw.put_metric_alarm(
        AlarmName=alarm_name, AlarmDescription="Pipeline failure alarm",
        Namespace=namespace, MetricName=metric,
        Dimensions=[{"Name": k, "Value": v} for k, v in dims.items()],
        Statistic="Sum", Period=period_s, EvaluationPeriods=periods,
        Threshold=threshold, ComparisonOperator="GreaterThanOrEqualToThreshold",
        TreatMissingData="notBreaching",
        AlarmActions=[sns_topic_arn], OKActions=[sns_topic_arn])


def state(alarm_name: str) -> str:
    alarms = cw.describe_alarms(AlarmNames=[alarm_name])["MetricAlarms"]
    return alarms[0]["StateValue"] if alarms else "MISSING"


def force_state(alarm_name: str, value: str = "ALARM", reason: str = "test") -> None:
    """Deterministically test the notification path without waiting for metrics."""
    cw.set_alarm_state(AlarmName=alarm_name, StateValue=value, StateReason=reason)
```

### Example 4 — All three together

```python
from src.common.aws import name
from src.cloudwatch_ops import alarms, logs, metrics
from src.sns_ops.topics import create_topic

g = f"/{name('ingest')}"
logs.ensure_group(g); logs.ensure_stream(g, "run-1")
logs.put(g, "run-1", ["INFO start", "ERROR boom", "INFO end"])
print(logs.search(g, "ERROR"))                       # ['ERROR boom']

topic = create_topic(name("alerts"))
metrics.put("FailedJobs", 1, stage="ingest")
alarms.failure_alarm(name("failed-jobs"), "FailedJobs", metrics.NAMESPACE, topic, stage="ingest")
alarms.force_state(name("failed-jobs"), "ALARM")     # tests the SNS path deterministically
print(alarms.state(name("failed-jobs")))
```

### Example 5 — Test

`tests/test_03_data_services.py`

```python
from src.common.aws import client, name
from src.cloudwatch_ops import alarms, logs, metrics
from src.sns_ops import topics


def test_cloudwatch():
    g = f"/{name('ingest')}"
    logs.ensure_group(g)
    logs.ensure_stream(g, "run-1")
    logs.put(g, "run-1", ["INFO start", "ERROR boom", "INFO end"])
    assert logs.search(g, "ERROR") == ["ERROR boom"]
    logs.put_metric_filter(g, "errors", "ERROR", "ErrorCount", metrics.NAMESPACE)

    metrics.put("FailedJobs", 1, stage="ingest")
    topic = topics.create_topic(name("cw-alerts"))
    alarms.failure_alarm(name("failed-jobs"), "FailedJobs", metrics.NAMESPACE, topic, stage="ingest")
    alarms.force_state(name("failed-jobs"), "ALARM")
    assert alarms.state(name("failed-jobs")) == "ALARM"
    assert client("cloudwatch").describe_alarms(AlarmNames=[name("failed-jobs")])["MetricAlarms"]
```

### Example 6 — Terraform

`terraform/cloudwatch.tf`

```hcl
resource "aws_cloudwatch_log_group" "ingest" {
  name              = "/${var.prefix}-ingest"
  retention_in_days = 14
  tags              = local.tags
}

resource "aws_cloudwatch_log_metric_filter" "errors" {
  name           = "${var.prefix}-errors"
  log_group_name = aws_cloudwatch_log_group.ingest.name
  pattern        = "ERROR"

  metric_transformation {
    name          = "ErrorCount"
    namespace     = "FlociDemo/Pipeline"
    value         = "1"
    default_value = "0"
  }
}

resource "aws_cloudwatch_metric_alarm" "failed_jobs" {
  alarm_name          = "${var.prefix}-failed-jobs"
  namespace           = "FlociDemo/Pipeline"
  metric_name         = "FailedJobs"
  statistic           = "Sum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
  dimensions          = { stage = "ingest" }
}

resource "aws_cloudwatch_metric_alarm" "dlq_not_empty" {
  alarm_name          = "${var.prefix}-dlq-not-empty"
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = aws_sqs_queue.dlq.name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  alarm_actions       = [aws_sns_topic.alerts.arn]
}
```

---

## Reference

Project files:

```
src/cloudwatch_ops/{logs,metrics,alarms}.py
terraform/cloudwatch.tf
tests/test_03_data_services.py     # test_cloudwatch
```

Related: [06 SNS](06-sns.md) (alarm actions), [14 SQS + DLQ](14-sqs-dlq.md) (the `dlq-not-empty` alarm), [02 Lambda](02-lambda.md) (log groups).
