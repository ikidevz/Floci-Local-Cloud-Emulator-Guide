# 05 — EventBridge Scheduler

> [Index](README.md) · [← 04 — Step Functions](04-step-functions.md) · [06 — SNS →](06-sns.md)

Runs rate, cron and one-time schedules against a target using an execution role.

---

## 1. Documentation

### Overview

- Scheduler (`boto3.client("scheduler")`) is **not** EventBridge Rules (`events`).
- A **schedule** has an expression (`rate`, `cron` or `at`), a **target** (ARN + role + JSON input) and optionally belongs to a **schedule group**.
- In this project a nightly schedule starts the ETL state machine.

### At a glance

| Item | Value |
|------|-------|
| boto3 client | `scheduler` |
| Terraform endpoint | `scheduler` |
| Needs first | an IAM role ([07](07-iam.md)) and the target (queue, Lambda, state machine…) |
| Docker socket | not required |

### What Floci supports

Floci implements schedule groups and schedules (create/get/update/delete/list/tag) **and fires them**: a background dispatcher ticks every 10 s (`FLOCI_SERVICES_SCHEDULER_TICK_INTERVAL_SECONDS`).

- **Expressions**: `at(...)`, `rate(N minutes|hours|days)` and 6-field `cron(...)` with `ScheduleExpressionTimezone`. Bad expressions return `ValidationException`.
- **Targets**: SQS, Lambda, SNS, ECS RunTask, EventBridge PutEvents, and unqualified Step Functions state machine ARNs.
- `Target.RoleArn` must be an IAM role ARN.
- Lambda, Step Functions and EventBridge targets require `Input` to be valid JSON.
- Failed deliveries retry with the target's `RetryPolicy`, then go to `DeadLetterConfig` if it names a standard SQS queue.
- `at(...)` schedules with `ActionAfterCompletion=DELETE` delete themselves after firing.

### Gotchas

- Cron has **six fields** and needs `?` in either day field. Day-of-week is `1-7` or `SUN-SAT`.
- `FlexibleTimeWindow` is required; `{"Mode": "OFF"}` means exact timing.
- `update_schedule` replaces the whole definition: re-send every field you want to keep (`set_enabled` does this).
- Pending retries live in memory in Floci and are dropped on restart or when the schedule is updated or deleted.

---

## 2. Basic Usage

> Prerequisite: Floci is running and the `AWS_*` environment variables are set ([guide 00](00-setup-floci.md)).

```python
import json
import boto3

iam, sqs, sch = boto3.client("iam"), boto3.client("sqs"), boto3.client("scheduler")

# 1. Role (Floci requires a real IAM role ARN) and a target queue
trust = {"Version": "2012-10-17", "Statement": [
    {"Effect": "Allow", "Principal": {"Service": "scheduler.amazonaws.com"}, "Action": "sts:AssumeRole"}]}
role_arn = iam.create_role(RoleName="demo-scheduler-role",
                           AssumeRolePolicyDocument=json.dumps(trust))["Role"]["Arn"]
queue_url = sqs.create_queue(QueueName="demo-tick")["QueueUrl"]
queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url,
                                     AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]

# 2. A schedule that fires every minute
sch.create_schedule(
    Name="demo-tick", ScheduleExpression="rate(1 minutes)",
    FlexibleTimeWindow={"Mode": "OFF"},            # required; OFF = exact timing
    Target={"Arn": queue_arn, "RoleArn": role_arn, "Input": json.dumps({"hello": "tick"})})
```

```python
# (needs Floci: the dispatcher fires the schedule)
print(sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=20).get("Messages"))
```

---

## 3. Examples

### Example 1 — Schedule helpers (group, upsert, enable/disable, list, delete)

`src/scheduler_ops/schedules.py`

```python
import json

from botocore.exceptions import ClientError

from src.common.aws import client

sch = client("scheduler")


def ensure_group(group: str) -> None:
    try:
        sch.create_schedule_group(Name=group)
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConflictException":
            raise


def upsert_schedule(schedule: str, expression: str, target_arn: str, role_arn: str, payload: dict,
                    group: str = "default", tz: str = "UTC", enabled: bool = True) -> str:
    """
    expression:  rate(5 minutes) | cron(0 2 * * ? *) | at(2026-12-31T23:59:00)
    Floci requires role_arn to be an IAM role ARN. Supported targets: SQS, Lambda, SNS,
    Step Functions (unqualified state machine ARN), EventBridge PutEvents, ECS RunTask.
    """
    kwargs = dict(
        Name=schedule, GroupName=group, ScheduleExpression=expression,
        ScheduleExpressionTimezone=tz, FlexibleTimeWindow={"Mode": "OFF"},
        State="ENABLED" if enabled else "DISABLED",
        Target={"Arn": target_arn, "RoleArn": role_arn, "Input": json.dumps(payload),
                "RetryPolicy": {"MaximumRetryAttempts": 2, "MaximumEventAgeInSeconds": 3600}},
    )
    if expression.startswith("at("):
        kwargs["ActionAfterCompletion"] = "DELETE"
    try:
        return sch.create_schedule(**kwargs)["ScheduleArn"]
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConflictException":
            raise
        return sch.update_schedule(**kwargs)["ScheduleArn"]


def set_enabled(schedule: str, group: str, enabled: bool) -> None:
    """update_schedule replaces the whole definition, so re-send the existing fields."""
    cur = sch.get_schedule(Name=schedule, GroupName=group)
    sch.update_schedule(Name=schedule, GroupName=group,
                        ScheduleExpression=cur["ScheduleExpression"],
                        ScheduleExpressionTimezone=cur.get("ScheduleExpressionTimezone", "UTC"),
                        FlexibleTimeWindow=cur["FlexibleTimeWindow"], Target=cur["Target"],
                        State="ENABLED" if enabled else "DISABLED")


def list_schedules(group: str | None = None) -> list[str]:
    kw = {"GroupName": group} if group else {}
    return [s["Name"] for p in sch.get_paginator("list_schedules").paginate(**kw)
            for s in p["Schedules"]]


def delete(schedule: str, group: str = "default") -> None:
    sch.delete_schedule(Name=schedule, GroupName=group)
```

### Example 2 — Nightly Step Functions run

```python
from src.common.aws import arn, name
from src.iam_ops.roles import create_all
from src.scheduler_ops.schedules import ensure_group, upsert_schedule

ensure_group(name("group"))
upsert_schedule(name("nightly-etl"), "cron(0 2 * * ? *)",
                arn("states", f"stateMachine:{name('etl')}"),
                create_all()["scheduler-role"], {"source": "scheduler"},
                group=name("group"), tz="Asia/Manila")
```

### Example 3 — Watch a schedule fire (every minute into SQS)

```python
from src.common.aws import client, name
from src.iam_ops.roles import create_all
from src.scheduler_ops.schedules import upsert_schedule
from src.sqs_ops.queues import queue_arn

q = client("sqs").create_queue(QueueName=name("tick"))["QueueUrl"]
upsert_schedule(name("tick"), "rate(1 minutes)", queue_arn(q), create_all()["scheduler-role"], {"hello": "tick"})
print(client("sqs").receive_message(QueueUrl=q, WaitTimeSeconds=20).get("Messages"))
```

### Example 4 — Test

`tests/test_04_compute_and_catalog.py`

```python
from src.common.aws import arn, client, name
from src.iam_ops import roles
from src.scheduler_ops import schedules


def test_scheduler_crud():
    r = roles.create_all()
    group = name("group")
    schedules.ensure_group(group)
    sm_arn = arn("states", f"stateMachine:{name('etl')}")
    schedules.upsert_schedule(name("nightly"), "cron(0 2 * * ? *)", sm_arn, r["scheduler-role"],
                              {"source": "scheduler"}, group=group, tz="Asia/Manila")
    schedules.upsert_schedule(name("nightly"), "rate(1 hours)", sm_arn, r["scheduler-role"],
                              {"source": "scheduler"}, group=group)  # update path
    assert name("nightly") in schedules.list_schedules(group)
    schedules.set_enabled(name("nightly"), group, False)
    assert client("scheduler").get_schedule(Name=name("nightly"), GroupName=group)["State"] == "DISABLED"
    schedules.delete(name("nightly"), group)
    assert name("nightly") not in schedules.list_schedules(group)
```

### Example 5 — Terraform

`terraform/scheduler.tf`

```hcl
resource "aws_scheduler_schedule_group" "main" {
  name = "${var.prefix}-group"
}

resource "aws_scheduler_schedule" "nightly_etl" {
  name                         = "${var.prefix}-nightly-etl"
  group_name                   = aws_scheduler_schedule_group.main.name
  schedule_expression          = "cron(0 2 * * ? *)"
  schedule_expression_timezone = "Asia/Manila"
  state                        = "ENABLED"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_sfn_state_machine.etl.arn
    role_arn = aws_iam_role.this["scheduler"].arn
    input    = jsonencode({ source = "scheduler" })

    retry_policy {
      maximum_retry_attempts       = 2
      maximum_event_age_in_seconds = 3600
    }
  }
}
```

---

## Reference

Project files:

```
src/scheduler_ops/schedules.py
terraform/scheduler.tf
```

Related: [04 Step Functions](04-step-functions.md) (target), [07 IAM](07-iam.md) (`scheduler-role`).
