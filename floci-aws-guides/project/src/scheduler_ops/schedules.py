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
