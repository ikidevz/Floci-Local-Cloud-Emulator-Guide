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
