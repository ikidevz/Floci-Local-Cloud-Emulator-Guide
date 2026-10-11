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
