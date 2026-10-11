import pytest
from botocore.exceptions import ClientError

from src.common.aws import client, name
from src.cloudwatch_ops import alarms, logs, metrics
from src.dynamodb_ops import idempotency, items, tables
from src.firehose_ops import producer as fh_producer
from src.firehose_ops import streams as fh_streams
from src.iam_ops import roles
from src.kinesis_ops import consumer, producer, streams
from src.s3_ops import buckets
from src.sns_ops import topics


def test_dynamodb():
    t = name("events")
    tables.create_events_table(t)
    tables.enable_ttl(t)
    items.put_event(t, "run-1", "2026-10-10T08:00:00Z", "STARTED", rows=120, cost=1.5)
    items.put_event(t, "run-1", "2026-10-10T08:05:00Z", "FAILED")
    assert len(items.events_for_run(t, "run-1")) == 2
    assert len(items.events_by_status(t, "FAILED")) == 1
    assert items.update_status(t, "run-1", "2026-10-10T08:05:00Z", "RETRIED")["status"] == "RETRIED"
    items.batch_put(t, [{"run_id": "run-2", "ts": f"t{i}", "status": "NEW"} for i in range(30)])
    assert len(items.events_for_run(t, "run-2")) == 30
    items.set_status_atomic(t, [("run-2", "t0"), ("run-2", "t1")], "DONE")
    assert items.get_event(t, "run-2", "t0")["status"] == "DONE"
    assert idempotency.claim(t, "run-3", "x") is True
    assert idempotency.claim(t, "run-3", "x") is False
    with pytest.raises(ClientError):
        items.update_status(t, "nope", "nope", "X")  # condition fails: item missing


def test_kinesis():
    s = name("events")
    arn = streams.create(s, shards=2)
    assert arn.endswith(s)
    assert len(streams.shard_ids(s)) == 2
    assert producer.put_many(s, [{"user": f"u{i % 5}", "n": i} for i in range(25)], "user") == 0
    got: list[dict] = []
    assert consumer.read_all(s, got.append) == 25
    assert {g["n"] for g in got} == set(range(25))


def test_firehose_direct_put():
    b = buckets.create_pipeline_buckets()["curated"]
    role = roles.create_all()["firehose-role"]
    s = name("to-s3")
    fh_streams.create_to_s3(s, role, b, prefix="events/", buffer_s=60, buffer_mb=1)
    assert fh_producer.send(s, [{"id": i} for i in range(20)]) == 0
    assert fh_producer.wait_for_objects(b, "events/", timeout=90)


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
