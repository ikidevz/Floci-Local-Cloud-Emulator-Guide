import json
import os
import time

import pytest

from data.make_orders import make
from src.athena_ops import query, setup as athena_setup
from src.common.aws import arn, client, name
from src.dynamodb_ops import items, tables
from src.glue_ops import catalog, crawlers, etl_local, jobs
from src.iam_ops import roles
from src.kinesis_ops import streams
from src.lambda_ops import functions, mappings, packaging
from src.s3_ops import buckets, notifications, objects
from src.scheduler_ops import schedules
from src.sfn_ops import executions, machines
from src.sns_ops import topics
from src.sqs_ops import queues

MOTO = os.getenv("EMULATOR") == "moto"  # moto cannot run Lambda containers or real SQL


def test_ingest_handler_logic_in_process(monkeypatch):
    """Run the Lambda code in-process against the emulator (no container needed)."""
    t = name("events")
    tables.create_events_table(t)
    s = name("events")
    streams.create(s, 1)
    monkeypatch.setenv("TABLE_NAME", t)
    monkeypatch.setenv("STREAM_NAME", s)
    monkeypatch.setenv("AWS_ENDPOINT_URL", os.environ["AWS_ENDPOINT_URL"])
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")  # inside real Lambda these come from the role
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    import importlib.util
    spec = importlib.util.spec_from_file_location("ingest_handler", "lambda_src/ingest/handler.py")
    h = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(h)

    s3_event = {"Records": [{"s3": {"bucket": {"name": "b"}, "object": {"key": "raw/o.csv"}},
                             "eventName": "ObjectCreated:Put"}]}
    sqs_event = {"Records": [
        {"eventSource": "aws:sqs", "messageId": "m1", "body": json.dumps(s3_event)},
        {"eventSource": "aws:sqs", "messageId": "m2", "body": json.dumps({"Event": "s3:TestEvent"})},
        {"eventSource": "aws:sqs", "messageId": "m3", "body": json.dumps({"fail": True})},
    ]}

    class Ctx:
        aws_request_id = "req-1"

    out = h.lambda_handler(sqs_event, Ctx())
    assert out == {"batchItemFailures": [{"itemIdentifier": "m3"}]}
    logged = items.events_for_run(t, "req-1")
    assert sorted(i["status"] for i in logged) == ["FAILED", "OK", "OK"]
    assert h.lambda_handler({"job": "j1"}, Ctx())["statusCode"] == 200


def test_clean_logic():
    from glue_scripts.clean_orders_shell import clean, parse_args
    rows = [{"order_id": "1", "amount": "10"}, {"order_id": "1", "amount": "10"},
            {"order_id": "2", "amount": "-1"}, {"order_id": "3", "amount": "abc"},
            {"order_id": "4", "amount": "2.5"}]
    assert [r["order_id"] for r in clean(rows)] == ["1", "4"]
    assert parse_args(["--A", "1", "--B", "2"]) == {"A": "1", "B": "2"}


def test_glue_catalog_job_crawler_and_local_etl():
    b = buckets.create_pipeline_buckets()
    role = roles.create_all()["glue-role"]
    objects.upload_file(b["raw"], "raw/orders/orders.csv", make("/tmp/orders_t.csv"))

    catalog.ensure_database(athena_setup.DB)
    catalog.ensure_csv_table(athena_setup.DB, "orders_raw", f"s3://{b['raw']}/raw/orders/",
                             catalog.ORDERS_COLUMNS)
    catalog.ensure_csv_table(athena_setup.DB, "orders_raw", f"s3://{b['raw']}/raw/orders/",
                             catalog.ORDERS_COLUMNS)  # idempotent (update path)
    assert "orders_raw" in catalog.list_tables(athena_setup.DB)
    assert catalog.get_columns(athena_setup.DB, "orders_raw")["amount"] == "double"

    crawlers.ensure_crawler(name("crawler"), role, athena_setup.DB, f"s3://{b['raw']}/raw/orders/")
    if MOTO:  # moto leaves crawlers RUNNING forever; Floci goes RUNNING -> READY (see Glue docs)
        client("glue").start_crawler(Name=name("crawler"))
    else:
        assert crawlers.run_crawler(name("crawler"), timeout=60) == "SUCCEEDED"

    script = jobs.upload_script(b["scripts"], "glue_scripts/clean_orders_shell.py", "glue/clean.py")
    jobs.ensure_job(name("clean-orders"), role, script, shell=True)
    jobs.ensure_job(name("clean-orders"), role, script, shell=True)  # idempotent (update path)
    assert jobs.run_and_wait(name("clean-orders"), {"--X": "1"}, timeout=60) == "SUCCEEDED"

    etl_local.run_clean_orders()
    text = objects.get_text(b["curated"], "clean/orders/orders.csv")
    lines = text.strip().splitlines()
    assert lines[0] == "order_id,customer_id,amount,status"
    assert len(lines) == 1 + 50  # 52 raw rows - 1 duplicate - 1 negative amount


def test_athena_queries():
    b = buckets.create_pipeline_buckets()
    objects.put_text(b["curated"], "clean/orders/orders.csv",
                     "order_id,customer_id,amount,status\nO-1,C-1,10.00,NEW\nO-2,C-1,5.50,NEW\nO-3,C-2,1.00,NEW\n")
    wg = athena_setup.bootstrap()
    sql = open("sql/athena/top_customers.sql").read()
    rows = query.run_and_fetch(sql, wg, athena_setup.DB)
    if not MOTO:  # real SQL engine (DuckDB in Floci)
        assert rows[0]["customer_id"] == "C-1"
        assert float(rows[0]["revenue"]) == 15.5
    assert isinstance(rows, list)


def test_step_functions_definition_and_lifecycle():
    r = roles.create_all()
    topic = topics.create_topic(name("alerts"))
    definition = machines.render_definition(
        "sfn_definitions/etl_pipeline.asl.json",
        GLUE_RUNNER_ARN=arn("lambda", f"function:{name('glue-runner')}"),
        GLUE_JOB_NAME=name("clean-orders"), SNS_TOPIC_ARN=topic)
    machines.check_definition(definition)
    sm = machines.upsert(name("etl"), definition, r["sfn-role"])
    assert machines.upsert(name("etl"), definition, r["sfn-role"]) == sm  # update path
    ex = executions.start(sm, {"source": "test"})
    assert ex
    # moto does not interpret ASL, so only assert the full run on Floci
    if not MOTO:
        pytest.skip("full execution is covered in test_05_e2e (needs deployed Lambdas)")


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


def test_lambda_package_and_deploy():
    z = packaging.zip_dir("lambda_src/ingest")
    assert z[:2] == b"PK"
    role = roles.create_all()["lambda-role"]
    q = queues.create_with_dlq(name("jobs"))
    arn_fn = functions.deploy(name("ingest"), "lambda_src/ingest", role,
                              env={"TABLE_NAME": name("events")}, dlq_arn=q["dlq_arn"])
    assert arn_fn.endswith(name("ingest"))
    functions.deploy(name("ingest"), "lambda_src/ingest", role)  # update path
    uuid = mappings.sqs_trigger(name("ingest"), q["main_arn"])
    assert mappings.list_mappings(name("ingest"))
    mappings.delete_mapping(uuid)
    if not MOTO:
        out = functions.invoke(name("ingest"), {"job": "smoke"})
        assert out["statusCode"] == 200
