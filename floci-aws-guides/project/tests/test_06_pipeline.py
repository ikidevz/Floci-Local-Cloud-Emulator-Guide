import json
import os
import time

import pytest

from data.make_orders import make
from src import bootstrap
from src.athena_ops import query
from src.athena_ops import setup as athena_setup
from src.common.aws import client, name
from src.dynamodb_ops import items
from src.glue_ops import etl_local
from src.s3_ops import objects
from src.sfn_ops import executions
from src.sqs_ops import dlq

MOTO = os.getenv("EMULATOR") == "moto"


@pytest.fixture(scope="module")
def pipeline():
    return bootstrap.main()


def test_bootstrap_wires_everything(pipeline):
    assert pipeline["state_machine"].endswith(name("etl"))
    assert client("lambda").get_function(FunctionName=name("ingest"))
    cfg = client("s3").get_bucket_notification_configuration(Bucket=pipeline["buckets"]["raw"])
    assert cfg["QueueConfigurations"][0]["QueueArn"] == pipeline["queue"]["main_arn"]
    assert bootstrap.main()["state_machine"] == pipeline["state_machine"]  # re-runnable


@pytest.mark.skipif(MOTO, reason="needs Floci: real Lambda containers + SFN interpreter + DuckDB")
def test_full_flow_on_floci(pipeline):
    raw = pipeline["buckets"]["raw"]

    # 1) upload -> S3 event -> SQS -> Lambda -> DynamoDB run log
    objects.upload_file(raw, "raw/orders/orders.csv", make("/tmp/orders_e2e.csv"))
    deadline = time.time() + 120
    logged = []
    while time.time() < deadline and not logged:
        resp = client("dynamodb").scan(TableName=name("events"))
        logged = [i for i in resp["Items"] if i["status"]["S"] == "OK"]
        time.sleep(3)
    assert logged, "ingest Lambda did not log an OK event"

    # 2) Step Functions drives the Glue job through the glue-runner Lambda
    out = executions.wait(executions.start(pipeline["state_machine"], {"source": "e2e"}), timeout=180)
    assert out["glue"]["state"] == "SUCCEEDED"

    # 3) Floci does not run Glue scripts: produce the curated data locally, then query it
    etl_local.run_clean_orders()
    rows = query.run_and_fetch("SELECT count(*) AS n FROM orders_clean", pipeline["workgroup"],
                               athena_setup.DB)
    assert int(rows[0]["n"]) == 50

    # 4) poison message ends in the DLQ
    client("sqs").send_message(QueueUrl=pipeline["queue"]["main_url"], MessageBody=json.dumps({"fail": True}))
    deadline = time.time() + 240
    while time.time() < deadline and dlq.depth(pipeline["queue"]["dlq_url"]) == 0:
        time.sleep(5)
    assert dlq.depth(pipeline["queue"]["dlq_url"]) >= 1
