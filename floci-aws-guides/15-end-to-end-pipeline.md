# 15 — End-to-End Pipeline

> [Index](README.md) · [← 14 — SQS + Dead Letter Queue](14-sqs-dlq.md)

All 14 services wired together.

---

## 1. Documentation

### Overview

```
                  EventBridge Scheduler (cron)
                              │
                              ▼
S3 raw/*.csv ─event─► SQS jobs ─► Lambda ingest ─► DynamoDB run log
                         │ 3 fails                └─► Kinesis ─► Lambda bridge ─► Firehose ─► S3 curated/events/
                         ▼
                      SQS DLQ ─► CloudWatch alarm ─► SNS alerts ◄── Step Functions ◄── Scheduler
                                                                       │
                                                       Lambda glue-runner ─► Glue job (state only on Floci)
                                          local ETL ─► S3 curated/clean/orders ─► Glue tables ─► Athena
                                                                                  └─► RDS Postgres (load)
```

### Services and the guide for each

| Service | Role in the pipeline | Guide |
|---------|----------------------|-------|
| IAM | roles for Lambda, Glue, Step Functions, Scheduler, Firehose | [07](07-iam.md) |
| S3 | `raw`, `curated`, `scripts`, `athena-results` buckets; upload events | [01](01-s3.md) |
| SQS + DLQ | job queue fed by S3 events; poison messages end in the DLQ | [14](14-sqs-dlq.md) |
| SNS | alerts topic | [06](06-sns.md) |
| DynamoDB | run log and idempotency | [13](13-dynamodb.md) |
| Kinesis Data Streams | event stream from `ingest` | [11](11-kinesis-data-streams.md) |
| Firehose | delivers events to `curated/events/` | [12](12-kinesis-firehose.md) |
| Lambda | `ingest`, `glue-runner`, `kinesis-to-firehose` | [02](02-lambda.md) |
| Glue | catalog, crawler, job | [03](03-glue.md) |
| Athena | SQL over curated data | [10](10-athena.md) |
| RDS Postgres | serving database loaded from curated data | [09](09-rds-postgres.md) |
| CloudWatch | logs, metrics, alarms to SNS | [08](08-cloudwatch.md) |
| Step Functions | ETL orchestration | [04](04-step-functions.md) |
| EventBridge Scheduler | nightly trigger | [05](05-eventbridge-scheduler.md) |

### Floci behaviors that shaped the design

- Glue **jobs and crawlers do not execute or discover anything** (state machine only), so tables are registered explicitly and `etl_local.py` runs the job logic locally.
- Step Functions has **no Glue integration**, so a `glue-runner` Lambda plus a Wait/Check/Choice loop drives Glue.
- Athena runs real SQL via DuckDB over **Glue-registered** tables, so DDL is not used.
- Firehose documents no Kinesis source, so the pipeline uses DirectPut plus a Lambda bridge.
- S3 notification setup sends an `s3:TestEvent` to SQS, so handlers ignore messages without `Records`.
- Scheduler fires real schedules (SQS, Lambda, SNS, Step Functions…) and needs an IAM role ARN.
- Lambda, RDS and Athena need the Docker socket; RDS needs ports `7001-7099`.

### Apply order (if you create things by hand)

| Step | Guide | Why first |
|------|-------|-----------|
| 1 | 07 IAM | every service needs a role |
| 2 | 01 S3 | data, scripts, results |
| 3 | 14 SQS+DLQ, 06 SNS | event plumbing |
| 4 | 13 DynamoDB, 11 Kinesis, 12 Firehose | stores and streams |
| 5 | 02 Lambda | needs role, table, queue, stream |
| 6 | 03 Glue, 10 Athena, 09 RDS | batch and serving |
| 7 | 08 CloudWatch | alarms need the SNS topic |
| 8 | 04 Step Functions | needs Lambda ARN and SNS topic |
| 9 | 05 Scheduler | needs the state machine ARN |

### Troubleshooting

1. `pytest tests/test_01_iam_s3.py` first: it tells you whether the endpoint and hostname work.
2. `docker logs floci` for emulator-side errors.
3. Lambda stuck or failing to start: confirm the Docker socket is mounted and `FLOCI_SERVICES_DOCKER_NETWORK` matches the compose network.
4. Queue URL host not resolving from your machine: add `127.0.0.1 floci` to your hosts file, or run inside the compose network ([guide 00](00-setup-floci.md)).
5. Step Functions failure: `executions.history(...)` names the failing state.
6. Athena failing on first query: it pulls the `floci-duck` image, which needs Docker and network access.

---

## 2. Basic Usage

> Prerequisite: Floci is running with the Docker socket mounted, and the project is set up ([guide 00](00-setup-floci.md)).

### Option A: boto3 (one command)

Do **not** also run Terraform on the same names.

```bash
python -m src.bootstrap
```

### Option B: Terraform

Same resources, same names. Pick one option per Floci instance.

```bash
cd terraform && terraform init && terraform apply -auto-approve && terraform output
```

### Run it by hand

Upload a file (S3 → SQS → Lambda → DynamoDB), then produce the curated data locally:

```bash
python -m src.bootstrap
python -m data.make_orders
python - <<'PY'
from src.common.aws import name
from src.s3_ops.objects import upload_file
upload_file(name("raw"), "raw/orders/orders.csv", "data/orders.csv")   # S3 -> SQS -> Lambda -> DynamoDB
PY
python -c "from src.glue_ops.etl_local import run_clean_orders as r; r()"   # produce curated data
```

### Tear down

```bash
cd terraform && terraform destroy -auto-approve   # Option B
docker compose down -v                            # wipe Floci and its persisted state
```

---

## 3. Examples

### Example 1 — `bootstrap.py`: the whole pipeline with boto3

Each line is annotated with the guide it comes from. It is re-runnable.

`src/bootstrap.py`

```python
"""Create the whole pipeline with boto3 (do NOT also run Terraform on the same names)."""
from src.athena_ops import setup as athena_setup
from src.cloudwatch_ops import alarms, logs, metrics
from src.common.aws import arn, name
from src.dynamodb_ops.tables import create_events_table, enable_ttl
from src.firehose_ops.streams import create_to_s3
from src.glue_ops import catalog, crawlers, jobs
from src.iam_ops.roles import create_all
from src.kinesis_ops.streams import create as create_stream
from src.lambda_ops.functions import allow_service, deploy
from src.lambda_ops.mappings import kinesis_trigger, sqs_trigger
from src.s3_ops.buckets import create_pipeline_buckets
from src.s3_ops.notifications import notify_sqs
from src.scheduler_ops.schedules import ensure_group, upsert_schedule
from src.sfn_ops import machines
from src.sns_ops.topics import create_topic
from src.sqs_ops.queues import allow_senders, create_with_dlq


def main() -> dict:
    out: dict = {}
    roles = create_all()                                                          # 07 IAM
    buckets = create_pipeline_buckets()                                           # 01 S3

    q = create_with_dlq(name("jobs"), max_receive=3, visibility_s=60)             # 14 SQS + DLQ
    allow_senders(q["main_url"], "s3.amazonaws.com", f"arn:aws:s3:::{buckets['raw']}")
    notify_sqs(buckets["raw"], q["main_arn"], prefix="raw/", suffix=".csv")

    topic = create_topic(name("alerts"))                                          # 06 SNS
    create_events_table(name("events"))                                           # 13 DynamoDB
    enable_ttl(name("events"))
    stream_arn = create_stream(name("events"), shards=1)                          # 11 Kinesis
    create_to_s3(name("to-s3"), roles["firehose-role"], buckets["curated"],       # 12 Firehose
                 prefix="events/", buffer_s=60, buffer_mb=1)

    ingest = deploy(name("ingest"), "lambda_src/ingest", roles["lambda-role"],    # 02 Lambda
                    env={"TABLE_NAME": name("events"), "STREAM_NAME": name("events")},
                    timeout=30, dlq_arn=q["dlq_arn"])
    runner = deploy(name("glue-runner"), "lambda_src/glue_runner", roles["lambda-role"])
    bridge = deploy(name("kinesis-to-firehose"), "lambda_src/kinesis_to_firehose", roles["lambda-role"],
                    env={"FIREHOSE_STREAM": name("to-s3")})
    sqs_trigger(name("ingest"), q["main_arn"])
    kinesis_trigger(name("kinesis-to-firehose"), stream_arn, start="TRIM_HORIZON")

    catalog.ensure_database(athena_setup.DB)                                      # 03 Glue
    crawlers.ensure_crawler(name("orders-crawler"), roles["glue-role"], athena_setup.DB,
                            f"s3://{buckets['raw']}/raw/orders/")
    script = jobs.upload_script(buckets["scripts"], "glue_scripts/clean_orders_shell.py",
                                "glue/clean_orders_shell.py")
    jobs.ensure_job(name("clean-orders"), roles["glue-role"], script, shell=True)
    wg = athena_setup.bootstrap()                                                 # 10 Athena (+ tables)

    g = f"/{name('ingest')}"                                                      # 08 CloudWatch
    logs.ensure_group(g)
    alarms.failure_alarm(name("failed-jobs"), "FailedJobs", metrics.NAMESPACE, topic, stage="ingest")

    definition = machines.render_definition(                                      # 04 Step Functions
        "sfn_definitions/etl_pipeline.asl.json",
        GLUE_RUNNER_ARN=runner, GLUE_JOB_NAME=name("clean-orders"), SNS_TOPIC_ARN=topic)
    sm_arn = machines.upsert(name("etl"), definition, roles["sfn-role"])

    ensure_group(name("group"))                                                   # 05 Scheduler
    upsert_schedule(name("nightly-etl"), "cron(0 2 * * ? *)", sm_arn, roles["scheduler-role"],
                    {"source": "scheduler"}, group=name("group"), tz="Asia/Manila")

    out.update(queue=q, topic=topic, ingest=ingest, bridge=bridge, runner=runner, workgroup=wg,
               state_machine=sm_arn, buckets=buckets, stream_arn=stream_arn)
    return out


if __name__ == "__main__":
    import json
    print(json.dumps(main(), indent=2))
```

### Example 2 — Terraform outputs

`terraform/outputs.tf`

```hcl
output "raw_bucket" { value = aws_s3_bucket.this["raw"].bucket }
output "queue_url" { value = aws_sqs_queue.main.url }
output "dlq_url" { value = aws_sqs_queue.dlq.url }
output "topic_arn" { value = aws_sns_topic.alerts.arn }
output "state_machine_arn" { value = aws_sfn_state_machine.etl.arn }
output "stream_name" { value = aws_kinesis_stream.events.name }
output "events_table" { value = aws_dynamodb_table.events.name }
output "athena_workgroup" { value = aws_athena_workgroup.main.name }
output "rds_endpoint" { value = aws_db_instance.postgres.endpoint }
```

### Example 3 — Test the whole flow

`test_bootstrap_wires_everything` runs against any emulator. `test_full_flow_on_floci` runs only on Floci (not executed where this was written).

`tests/test_06_pipeline.py`

```python
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
```

---

## Reference

Related: every guide above. Start with [00 Setup](00-setup-floci.md).
