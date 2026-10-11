# AWS on Floci: boto3 + Terraform Guides (v2, tested code)

One guide per service. Each guide has: Floci-specific behavior (taken from Floci's own docs), file structure, **code included straight from the runnable `project/` folder**, a test, Terraform, and gotchas.

[Floci](https://github.com/floci-io/floci) is a free, open-source local AWS emulator on `http://localhost:4566`.

## Layout

```
floci-aws-guides/
├── README.md                      you are here
├── 00-setup-floci.md … 15-end-to-end-pipeline.md
└── project/                       the runnable code the guides are built from
    ├── docker-compose.yml  Makefile  requirements.txt  .env.example  pytest.ini
    ├── src/<service>_ops/         boto3 modules (idempotent, with polling and error handling)
    ├── lambda_src/                3 Lambda handlers
    ├── glue_scripts/  sfn_definitions/  sql/  data/
    ├── terraform/                 17 files, one per service area
    └── tests/                     pytest suite
```

## Guide index

| # | Guide | Services |
|---|-------|----------|
| 00 | [Setup & foundation](00-setup-floci.md) | Docker, client factory, harness, Terraform provider |
| 01 | [S3](01-s3.md) | buckets, objects, notifications |
| 02 | [Lambda](02-lambda.md) | 3 functions, triggers, packaging |
| 03 | [Glue](03-glue.md) | catalog, crawlers, jobs, local ETL |
| 04 | [Step Functions](04-step-functions.md) | ASL with polling loop |
| 05 | [EventBridge Scheduler](05-eventbridge-scheduler.md) | cron/rate/at |
| 06 | [SNS](06-sns.md) | topics, filters |
| 07 | [IAM](07-iam.md) | roles, least-privilege policies |
| 08 | [CloudWatch](08-cloudwatch.md) | logs, metrics, alarms |
| 09 | [RDS Postgres](09-rds-postgres.md) | instance, load, query |
| 10 | [Athena](10-athena.md) | workgroups, queries on Glue tables |
| 11 | [Kinesis Data Streams](11-kinesis-data-streams.md) | produce/consume |
| 12 | [Kinesis Firehose](12-kinesis-firehose.md) | delivery to S3 |
| 13 | [DynamoDB](13-dynamodb.md) | GSI, TTL, transactions |
| 14 | [SQS + DLQ](14-sqs-dlq.md) | redrive policy, poison messages |
| 15 | [End-to-end](15-end-to-end-pipeline.md) | everything wired together |

Suggested order: `00 → 07 → 01 → 14 → 06 → 02 → 13 → 11 → 12 → 03 → 10 → 09 → 08 → 04 → 05 → 15`.

## Quick start

```bash
cd project
cp .env.example .env
docker compose up -d
echo "127.0.0.1 floci" | sudo tee -a /etc/hosts   # so URLs Floci returns resolve from your host
python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt
pytest tests/test_01_iam_s3.py                     # smoke check
make test                                          # full suite
python -m src.bootstrap                            # build the whole pipeline
```

## What was verified, and what was not

| Check | Result |
|-------|--------|
| Pytest suite against an AWS mock server (S3, SQS, SNS, DynamoDB, Kinesis, Firehose, IAM, CloudWatch, Glue, Athena, Lambda create/deploy, Step Functions, Scheduler) | 16 passed, 2 skipped |
| RDS load/query code against a real local Postgres | passed |
| Lambda handler logic run in-process against the mock | passed |
| Terraform: HCL syntax parsed, every resource and variable reference resolved | passed |
| **Terraform `plan/apply`** | **not run** (no Terraform binary available) |
| **Anything executed on Floci itself** | **not run** (needs Docker). `test_full_flow_on_floci` is written but unexecuted |

Run `make test` on your Floci first and send me any failure; the Floci-specific behavior in the guides comes from Floci's documentation, not from running it.

## Floci behaviors that shaped the code

- Glue **jobs and crawlers do not execute or discover anything** (state machine only) → tables are registered explicitly, and `etl_local.py` runs the job logic locally.
- Step Functions has **no Glue integration** → a `glue-runner` Lambda plus a Wait/Check/Choice loop.
- Athena runs real SQL via DuckDB over **Glue-registered** tables, so DDL is not used.
- Firehose documents no Kinesis source → DirectPut plus a Lambda bridge.
- S3 notification setup sends an `s3:TestEvent` to SQS → handlers ignore messages without `Records`.
- Scheduler fires real schedules (SQS, Lambda, SNS, Step Functions…) and needs an IAM role ARN.
- Lambda/RDS/Athena need the Docker socket; RDS needs ports `7001-7099`.
