# Google Cloud on Floci-style Local Guides: Python + Terraform (uniform layout)

One guide per Google Cloud service, plus 6 multi-service projects (including batch, stream and CDC). Each guide has: documentation (overview, at a glance, local behavior), basic usage, examples, and a reference section.

> **Heads-up:** Floci emulates AWS. Google Cloud has no single Floci equivalent, so this set uses the official or community emulator for each service (all wired in one `docker-compose.yml`), with the same "run it locally, no cloud bill" workflow as the AWS guides.

## Layout

```
floci-gcp-guides/
├── README.md                      you are here
├── 00-setup-floci.md … 16-project-cdc.md
└── project/                       shared local stack
    └── docker-compose.yml  .env.local  requirements.txt
```

## Guide index

| # | Guide | AWS equivalent | Local emulator |
|---|-------|----------------|----------------|
| 00 | [Setup & foundation](00-setup-floci.md) | | one compose file, env vars, dependencies |
| 01 | [Cloud Storage](01-cloud-storage.md) | S3 | fake-gcs-server |
| 02 | [Pub/Sub](02-pubsub.md) | SNS / SQS / Kinesis | gcloud pubsub emulator |
| 03 | [BigQuery](03-bigquery.md) | Athena / Redshift | goccy/bigquery-emulator |
| 04 | [Firestore](04-firestore.md) | DynamoDB | gcloud firestore emulator |
| 05 | [Bigtable](05-bigtable.md) | Keyspaces / DynamoDB | gcloud bigtable emulator |
| 06 | [Spanner](06-spanner.md) | Aurora | Spanner emulator |
| 07 | [Cloud Tasks](07-cloud-tasks.md) | SQS | cloud-tasks-emulator |
| 08 | [Cloud SQL (Postgres)](08-cloud-sql.md) | RDS | plain Postgres 16 |
| 09 | [Cloud Functions](09-cloud-functions.md) | Lambda | functions-framework |
| 10 | [Cloud Run](10-cloud-run.md) | Fargate / App Runner | Docker |

### Projects

| # | Project |
|---|---------|
| 11 | [Project 1: Event-Driven Ingest: Cloud Storage → Pub/Sub → BigQuery](11-project-event-ingest.md) |
| 12 | [Project 2: Order Queue: Cloud Tasks → Cloud Run Worker → Firestore](12-project-order-queue.md) |
| 13 | [Project 3: Telemetry Pipeline: Pub/Sub → Bigtable → BigQuery](13-project-telemetry.md) |
| 14 | [Project 4: Batch: Cloud SQL → Cloud Storage → BigQuery (nightly ETL)](14-project-batch-etl.md) |
| 15 | [Project 5: Stream: Pub/Sub → Windowed Aggregation → Firestore + BigQuery](15-project-stream.md) |
| 16 | [Project 6: CDC: Cloud SQL → Pub/Sub → BigQuery changelog + Firestore replica](16-project-cdc.md) |

Suggested order: `00 → 01 … 10 → 11 … 16`. The projects wire services together, so do the service guides first.

## Quick start

```bash
cd project
docker compose up -d
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
source .env.local
```

## What was verified, and what was not

These files do not record any test run. Treat all code as unverified until you run it against the emulators.

## Emulator behaviors that shaped the code

- Emulator coverage differs from real GCP (IAM, quotas and some features are not emulated). Treat them as dev/test only.
- Terraform sections target **real GCP**: most GCP emulators do not expose the Admin APIs Terraform needs.
- The emulators do not emit bucket notifications, so the uploader in project 11 publishes the Pub/Sub event itself. Real GCP does this with a bucket notification config.
