# Floci Guides: AWS, Azure and Google Cloud, run locally

Hands-on guides for working with cloud services **on your own machine**, with no cloud bill. Every service has one markdown guide with the same anatomy, and the code in each guide comes from a runnable `project/` folder next to it.

| Cloud | Folder | Local backend | Guides |
|-------|--------|---------------|--------|
| AWS | [`floci-aws-guides/`](floci-aws-guides/README.md) | [Floci](https://github.com/floci-io/floci) on `localhost:4566` | 00 setup, 14 services, 1 end-to-end pipeline |
| Azure | [`floci-azure-guides/`](floci-azure-guides/README.md) | [Floci AZ](https://floci.io/floci-az/services/) on `localhost:4577` | 00 setup, 8 services |
| Google Cloud | [`floci-gcp-guides/`](floci-gcp-guides/README.md) | one official or community emulator per service (Floci itself emulates AWS) | 00 setup, 10 services, 6 projects |

## Layout

```
floci-guides/
├── README.md                          you are here
├── floci-aws-guides/
│   ├── README.md  00-setup-floci.md  01-s3.md … 15-end-to-end-pipeline.md
│   └── project/                       src/, lambda_src/, terraform/, tests/ …
├── floci-azure-guides/
│   ├── README.md  00-setup-floci.md  01-blob-storage.md … 08-event-grid.md
│   └── project/01-blob-storage/ … 08-event-grid/     one standalone project per service
└── floci-gcp-guides/
    ├── README.md  00-setup-floci.md  01-cloud-storage.md … 16-project-cdc.md
    └── project/                       docker-compose.yml, .env.local, requirements.txt
```

All three folders follow the same rules:

- `README.md` is the index: layout, guide index, quick start, what was verified, behaviors that shaped the code.
- `00-setup-floci.md` is read first: how the local backend runs, ports, credentials, conventions.
- `NN-<service>.md` is one service per file, numbered, with prev/next links at the top.
- `project/` holds the runnable code the guides are built from.

## Anatomy of every guide

| Section | What is in it |
|---------|---------------|
| **1. Documentation** | Overview · At a glance (table) · How it behaves on Floci · Key concepts · Gotchas · What was verified |
| **2. Basic Usage** | The shortest path from a running backend to a working call |
| **3. Examples** | Numbered examples with real code, a test, and Terraform (or real-cloud notes) |
| **Reference** | Project files for the guide, and links to related guides |

Not every guide has every sub-section yet. The AWS guides are complete. The Azure guides have no separate "Terraform" code, only the real-Azure resource names, because Floci AZ's Terraform support is limited. The GCP guides have no *Key concepts*, *Gotchas* or *What was verified* content, because the source files never had any. Nothing was invented to fill the gaps.

## Same capability, three clouds

A dash means this set has no guide for it.

| Capability | AWS | Azure | Google Cloud |
|------------|-----|-------|--------------|
| Object storage | [01 S3](floci-aws-guides/01-s3.md) | [01 Blob Storage](floci-azure-guides/01-blob-storage.md) | [01 Cloud Storage](floci-gcp-guides/01-cloud-storage.md) |
| Key-value / NoSQL | [13 DynamoDB](floci-aws-guides/13-dynamodb.md) | [02 Table Storage](floci-azure-guides/02-table-storage.md) | [04 Firestore](floci-gcp-guides/04-firestore.md), [05 Bigtable](floci-gcp-guides/05-bigtable.md) |
| Serverless functions | [02 Lambda](floci-aws-guides/02-lambda.md) | [03 Azure Functions](floci-azure-guides/03-azure-functions.md) | [09 Cloud Functions](floci-gcp-guides/09-cloud-functions.md) |
| Relational database | [09 RDS Postgres](floci-aws-guides/09-rds-postgres.md) | [04 Azure SQL Database](floci-azure-guides/04-azure-sql-database.md) | [08 Cloud SQL](floci-gcp-guides/08-cloud-sql.md), [06 Spanner](floci-gcp-guides/06-spanner.md) |
| Secrets | none | [05 Key Vault](floci-azure-guides/05-key-vault.md) | none |
| Streaming ingest | [11 Kinesis Data Streams](floci-aws-guides/11-kinesis-data-streams.md), [12 Firehose](floci-aws-guides/12-kinesis-firehose.md) | [06 Event Hubs](floci-azure-guides/06-event-hubs.md) | [02 Pub/Sub](floci-gcp-guides/02-pubsub.md) |
| Topics and queues | [06 SNS](floci-aws-guides/06-sns.md), [14 SQS + DLQ](floci-aws-guides/14-sqs-dlq.md) | [07 Service Bus](floci-azure-guides/07-service-bus.md) | [02 Pub/Sub](floci-gcp-guides/02-pubsub.md), [07 Cloud Tasks](floci-gcp-guides/07-cloud-tasks.md) |
| Event routing / webhooks | [05 EventBridge Scheduler](floci-aws-guides/05-eventbridge-scheduler.md) | [08 Event Grid](floci-azure-guides/08-event-grid.md) | none |
| SQL analytics | [10 Athena](floci-aws-guides/10-athena.md) | none | [03 BigQuery](floci-gcp-guides/03-bigquery.md) |
| Orchestration and ETL | [03 Glue](floci-aws-guides/03-glue.md), [04 Step Functions](floci-aws-guides/04-step-functions.md) | none | none |
| Containers | none | none | [10 Cloud Run](floci-gcp-guides/10-cloud-run.md) |
| Identity and monitoring | [07 IAM](floci-aws-guides/07-iam.md), [08 CloudWatch](floci-aws-guides/08-cloudwatch.md) | none | none |
| Multi-service projects | [15 End-to-end](floci-aws-guides/15-end-to-end-pipeline.md) | none | [11 to 16](floci-gcp-guides/README.md#projects) (ingest, queue, telemetry, batch, stream, CDC) |

## Quick start

Docker is required for all three. Pick one cloud.

```bash
# AWS
cd floci-aws-guides/project && cp .env.example .env && docker compose up -d
echo "127.0.0.1 floci" | sudo tee -a /etc/hosts
python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt && make test

# Azure (one service at a time: they share port 4577)
cd floci-azure-guides/project/01-blob-storage && docker compose up -d
pip install -r requirements.txt && pytest tests -v

# Google Cloud
cd floci-gcp-guides/project && docker compose up -d && source .env.local
python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt
```

Then open that folder's `README.md` and follow its suggested order.

## What was verified, and what was not

Read this before trusting any of the code. Details are in each folder's README.

| Cloud | Verified | Not verified |
|-------|----------|--------------|
| AWS | 16 tests passed, 2 skipped against an AWS mock server; RDS code against a real local Postgres; Terraform syntax and references | Terraform `plan/apply`; anything run on Floci itself |
| Azure | 61 tests passed, 2 skipped, using Azurite, fakes, SQLite and a local HTTP server | Floci AZ itself, SQL Server, the AMQP brokers |
| Google Cloud | nothing is recorded | everything; run it against the emulators first |

The Floci-specific behavior in the AWS and Azure guides comes from Floci's own documentation, not from running Floci. Run the tests on your machine and report any failure.

## Things that differ between the clouds

- **Ports.** AWS is one endpoint on `4566`. Azure is one port, `4577`, routed by URL path (`/<account>`, `/<account>-table`, and so on). GCP is a different port per emulator.
- **Terraform.** AWS ships a full Terraform folder pointed at Floci. Azure provisions through the emulator's own APIs and documents the `azurerm` resources for real Azure. GCP Terraform targets real GCP only.
- **Docker socket.** Needed for Lambda, RDS and Athena on AWS; for Functions, Azure SQL, Event Hubs and Service Bus on Azure; not needed on GCP.
