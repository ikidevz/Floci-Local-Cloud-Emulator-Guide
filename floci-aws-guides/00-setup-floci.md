# 00 — Setup & Shared Foundation

> [Index](README.md) · [01 — S3 →](01-s3.md)

Everything the other guides rely on: running Floci, the shared boto3 client factory, polling helpers, the test harness, and how Terraform points at Floci.

---

## 1. Documentation

### Overview

- [Floci](https://github.com/floci-io/floci) is a free, open-source local AWS emulator. One endpoint, `http://localhost:4566`, serves every AWS API.
- Any credentials are accepted (`test` / `test`). Never put real AWS keys in `.env`.
- Every guide uses the same client factory, the same naming convention and idempotent helpers, so any snippet can be re-run safely.

### At a glance

| Item | Value |
|------|-------|
| Endpoint | `http://localhost:4566` |
| Credentials | anything (`test` / `test`) |
| Default region | `us-east-1` |
| Docker socket | required for Lambda, RDS and Athena |
| Extra ports | `7001-7099` (RDS proxy ports) |
| Terraform provider | `hashicorp/aws ~> 6.0` (the version Floci's own compatibility suite uses) |

### Why the Docker settings matter

- **Docker socket**: Lambda, RDS and the Athena SQL engine (a `floci-duck` sidecar) run as real containers.
- **`7001-7099`**: Floci's RDS proxy ports. You connect to Postgres on whatever port `describe_db_instances` returns, so the whole range must be published.
- **`FLOCI_HOSTNAME: floci`**: Floci puts this hostname in the URLs it returns (SQS `QueueUrl`, for example) and in the `AWS_ENDPOINT_URL` it injects into Lambda containers. Code running **on your host** must be able to resolve `floci`: add `127.0.0.1 floci` to your hosts file, or run the code inside the compose network with `docker compose --profile tools run --rm app pytest`.
- **`FLOCI_SERVICES_DOCKER_NETWORK: floci-net`**: the network that Lambda and RDS containers join so they can reach Floci.

### Conventions

- Resource names come from `name("suffix")` → `floci-demo-suffix`.
- Python helpers are idempotent (they tolerate "already exists"), so you can re-run them.
- Terraform and the boto3 `bootstrap` create the **same names**. Use one or the other on a given Floci, not both.
- `conftest.py` sets a random `PROJECT_PREFIX` per test session, so reruns never collide with leftovers in a persistent Floci.

### What was verified

| Check | Result |
|-------|--------|
| Pytest suite against an AWS mock server | 16 passed, 2 skipped |
| RDS load/query code against a real local Postgres | passed |
| Terraform HCL syntax and reference check | passed |
| Terraform `plan` / `apply` | **not run** (no Terraform binary available) |
| Anything executed on Floci itself (`test_full_flow_on_floci`) | **not run** (needs Docker) |

Run `make test` against your own Floci before relying on the Floci-specific behavior. It comes from Floci's documentation, not from running it.

---

## 2. Basic Usage

### Step 1: Start Floci

```bash
docker compose up -d
docker logs -f floci
```

Quick start without compose. There is no Docker socket mount, so Lambda, RDS and Athena will not work:

```bash
docker run -d --name floci -p 4566:4566 floci/floci:latest
```

The full `docker-compose.yml` is in [Example 1](#example-1--full-docker-composeyml).

### Step 2: Point your tools at it

```bash
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_DEFAULT_REGION=us-east-1
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
```

Recent versions of boto3 and the AWS CLI read `AWS_ENDPOINT_URL` automatically, so the snippets in every other guide need no endpoint code.

### Step 3: Verify

```python
import boto3

print(boto3.client("sts").get_caller_identity())   # returns an account id: Floci is up
```

```bash
aws sts get-caller-identity
aws s3 mb s3://smoke-test && aws s3 ls
```

### Step 4: Install the Python dependencies

```bash
cp .env.example .env
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

---

## 3. Examples

### Example 1 — Full `docker-compose.yml`

Floci plus an optional `app` container that runs your Python code inside the same Docker network (no hosts-file change needed).

`docker-compose.yml`

```yaml
name: floci-aws

services:
  floci:
    image: floci/floci:latest
    container_name: floci
    ports:
      - "4566:4566"            # every AWS API
      - "7001-7099:7001-7099"  # RDS proxy ports (needed to reach Postgres from the host)
    environment:
      FLOCI_HOSTNAME: floci                      # URLs (e.g. SQS QueueUrl) use this name
      FLOCI_SERVICES_DOCKER_NETWORK: floci-net   # Lambda / RDS / Athena containers join this network
      FLOCI_STORAGE_MODE: persistent
      FLOCI_STORAGE_PERSISTENT_PATH: /app/data
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock  # Lambda, RDS and the Athena sidecar are real containers
      - floci-data:/app/data
    networks: [floci-net]

  # Optional: run the Python code inside the same Docker network (no hosts-file change needed)
  #   docker compose --profile tools run --rm app pytest
  app:
    image: python:3.12-slim
    profiles: ["tools"]
    working_dir: /work
    volumes:
      - .:/work
    environment:
      AWS_ENDPOINT_URL: http://floci:4566
      AWS_DEFAULT_REGION: us-east-1
      AWS_ACCESS_KEY_ID: test
      AWS_SECRET_ACCESS_KEY: test
    command: sh -c "pip install -q -r requirements.txt && sleep infinity"
    depends_on: [floci]
    networks: [floci-net]

networks:
  floci-net:
    name: floci-net

volumes:
  floci-data:
```

### Example 2 — Environment and dependencies

`.env.example`

```text
AWS_ENDPOINT_URL=http://localhost:4566
AWS_DEFAULT_REGION=us-east-1
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
PROJECT_PREFIX=floci-demo
```

`requirements.txt`

```text
boto3>=1.35
python-dotenv>=1.0
psycopg2-binary>=2.9
pandas>=2.2
pytest>=8.0
```

### Example 3 — Shared client factory

One place that points every boto3 client/resource at Floci (or real AWS if the endpoint is unset). `src/__init__.py` and `src/common/__init__.py` are empty files.

`src/common/aws.py`

```python
"""Single place that points every boto3 client/resource at Floci (or real AWS if unset)."""
import os
from functools import lru_cache

import boto3
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()

ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
PREFIX = os.getenv("PROJECT_PREFIX", "floci-demo")

_CONFIG = Config(
    retries={"max_attempts": 5, "mode": "standard"},
    s3={"addressing_style": "path"},  # path-style: http://localhost:4566/<bucket>/<key>
)

_COMMON = dict(
    endpoint_url=ENDPOINT_URL,
    region_name=REGION,
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
    config=_CONFIG,
)


def client(service: str):
    return boto3.client(service, **_COMMON)


def resource(service: str):
    return boto3.resource(service, **_COMMON)


@lru_cache(maxsize=1)
def account_id() -> str:
    """Ask the emulator instead of hard-coding an account id."""
    return client("sts").get_caller_identity()["Account"]


def arn(service: str, resource_path: str, *, region: bool = True) -> str:
    """Build an ARN. IAM and S3 ARNs have no region."""
    r = REGION if region else ""
    return f"arn:aws:{service}:{r}:{account_id()}:{resource_path}"


def name(suffix: str) -> str:
    """Consistent naming: <prefix>-<suffix>."""
    return f"{PREFIX}-{suffix}"
```

### Example 4 — Polling helper

Used by every guide that waits for a status (Lambda, Glue, Athena, RDS, Step Functions…).

`src/common/waiters.py`

```python
import time
from typing import Callable, Iterable


def wait_until(check: Callable[[], str], *, done: Iterable[str], failed: Iterable[str] = (),
               timeout: int = 120, interval: float = 1.0) -> str:
    """Poll check() (returns a status string) until it reaches a done or failed state."""
    done, failed = set(done), set(failed)
    deadline = time.time() + timeout
    status = None
    while time.time() < deadline:
        status = check()
        if status in done:
            return status
        if status in failed:
            raise RuntimeError(f"Reached failure state: {status}")
        time.sleep(interval)
    raise TimeoutError(f"Timed out after {timeout}s (last status: {status})")
```

### Example 5 — Test harness

`pytest.ini`

```text
[pytest]
pythonpath = .
testpaths = tests
addopts = -q
```

`tests/conftest.py`

```python
import os
import uuid

# Unique prefix per test session so reruns never collide. Must run BEFORE src.common.aws is imported.
os.environ["PROJECT_PREFIX"] = "ft" + uuid.uuid4().hex[:6]

import pytest  # noqa: E402
from botocore.exceptions import EndpointConnectionError  # noqa: E402

from src.common.aws import client  # noqa: E402


@pytest.fixture(scope="session", autouse=True)
def emulator_up():
    try:
        client("sts").get_caller_identity()
    except EndpointConnectionError:
        pytest.exit("No AWS endpoint reachable. Start Floci: docker compose up -d", returncode=2)
```

```bash
pytest tests/test_01_iam_s3.py      # first smoke check
make unit                           # S3, IAM, SQS, SNS, DynamoDB, Kinesis, Firehose, CloudWatch
make test                           # everything
```

### Example 6 — Terraform pointed at Floci

The endpoint must be set for **every** service your configuration touches. OpenTofu works the same way: replace `terraform` with `tofu`.

`terraform/providers.tf`

```hcl
terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0" # the version Floci's own compatibility suite runs against
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }
}

variable "endpoint" {
  type    = string
  default = "http://localhost:4566"
}

provider "aws" {
  region     = var.region
  access_key = "test"
  secret_key = "test"

  # Floci is not real AWS: skip the checks that need real AWS endpoints
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
  s3_use_path_style           = true

  endpoints {
    s3             = var.endpoint
    sqs            = var.endpoint
    sns            = var.endpoint
    dynamodb       = var.endpoint
    lambda         = var.endpoint
    iam            = var.endpoint
    sts            = var.endpoint
    kinesis        = var.endpoint
    firehose       = var.endpoint
    glue           = var.endpoint
    athena         = var.endpoint
    rds            = var.endpoint
    cloudwatch     = var.endpoint
    logs           = var.endpoint
    events         = var.endpoint
    sfn            = var.endpoint
    scheduler      = var.endpoint
  }
}
```

`terraform/variables.tf`

```hcl
variable "region" {
  type    = string
  default = "us-east-1"
}

variable "prefix" {
  type    = string
  default = "floci-demo"
}

variable "db_password" {
  type      = string
  default   = "ChangeMe123!" # local only; use a secret manager on real AWS
  sensitive = true
}

locals {
  tags = {
    Project = var.prefix
    Env     = "floci-local"
  }
}
```

```bash
cd terraform
terraform init
terraform apply -auto-approve
```

### Example 7 — Makefile

`Makefile`

```makefile
.PHONY: up down reset install test unit pipeline tf-init tf-apply tf-destroy

up:        ; docker compose up -d
down:      ; docker compose down
reset:     ; docker compose down -v && docker compose up -d
install:   ; python -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt
test:      ; pytest                       # all tests against $$AWS_ENDPOINT_URL (Floci)
unit:      ; pytest tests/test_01_iam_s3.py tests/test_02_sqs_sns.py tests/test_03_data_services.py
pipeline:  ; python -m src.bootstrap      # create everything with boto3
tf-init:   ; cd terraform && terraform init
tf-apply:  ; cd terraform && terraform apply -auto-approve
tf-destroy:; cd terraform && terraform destroy -auto-approve
```

---

## Reference

- Suggested order through the guides: `00 → 07 → 01 → 14 → 06 → 02 → 13 → 11 → 12 → 03 → 10 → 09 → 08 → 04 → 05 → 15`.
