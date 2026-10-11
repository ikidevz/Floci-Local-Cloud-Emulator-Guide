# 07 — IAM

> [Index](README.md) · [← 06 — SNS](06-sns.md) · [08 — CloudWatch →](08-cloudwatch.md)

Roles for Lambda, Glue, Step Functions, Scheduler and Firehose. Create these **first**.

---

## 1. Documentation

### Overview

- A **role** has a **trust policy** (who may assume it) and **permission policies** (what it may do).
- This project creates five roles, each with a least-privilege inline policy scoped to resources named `<prefix>-*`.

| Role suffix | Trusted principal | Used by |
|-------------|-------------------|---------|
| `lambda-role` | `lambda.amazonaws.com` | Lambda functions |
| `glue-role` | `glue.amazonaws.com` | Glue jobs and crawlers |
| `sfn-role` | `states.amazonaws.com` | Step Functions |
| `scheduler-role` | `scheduler.amazonaws.com` | EventBridge Scheduler |
| `firehose-role` | `firehose.amazonaws.com` | Firehose delivery streams |

### At a glance

| Item | Value |
|------|-------|
| boto3 client | `iam` |
| Terraform endpoint | `iam` |
| Needs first | nothing: this is the first guide to apply after setup |
| Docker socket | not required |

### How it behaves on Floci

- Floci stores roles and policies and returns real-looking ARNs. Do not assume it **enforces** policies like AWS does (check Floci's IAM docs for what is enforced). Write least-privilege policies anyway, so the same code is safe on real AWS.
- Scheduler requires `Target.RoleArn` to be a real IAM role ARN, so roles must exist before schedules.

### Gotchas

- Trust policy (who may **assume** the role) is separate from the permission policy (what the role may **do**). Most real-AWS `AccessDenied` bugs are a wrong trust principal.
- IAM is global: role ARNs have no region (`arn("iam", ..., region=False)`).
- `glue:*` on `*` is a local convenience. Narrow it before real AWS.

---

## 2. Basic Usage

> Prerequisite: Floci is running and the `AWS_*` environment variables are set ([guide 00](00-setup-floci.md)).

```python
import json
import boto3

iam = boto3.client("iam")

# 1. Create a role. The trust policy names the service that may assume it.
trust = {"Version": "2012-10-17", "Statement": [
    {"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}]}
role_arn = iam.create_role(RoleName="demo-lambda-role",
                           AssumeRolePolicyDocument=json.dumps(trust))["Role"]["Arn"]

# 2. Attach a permission policy (inline, so put_role_policy is idempotent)
permissions = {"Version": "2012-10-17", "Statement": [
    {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject"], "Resource": "arn:aws:s3:::demo-*/*"}]}
iam.put_role_policy(RoleName="demo-lambda-role", PolicyName="inline",
                    PolicyDocument=json.dumps(permissions))

# 3. Inspect
print(role_arn)
print(iam.list_role_policies(RoleName="demo-lambda-role")["PolicyNames"])   # ['inline']

# 4. Clean up
iam.delete_role_policy(RoleName="demo-lambda-role", PolicyName="inline")
iam.delete_role(RoleName="demo-lambda-role")
```

---

## 3. Examples

### Example 1 — Least-privilege policy documents

`src/iam_ops/policies.py`

```python
"""Least-privilege policy documents. `p` is the project prefix (e.g. floci-demo)."""


def trust(service: str) -> dict:
    return {"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Principal": {"Service": service}, "Action": "sts:AssumeRole"}]}


def doc(*statements: dict) -> dict:
    return {"Version": "2012-10-17", "Statement": list(statements)}


def s3_rw(p: str) -> dict:
    return {"Effect": "Allow",
            "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket",
                       "s3:GetBucketLocation", "s3:AbortMultipartUpload"],
            "Resource": [f"arn:aws:s3:::{p}-*", f"arn:aws:s3:::{p}-*/*"]}


LOGS = {"Effect": "Allow",
        "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
        "Resource": "*"}


def lambda_policy(p: str) -> dict:
    return doc(
        LOGS, s3_rw(p),
        {"Effect": "Allow",
         "Action": ["dynamodb:PutItem", "dynamodb:GetItem", "dynamodb:UpdateItem", "dynamodb:Query"],
         "Resource": f"arn:aws:dynamodb:*:*:table/{p}-*"},
        {"Effect": "Allow",
         "Action": ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:SendMessage"],
         "Resource": f"arn:aws:sqs:*:*:{p}-*"},
        {"Effect": "Allow",
         "Action": ["kinesis:GetRecords", "kinesis:GetShardIterator", "kinesis:DescribeStream",
                    "kinesis:ListShards", "kinesis:PutRecord", "kinesis:PutRecords"],
         "Resource": f"arn:aws:kinesis:*:*:stream/{p}-*"},
        {"Effect": "Allow", "Action": ["firehose:PutRecord", "firehose:PutRecordBatch"],
         "Resource": f"arn:aws:firehose:*:*:deliverystream/{p}-*"},
        {"Effect": "Allow",
         "Action": ["glue:StartJobRun", "glue:GetJobRun", "glue:StartCrawler", "glue:GetCrawler"],
         "Resource": "*"},
    )


def glue_policy(p: str) -> dict:
    return doc(LOGS, s3_rw(p), {"Effect": "Allow", "Action": ["glue:*"], "Resource": "*"})


def sfn_policy(p: str) -> dict:
    return doc(
        {"Effect": "Allow", "Action": ["lambda:InvokeFunction"],
         "Resource": f"arn:aws:lambda:*:*:function:{p}-*"},
        {"Effect": "Allow", "Action": ["sns:Publish"], "Resource": f"arn:aws:sns:*:*:{p}-*"},
        LOGS,
    )


def scheduler_policy(p: str) -> dict:
    return doc({"Effect": "Allow", "Action": ["states:StartExecution"],
                "Resource": f"arn:aws:states:*:*:stateMachine:{p}-*"})


def firehose_policy(p: str) -> dict:
    return doc(s3_rw(p), LOGS)
```

### Example 2 — Create, reuse and delete all pipeline roles

`ensure_role` creates the role (or reuses it) and overwrites the inline policy, so `create_all()` is idempotent.

`src/iam_ops/roles.py`

```python
import json

from botocore.exceptions import ClientError

from src.common.aws import PREFIX, client, name
from src.iam_ops import policies as pol

iam = client("iam")

# role suffix -> (trusted service principal, policy builder)
ROLES = {
    "lambda-role": ("lambda.amazonaws.com", pol.lambda_policy),
    "glue-role": ("glue.amazonaws.com", pol.glue_policy),
    "sfn-role": ("states.amazonaws.com", pol.sfn_policy),
    "scheduler-role": ("scheduler.amazonaws.com", pol.scheduler_policy),
    "firehose-role": ("firehose.amazonaws.com", pol.firehose_policy),
}


def ensure_role(role_name: str, service: str, policy_doc: dict) -> str:
    """Create the role (or reuse it) and set its inline policy. Returns the role ARN."""
    try:
        arn = iam.create_role(RoleName=role_name,
                              AssumeRolePolicyDocument=json.dumps(pol.trust(service)),
                              Description="Floci demo role")["Role"]["Arn"]
    except ClientError as e:
        if e.response["Error"]["Code"] != "EntityAlreadyExists":
            raise
        arn = iam.get_role(RoleName=role_name)["Role"]["Arn"]
    # put_role_policy overwrites, so this is idempotent
    iam.put_role_policy(RoleName=role_name, PolicyName=f"{role_name}-inline",
                        PolicyDocument=json.dumps(policy_doc))
    return arn


def create_all() -> dict[str, str]:
    """Create every pipeline role. Returns {suffix: role_arn}."""
    return {suffix: ensure_role(name(suffix), svc, builder(PREFIX))
            for suffix, (svc, builder) in ROLES.items()}


def role_arns() -> dict[str, str]:
    return {s: iam.get_role(RoleName=name(s))["Role"]["Arn"] for s in ROLES}


def delete_role(role_name: str) -> None:
    for p in iam.list_role_policies(RoleName=role_name)["PolicyNames"]:
        iam.delete_role_policy(RoleName=role_name, PolicyName=p)
    for p in iam.list_attached_role_policies(RoleName=role_name)["AttachedPolicies"]:
        iam.detach_role_policy(RoleName=role_name, PolicyArn=p["PolicyArn"])
    iam.delete_role(RoleName=role_name)


def delete_all() -> None:
    for suffix in ROLES:
        try:
            delete_role(name(suffix))
        except ClientError as e:
            if e.response["Error"]["Code"] != "NoSuchEntity":
                raise


if __name__ == "__main__":
    for k, v in create_all().items():
        print(f"{k:16} {v}")
```

```bash
python -m src.iam_ops.roles
```

### Example 3 — Test

`tests/test_01_iam_s3.py`

```python
from src.common.aws import name
from src.iam_ops import roles


def test_iam_roles_roundtrip():
    arns = roles.create_all()
    assert set(arns) == set(roles.ROLES)
    assert roles.create_all() == arns  # idempotent
    for suffix in ("lambda-role", "glue-role"):
        assert arns[suffix].endswith(name(suffix))
```

### Example 4 — Terraform

`terraform/iam.tf`

```hcl
locals {
  roles = {
    lambda    = "lambda.amazonaws.com"
    glue      = "glue.amazonaws.com"
    sfn       = "states.amazonaws.com"
    scheduler = "scheduler.amazonaws.com"
    firehose  = "firehose.amazonaws.com"
  }

  s3_rw = {
    Effect = "Allow"
    Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket",
    "s3:GetBucketLocation", "s3:AbortMultipartUpload"]
    Resource = ["arn:aws:s3:::${var.prefix}-*", "arn:aws:s3:::${var.prefix}-*/*"]
  }

  logs = {
    Effect   = "Allow"
    Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    Resource = "*"
  }
}

resource "aws_iam_role" "this" {
  for_each = local.roles
  name     = "${var.prefix}-${each.key}-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = each.value }
      Action    = "sts:AssumeRole"
    }]
  })
  tags = local.tags
}

resource "aws_iam_role_policy" "lambda" {
  name = "inline"
  role = aws_iam_role.this["lambda"].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      local.logs,
      local.s3_rw,
      {
        Effect   = "Allow"
        Action   = ["dynamodb:PutItem", "dynamodb:GetItem", "dynamodb:UpdateItem", "dynamodb:Query"]
        Resource = "arn:aws:dynamodb:*:*:table/${var.prefix}-*"
      },
      {
        Effect   = "Allow"
        Action   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:SendMessage"]
        Resource = "arn:aws:sqs:*:*:${var.prefix}-*"
      },
      {
        Effect = "Allow"
        Action = ["kinesis:GetRecords", "kinesis:GetShardIterator", "kinesis:DescribeStream",
        "kinesis:ListShards", "kinesis:PutRecord", "kinesis:PutRecords"]
        Resource = "arn:aws:kinesis:*:*:stream/${var.prefix}-*"
      },
      {
        Effect   = "Allow"
        Action   = ["firehose:PutRecord", "firehose:PutRecordBatch"]
        Resource = "arn:aws:firehose:*:*:deliverystream/${var.prefix}-*"
      },
      {
        Effect   = "Allow"
        Action   = ["glue:StartJobRun", "glue:GetJobRun", "glue:StartCrawler", "glue:GetCrawler"]
        Resource = "*"
      },
    ]
  })
}

resource "aws_iam_role_policy" "glue" {
  name = "inline"
  role = aws_iam_role.this["glue"].id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [local.logs, local.s3_rw, { Effect = "Allow", Action = ["glue:*"], Resource = "*" }]
  })
}

resource "aws_iam_role_policy" "sfn" {
  name = "inline"
  role = aws_iam_role.this["sfn"].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      local.logs,
      { Effect = "Allow", Action = ["lambda:InvokeFunction"], Resource = "arn:aws:lambda:*:*:function:${var.prefix}-*" },
      { Effect = "Allow", Action = ["sns:Publish"], Resource = "arn:aws:sns:*:*:${var.prefix}-*" },
    ]
  })
}

resource "aws_iam_role_policy" "scheduler" {
  name = "inline"
  role = aws_iam_role.this["scheduler"].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["states:StartExecution"]
      Resource = "arn:aws:states:*:*:stateMachine:${var.prefix}-*"
    }]
  })
}

resource "aws_iam_role_policy" "firehose" {
  name = "inline"
  role = aws_iam_role.this["firehose"].id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [local.s3_rw, local.logs]
  })
}
```

---

## Reference

Project files:

```
src/iam_ops/{policies,roles}.py
terraform/iam.tf
```

Related: every other guide. Lambda, Glue, Step Functions, Scheduler and Firehose all take a role ARN from here.
