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
