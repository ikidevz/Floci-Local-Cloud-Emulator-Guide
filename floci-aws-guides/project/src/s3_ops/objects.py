import json
from pathlib import Path

from src.common.aws import client

s3 = client("s3")


def upload_file(bucket: str, key: str, path: str, content_type: str | None = None) -> None:
    extra = {"ContentType": content_type} if content_type else None
    s3.upload_file(path, bucket, key, ExtraArgs=extra)


def put_text(bucket: str, key: str, text: str, content_type: str = "text/plain") -> None:
    s3.put_object(Bucket=bucket, Key=key, Body=text.encode(), ContentType=content_type)


def get_text(bucket: str, key: str) -> str:
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode()


def put_json(bucket: str, key: str, payload: dict) -> None:
    put_text(bucket, key, json.dumps(payload), "application/json")


def get_json(bucket: str, key: str) -> dict:
    return json.loads(get_text(bucket, key))


def download(bucket: str, key: str, dest: str) -> Path:
    Path(dest).parent.mkdir(parents=True, exist_ok=True)
    s3.download_file(bucket, key, dest)
    return Path(dest)


def list_keys(bucket: str, prefix: str = "") -> list[str]:
    keys: list[str] = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        keys += [o["Key"] for o in page.get("Contents", [])]
    return keys


def presigned_get(bucket: str, key: str, expires: int = 900) -> str:
    return s3.generate_presigned_url("get_object", Params={"Bucket": bucket, "Key": key},
                                     ExpiresIn=expires)


def copy(src_bucket: str, src_key: str, dst_bucket: str, dst_key: str) -> None:
    s3.copy_object(Bucket=dst_bucket, Key=dst_key,
                   CopySource={"Bucket": src_bucket, "Key": src_key})


def delete(bucket: str, key: str) -> None:
    s3.delete_object(Bucket=bucket, Key=key)
