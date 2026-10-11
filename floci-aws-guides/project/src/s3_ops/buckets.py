from botocore.exceptions import ClientError

from src.common.aws import REGION, client, name

s3 = client("s3")

PIPELINE_BUCKETS = ("raw", "curated", "scripts", "athena-results")


def create_bucket(bucket: str, versioning: bool = False) -> str:
    kwargs = {"Bucket": bucket}
    if REGION != "us-east-1":  # us-east-1 must NOT send a LocationConstraint
        kwargs["CreateBucketConfiguration"] = {"LocationConstraint": REGION}
    try:
        s3.create_bucket(**kwargs)
    except ClientError as e:
        if e.response["Error"]["Code"] not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            raise
    if versioning:
        s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    return bucket


def set_lifecycle(bucket: str, prefix: str = "raw/", days: int = 30) -> None:
    s3.put_bucket_lifecycle_configuration(
        Bucket=bucket,
        LifecycleConfiguration={"Rules": [{
            "ID": f"expire-{prefix.strip('/').replace('/', '-')}",
            "Status": "Enabled",
            "Filter": {"Prefix": prefix},
            "Expiration": {"Days": days},
        }]},
    )


def create_pipeline_buckets() -> dict[str, str]:
    out = {}
    for suffix in PIPELINE_BUCKETS:
        out[suffix] = create_bucket(name(suffix), versioning=(suffix == "raw"))
    set_lifecycle(out["raw"])
    return out


def empty_and_delete(bucket: str) -> None:
    """Delete every object version and delete marker, then the bucket."""
    for page in s3.get_paginator("list_object_versions").paginate(Bucket=bucket):
        objs = [{"Key": v["Key"], "VersionId": v["VersionId"]}
                for v in page.get("Versions", []) + page.get("DeleteMarkers", [])]
        if objs:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": objs})
    s3.delete_bucket(Bucket=bucket)


def list_buckets() -> list[str]:
    return [b["Name"] for b in s3.list_buckets()["Buckets"]]


if __name__ == "__main__":
    print(create_pipeline_buckets())
    print(list_buckets())
