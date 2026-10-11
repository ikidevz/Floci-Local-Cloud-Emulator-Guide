from botocore.exceptions import ClientError

from src.common.aws import client
from src.common.waiters import wait_until

fh = client("firehose")


def create_to_s3(stream: str, role_arn: str, bucket: str, prefix: str = "firehose/",
                 buffer_s: int = 60, buffer_mb: int = 1, gzip: bool = False) -> str:
    """DirectPut delivery stream -> S3. Returns the stream ARN."""
    cfg = {
        "RoleARN": role_arn,
        "BucketARN": f"arn:aws:s3:::{bucket}",
        "Prefix": prefix,
        "ErrorOutputPrefix": prefix + "errors/!{firehose:error-output-type}/",
        "BufferingHints": {"IntervalInSeconds": buffer_s, "SizeInMBs": buffer_mb},
        "CompressionFormat": "GZIP" if gzip else "UNCOMPRESSED",
    }
    try:
        arn = fh.create_delivery_stream(DeliveryStreamName=stream, DeliveryStreamType="DirectPut",
                                        ExtendedS3DestinationConfiguration=cfg)["DeliveryStreamARN"]
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceInUseException":
            raise
        arn = fh.describe_delivery_stream(DeliveryStreamName=stream)[
            "DeliveryStreamDescription"]["DeliveryStreamARN"]
    wait_until(lambda: fh.describe_delivery_stream(DeliveryStreamName=stream)
               ["DeliveryStreamDescription"]["DeliveryStreamStatus"], done={"ACTIVE"}, timeout=60)
    return arn


def delete(stream: str) -> None:
    fh.delete_delivery_stream(DeliveryStreamName=stream, AllowForceDelete=True)
