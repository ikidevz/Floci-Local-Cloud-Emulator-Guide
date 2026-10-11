from botocore.exceptions import ClientError

from src.common.aws import client

lam = client("lambda")


def sqs_trigger(fn_name: str, queue_arn: str, batch_size: int = 10) -> str:
    return lam.create_event_source_mapping(
        EventSourceArn=queue_arn, FunctionName=fn_name, BatchSize=batch_size,
        FunctionResponseTypes=["ReportBatchItemFailures"])["UUID"]


def kinesis_trigger(fn_name: str, stream_arn: str, batch_size: int = 100,
                    start: str = "LATEST") -> str:
    return lam.create_event_source_mapping(
        EventSourceArn=stream_arn, FunctionName=fn_name, BatchSize=batch_size,
        StartingPosition=start, MaximumRetryAttempts=2, BisectBatchOnFunctionError=True)["UUID"]


def list_mappings(fn_name: str) -> list[dict]:
    return lam.list_event_source_mappings(FunctionName=fn_name)["EventSourceMappings"]


def delete_mapping(uuid: str) -> None:
    try:
        lam.delete_event_source_mapping(UUID=uuid)
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
