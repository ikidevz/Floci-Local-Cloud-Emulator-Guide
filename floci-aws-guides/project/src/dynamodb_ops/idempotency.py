from botocore.exceptions import ClientError

from src.common.aws import client

ddb = client("dynamodb")


def claim(table: str, run_id: str, ts: str) -> bool:
    """True the first time a (run_id, ts) is seen; False on duplicates (e.g. SQS redelivery)."""
    try:
        ddb.put_item(TableName=table,
                     Item={"run_id": {"S": run_id}, "ts": {"S": ts}, "status": {"S": "CLAIMED"}},
                     ConditionExpression="attribute_not_exists(run_id)")
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise
