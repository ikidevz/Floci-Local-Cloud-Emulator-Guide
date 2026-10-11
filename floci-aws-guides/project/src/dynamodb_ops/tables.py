from botocore.exceptions import ClientError

from src.common.aws import client
from src.common.waiters import wait_until

ddb = client("dynamodb")


def create_events_table(table: str) -> str:
    """PK = run_id, SK = ts; GSI by status. Returns the table ARN."""
    try:
        ddb.create_table(
            TableName=table,
            BillingMode="PAY_PER_REQUEST",
            AttributeDefinitions=[
                {"AttributeName": "run_id", "AttributeType": "S"},
                {"AttributeName": "ts", "AttributeType": "S"},
                {"AttributeName": "status", "AttributeType": "S"},
            ],
            KeySchema=[{"AttributeName": "run_id", "KeyType": "HASH"},
                       {"AttributeName": "ts", "KeyType": "RANGE"}],
            GlobalSecondaryIndexes=[{
                "IndexName": "status-ts-index",
                "KeySchema": [{"AttributeName": "status", "KeyType": "HASH"},
                              {"AttributeName": "ts", "KeyType": "RANGE"}],
                "Projection": {"ProjectionType": "ALL"}}],
            StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_AND_OLD_IMAGES"},
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceInUseException":
            raise
    wait_until(lambda: ddb.describe_table(TableName=table)["Table"]["TableStatus"],
               done={"ACTIVE"}, timeout=60)
    return ddb.describe_table(TableName=table)["Table"]["TableArn"]


def enable_ttl(table: str, attribute: str = "expires_at") -> None:
    ddb.update_time_to_live(TableName=table,
                            TimeToLiveSpecification={"Enabled": True, "AttributeName": attribute})


def delete(table: str) -> None:
    ddb.delete_table(TableName=table)
