from botocore.exceptions import ClientError

from src.common.aws import client
from src.common.waiters import wait_until

rds = client("rds")


def create_postgres(identifier: str, db_name: str = "appdb", user: str = "app",
                    password: str = "ChangeMe123!", version: str = "16",
                    instance_class: str = "db.t3.micro", storage_gb: int = 20) -> dict:
    """Create a Postgres instance (Floci starts a real container) and wait for 'available'."""
    try:
        rds.create_db_instance(
            DBInstanceIdentifier=identifier, Engine="postgres", EngineVersion=version,
            DBInstanceClass=instance_class, AllocatedStorage=storage_gb,
            DBName=db_name, MasterUsername=user, MasterUserPassword=password,
            PubliclyAccessible=True, BackupRetentionPeriod=0)
    except ClientError as e:
        if e.response["Error"]["Code"] != "DBInstanceAlreadyExists":
            raise
    wait_until(lambda: describe(identifier)["DBInstanceStatus"],
               done={"available"}, failed={"failed", "incompatible-parameters"}, timeout=300, interval=3)
    return describe(identifier)


def describe(identifier: str) -> dict:
    return rds.describe_db_instances(DBInstanceIdentifier=identifier)["DBInstances"][0]


def endpoint(identifier: str) -> tuple[str, int]:
    ep = describe(identifier)["Endpoint"]
    return ep["Address"], int(ep["Port"])


def snapshot(identifier: str, snapshot_id: str) -> None:
    rds.create_db_snapshot(DBInstanceIdentifier=identifier, DBSnapshotIdentifier=snapshot_id)


def delete(identifier: str) -> None:
    rds.delete_db_instance(DBInstanceIdentifier=identifier, SkipFinalSnapshot=True,
                           DeleteAutomatedBackups=True)
