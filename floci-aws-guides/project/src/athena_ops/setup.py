from botocore.exceptions import ClientError

from src.common.aws import client, name
from src.glue_ops import catalog

athena = client("athena")
DB = "floci_demo_db"


def ensure_workgroup(wg: str, results_bucket: str) -> None:
    try:
        athena.create_work_group(
            Name=wg,
            Configuration={
                "ResultConfiguration": {"OutputLocation": f"s3://{results_bucket}/results/"},
                "EnforceWorkGroupConfiguration": True,
                "PublishCloudWatchMetricsEnabled": False,
            })
    except ClientError as e:
        if "already" not in e.response["Error"]["Message"].lower():
            raise


def bootstrap(wg: str | None = None) -> str:
    """
    Databases and tables come from the Glue catalog (Floci's Athena reads them from there).
    Athena DDL such as CREATE EXTERNAL TABLE is NOT what Floci uses, so register via Glue.
    """
    wg = wg or name("wg")
    ensure_workgroup(wg, name("athena-results"))
    catalog.ensure_database(DB)
    catalog.ensure_csv_table(DB, "orders_raw", f"s3://{name('raw')}/raw/orders/", catalog.ORDERS_COLUMNS)
    catalog.ensure_csv_table(DB, "orders_clean", f"s3://{name('curated')}/clean/orders/",
                             catalog.ORDERS_COLUMNS)
    return wg
