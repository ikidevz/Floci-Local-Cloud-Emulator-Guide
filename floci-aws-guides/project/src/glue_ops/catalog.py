"""
Glue Data Catalog. On Floci, crawlers do NOT discover schemas or write tables,
so register tables explicitly with create_csv_table (Athena reads these tables).
"""
from botocore.exceptions import ClientError

from src.common.aws import client

glue = client("glue")

ORDERS_COLUMNS = {"order_id": "string", "customer_id": "string", "amount": "double", "status": "string"}


def ensure_database(db: str, description: str = "") -> None:
    try:
        glue.create_database(DatabaseInput={"Name": db, "Description": description})
    except ClientError as e:
        if e.response["Error"]["Code"] != "AlreadyExistsException":
            raise


def ensure_csv_table(db: str, table: str, location: str, columns: dict[str, str]) -> None:
    """location must be a folder prefix ending in '/'."""
    table_input = {
        "Name": table,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {"classification": "csv", "skip.header.line.count": "1"},
        "StorageDescriptor": {
            "Columns": [{"Name": k, "Type": v} for k, v in columns.items()],
            "Location": location,
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                          "Parameters": {"field.delim": ","}},
        },
    }
    try:
        glue.create_table(DatabaseName=db, TableInput=table_input)
    except ClientError as e:
        if e.response["Error"]["Code"] != "AlreadyExistsException":
            raise
        glue.update_table(DatabaseName=db, TableInput=table_input)


def list_tables(db: str) -> list[str]:
    return [t["Name"] for p in glue.get_paginator("get_tables").paginate(DatabaseName=db)
            for t in p["TableList"]]


def get_columns(db: str, table: str) -> dict[str, str]:
    cols = glue.get_table(DatabaseName=db, Name=table)["Table"]["StorageDescriptor"]["Columns"]
    return {c["Name"]: c["Type"] for c in cols}
