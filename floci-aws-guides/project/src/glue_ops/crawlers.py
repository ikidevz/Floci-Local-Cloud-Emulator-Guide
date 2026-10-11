from botocore.exceptions import ClientError

from src.common.aws import client
from src.common.waiters import wait_until

glue = client("glue")


def ensure_crawler(crawler: str, role_arn: str, db: str, s3_path: str, schedule: str | None = None) -> None:
    kwargs = dict(Name=crawler, Role=role_arn, DatabaseName=db,
                  Targets={"S3Targets": [{"Path": s3_path}]},
                  SchemaChangePolicy={"UpdateBehavior": "UPDATE_IN_DATABASE", "DeleteBehavior": "LOG"})
    if schedule:
        kwargs["Schedule"] = schedule  # e.g. "cron(0 2 * * ? *)"
    try:
        glue.create_crawler(**kwargs)
    except ClientError as e:
        if e.response["Error"]["Code"] != "AlreadyExistsException":
            raise
        cur = glue.get_crawler(Name=crawler)["Crawler"]
        same = (cur["Role"].endswith(role_arn.split("/")[-1]) and cur["DatabaseName"] == db
                and cur["Targets"].get("S3Targets", [{}])[0].get("Path") == s3_path)
        if same:
            return  # nothing to change (also avoids CrawlerRunningException on a busy crawler)
        glue.update_crawler(**kwargs)


def run_crawler(crawler: str, timeout: int = 300) -> str:
    """Start a crawl and wait for READY. Returns LastCrawl.Status (SUCCEEDED/FAILED/CANCELLED)."""
    glue.start_crawler(Name=crawler)
    wait_until(lambda: glue.get_crawler(Name=crawler)["Crawler"]["State"],
               done={"READY"}, timeout=timeout, interval=1)
    return glue.get_crawler(Name=crawler)["Crawler"].get("LastCrawl", {}).get("Status", "UNKNOWN")
