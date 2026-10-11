"""
Glue runner Lambda: lets Step Functions drive Glue without the (unsupported-on-Floci) glue:startJobRun.sync.

event = {"action": "start_job" | "job_status" | "start_crawler" | "crawler_status", ...}
"""
import boto3

glue = boto3.client("glue")


def lambda_handler(event, context):
    action = event["action"]

    if action == "start_job":
        run_id = glue.start_job_run(JobName=event["job"], Arguments=event.get("arguments", {}))["JobRunId"]
        return {"job": event["job"], "run_id": run_id}

    if action == "job_status":
        run = glue.get_job_run(JobName=event["job"], RunId=event["run_id"])["JobRun"]
        return {"job": event["job"], "run_id": event["run_id"], "state": run["JobRunState"],
                "error": run.get("ErrorMessage", "")}

    if action == "start_crawler":
        glue.start_crawler(Name=event["crawler"])
        return {"crawler": event["crawler"]}

    if action == "crawler_status":
        c = glue.get_crawler(Name=event["crawler"])["Crawler"]
        return {"crawler": event["crawler"], "state": c["State"],
                "last_crawl": c.get("LastCrawl", {}).get("Status", "")}

    raise ValueError(f"unknown action: {action}")
