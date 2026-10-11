"""Create the whole pipeline with boto3 (do NOT also run Terraform on the same names)."""
from src.athena_ops import setup as athena_setup
from src.cloudwatch_ops import alarms, logs, metrics
from src.common.aws import arn, name
from src.dynamodb_ops.tables import create_events_table, enable_ttl
from src.firehose_ops.streams import create_to_s3
from src.glue_ops import catalog, crawlers, jobs
from src.iam_ops.roles import create_all
from src.kinesis_ops.streams import create as create_stream
from src.lambda_ops.functions import allow_service, deploy
from src.lambda_ops.mappings import kinesis_trigger, sqs_trigger
from src.s3_ops.buckets import create_pipeline_buckets
from src.s3_ops.notifications import notify_sqs
from src.scheduler_ops.schedules import ensure_group, upsert_schedule
from src.sfn_ops import machines
from src.sns_ops.topics import create_topic
from src.sqs_ops.queues import allow_senders, create_with_dlq


def main() -> dict:
    out: dict = {}
    roles = create_all()                                                          # 07 IAM
    buckets = create_pipeline_buckets()                                           # 01 S3

    q = create_with_dlq(name("jobs"), max_receive=3, visibility_s=60)             # 14 SQS + DLQ
    allow_senders(q["main_url"], "s3.amazonaws.com", f"arn:aws:s3:::{buckets['raw']}")
    notify_sqs(buckets["raw"], q["main_arn"], prefix="raw/", suffix=".csv")

    topic = create_topic(name("alerts"))                                          # 06 SNS
    create_events_table(name("events"))                                           # 13 DynamoDB
    enable_ttl(name("events"))
    stream_arn = create_stream(name("events"), shards=1)                          # 11 Kinesis
    create_to_s3(name("to-s3"), roles["firehose-role"], buckets["curated"],       # 12 Firehose
                 prefix="events/", buffer_s=60, buffer_mb=1)

    ingest = deploy(name("ingest"), "lambda_src/ingest", roles["lambda-role"],    # 02 Lambda
                    env={"TABLE_NAME": name("events"), "STREAM_NAME": name("events")},
                    timeout=30, dlq_arn=q["dlq_arn"])
    runner = deploy(name("glue-runner"), "lambda_src/glue_runner", roles["lambda-role"])
    bridge = deploy(name("kinesis-to-firehose"), "lambda_src/kinesis_to_firehose", roles["lambda-role"],
                    env={"FIREHOSE_STREAM": name("to-s3")})
    sqs_trigger(name("ingest"), q["main_arn"])
    kinesis_trigger(name("kinesis-to-firehose"), stream_arn, start="TRIM_HORIZON")

    catalog.ensure_database(athena_setup.DB)                                      # 03 Glue
    crawlers.ensure_crawler(name("orders-crawler"), roles["glue-role"], athena_setup.DB,
                            f"s3://{buckets['raw']}/raw/orders/")
    script = jobs.upload_script(buckets["scripts"], "glue_scripts/clean_orders_shell.py",
                                "glue/clean_orders_shell.py")
    jobs.ensure_job(name("clean-orders"), roles["glue-role"], script, shell=True)
    wg = athena_setup.bootstrap()                                                 # 10 Athena (+ tables)

    g = f"/{name('ingest')}"                                                      # 08 CloudWatch
    logs.ensure_group(g)
    alarms.failure_alarm(name("failed-jobs"), "FailedJobs", metrics.NAMESPACE, topic, stage="ingest")

    definition = machines.render_definition(                                      # 04 Step Functions
        "sfn_definitions/etl_pipeline.asl.json",
        GLUE_RUNNER_ARN=runner, GLUE_JOB_NAME=name("clean-orders"), SNS_TOPIC_ARN=topic)
    sm_arn = machines.upsert(name("etl"), definition, roles["sfn-role"])

    ensure_group(name("group"))                                                   # 05 Scheduler
    upsert_schedule(name("nightly-etl"), "cron(0 2 * * ? *)", sm_arn, roles["scheduler-role"],
                    {"source": "scheduler"}, group=name("group"), tz="Asia/Manila")

    out.update(queue=q, topic=topic, ingest=ingest, bridge=bridge, runner=runner, workgroup=wg,
               state_machine=sm_arn, buckets=buckets, stream_arn=stream_arn)
    return out


if __name__ == "__main__":
    import json
    print(json.dumps(main(), indent=2))
