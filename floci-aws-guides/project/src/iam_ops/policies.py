"""Least-privilege policy documents. `p` is the project prefix (e.g. floci-demo)."""


def trust(service: str) -> dict:
    return {"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Principal": {"Service": service}, "Action": "sts:AssumeRole"}]}


def doc(*statements: dict) -> dict:
    return {"Version": "2012-10-17", "Statement": list(statements)}


def s3_rw(p: str) -> dict:
    return {"Effect": "Allow",
            "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket",
                       "s3:GetBucketLocation", "s3:AbortMultipartUpload"],
            "Resource": [f"arn:aws:s3:::{p}-*", f"arn:aws:s3:::{p}-*/*"]}


LOGS = {"Effect": "Allow",
        "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
        "Resource": "*"}


def lambda_policy(p: str) -> dict:
    return doc(
        LOGS, s3_rw(p),
        {"Effect": "Allow",
         "Action": ["dynamodb:PutItem", "dynamodb:GetItem", "dynamodb:UpdateItem", "dynamodb:Query"],
         "Resource": f"arn:aws:dynamodb:*:*:table/{p}-*"},
        {"Effect": "Allow",
         "Action": ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:SendMessage"],
         "Resource": f"arn:aws:sqs:*:*:{p}-*"},
        {"Effect": "Allow",
         "Action": ["kinesis:GetRecords", "kinesis:GetShardIterator", "kinesis:DescribeStream",
                    "kinesis:ListShards", "kinesis:PutRecord", "kinesis:PutRecords"],
         "Resource": f"arn:aws:kinesis:*:*:stream/{p}-*"},
        {"Effect": "Allow", "Action": ["firehose:PutRecord", "firehose:PutRecordBatch"],
         "Resource": f"arn:aws:firehose:*:*:deliverystream/{p}-*"},
        {"Effect": "Allow",
         "Action": ["glue:StartJobRun", "glue:GetJobRun", "glue:StartCrawler", "glue:GetCrawler"],
         "Resource": "*"},
    )


def glue_policy(p: str) -> dict:
    return doc(LOGS, s3_rw(p), {"Effect": "Allow", "Action": ["glue:*"], "Resource": "*"})


def sfn_policy(p: str) -> dict:
    return doc(
        {"Effect": "Allow", "Action": ["lambda:InvokeFunction"],
         "Resource": f"arn:aws:lambda:*:*:function:{p}-*"},
        {"Effect": "Allow", "Action": ["sns:Publish"], "Resource": f"arn:aws:sns:*:*:{p}-*"},
        LOGS,
    )


def scheduler_policy(p: str) -> dict:
    return doc({"Effect": "Allow", "Action": ["states:StartExecution"],
                "Resource": f"arn:aws:states:*:*:stateMachine:{p}-*"})


def firehose_policy(p: str) -> dict:
    return doc(s3_rw(p), LOGS)
