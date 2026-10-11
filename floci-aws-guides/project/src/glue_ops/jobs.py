from botocore.exceptions import ClientError

from src.common.aws import client
from src.common.waiters import wait_until

glue = client("glue")
s3 = client("s3")

TERMINAL_FAIL = {"FAILED", "ERROR", "TIMEOUT", "STOPPED"}


def upload_script(bucket: str, local_path: str, key: str) -> str:
    s3.upload_file(local_path, bucket, key)
    return f"s3://{bucket}/{key}"


def ensure_job(job: str, role_arn: str, script_s3: str, shell: bool = True,
               default_args: dict | None = None) -> None:
    kwargs = dict(
        Name=job, Role=role_arn,
        Command={"Name": "pythonshell" if shell else "glueetl", "ScriptLocation": script_s3,
                 "PythonVersion": "3.9" if shell else "3"},
        DefaultArguments={"--job-language": "python", **(default_args or {})},
        MaxRetries=0, Timeout=30,
    )
    if shell:
        kwargs["MaxCapacity"] = 0.0625
    else:
        kwargs.update(GlueVersion="4.0", WorkerType="G.1X", NumberOfWorkers=2)
    try:
        glue.create_job(**kwargs)
    except ClientError as e:
        if e.response["Error"]["Code"] != "AlreadyExistsException":
            raise
        glue.update_job(JobName=job, JobUpdate={k: v for k, v in kwargs.items() if k != "Name"})


def start(job: str, arguments: dict | None = None) -> str:
    return glue.start_job_run(JobName=job, Arguments=arguments or {})["JobRunId"]


def run_and_wait(job: str, arguments: dict | None = None, timeout: int = 900) -> str:
    """Start a run and poll until SUCCEEDED. Raises on FAILED/ERROR/TIMEOUT/STOPPED."""
    run_id = start(job, arguments)
    return wait_until(lambda: glue.get_job_run(JobName=job, RunId=run_id)["JobRun"]["JobRunState"],
                      done={"SUCCEEDED"}, failed=TERMINAL_FAIL, timeout=timeout, interval=1)
