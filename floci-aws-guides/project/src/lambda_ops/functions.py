import json

from botocore.exceptions import ClientError

from src.common.aws import client
from src.common.waiters import wait_until
from src.lambda_ops.packaging import zip_dir

lam = client("lambda")


def wait_active(fn_name: str, timeout: int = 180) -> None:
    """Wait for State=Active and LastUpdateStatus=Successful (cold start pulls a runtime image)."""
    def status() -> str:
        cfg = lam.get_function(FunctionName=fn_name)["Configuration"]
        state = cfg.get("State", "Active")
        if state != "Active":
            return state
        update = cfg.get("LastUpdateStatus", "Successful")
        return "Active" if update == "Successful" else update

    wait_until(status, done={"Active"}, failed={"Failed"}, timeout=timeout, interval=2)


def deploy(fn_name: str, src_dir: str, role_arn: str, handler: str = "handler.lambda_handler",
           runtime: str = "python3.12", env: dict | None = None, timeout: int = 30,
           memory: int = 256, dlq_arn: str | None = None) -> str:
    """Create or update a function from a source directory. Returns the function ARN."""
    code = zip_dir(src_dir)
    cfg = dict(Runtime=runtime, Role=role_arn, Handler=handler, Timeout=timeout, MemorySize=memory,
               Environment={"Variables": env or {}})
    if dlq_arn:
        cfg["DeadLetterConfig"] = {"TargetArn": dlq_arn}
    try:
        arn = lam.create_function(FunctionName=fn_name, Code={"ZipFile": code}, **cfg)["FunctionArn"]
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceConflictException":
            raise
        lam.update_function_code(FunctionName=fn_name, ZipFile=code)
        wait_active(fn_name)
        arn = lam.update_function_configuration(FunctionName=fn_name, **cfg)["FunctionArn"]
    wait_active(fn_name)
    return arn


def invoke(fn_name: str, payload: dict, async_: bool = False) -> dict:
    resp = lam.invoke(FunctionName=fn_name, InvocationType="Event" if async_ else "RequestResponse",
                      Payload=json.dumps(payload).encode())
    if async_:
        return {"status": resp["StatusCode"]}
    body = json.loads(resp["Payload"].read() or b"null")
    if "FunctionError" in resp:
        raise RuntimeError(f"Lambda error: {body}")
    return body


def allow_service(fn_name: str, principal: str, source_arn: str, sid: str) -> None:
    """Resource policy so S3 / SNS / Events can invoke the function."""
    try:
        lam.add_permission(FunctionName=fn_name, StatementId=sid, Action="lambda:InvokeFunction",
                           Principal=principal, SourceArn=source_arn)
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceConflictException":
            raise


def delete(fn_name: str) -> None:
    try:
        lam.delete_function(FunctionName=fn_name)
    except lam.exceptions.ResourceNotFoundException:
        pass
