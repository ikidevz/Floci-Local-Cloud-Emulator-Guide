import json
import uuid

from src.common.aws import client
from src.common.waiters import wait_until

sfn = client("stepfunctions")


def start(sm_arn: str, payload: dict, exec_name: str | None = None) -> str:
    return sfn.start_execution(stateMachineArn=sm_arn, name=exec_name or f"run-{uuid.uuid4().hex[:8]}",
                               input=json.dumps(payload))["executionArn"]


def wait(exec_arn: str, timeout: int = 600) -> dict:
    """Wait for SUCCEEDED and return the output. Raises with the error/cause on failure."""
    try:
        wait_until(lambda: sfn.describe_execution(executionArn=exec_arn)["status"],
                   done={"SUCCEEDED"}, failed={"FAILED", "TIMED_OUT", "ABORTED"},
                   timeout=timeout, interval=2)
    except RuntimeError as exc:
        d = sfn.describe_execution(executionArn=exec_arn)
        raise RuntimeError(f"{exc}: {d.get('error')} / {d.get('cause')}") from exc
    return json.loads(sfn.describe_execution(executionArn=exec_arn).get("output") or "{}")


def history(exec_arn: str) -> list[tuple[str, str]]:
    """(event type, state name) pairs. Read this first when an execution fails."""
    events = sfn.get_execution_history(executionArn=exec_arn)["events"]
    return [(e["type"], e.get("stateEnteredEventDetails", {}).get("name", "")) for e in events]


def stop(exec_arn: str, cause: str = "manual stop") -> None:
    sfn.stop_execution(executionArn=exec_arn, cause=cause)
