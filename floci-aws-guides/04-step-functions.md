# 04 — Step Functions

> [Index](README.md) · [← 03 — Glue (Catalog, Jobs, Crawlers)](03-glue.md) · [05 — EventBridge Scheduler →](05-eventbridge-scheduler.md)

State machines written in Amazon States Language (ASL). This pipeline drives a Glue job with a Wait → Check → Choice polling loop.

---

## 1. Documentation

### Overview

- A **state machine** is a JSON (ASL) definition of states (`Task`, `Wait`, `Choice`, `Pass`, `Fail`, `Succeed`).
- An **execution** is one run of a state machine with an input document.
- In this project the machine starts a Glue job through the `glue-runner` Lambda, polls it, then publishes success or failure to SNS.

### At a glance

| Item | Value |
|------|-------|
| boto3 client | `stepfunctions` |
| Terraform endpoint | `sfn` |
| Needs first | an IAM role ([07](07-iam.md)); the `glue-runner` Lambda ([02](02-lambda.md)) and SNS topic ([06](06-sns.md)) for the full definition |
| Docker socket | not required (only for the Lambda it calls) |

### What Floci supports

Floci interprets ASL itself. Service integrations it documents:

- **Lambda**: direct ARN or `arn:aws:states:::lambda:invoke`
- **SNS** publish
- **Nested state machines**: `states:startExecution[.sync[:2]]`
- **EventBridge**: `events:putEvents`
- **ECS**: `ecs:runTask.sync`
- **`aws-sdk:` integrations**: sfn, scheduler, rdsdata, redshiftdata, sns

**There is no Glue integration**, so `glue:startJobRun.sync` will not work on Floci. This pipeline drives Glue through the `glue-runner` Lambda with a **Wait → Check → Choice** loop. The same definition also works on real AWS.

Other notes:

- `Wait` and `Retry` backoff are capped at 30 s by default (`FLOCI_SERVICES_STEPFUNCTIONS_MAX_WAIT_SECONDS`).
- `GetExecutionHistory` records `TaskScheduled`/`TaskSucceeded` and `LambdaFunction*` events.

### Key concepts

- **State types** used here: `Task` (call Lambda or SNS), `Wait`, `Choice` (branch on a value), `Fail`.
- **`Retry` / `Catch`**: retry transient errors, then route failures to a handler state.
- **Execution role**: the IAM role Step Functions assumes to call Lambda and SNS.

### Gotchas

- Use `ResultPath` so state results do not overwrite the original input.
- The execution role needs `lambda:InvokeFunction` and `sns:Publish` (see [07](07-iam.md)).
- When a run fails, read `executions.history(...)` first: it names the failing state.
- `templatefile` and ASL both use `${...}`; Terraform substitutes the named variables. A literal `${` in ASL must be written `$${`.

---

## 2. Basic Usage

> Prerequisite: Floci is running and the `AWS_*` environment variables are set ([guide 00](00-setup-floci.md)).

```python
import json, time
import boto3

iam, sfn = boto3.client("iam"), boto3.client("stepfunctions")

# 1. Execution role
trust = {"Version": "2012-10-17", "Statement": [
    {"Effect": "Allow", "Principal": {"Service": "states.amazonaws.com"}, "Action": "sts:AssumeRole"}]}
role_arn = iam.create_role(RoleName="demo-sfn-role",
                           AssumeRolePolicyDocument=json.dumps(trust))["Role"]["Arn"]

# 2. A one-state machine
definition = {"StartAt": "Hello",
              "States": {"Hello": {"Type": "Pass", "Result": {"msg": "hi"},
                                   "ResultPath": "$.greeting", "End": True}}}
sm_arn = sfn.create_state_machine(name="demo-sm", definition=json.dumps(definition),
                                  roleArn=role_arn)["stateMachineArn"]

# 3. Run it and wait for the result
ex_arn = sfn.start_execution(stateMachineArn=sm_arn, input=json.dumps({"source": "manual"}))["executionArn"]
for _ in range(30):
    d = sfn.describe_execution(executionArn=ex_arn)
    if d["status"] != "RUNNING":
        break
    time.sleep(1)
print(d["status"], d.get("output"))   # SUCCEEDED {"source": "manual", "greeting": {"msg": "hi"}}
```

---

## 3. Examples

### Example 1 — ETL state machine (Glue via Lambda polling loop)

`sfn_definitions/etl_pipeline.asl.json`

```json
{
  "Comment": "Glue job orchestrated via Lambda (Floci has no glue:startJobRun.sync integration)",
  "StartAt": "StartGlueJob",
  "States": {
    "StartGlueJob": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "FunctionName": "${GLUE_RUNNER_ARN}",
        "Payload": { "action": "start_job", "job": "${GLUE_JOB_NAME}" }
      },
      "ResultSelector": { "run_id.$": "$.Payload.run_id" },
      "ResultPath": "$.glue",
      "Retry": [
        { "ErrorEquals": ["Lambda.ServiceException", "Lambda.TooManyRequestsException"],
          "IntervalSeconds": 2, "MaxAttempts": 3, "BackoffRate": 2.0 }
      ],
      "Catch": [ { "ErrorEquals": ["States.ALL"], "ResultPath": "$.error", "Next": "NotifyFailure" } ],
      "Next": "WaitForJob"
    },
    "WaitForJob": { "Type": "Wait", "Seconds": 5, "Next": "CheckJob" },
    "CheckJob": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "FunctionName": "${GLUE_RUNNER_ARN}",
        "Payload": { "action": "job_status", "job": "${GLUE_JOB_NAME}", "run_id.$": "$.glue.run_id" }
      },
      "ResultSelector": { "run_id.$": "$.Payload.run_id", "state.$": "$.Payload.state" },
      "ResultPath": "$.glue",
      "Catch": [ { "ErrorEquals": ["States.ALL"], "ResultPath": "$.error", "Next": "NotifyFailure" } ],
      "Next": "JobDone"
    },
    "JobDone": {
      "Type": "Choice",
      "Choices": [
        { "Variable": "$.glue.state", "StringEquals": "SUCCEEDED", "Next": "NotifySuccess" },
        { "Or": [
            { "Variable": "$.glue.state", "StringEquals": "FAILED" },
            { "Variable": "$.glue.state", "StringEquals": "ERROR" },
            { "Variable": "$.glue.state", "StringEquals": "TIMEOUT" },
            { "Variable": "$.glue.state", "StringEquals": "STOPPED" }
          ], "Next": "NotifyFailure" }
      ],
      "Default": "WaitForJob"
    },
    "NotifySuccess": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "${SNS_TOPIC_ARN}",
        "Subject": "ETL succeeded",
        "Message.$": "States.JsonToString($.glue)"
      },
      "End": true
    },
    "NotifyFailure": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "${SNS_TOPIC_ARN}",
        "Subject": "ETL FAILED",
        "Message.$": "States.JsonToString($)"
      },
      "Next": "PipelineFailed"
    },
    "PipelineFailed": { "Type": "Fail", "Error": "PipelineFailed", "Cause": "See SNS notification" }
  }
}
```

### Example 2 — Render, validate and create/update the machine

`check_definition` catches the usual typos (missing `StartAt`, a `Next`/`Default`/`Catch` pointing at a state that does not exist) before you create the machine.

`src/sfn_ops/machines.py`

```python
import json
from pathlib import Path
from string import Template

from botocore.exceptions import ClientError

from src.common.aws import client

sfn = client("stepfunctions")


def render_definition(path: str, **subs: str) -> str:
    """Replace ${VAR} placeholders in the ASL file. Raises if one is left unresolved."""
    text = Template(Path(path).read_text()).safe_substitute(**subs)
    if "${" in text:
        raise ValueError("unresolved placeholder in ASL: " + text[text.index("${"):][:40])
    json.loads(text)  # must be valid JSON
    return text


def check_definition(definition: str) -> None:
    """Static checks that catch the usual typos: StartAt exists, every Next/Default target exists."""
    d = json.loads(definition)
    states = d["States"]
    assert d["StartAt"] in states, f"StartAt {d['StartAt']} missing"

    def targets(s: dict):
        for key in ("Next", "Default"):
            if key in s:
                yield s[key]
        for c in s.get("Choices", []):
            if "Next" in c:
                yield c["Next"]
        for c in s.get("Catch", []):
            yield c["Next"]

    for sname, s in states.items():
        for t in targets(s):
            assert t in states, f"{sname} -> unknown state {t}"
        if s["Type"] not in ("Choice", "Fail", "Succeed"):
            assert "Next" in s or s.get("End"), f"{sname} has neither Next nor End"


def upsert(sm_name: str, definition: str, role_arn: str, sm_type: str = "STANDARD") -> str:
    try:
        return sfn.create_state_machine(name=sm_name, definition=definition, roleArn=role_arn,
                                        type=sm_type)["stateMachineArn"]
    except ClientError as e:
        if e.response["Error"]["Code"] != "StateMachineAlreadyExists":
            raise
        arn = next(m["stateMachineArn"] for m in sfn.list_state_machines()["stateMachines"]
                   if m["name"] == sm_name)
        sfn.update_state_machine(stateMachineArn=arn, definition=definition, roleArn=role_arn)
        return arn


def delete(arn: str) -> None:
    sfn.delete_state_machine(stateMachineArn=arn)
```

### Example 3 — Start, wait, history and stop

`src/sfn_ops/executions.py`

```python
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
```

### Example 4 — Create and run the ETL machine

```python
from src.common.aws import arn, name
from src.iam_ops.roles import create_all
from src.sfn_ops import executions, machines

definition = machines.render_definition(
    "sfn_definitions/etl_pipeline.asl.json",
    GLUE_RUNNER_ARN=arn("lambda", f"function:{name('glue-runner')}"),
    GLUE_JOB_NAME=name("clean-orders"),
    SNS_TOPIC_ARN=arn("sns", name("alerts")),
)
machines.check_definition(definition)
sm = machines.upsert(name("etl"), definition, create_all()["sfn-role"])
ex = executions.start(sm, {"source": "manual"})
print(executions.wait(ex))        # {'source': 'manual', 'glue': {'run_id': ..., 'state': 'SUCCEEDED'}}
print(executions.history(ex))
```

### Example 5 — Test

The full execution is covered by `test_full_flow_on_floci` in [guide 15](15-end-to-end-pipeline.md).

`tests/test_04_compute_and_catalog.py`

```python
import os
import pytest
from src.common.aws import arn, client, name
from src.iam_ops import roles
from src.sfn_ops import executions, machines
from src.sns_ops import topics

MOTO = os.getenv("EMULATOR") == "moto"  # moto cannot run Lambda containers or real SQL


def test_step_functions_definition_and_lifecycle():
    r = roles.create_all()
    topic = topics.create_topic(name("alerts"))
    definition = machines.render_definition(
        "sfn_definitions/etl_pipeline.asl.json",
        GLUE_RUNNER_ARN=arn("lambda", f"function:{name('glue-runner')}"),
        GLUE_JOB_NAME=name("clean-orders"), SNS_TOPIC_ARN=topic)
    machines.check_definition(definition)
    sm = machines.upsert(name("etl"), definition, r["sfn-role"])
    assert machines.upsert(name("etl"), definition, r["sfn-role"]) == sm  # update path
    ex = executions.start(sm, {"source": "test"})
    assert ex
    # moto does not interpret ASL, so only assert the full run on Floci
    if not MOTO:
        pytest.skip("full execution is covered in test_05_e2e (needs deployed Lambdas)")
```

### Example 6 — Terraform

`terraform/stepfunctions.tf`

```hcl
resource "aws_sfn_state_machine" "etl" {
  name     = "${var.prefix}-etl"
  role_arn = aws_iam_role.this["sfn"].arn
  type     = "STANDARD"

  definition = templatefile("${path.module}/../sfn_definitions/etl_pipeline.asl.json", {
    GLUE_RUNNER_ARN = aws_lambda_function.glue_runner.arn
    GLUE_JOB_NAME   = aws_glue_job.clean_orders.name
    SNS_TOPIC_ARN   = aws_sns_topic.alerts.arn
  })

  tags = local.tags
}
```

---

## Reference

Project files:

```
sfn_definitions/etl_pipeline.asl.json
src/sfn_ops/{machines,executions}.py
terraform/stepfunctions.tf
```

Related: [02 Lambda](02-lambda.md) (`glue-runner`), [03 Glue](03-glue.md), [06 SNS](06-sns.md), [05 Scheduler](05-eventbridge-scheduler.md) (starts the machine on a schedule).
