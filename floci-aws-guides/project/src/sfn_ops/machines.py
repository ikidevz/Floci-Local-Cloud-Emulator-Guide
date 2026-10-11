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
