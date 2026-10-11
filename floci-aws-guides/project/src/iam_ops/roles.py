import json

from botocore.exceptions import ClientError

from src.common.aws import PREFIX, client, name
from src.iam_ops import policies as pol

iam = client("iam")

# role suffix -> (trusted service principal, policy builder)
ROLES = {
    "lambda-role": ("lambda.amazonaws.com", pol.lambda_policy),
    "glue-role": ("glue.amazonaws.com", pol.glue_policy),
    "sfn-role": ("states.amazonaws.com", pol.sfn_policy),
    "scheduler-role": ("scheduler.amazonaws.com", pol.scheduler_policy),
    "firehose-role": ("firehose.amazonaws.com", pol.firehose_policy),
}


def ensure_role(role_name: str, service: str, policy_doc: dict) -> str:
    """Create the role (or reuse it) and set its inline policy. Returns the role ARN."""
    try:
        arn = iam.create_role(RoleName=role_name,
                              AssumeRolePolicyDocument=json.dumps(pol.trust(service)),
                              Description="Floci demo role")["Role"]["Arn"]
    except ClientError as e:
        if e.response["Error"]["Code"] != "EntityAlreadyExists":
            raise
        arn = iam.get_role(RoleName=role_name)["Role"]["Arn"]
    # put_role_policy overwrites, so this is idempotent
    iam.put_role_policy(RoleName=role_name, PolicyName=f"{role_name}-inline",
                        PolicyDocument=json.dumps(policy_doc))
    return arn


def create_all() -> dict[str, str]:
    """Create every pipeline role. Returns {suffix: role_arn}."""
    return {suffix: ensure_role(name(suffix), svc, builder(PREFIX))
            for suffix, (svc, builder) in ROLES.items()}


def role_arns() -> dict[str, str]:
    return {s: iam.get_role(RoleName=name(s))["Role"]["Arn"] for s in ROLES}


def delete_role(role_name: str) -> None:
    for p in iam.list_role_policies(RoleName=role_name)["PolicyNames"]:
        iam.delete_role_policy(RoleName=role_name, PolicyName=p)
    for p in iam.list_attached_role_policies(RoleName=role_name)["AttachedPolicies"]:
        iam.detach_role_policy(RoleName=role_name, PolicyArn=p["PolicyArn"])
    iam.delete_role(RoleName=role_name)


def delete_all() -> None:
    for suffix in ROLES:
        try:
            delete_role(name(suffix))
        except ClientError as e:
            if e.response["Error"]["Code"] != "NoSuchEntity":
                raise


if __name__ == "__main__":
    for k, v in create_all().items():
        print(f"{k:16} {v}")
