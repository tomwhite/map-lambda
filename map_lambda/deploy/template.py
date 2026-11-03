"""
The CloudFormation template for a spec: the dispatchers, the workers at each memory size,
their roles and their log groups, named as the client expects.
"""

import json
import re
from typing import Any

from .spec import Function, Spec, SpecError

BUCKET_PARAMETER = "CodeBucket"

OUTPUT_BUCKETS = "AllowedBuckets"

_ASSUME_ROLE = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }
    ],
}

_BASIC_EXECUTION = {
    "Fn::Sub": "arn:${AWS::Partition}:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}


def logical_id(*parts: str) -> str:
    """A CloudFormation resource name, which allows only letters and digits."""
    return "".join(
        word[:1].upper() + word[1:] for part in parts for word in re.split(r"[^A-Za-z0-9]+", part)
    )


def code_parameter(binary: str) -> str:
    return logical_id(binary, "CodeKey")


def template(spec: Spec, buckets: list[str]) -> dict[str, Any]:
    """
    The whole stack. Roles are left unnamed, so the same project can be deployed in
    several regions without their global names clashing.
    """
    resources: dict[str, Any] = {}
    worker_arns = []

    for function in spec.functions:
        role = logical_id(function.name, "Role")
        if role in resources:
            raise SpecError(f"{function.name!r} clashes with another function's resource names")
        resources[role] = _role(_bucket_policies(function, buckets))
        for i, (name, memory) in enumerate(function.deployed(spec.project)):
            base = logical_id(function.name, "" if i == 0 else str(memory))
            resource = _add_function(resources, base, name, function, memory, role)
            worker_arns.append({"Fn::GetAtt": [resource, "Arn"]})

    invoke = {
        "PolicyName": "invoke-workers",
        "PolicyDocument": {
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Action": "lambda:InvokeFunction", "Resource": worker_arns}
            ],
        },
    }
    resources["DispatcherRole"] = _role([invoke])
    for dispatcher in spec.dispatchers:
        ((name, memory),) = dispatcher.deployed(spec.project)
        _add_function(
            resources, logical_id(dispatcher.name), name, dispatcher, memory, "DispatcherRole"
        )

    return {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": f"map-lambda functions for {spec.project}",
        "Parameters": {
            BUCKET_PARAMETER: {"Type": "String"},
            **{code_parameter(binary): {"Type": "String"} for binary in spec.binaries},
        },
        "Resources": resources,
        "Outputs": {
            # JSON, as outputs are strings, and CloudFormation refuses an empty one.
            OUTPUT_BUCKETS: {"Value": json.dumps(sorted(buckets))},
        },
    }


def _add_function(
    resources: dict[str, Any],
    base: str,
    name: str,
    function: Function,
    memory: int,
    role: str,
) -> str:
    """Adds the function and its log group, returning the function's resource name."""
    resource, log_group = f"{base}Function", f"{base}LogGroup"
    if resource in resources or log_group in resources:
        raise SpecError(f"{name!r} clashes with another function's resource names")
    # Created by the stack rather than by the first invocation, so deleting the stack deletes it.
    resources[log_group] = {
        "Type": "AWS::Logs::LogGroup",
        "Properties": {"LogGroupName": f"/aws/lambda/{name}"},
    }
    resources[resource] = {
        "Type": "AWS::Lambda::Function",
        "DependsOn": log_group,
        "Properties": {
            "FunctionName": name,
            "Runtime": "provided.al2023",
            "Architectures": ["arm64"],
            "Handler": "bootstrap",
            "MemorySize": memory,
            "Timeout": function.timeout,
            "Role": {"Fn::GetAtt": [role, "Arn"]},
            "Code": {
                "S3Bucket": {"Ref": BUCKET_PARAMETER},
                "S3Key": {"Ref": code_parameter(function.binary)},
            },
        },
    }
    return resource


def _role(policies: list[dict[str, Any]]) -> dict[str, Any]:
    properties: dict[str, Any] = {
        "AssumeRolePolicyDocument": _ASSUME_ROLE,
        "ManagedPolicyArns": [_BASIC_EXECUTION],
    }
    if policies:
        properties["Policies"] = policies
    return {"Type": "AWS::IAM::Role", "Properties": properties}


def _bucket_policies(function: Function, buckets: list[str]) -> list[dict[str, Any]]:
    statements = [
        {
            "Effect": "Allow",
            "Action": list(access.action),
            "Resource": [access.resource.replace("{bucket}", bucket) for bucket in sorted(buckets)],
        }
        for access in function.bucket_access
        if buckets
    ]
    if not statements:
        return []
    return [
        {
            "PolicyName": "bucket-access",
            "PolicyDocument": {"Version": "2012-10-17", "Statement": statements},
        }
    ]
