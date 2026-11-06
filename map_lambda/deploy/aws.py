"""
The AWS side of a deploy: the code bucket, and creating, updating and deleting the
stack through change sets.
"""

import json
import sys
import time
import uuid
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import click

from .template import OUTPUT_BUCKETS

POLL_SECONDS = 2.0

# CloudFormation copies a stack's tags onto its resources, so a project's functions,
# roles and logs can be found and costed together.
PROJECT_TAG = "map-lambda:project"

_NO_CHANGES = ("didn't contain changes", "No updates are to be performed")

_SUCCEEDED = ("CREATE_COMPLETE", "UPDATE_COMPLETE", "DELETE_COMPLETE")

_ACTION_COLOURS = {"Add": "green", "Modify": "yellow", "Remove": "red"}


class DeployError(RuntimeError):
    """A deploy that failed or was refused, saying why."""


def say(line: str) -> None:
    click.echo(line, err=True)


def confirm(question: str, *, yes: bool) -> bool:
    return yes or click.confirm(question, default=False, err=True)


@contextmanager
def step(doing: str) -> Iterator[None]:
    """Says `doing ...`, then `done` and how long it took, or `failed`."""
    started = time.monotonic()
    click.echo(f"{doing} ...", nl=False, err=True)
    try:
        yield
    except BaseException:
        say(click.style(" failed", fg="red"))
        raise
    say(f" done in {time.monotonic() - started:.0f}s")


@dataclass(frozen=True)
class Stack:
    status: str
    outputs: dict[str, str]
    updated: datetime | None = None

    @property
    def buckets(self) -> list[str]:
        return json.loads(self.outputs.get(OUTPUT_BUCKETS, "[]"))


def code_bucket(account: str, region: str) -> str:
    """Per account and region, as Lambda only takes code from a bucket in its own region."""
    return f"map-lambda-deploy-{account}-{region}"


def bucket_exists(s3: Any, bucket: str) -> bool:
    try:
        s3.head_bucket(Bucket=bucket)
        return True
    except s3.exceptions.ClientError as e:
        if e.response["Error"]["Code"] not in ("404", "NoSuchBucket"):
            raise
        return False


def object_exists(s3: Any, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except s3.exceptions.ClientError as e:
        if e.response["Error"]["Code"] not in ("404", "NoSuchKey"):
            raise
        return False


def create_bucket(s3: Any, bucket: str, region: str) -> None:
    # us-east-1 is the one region that refuses being named as a location.
    config = (
        {}
        if region == "us-east-1"
        else {"CreateBucketConfiguration": {"LocationConstraint": region}}
    )
    s3.create_bucket(Bucket=bucket, **config)


def keys_under(s3: Any, bucket: str, prefix: str) -> list[str]:
    keys: list[str] = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        keys.extend(o["Key"] for o in page.get("Contents", []))
    return keys


def delete_keys(s3: Any, bucket: str, keys: list[str]) -> None:
    for start in range(0, len(keys), 1000):
        batch = [{"Key": key} for key in keys[start : start + 1000]]
        s3.delete_objects(Bucket=bucket, Delete={"Objects": batch, "Quiet": True})


def delete_bucket_if_empty(s3: Any, bucket: str) -> list[str]:
    """
    Delete the bucket unless something else is in it, such as another project's zips,
    returning what's left at its top level. S3 refuses to delete a bucket with anything
    in it, so a deploy running at the same time can't lose its zips.
    """
    try:
        s3.delete_bucket(Bucket=bucket)
        return []
    except s3.exceptions.ClientError as e:
        if e.response["Error"]["Code"] != "BucketNotEmpty":
            raise
    listed = s3.list_objects_v2(Bucket=bucket, Delimiter="/")
    left = [p["Prefix"] for p in listed.get("CommonPrefixes", [])]
    return sorted(left + [o["Key"] for o in listed.get("Contents", [])])


def upload(s3: Any, bucket: str, key: str, path: Path) -> None:
    # In parts above 8 MB, so no one request has to finish before its signature expires.
    s3.upload_file(str(path), bucket, key)


def describe(cfn: Any, stack_name: str) -> Stack | None:
    try:
        (stack,) = cfn.describe_stacks(StackName=stack_name)["Stacks"]
    except cfn.exceptions.ClientError as e:
        if "does not exist" in e.response["Error"]["Message"]:
            return None
        raise
    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
    updated = stack.get("LastUpdatedTime") or stack.get("CreationTime")
    return Stack(stack["StackStatus"], outputs, updated)


def resources(cfn: Any, stack_name: str) -> list[tuple[str, str, str]]:
    """Each resource's type, name and status, sorted by type then name."""
    found = []
    kwargs: dict[str, str] = {}
    while True:
        page = cfn.list_stack_resources(StackName=stack_name, **kwargs)
        for r in page["StackResourceSummaries"]:
            name = r.get("PhysicalResourceId") or r["LogicalResourceId"]
            found.append((r["ResourceType"], name, r["ResourceStatus"]))
        if "NextToken" not in page:
            return sorted(found)
        kwargs = {"NextToken": page["NextToken"]}


def apply(
    cfn: Any,
    stack_name: str,
    template: dict[str, Any],
    parameters: list[dict[str, Any]],
    *,
    yes: bool = False,
    before: Sequence[str] = (),
    prepare: Callable[[], None] = lambda: None,
    dry_run: bool = False,
    sleep: Callable[[float], None] = time.sleep,
) -> bool:
    """
    Create or update the stack. Shows `before`, the steps `prepare` will take, then the
    stack's changes, and asks once before doing any of it (not with `yes`), or with
    `dry_run` stops there.
    False when there was nothing to change, or nothing was changed.
    """
    current = describe(cfn, stack_name)
    if current is not None and current.status == "ROLLBACK_COMPLETE":
        # A failed first create leaves a stack with nothing in it, which can only be
        # deleted.
        say(f"Deleting {stack_name}, left over from a failed create")
        delete(cfn, stack_name, sleep=sleep)
        current = None
    if (
        current is not None
        and current.status.endswith("_IN_PROGRESS")
        and (current.status != "REVIEW_IN_PROGRESS")
    ):
        raise DeployError(f"{stack_name} is busy ({current.status}): try again when it's done")
    creating = current is None or current.status == "REVIEW_IN_PROGRESS"

    change_set = f"deploy-{int(time.time())}"
    with step(f"Planning the changes to {stack_name}"):
        cfn.create_change_set(
            StackName=stack_name,
            ChangeSetName=change_set,
            ChangeSetType="CREATE" if creating else "UPDATE",
            TemplateBody=json.dumps(template),
            Parameters=parameters,
            Capabilities=["CAPABILITY_IAM"],
            Tags=[{"Key": PROJECT_TAG, "Value": stack_name}],
        )
        while True:
            described = cfn.describe_change_set(StackName=stack_name, ChangeSetName=change_set)
            if described["Status"] not in ("CREATE_PENDING", "CREATE_IN_PROGRESS"):
                break
            sleep(POLL_SECONDS)
    if described["Status"] == "FAILED":
        cfn.delete_change_set(StackName=stack_name, ChangeSetName=change_set)
        reason = described.get("StatusReason", "")
        if any(phrase in reason for phrase in _NO_CHANGES):
            say(f"{stack_name} is up to date")
            return False
        raise DeployError(f"CloudFormation refused the change: {reason}")

    changes = _changes(cfn, stack_name, change_set, described)
    for line in before:
        say(f"  {line}")
    say(f"  {'Create' if creating else 'Update'} stack {stack_name}:")
    for change in changes:
        say(f"    {change}")
    if dry_run:
        say("Dry run: nothing was changed")
    if dry_run or not confirm("Continue?", yes=yes):
        cfn.delete_change_set(StackName=stack_name, ChangeSetName=change_set)
        if creating:
            # Creating the change set made an empty stack, which would otherwise linger.
            cfn.delete_stack(StackName=stack_name)
        return False

    prepare()
    token = str(uuid.uuid4())
    cfn.execute_change_set(StackName=stack_name, ChangeSetName=change_set, ClientRequestToken=token)
    verb = "Creating" if creating else "Updating"
    status = _follow(cfn, stack_name, token, len(changes), f"{verb} stack {stack_name}", sleep)
    if status not in ("CREATE_COMPLETE", "UPDATE_COMPLETE"):
        raise DeployError(f"{stack_name} ended {status}: see the failures above")
    return True


def delete(cfn: Any, stack_name: str, *, sleep: Callable[[float], None] = time.sleep) -> None:
    (stack,) = cfn.describe_stacks(StackName=stack_name)["Stacks"]
    resources = cfn.describe_stack_resources(StackName=stack_name)["StackResources"]
    token = str(uuid.uuid4())
    cfn.delete_stack(StackName=stack_name, ClientRequestToken=token)
    # By id, since a deleted stack can't be described by name.
    status = _follow(
        cfn, stack["StackId"], token, len(resources), f"Deleting stack {stack_name}", sleep
    )
    if status != "DELETE_COMPLETE":
        raise DeployError(f"{stack_name} ended {status}: see the failures above")


def _changes(cfn: Any, stack_name: str, change_set: str, described: dict[str, Any]) -> list[str]:
    lines = []
    while True:
        for change in described.get("Changes", []):
            resource = change["ResourceChange"]
            line = f"{resource['Action']:<8} {resource['LogicalResourceId']}"
            line += f" ({resource['ResourceType']})"
            colour = _ACTION_COLOURS.get(resource["Action"])
            if resource.get("Replacement") == "True":
                line += ", replacing it"
                colour = "red"
            lines.append(click.style(line, fg=colour) if colour else line)
        if "NextToken" not in described:
            return lines
        described = cfn.describe_change_set(
            StackName=stack_name, ChangeSetName=change_set, NextToken=described["NextToken"]
        )


def _follow(
    cfn: Any,
    stack_id: str,
    token: str,
    total: int,
    doing: str,
    sleep: Callable[[float], None],
) -> str:
    """
    Show a progress bar of the operation's resources until the stack settles, and any
    failures with their reasons, returning the stack's status.
    """
    seen: set[str] = set()
    finished: set[str] = set()
    started = time.monotonic()
    with click.progressbar(
        length=total, label=doing, show_pos=True, show_eta=False, file=sys.stderr
    ) as bar:
        while True:
            (stack,) = cfn.describe_stacks(StackName=stack_id)["Stacks"]
            events = cfn.describe_stack_events(StackName=stack_id)["StackEvents"]
            for event in reversed(events):
                if event.get("ClientRequestToken") != token or event["EventId"] in seen:
                    continue
                seen.add(event["EventId"])
                status = event["ResourceStatus"]
                if event["ResourceType"] == "AWS::CloudFormation::Stack":
                    continue
                if status.endswith(("_COMPLETE", "_SKIPPED")):
                    finished.add(event["LogicalResourceId"])
                elif status.endswith("_FAILED") and event.get("ResourceStatusReason"):
                    reason = f"{event['LogicalResourceId']}: {event['ResourceStatusReason']}"
                    say(f"  {click.style(reason, fg='red')}")
            bar.update(min(len(finished), total) - bar.pos)
            if not stack["StackStatus"].endswith("_IN_PROGRESS"):
                break
            sleep(POLL_SECONDS)
    status = stack["StackStatus"]
    colour = "green" if status in _SUCCEEDED else "red"
    say(click.style(f"{doing}: {status} in {time.monotonic() - started:.0f}s", fg=colour))
    return status
