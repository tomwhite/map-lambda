"""
Deploying: reading a spec, the template it makes, naming zips by their contents, the
change-set flow, deleting and listing, against stubbed AWS clients.
"""

import json
import zipfile
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import boto3
import click
import pytest
from botocore.exceptions import ClientError  # type: ignore[import-untyped]
from botocore.stub import ANY, Stubber  # type: ignore[import-untyped]
from click.testing import CliRunner

import map_lambda
import map_lambda.deploy
from map_lambda.deploy import aws, build, cli, spec, template
from map_lambda.deploy.aws import DeployError
from map_lambda.deploy.spec import SpecError

NOW = datetime(2026, 11, 5, 12, 0, tzinfo=UTC)

TOKEN = "token"


def _spec(**functions: Any) -> spec.Spec:
    return spec.parse(
        {"project": "proj", "functions": functions or {"work": {"memory": 1024}}}, base=Path()
    )


class Terminal:
    """
    Answers the deploy's question with `answer`, and gives what it said on stderr as lines,
    the question and each step of a progress bar included.
    """

    def __init__(self, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch):
        self.answer = True
        self._capsys = capsys
        monkeypatch.setattr(click, "confirm", self._confirm)
        monkeypatch.setattr(click, "progressbar", self._progressbar)

    def _confirm(self, question: str, **_: Any) -> bool:
        click.echo(question, err=True)
        return self.answer

    @contextmanager
    def _progressbar(self, *, length: int, label: str, **_: Any) -> Iterator[Any]:
        bar = SimpleNamespace(pos=0)

        def update(steps: int) -> None:
            if steps:
                bar.pos += steps
                click.echo(f"{label} {bar.pos}/{length}", err=True)

        bar.update = update
        yield bar

    def lines(self) -> list[str]:
        return self._capsys.readouterr().err.splitlines()


@pytest.fixture
def term(capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch) -> Terminal:
    return Terminal(capsys, monkeypatch)


def test_the_echo_spec_deploys_the_dispatcher_and_echo_from_this_repo():
    echo = spec.load(cli.ECHO_SPEC)

    assert echo.project == "map-lambda"
    assert echo.binaries == {"dispatcher": "map-lambda-dispatcher", "echo": "echo"}
    assert echo.workspace == (Path(__file__).parents[1] / "rust").resolve()


def test_the_project_defaults_to_the_distribution_name():
    raw = {"functions": {"w": {"memory": 1024}}}

    assert spec.parse(raw, base=Path(), distribution="My_Tool.lambda").project == "my-tool-lambda"
    assert spec.parse({**raw, "project": "p"}, base=Path(), distribution="d").project == "p"


def test_a_spec_in_an_installed_package_takes_the_distribution_s_name(tmp_path):
    assert spec.distribution_of(cli.ECHO_SPEC) == "map-lambda"
    assert map_lambda.deploy.project(cli.ECHO_SPEC) == "map-lambda"
    assert spec.distribution_of(tmp_path / "deploy.toml") is None


def test_the_deploy_names_functions_as_a_session_calls_them():
    project = _spec(work={"memory": [1024, 3538]})
    session = map_lambda.Session(project="proj")
    (work,) = project.functions

    assert [name for name, _ in work.deployed("proj")] == [
        session.function_name("work"),
        session.function_name("work", memory=1024),
        session.function_name("work", memory=3538),
    ]
    assert project.dispatcher.deployed("proj") == [
        (session.function_name(map_lambda.DISPATCHER), 3538)
    ]


def test_a_function_defaults_its_package_and_binary_to_its_name():
    (work,) = _spec(work={"memory": 1024}).functions

    assert (work.package, work.binary, work.memory, work.timeout) == ("work", "work", 1024, 900)


def test_the_dispatcher_defaults_to_a_crate_called_dispatcher():
    dispatcher = _spec().dispatcher

    assert (dispatcher.package, dispatcher.binary) == ("dispatcher", "dispatcher")
    assert dispatcher.memory == 3538


def test_a_list_of_sizes_adds_a_suffixed_name_for_each_to_the_plain_default():
    one, several = _spec(one={"memory": 1024}, several={"memory": [1024, 2048]}).functions

    assert one.deployed("proj") == [("proj-one", 1024)]
    assert several.deployed("proj") == [
        ("proj-several", 1024),
        ("proj-several-1024", 1024),
        ("proj-several-2048", 2048),
    ]


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        ({"functions": {"w": {"memory": 1024}}}, "outside an installed package"),
        ({"project": "Proj", "functions": {"w": {"memory": 1024}}}, "lowercase"),
        ({"project": "p"}, "no functions"),
        ({"project": "p", "functions": {"w": {}}}, "memory is required"),
        ({"project": "p", "functions": {"w": {"memory": 64}}}, "128 to 10240"),
        ({"project": "p", "functions": {"w": {"memory": [512, 512]}}}, "twice"),
        ({"project": "p", "functions": {"w": {"memory": 512, "timeout": 901}}}, "1 to 900"),
        ({"project": "p", "functions": {"w": {"memory": 512, "memroy": 1}}}, "unknown memroy"),
        ({"project": "p", "functions": {"dispatcher": {"memory": 512}}}, "can't be called"),
        ({"project": "p", "functions": {"w": {"memory": 512}}, "extra": 1}, "unknown extra"),
        (
            {
                "project": "p",
                "functions": {
                    "w": {"memory": 512, "bucket_access": [{"action": "s3:*", "resource": "*"}]}
                },
            },
            "{bucket}",
        ),
        ({"project": "p" * 60, "functions": {"w": {"memory": 512}}}, "64 characters"),
    ],
)
def test_a_spec_that_cant_be_deployed_says_what_to_change(raw, message):
    with pytest.raises(SpecError, match=message.replace("{", r"\{")):
        spec.parse(raw, base=Path())


def test_the_template_has_a_function_and_log_group_per_deployed_name():
    stack = template.template(_spec(work={"memory": [1024, 2048]}), [])
    resources = stack["Resources"]
    functions = {
        r["Properties"]["FunctionName"]: r["Properties"]["MemorySize"]
        for r in resources.values()
        if r["Type"] == "AWS::Lambda::Function"
    }

    assert functions == {
        "proj-dispatcher": 3538,
        "proj-work": 1024,
        "proj-work-1024": 1024,
        "proj-work-2048": 2048,
    }
    assert (
        resources["Work2048LogGroup"]["Properties"]["LogGroupName"] == "/aws/lambda/proj-work-2048"
    )
    assert resources["Work2048Function"]["DependsOn"] == "Work2048LogGroup"
    assert resources["WorkFunction"]["Properties"]["Code"]["S3Key"] == {"Ref": "WorkCodeKey"}
    assert set(stack["Parameters"]) == {"CodeBucket", "DispatcherCodeKey", "WorkCodeKey"}


def test_the_dispatcher_may_invoke_every_worker_and_nothing_else():
    resources = template.template(_spec(a={"memory": 512}, b={"memory": [512, 1024]}), [])[
        "Resources"
    ]
    (policy,) = resources["DispatcherRole"]["Properties"]["Policies"]
    (statement,) = policy["PolicyDocument"]["Statement"]

    assert statement["Resource"] == [
        {"Fn::GetAtt": [name, "Arn"]}
        for name in ("AFunction", "BFunction", "B512Function", "B1024Function")
    ]


def test_bucket_access_is_granted_on_each_allowed_bucket_and_only_then():
    access = [{"action": ["s3:GetObject"], "resource": "arn:aws:s3:::{bucket}/*"}]
    project = _spec(work={"memory": 512, "bucket_access": access})

    without = template.template(project, [])["Resources"]["WorkRole"]["Properties"]
    with_two = template.template(project, ["b2", "b1"])["Resources"]["WorkRole"]["Properties"]

    assert "Policies" not in without
    (policy,) = with_two["Policies"]
    assert policy["PolicyDocument"]["Statement"] == [
        {
            "Effect": "Allow",
            "Action": ["s3:GetObject"],
            "Resource": ["arn:aws:s3:::b1/*", "arn:aws:s3:::b2/*"],
        }
    ]


def test_the_allowed_buckets_are_kept_in_the_stack_for_the_next_deploy():
    outputs = template.template(_spec(), ["b2", "b1"])["Outputs"]
    stack = aws.Stack("UPDATE_COMPLETE", {k: v["Value"] for k, v in outputs.items()})

    assert stack.buckets == ["b1", "b2"]


def test_names_that_make_the_same_resource_names_are_refused():
    with pytest.raises(SpecError, match="clashes"):
        template.template(_spec(**{"a-b1": {"memory": 512}, "a-b-1": {"memory": 512}}), [])


def _zip(path: Path, contents: bytes, date: tuple[int, int, int, int, int, int]) -> Path:
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(zipfile.ZipInfo("bootstrap", date_time=date), contents)
    return path


def test_a_rebuilt_zip_of_the_same_binary_keeps_its_key(tmp_path):
    first = _zip(tmp_path / "a.zip", b"binary", (2026, 1, 1, 0, 0, 0))
    rebuilt = _zip(tmp_path / "b.zip", b"binary", (2026, 6, 1, 12, 0, 0))
    changed = _zip(tmp_path / "c.zip", b"changed", (2026, 1, 1, 0, 0, 0))

    assert build.code_key("proj", "work", first) == build.code_key("proj", "work", rebuilt)
    assert build.code_key("proj", "work", first) != build.code_key("proj", "work", changed)
    assert build.code_key("proj", "work", first).startswith("proj/work-")


class Captured:
    """Matches any parameter value, keeping it for the test to inspect."""

    value: Any = None

    def __eq__(self, other: object) -> bool:
        self.value = other
        return True


class Stubbed:
    """
    CloudFormation and S3 clients answering from a queue of expected calls. Stubber checks
    each call's parameters, and each queued response, against the services' API models.
    """

    account = "123456789012"
    region = "eu-west-1"

    def __init__(self) -> None:
        session = boto3.Session(
            aws_access_key_id="key", aws_secret_access_key="secret", region_name=self.region
        )
        self.cfn = session.client("cloudformation")
        self.s3 = session.client("s3")
        self.cfn_stub = Stubber(self.cfn)
        self.s3_stub = Stubber(self.s3)
        self.bucket = aws.code_bucket(self.account, self.region)
        self.template = Captured()

    def no_stack(self) -> None:
        self.cfn_stub.add_client_error(
            "describe_stacks",
            "ValidationError",
            "Stack with id proj does not exist",
            400,
            expected_params={"StackName": "proj"},
        )

    def stack(self, status: str, buckets: list[str] | None = None, name: str = "proj") -> None:
        outputs = [] if buckets is None else [_buckets_output(buckets)]
        stack = {
            "StackName": "proj",
            "StackId": "id",
            "CreationTime": NOW,
            "StackStatus": status,
            "Outputs": outputs,
        }
        self.cfn_stub.add_response("describe_stacks", {"Stacks": [stack]}, {"StackName": name})

    def change_set(self, kind: str, parameters: Any = ANY) -> None:
        self.cfn_stub.add_response(
            "create_change_set",
            {"Id": "change-set", "StackId": "id"},
            {
                "StackName": "proj",
                "ChangeSetName": ANY,
                "ChangeSetType": kind,
                "TemplateBody": self.template,
                "Parameters": parameters,
                "Capabilities": ["CAPABILITY_IAM"],
                "Tags": [{"Key": "map-lambda:project", "Value": "proj"}],
            },
        )

    def described_change_set(self, status: str = "CREATE_COMPLETE", reason: str = "") -> None:
        change = {
            "Type": "Resource",
            "ResourceChange": {
                "Action": "Add",
                "LogicalResourceId": "WorkFunction",
                "ResourceType": "AWS::Lambda::Function",
            },
        }
        response: dict[str, Any] = {"Status": status}
        if reason:
            response["StatusReason"] = reason
        else:
            response["Changes"] = [change]
        self.cfn_stub.add_response(
            "describe_change_set", response, {"StackName": "proj", "ChangeSetName": ANY}
        )

    def delete_change_set(self) -> None:
        self.cfn_stub.add_response(
            "delete_change_set", {}, {"StackName": "proj", "ChangeSetName": ANY}
        )

    def delete_empty_stack(self) -> None:
        self.cfn_stub.add_response("delete_stack", {}, {"StackName": "proj"})

    def execute(self) -> None:
        self.cfn_stub.add_response(
            "execute_change_set",
            {},
            {"StackName": "proj", "ChangeSetName": ANY, "ClientRequestToken": TOKEN},
        )

    def events(self, status: str, name: str = "proj") -> None:
        def event(event_id: str, token: str, logical_id: str, status: str, reason: str) -> Any:
            return {
                "StackId": "id",
                "EventId": event_id,
                "StackName": "proj",
                "Timestamp": NOW,
                "ClientRequestToken": token,
                "LogicalResourceId": logical_id,
                "ResourceType": "AWS::Lambda::Function",
                "ResourceStatus": status,
                "ResourceStatusReason": reason,
            }

        events = [
            event("2", TOKEN, "WorkFunction", status, "role not ready"),
            event("1", "an earlier operation", "Old", "CREATE_FAILED", "an old failure"),
        ]
        self.cfn_stub.add_response(
            "describe_stack_events", {"StackEvents": events}, {"StackName": name}
        )

    def settles(self, status: str, resource_status: str = "CREATE_COMPLETE") -> None:
        self.stack(status)
        self.events(resource_status)

    def delete_stack(self) -> None:
        self.stack("CREATE_COMPLETE")
        resource = {
            "LogicalResourceId": "WorkFunction",
            "ResourceType": "AWS::Lambda::Function",
            "Timestamp": NOW,
            "ResourceStatus": "CREATE_COMPLETE",
        }
        self.cfn_stub.add_response(
            "describe_stack_resources", {"StackResources": [resource]}, {"StackName": "proj"}
        )
        self.cfn_stub.add_response(
            "delete_stack", {}, {"StackName": "proj", "ClientRequestToken": TOKEN}
        )
        self.stack("DELETE_COMPLETE", name="id")
        self.events("DELETE_COMPLETE", name="id")

    def stack_resources(self) -> None:
        def summary(kind: str, name: str) -> Any:
            return {
                "LogicalResourceId": "L",
                "PhysicalResourceId": name,
                "ResourceType": kind,
                "LastUpdatedTimestamp": NOW,
                "ResourceStatus": "CREATE_COMPLETE",
            }

        self.cfn_stub.add_response(
            "list_stack_resources",
            {
                "StackResourceSummaries": [
                    summary("AWS::Lambda::Function", "proj-work"),
                    summary("AWS::IAM::Role", "proj-WorkRole-X"),
                ],
                "NextToken": "2",
            },
            {"StackName": "proj"},
        )
        self.cfn_stub.add_response(
            "list_stack_resources",
            {"StackResourceSummaries": [summary("AWS::Lambda::Function", "proj-dispatcher")]},
            {"StackName": "proj", "NextToken": "2"},
        )

    def code_bucket(self, exists: bool) -> None:
        if exists:
            self.s3_stub.add_response("head_bucket", {}, {"Bucket": self.bucket})
        else:
            self.s3_stub.add_client_error(
                "head_bucket", "404", "Not Found", 404, expected_params={"Bucket": self.bucket}
            )

    def zip_exists(self, key: str) -> None:
        self.s3_stub.add_response("head_object", {}, {"Bucket": self.bucket, "Key": key})

    def create_bucket(self) -> None:
        self.s3_stub.add_response(
            "create_bucket",
            {},
            {
                "Bucket": self.bucket,
                "CreateBucketConfiguration": {"LocationConstraint": self.region},
            },
        )

    def upload(self) -> None:
        # The transfer manager adds checksum parameters of its own choosing.
        self.s3_stub.add_response("put_object", {}, None)

    def zips(self, keys: list[str]) -> None:
        self.s3_stub.add_response(
            "list_objects_v2",
            {"Contents": [{"Key": key} for key in keys]},
            {"Bucket": self.bucket, "Prefix": "proj/"},
        )

    def delete_zips(self, keys: list[str]) -> None:
        self.s3_stub.add_response(
            "delete_objects",
            {},
            {
                "Bucket": self.bucket,
                "Delete": {"Objects": [{"Key": key} for key in keys], "Quiet": True},
            },
        )

    def delete_bucket(self, left: list[str] = []) -> None:  # noqa: B006
        if not left:
            self.s3_stub.add_response("delete_bucket", {}, {"Bucket": self.bucket})
            return
        self.s3_stub.add_client_error(
            "delete_bucket",
            "BucketNotEmpty",
            "The bucket you tried to delete is not empty",
            409,
            expected_params={"Bucket": self.bucket},
        )
        self.s3_stub.add_response(
            "list_objects_v2",
            {"CommonPrefixes": [{"Prefix": prefix} for prefix in left]},
            {"Bucket": self.bucket, "Delimiter": "/"},
        )


def _buckets_output(buckets: list[str]) -> dict[str, str]:
    return {"OutputKey": template.OUTPUT_BUCKETS, "OutputValue": json.dumps(buckets)}


@pytest.fixture
def stubbed(monkeypatch: pytest.MonkeyPatch) -> Iterator[Stubbed]:
    clients = Stubbed()
    monkeypatch.setattr(aws, "uuid", SimpleNamespace(uuid4=lambda: TOKEN))
    monkeypatch.setattr(aws, "POLL_SECONDS", 0)
    monkeypatch.setattr(cli, "_Clients", lambda *_: clients)
    with clients.cfn_stub, clients.s3_stub:
        yield clients
        clients.cfn_stub.assert_no_pending_responses()
        clients.s3_stub.assert_no_pending_responses()


def _apply(stubbed: Stubbed, **kwargs: Any) -> bool:
    stack = template.template(_spec(), [])
    return aws.apply(stubbed.cfn, "proj", stack, [], sleep=lambda _: None, **kwargs)


def _first_deploy(stubbed: Stubbed) -> None:
    stubbed.no_stack()
    stubbed.change_set("CREATE", parameters=[])
    stubbed.described_change_set()


def test_a_first_deploy_shows_the_changes_then_creates_the_stack(stubbed, term):
    _first_deploy(stubbed)
    stubbed.execute()
    stubbed.settles("CREATE_COMPLETE")

    assert _apply(stubbed)

    said = term.lines()
    assert "    Add      WorkFunction (AWS::Lambda::Function)" in said
    assert said[-1].startswith("Creating stack proj: CREATE_COMPLETE in ")


def test_everything_is_shown_and_asked_about_once_before_anything_is_done(stubbed, term):
    _first_deploy(stubbed)
    stubbed.execute()
    stubbed.settles("CREATE_COMPLETE")

    _apply(stubbed, before=["Upload a.zip"], prepare=lambda: click.echo("prepared", err=True))

    said = term.lines()
    asked = said.index("Continue?")
    assert said.index("  Upload a.zip") < said.index("  Create stack proj:") < asked
    assert said.index("prepared") == asked + 1


def test_yes_does_it_without_asking(stubbed, term):
    _first_deploy(stubbed)
    stubbed.execute()
    stubbed.settles("CREATE_COMPLETE")

    assert _apply(stubbed, yes=True)

    assert "Continue?" not in term.lines()


def test_declining_a_first_deploy_does_nothing_and_leaves_no_empty_stack(stubbed, term):
    _first_deploy(stubbed)
    stubbed.delete_change_set()
    stubbed.delete_empty_stack()
    prepared: list[bool] = []
    term.answer = False

    assert not _apply(stubbed, prepare=lambda: prepared.append(True))

    assert prepared == []


def test_a_dry_run_shows_the_changes_then_stops_without_asking(stubbed, term):
    _first_deploy(stubbed)
    stubbed.delete_change_set()
    stubbed.delete_empty_stack()
    prepared: list[bool] = []

    assert not _apply(
        stubbed, before=["Upload a.zip"], prepare=lambda: prepared.append(True), dry_run=True
    )

    assert prepared == []
    assert term.lines()[-3:] == [
        "  Create stack proj:",
        "    Add      WorkFunction (AWS::Lambda::Function)",
        "Dry run: nothing was changed",
    ]


def test_a_dry_run_of_an_update_leaves_the_stack_alone(stubbed, term):
    stubbed.stack("CREATE_COMPLETE")
    stubbed.change_set("UPDATE", parameters=[])
    stubbed.described_change_set()
    stubbed.delete_change_set()

    assert not _apply(stubbed, dry_run=True)

    assert term.lines()[-1] == "Dry run: nothing was changed"


def test_progress_is_a_bar_of_the_resources_done(stubbed, term):
    _first_deploy(stubbed)
    stubbed.execute()
    stubbed.settles("CREATE_IN_PROGRESS")
    stubbed.settles("CREATE_COMPLETE")

    _apply(stubbed)

    said = term.lines()
    assert said.count("Creating stack proj 1/1") == 1
    assert not any("an old failure" in line for line in said), "an event from another operation"


def test_planning_says_when_it_is_done(stubbed, term):
    stubbed.no_stack()
    stubbed.change_set("CREATE", parameters=[])
    stubbed.described_change_set("CREATE_IN_PROGRESS")
    stubbed.described_change_set()
    stubbed.delete_change_set()
    stubbed.delete_empty_stack()
    term.answer = False

    _apply(stubbed)

    assert term.lines()[0] == "Planning the changes to proj ... done in 0s"


def test_planning_that_fails_finishes_its_line(stubbed, term):
    stubbed.no_stack()
    stubbed.cfn_stub.add_client_error("create_change_set", "ValidationError", "bad template")

    with pytest.raises(ClientError):
        _apply(stubbed)

    assert term.lines() == ["Planning the changes to proj ... failed"]


def test_changes_and_outcomes_are_coloured(stubbed, term):
    stubbed.stack("UPDATE_COMPLETE")
    stubbed.change_set("UPDATE", parameters=[])
    stubbed.described_change_set()
    stubbed.execute()
    stubbed.settles("UPDATE_ROLLBACK_COMPLETE", resource_status="UPDATE_FAILED")

    with CliRunner().isolation(color=True) as (_, stderr, _), pytest.raises(DeployError):
        _apply(stubbed)

    said = stderr.getvalue().decode().splitlines()
    green_add = click.style("Add      WorkFunction (AWS::Lambda::Function)", fg="green")
    assert f"    {green_add}" in said
    assert f"  {click.style('WorkFunction: role not ready', fg='red')}" in said
    red = click.style("", fg="red", reset=False)
    assert said[-1].startswith(f"{red}Updating stack proj: UPDATE_ROLLBACK_COMPLETE in ")


def test_a_deploy_with_nothing_to_change_says_so_and_stops(stubbed, term):
    stubbed.stack("UPDATE_COMPLETE")
    stubbed.change_set("UPDATE", parameters=[])
    stubbed.described_change_set(
        "FAILED", reason="The submitted information didn't contain changes."
    )
    stubbed.delete_change_set()

    assert not _apply(stubbed)

    assert term.lines()[-1] == "proj is up to date"


def test_a_failed_deploy_raises_and_shows_why(stubbed, term):
    stubbed.stack("UPDATE_COMPLETE")
    stubbed.change_set("UPDATE", parameters=[])
    stubbed.described_change_set()
    stubbed.execute()
    stubbed.settles("UPDATE_ROLLBACK_COMPLETE", resource_status="UPDATE_FAILED")

    with pytest.raises(DeployError, match="UPDATE_ROLLBACK_COMPLETE"):
        _apply(stubbed)

    assert "  WorkFunction: role not ready" in term.lines()


def test_a_stack_left_by_a_failed_create_is_deleted_before_trying_again(stubbed, term):
    stubbed.stack("ROLLBACK_COMPLETE")
    stubbed.delete_stack()
    stubbed.change_set("CREATE", parameters=[])
    stubbed.described_change_set()
    stubbed.execute()
    stubbed.settles("CREATE_COMPLETE")

    assert _apply(stubbed)


def test_a_busy_stack_is_left_alone(stubbed):
    stubbed.stack("UPDATE_IN_PROGRESS")

    with pytest.raises(DeployError, match="busy"):
        _apply(stubbed)


def _zips(tmp_path: Path) -> dict[str, Path]:
    return {
        binary: _zip(tmp_path / f"{binary}.zip", binary.encode(), (2026, 1, 1, 0, 0, 0))
        for binary in ("work", "dispatcher")
    }


def _deploy(
    monkeypatch: pytest.MonkeyPatch,
    term: Terminal,
    zips: dict[str, Path],
    allow: list[str],
    dry_run: bool = False,
) -> list[str]:
    monkeypatch.setattr(build, "build", lambda *_: zips)
    cli._deploy(_spec(), None, from_source=True, allow=allow, dry_run=dry_run)
    return term.lines()


def _parameters(stubbed: Stubbed, zips: dict[str, Path]) -> list[dict[str, str]]:
    return [{"ParameterKey": template.BUCKET_PARAMETER, "ParameterValue": stubbed.bucket}] + [
        {
            "ParameterKey": template.code_parameter(binary),
            "ParameterValue": build.code_key("proj", binary, path),
        }
        for binary, path in zips.items()
    ]


def _allowed(stubbed: Stubbed) -> list[str]:
    outputs = json.loads(stubbed.template.value)["Outputs"]
    return json.loads(outputs[template.OUTPUT_BUCKETS]["Value"])


def test_a_first_deploy_makes_the_bucket_uploads_and_allows_buckets(
    stubbed, term, monkeypatch, tmp_path
):
    zips = _zips(tmp_path)
    stubbed.code_bucket(exists=False)
    stubbed.no_stack()
    stubbed.no_stack()
    stubbed.change_set("CREATE", parameters=_parameters(stubbed, zips))
    stubbed.described_change_set()
    stubbed.create_bucket()
    stubbed.upload()
    stubbed.upload()
    stubbed.execute()
    stubbed.settles("CREATE_COMPLETE")

    said = _deploy(monkeypatch, term, zips, ["b", "a"])

    assert _allowed(stubbed) == ["a", "b"]
    assert "  Allow the workers to use a" in said
    assert f"Creating bucket {stubbed.bucket} ... done in 0s" in said
    uploaded = [line for line in said if line.startswith("Uploading proj/")]
    assert len(uploaded) == 2
    assert all(line.endswith(" MB) ... done in 0s") for line in uploaded)
    assert said[-1] == "Ready"


def test_a_deploy_keeps_the_buckets_already_allowed_and_the_zips_already_uploaded(
    stubbed, term, monkeypatch, tmp_path
):
    zips = _zips(tmp_path)
    stubbed.code_bucket(exists=True)
    for binary, path in zips.items():
        stubbed.zip_exists(build.code_key("proj", binary, path))
    stubbed.stack("CREATE_COMPLETE", buckets=["a"])
    stubbed.stack("CREATE_COMPLETE", buckets=["a"])
    stubbed.change_set("UPDATE", parameters=_parameters(stubbed, zips))
    stubbed.described_change_set()
    stubbed.execute()
    stubbed.settles("UPDATE_COMPLETE")

    said = _deploy(monkeypatch, term, zips, ["b"])

    assert _allowed(stubbed) == ["a", "b"]
    assert "  Allow the workers to use b" in said
    assert "  Allow the workers to use a" not in said
    assert not any(line.startswith("  Upload") for line in said)


def test_a_dry_run_deploy_makes_no_bucket_and_uploads_nothing(stubbed, term, monkeypatch, tmp_path):
    zips = _zips(tmp_path)
    stubbed.code_bucket(exists=False)
    stubbed.no_stack()
    stubbed.no_stack()
    stubbed.change_set("CREATE", parameters=_parameters(stubbed, zips))
    stubbed.described_change_set()
    stubbed.delete_change_set()
    stubbed.delete_empty_stack()

    said = _deploy(monkeypatch, term, zips, [], dry_run=True)

    assert f"  Create bucket {stubbed.bucket}" in said
    assert said[-1] == "Dry run: nothing was changed"


def _delete(term: Terminal) -> list[str]:
    cli._delete(_spec(), None)
    return term.lines()


def test_delete_deletes_the_stack_the_zips_and_the_emptied_bucket(stubbed, term):
    zips = ["proj/work-1.zip", "proj/dispatcher-2.zip"]
    stubbed.stack("CREATE_COMPLETE")
    stubbed.code_bucket(exists=True)
    stubbed.zips(zips)
    stubbed.delete_stack()
    stubbed.delete_zips(zips)
    stubbed.delete_bucket()

    said = _delete(term)

    assert f"  Delete its 2 zips from {stubbed.bucket}" in said
    assert said[-1] == "Removed"


def test_delete_keeps_a_bucket_holding_another_project_s_zips(stubbed, term):
    stubbed.stack("CREATE_COMPLETE")
    stubbed.code_bucket(exists=True)
    stubbed.zips(["proj/work-1.zip"])
    stubbed.delete_stack()
    stubbed.delete_zips(["proj/work-1.zip"])
    stubbed.delete_bucket(left=["other/"])

    said = _delete(term)

    assert f"Kept {stubbed.bucket}, which still holds other/" in said


def test_delete_still_removes_zips_left_after_the_stack_went(stubbed, term):
    stubbed.no_stack()
    stubbed.code_bucket(exists=True)
    stubbed.zips(["proj/work-1.zip"])
    stubbed.delete_zips(["proj/work-1.zip"])
    stubbed.delete_bucket()

    assert _delete(term)[-1] == "Removed"


def test_delete_with_nothing_deployed_says_so(stubbed, term):
    stubbed.no_stack()
    stubbed.code_bucket(exists=False)

    assert _delete(term) == [f"There is nothing of proj in {stubbed.region}"]


def _list(stubbed: Stubbed, capsys: pytest.CaptureFixture[str]) -> list[str]:
    cli._list(_spec(), None)
    return capsys.readouterr().out.splitlines()


def test_list_shows_every_resource_the_buckets_and_the_zips(stubbed, capsys):
    stubbed.stack("CREATE_COMPLETE", buckets=["data"])
    stubbed.code_bucket(exists=True)
    stubbed.zips(["proj/work-1.zip"])
    stubbed.stack_resources()

    assert _list(stubbed, capsys) == [
        f"Stack proj in account {stubbed.account}, {stubbed.region}: CREATE_COMPLETE, "
        "updated 2026-11-05 12:00 UTC",
        "  AWS::IAM::Role         proj-WorkRole-X  CREATE_COMPLETE",
        "  AWS::Lambda::Function  proj-dispatcher  CREATE_COMPLETE",
        "  AWS::Lambda::Function  proj-work  CREATE_COMPLETE",
        "Allowed buckets: data",
        f"Zips in {stubbed.bucket}:",
        "  proj/work-1.zip",
    ]


def test_list_shows_zips_left_without_a_stack(stubbed, capsys):
    stubbed.no_stack()
    stubbed.code_bucket(exists=True)
    stubbed.zips(["proj/work-1.zip"])

    lines = _list(stubbed, capsys)

    assert lines[0] == f"There is no proj stack in account {stubbed.account}, {stubbed.region}"
    assert lines[-1] == "  proj/work-1.zip"


def test_list_with_nothing_deployed_says_so(stubbed, capsys):
    stubbed.no_stack()
    stubbed.code_bucket(exists=False)

    assert _list(stubbed, capsys) == [
        f"There is nothing of proj in account {stubbed.account}, {stubbed.region}"
    ]


def _invoke(command: click.Command, argv: list[str]) -> Any:
    return CliRunner().invoke(command, argv, catch_exceptions=False)


@pytest.mark.parametrize(
    "argv",
    [
        ["--region", "eu-west-1", "delete"],
        ["delete", "--region", "eu-west-1"],
        ["--region", "us-east-1", "delete", "--region", "eu-west-1"],
    ],
)
def test_the_region_can_go_before_or_after_the_command(monkeypatch, argv):
    regions = []
    monkeypatch.setattr(cli, "_delete", lambda project, region, **_: regions.append(region))

    _invoke(cli.group(), argv)

    assert regions == ["eu-west-1"]


def test_without_a_region_the_environment_s_is_used(monkeypatch):
    regions = []
    monkeypatch.setattr(cli, "_delete", lambda project, region, **_: regions.append(region))

    _invoke(cli.group(), ["delete"])

    assert regions == [None]


def test_an_application_adds_the_commands_to_its_own_cli_with_its_spec(monkeypatch, tmp_path):
    spec_path = tmp_path / "deploy.toml"
    spec_path.write_text('project = "app"\n[functions.work]\nmemory = 1024\n')
    calls = []

    def deploy(project: spec.Spec, region: str | None, **kwargs: Any) -> None:
        calls.append((project.project, region, kwargs))

    monkeypatch.setattr(cli, "_deploy", deploy)

    @click.group()
    def app() -> None:
        pass

    for command in cli.commands(spec_path):
        app.add_command(command)

    _invoke(app, ["deploy", "--from-source", "--allow-bucket", "a", "--allow-bucket", "b", "--yes"])

    assert calls == [
        ("app", None, {"yes": True, "from_source": True, "allow": ("a", "b"), "dry_run": False})
    ]
    assert set(app.commands) == {"deploy", "delete", "list", "allow-bucket"}


def test_a_failure_is_one_line_saying_why(monkeypatch):
    result = CliRunner().invoke(cli.group(), ["deploy"])

    assert result.exit_code == 1
    assert result.output == (
        "Error: release zips aren't published yet: deploy with --from-source\n"
    )
