"""
The `deploy`, `delete`, `list`, `allow-bucket` and `build` commands for a project's spec: run by
`main` from the project's own entry point, or added to its own click CLI from `commands`.
"""

import subprocess
import tempfile
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import click

from . import aws, build, release, spec, template
from .aws import DeployError, say
from .spec import Spec, SpecError

ECHO_SPEC = Path(__file__).with_name("echo.toml")

_SPEC = "map_lambda.deploy.spec"
_REGION = "map_lambda.deploy.region"
_REGION_HELP = "The AWS region, if not the environment's."


def main(spec_path: Path | None = None, argv: list[str] | None = None) -> None:
    """
    Run a command against the spec at `spec_path`. Without one, `--spec` names it,
    defaulting to map-lambda's own echo project.
    """
    group(spec_path).main(args=argv)


def group(spec_path: Path | None = None) -> click.Group:
    """The commands as one click group, which takes `--region` before a command too."""
    params: list[click.Parameter] = [click.Option(["--region"], help=_REGION_HELP)]
    if spec_path is None:
        params.append(
            click.Option(
                ["--spec"],
                type=click.Path(exists=True, dir_okay=False, path_type=Path),
                default=ECHO_SPEC,
                help="The deploy spec. [default: map-lambda's own echo project]",
            )
        )

    def remember(region: str | None, spec: Path | None = None) -> None:
        meta = click.get_current_context().meta
        meta[_REGION] = region
        meta[_SPEC] = spec

    return click.Group(
        help="Deploy a map-lambda project to AWS.",
        params=params,
        callback=remember,
        commands=commands(spec_path),
    )


def commands(spec_path: Path | None = None) -> list[click.Command]:
    """
    `deploy`, `delete`, `list`, `allow-bucket` and `build` for the spec at `spec_path`, to add to
    an application's own click group. Without one, the spec comes from `group`'s `--spec`.
    """

    def project() -> Spec:
        return spec.load(spec_path or click.get_current_context().meta[_SPEC])

    def resolved(region: str | None) -> str | None:
        return region or click.get_current_context().meta.get(_REGION)

    region_option = click.option("--region", help=_REGION_HELP)

    @click.command("deploy")
    @click.option("--from-source", is_flag=True, help="Build the zips with cargo lambda.")
    @click.option(
        "--allow-bucket",
        "allow",
        multiple=True,
        metavar="BUCKET",
        help="Let the workers use a bucket too, as allow-bucket does. May be repeated.",
    )
    @click.option("--dry-run", is_flag=True, help="Show what would change, then stop.")
    @click.option("--yes", is_flag=True, help="Don't ask before changing anything.")
    @region_option
    def deploy(
        from_source: bool, allow: tuple[str, ...], dry_run: bool, yes: bool, region: str | None
    ) -> None:
        """
        Create or update the project's stack, with the zips from the release matching
        the installed version.
        """
        with _errors():
            _deploy(
                project(),
                resolved(region),
                yes=yes,
                from_source=from_source,
                allow=allow,
                dry_run=dry_run,
            )

    @click.command("delete")
    @click.option("--yes", is_flag=True, help="Don't ask first.")
    @region_option
    def delete(yes: bool, region: str | None) -> None:
        """Delete the project's stack and zips, but no data."""
        with _errors():
            _delete(project(), resolved(region), yes=yes)

    @click.command("list")
    @region_option
    def list_(region: str | None) -> None:
        """Show what the project has deployed."""
        with _errors():
            _list(project(), resolved(region))

    @click.command("allow-bucket")
    @click.argument("bucket")
    @click.option("--remove", is_flag=True, help="Take the access away again.")
    @click.option("--yes", is_flag=True, help="Don't ask first.")
    @region_option
    def allow_bucket(bucket: str, remove: bool, yes: bool, region: str | None) -> None:
        """Let the workers use a bucket."""
        with _errors():
            _allow_bucket(project(), resolved(region), bucket, remove=remove, yes=yes)

    @click.command("build")
    @click.argument("out", type=click.Path(file_okay=False, path_type=Path))
    def build_(out: Path) -> None:
        """
        Build the zips a release attaches into OUT, and their checksums beside the spec,
        for the release's wheels to carry.
        """
        with _errors(), tempfile.TemporaryDirectory(prefix="map-lambda-") as lambda_dir:
            released = project()
            zips = build.build(released, Path(lambda_dir))
            for path in release.write(released, zips, out):
                click.echo(path)

    return [deploy, delete, list_, allow_bucket, build_]


@contextmanager
def _errors() -> Iterator[None]:
    try:
        yield
    except (SpecError, DeployError) as e:
        raise click.ClickException(str(e)) from None
    except subprocess.CalledProcessError:
        raise click.ClickException("the build failed: see cargo's output above") from None


def _deploy(
    project: Spec,
    region: str | None,
    *,
    yes: bool = False,
    from_source: bool,
    allow: Sequence[str] = (),
    dry_run: bool = False,
) -> None:
    with tempfile.TemporaryDirectory(prefix="map-lambda-") as lambda_dir:
        get = build.build if from_source else release.download
        zips = get(project, Path(lambda_dir))
        keys = {
            binary: build.code_key(project.project, binary, path) for binary, path in zips.items()
        }
        clients = _Clients(project, region)
        bucket = aws.code_bucket(clients.account, clients.region)
        new_bucket = not aws.bucket_exists(clients.s3, bucket)
        uploads = [
            (keys[binary], path)
            for binary, path in zips.items()
            if new_bucket or not aws.object_exists(clients.s3, bucket, keys[binary])
        ]

        def prepare() -> None:
            if new_bucket:
                with aws.step(f"Creating bucket {bucket}"):
                    aws.create_bucket(clients.s3, bucket, clients.region)
            for key, path in uploads:
                with aws.step(f"Uploading {key} ({path.stat().st_size / 1e6:.1f} MB)"):
                    aws.upload(clients.s3, bucket, key, path)

        current = aws.describe(clients.cfn, project.project)
        allowed = [] if current is None else current.buckets
        buckets = sorted(set(allowed) | set(allow))
        parameters = [{"ParameterKey": template.BUCKET_PARAMETER, "ParameterValue": bucket}] + [
            {"ParameterKey": template.code_parameter(binary), "ParameterValue": key}
            for binary, key in keys.items()
        ]
        say(f"Deploying {project.project} to account {clients.account}, {clients.region}")
        before = [f"Create bucket {bucket}"] if new_bucket else []
        before += [f"Upload {key} ({path.stat().st_size / 1e6:.1f} MB)" for key, path in uploads]
        before += [f"Allow the workers to use {b}" for b in buckets if b not in allowed]
        stack = template.template(project, buckets)
        if aws.apply(
            clients.cfn,
            project.project,
            stack,
            parameters,
            yes=yes,
            before=before,
            prepare=prepare,
            dry_run=dry_run,
        ):
            say(click.style("Ready", fg="green"))


def _delete(project: Spec, region: str | None, *, yes: bool = False) -> None:
    clients = _Clients(project, region)
    stack = aws.describe(clients.cfn, project.project)
    bucket = aws.code_bucket(clients.account, clients.region)
    has_bucket = aws.bucket_exists(clients.s3, bucket)
    # Zips are useless once their stack is gone: they're only fetched again to roll
    # back an update.
    zips = aws.keys_under(clients.s3, bucket, f"{project.project}/") if has_bucket else []
    if stack is None and not has_bucket:
        say(f"There is nothing of {project.project} in {clients.region}")
        return

    say(f"Removing {project.project} from account {clients.account}, {clients.region}:")
    steps = []
    if stack is not None:
        steps.append(f"Delete stack {project.project} (functions, roles, logs)")
    if zips:
        steps.append(f"Delete its {len(zips)} zips from {bucket}")
    if has_bucket:
        steps.append(f"Delete {bucket}, unless other projects' zips are in it")
    for step in steps:
        say(f"  {click.style(step, fg='red')}")
    if not aws.confirm("Continue?", yes=yes):
        return

    if stack is not None:
        aws.delete(clients.cfn, project.project)
    if has_bucket:
        aws.delete_keys(clients.s3, bucket, zips)
        left = aws.delete_bucket_if_empty(clients.s3, bucket)
        if left:
            say(f"Kept {bucket}, which still holds {', '.join(left)}")
    say(click.style("Removed", fg="green"))


def _list(project: Spec, region: str | None) -> None:
    clients = _Clients(project, region)
    stack = aws.describe(clients.cfn, project.project)
    bucket = aws.code_bucket(clients.account, clients.region)
    zips = (
        aws.keys_under(clients.s3, bucket, f"{project.project}/")
        if aws.bucket_exists(clients.s3, bucket)
        else []
    )
    where = f"account {clients.account}, {clients.region}"
    if stack is None and not zips:
        click.echo(f"There is nothing of {project.project} in {where}")
        return
    if stack is None:
        click.echo(f"There is no {project.project} stack in {where}")
    else:
        updated = f", updated {stack.updated:%Y-%m-%d %H:%M %Z}" if stack.updated else ""
        click.echo(f"Stack {project.project} in {where}: {stack.status}{updated}")
        resources = aws.resources(clients.cfn, project.project)
        width = max((len(kind) for kind, _, _ in resources), default=0)
        for kind, name, status in resources:
            click.echo(f"  {kind:<{width}}  {name}  {status}")
        click.echo(f"Allowed buckets: {', '.join(stack.buckets) or 'none'}")
    if zips:
        click.echo(f"Zips in {bucket}:")
        for key in zips:
            click.echo(f"  {key}")


def _allow_bucket(
    project: Spec, region: str | None, bucket: str, *, remove: bool, yes: bool = False
) -> None:
    clients = _Clients(project, region)
    current = aws.describe(clients.cfn, project.project)
    if current is None:
        raise DeployError(f"there is no {project.project} stack in {clients.region}: deploy first")
    buckets = set(current.buckets)
    if remove:
        buckets.discard(bucket)
    else:
        buckets.add(bucket)
    if buckets == set(current.buckets):
        say(f"{bucket} is already {'not ' if remove else ''}allowed")
        return
    # The code stays as deployed, so the spec only needs to match it, not a build.
    params = template.template(project, [])["Parameters"]
    parameters = [{"ParameterKey": name, "UsePreviousValue": True} for name in params]
    action = "Taking away" if remove else "Allowing"
    say(f"{action} {bucket} for {project.project} in account {clients.account}, {clients.region}")
    aws.apply(
        clients.cfn,
        project.project,
        template.template(project, sorted(buckets)),
        parameters,
        yes=yes,
    )


class _Clients:
    """boto3 clients, imported only when a command needs AWS."""

    def __init__(self, project: Spec, region: str | None) -> None:
        try:
            import boto3
        except ImportError:
            raise DeployError("deploying needs boto3: install map-lambda[deploy]") from None
        if project.region is not None and region not in (None, project.region):
            raise DeployError(f"{project.project} has to be deployed in {project.region}")
        session = boto3.Session(region_name=region or project.region)
        if session.region_name is None:
            raise DeployError("no AWS region: pass --region or set AWS_REGION")
        self.region: str = session.region_name
        self.cfn: Any = session.client("cloudformation")
        self.s3: Any = session.client("s3")
        self.account: str = session.client("sts").get_caller_identity()["Account"]
