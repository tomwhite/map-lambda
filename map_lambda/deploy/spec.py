"""
A project's deploy spec: which functions it has, their memory and timeouts, and what
they may do with the buckets a deployment allows.
"""

import importlib.metadata
import importlib.util
import json
import re
import tomllib
import urllib.parse
import urllib.request
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

DISPATCHER = "dispatcher"

LAMBDA_MAX_TIMEOUT = 900

LAMBDA_MEMORY_RANGE = range(128, 10241)

LAMBDA_MAX_NAME = 64

_NAME = re.compile(r"^[a-z][a-z0-9-]*$")


class SpecError(ValueError):
    """A spec that can't be deployed, saying what to change."""


@dataclass(frozen=True)
class BucketAccess:
    """One IAM statement, applied to each allowed bucket in turn."""

    action: tuple[str, ...]
    resource: str
    """An ARN containing `{bucket}`, such as `arn:aws:s3:::{bucket}/*`."""


@dataclass(frozen=True)
class Function:
    """
    One function in the spec. `memory = 1024` deploys it once, under its plain name.
    `memory = [1024, 3538]` deploys the plain name at the first size, and every size
    with the size as a suffix, so a client can name any size by number.
    """

    name: str
    package: str
    """The Cargo package that builds it."""
    binary: str
    memory: int
    """The default size, deployed under the plain name."""
    timeout: int
    sizes: tuple[int, ...] = ()
    """The sizes deployed with a suffix, from a list of them."""
    bucket_access: tuple[BucketAccess, ...] = ()

    def deployed(self, project: str) -> list[tuple[str, int]]:
        """Each deployed name, with its memory, in the client's naming convention."""
        plain = f"{project}-{self.name}"
        return [(plain, self.memory)] + [(f"{plain}-{size}", size) for size in self.sizes]


@dataclass(frozen=True)
class Spec:
    project: str
    """
    The stack's name, and the prefix of every function's name. Defaults to the name of
    the installed distribution whose package holds the spec.
    """
    dispatcher: Function
    """The regular dispatcher, for calls that don't combine."""
    functions: tuple[Function, ...]
    combiners: tuple[Function, ...] = ()
    """
    Combining dispatchers, one for each combiner, each named `dispatcher-<combiner>`.
    """
    region: str | None = None
    """A region the project's data requires, which can't be overridden."""
    workspace: Path | None = None
    """The Cargo workspace a `--from-source` deploy builds."""
    release_url: str | None = None
    """Where a plain deploy downloads the zips from, with `{version}` for the release's."""
    distribution: str | None = None
    """The installed distribution holding the spec, whose version picks the release."""
    sums: Path | None = None
    """
    Where a release build writes the zips' checksums, beside the spec so its wheel
    carries them, for a plain deploy to check the release's zips against.
    """

    @property
    def binaries(self) -> dict[str, str]:
        """Each binary to build, with its package."""
        return {f.binary: f.package for f in (*self.dispatchers, *self.functions)}

    @property
    def dispatchers(self) -> tuple[Function, ...]:
        return (self.dispatcher, *self.combiners)


def load(path: Path) -> Spec:
    """Read a spec file. `workspace` in it is relative to the file."""
    with open(path, "rb") as f:
        raw = tomllib.load(f)
    project = parse(raw, base=path.parent, distribution=distribution_of(path))
    return replace(project, sums=path.with_suffix(".sha256"))


def distribution_of(path: Path) -> str | None:
    """The name of the installed distribution whose package holds `path`, if any."""
    owners = importlib.metadata.packages_distributions()
    path = path.resolve()
    for parent in path.parents:
        if parent.name not in owners:
            continue
        found = importlib.util.find_spec(parent.name)
        locations = (found and found.submodule_search_locations) or []
        if any(Path(location).resolve() == parent for location in locations):
            return owners[parent.name][0]
    # An editable install needn't list its package's files, only where its source is.
    editable = {
        source: dist.metadata["Name"]
        for dist in importlib.metadata.distributions()
        if (source := _editable_source(dist)) is not None
    }
    for parent in path.parents:
        if parent in editable:
            return editable[parent]
    return None


def _editable_source(dist: importlib.metadata.Distribution) -> Path | None:
    text = dist.read_text("direct_url.json")
    if text is None:
        return None
    direct_url = json.loads(text)
    url = urllib.parse.urlparse(direct_url.get("url", ""))
    if url.scheme != "file" or not direct_url.get("dir_info", {}).get("editable"):
        return None
    return Path(urllib.request.url2pathname(url.path)).resolve()


def parse(raw: dict[str, Any], base: Path, distribution: str | None = None) -> Spec:
    raw = dict(raw)
    project = _take(raw, "project", str)
    if project is None:
        if distribution is None:
            raise SpecError("project is required for a spec outside an installed package")
        project = re.sub(r"[-_.]+", "-", distribution).lower()
    if not _NAME.match(project):
        raise SpecError(f"project {project!r} must be lowercase letters, digits and hyphens")
    region = _take(raw, "region", str)
    workspace = _take(raw, "workspace", str)
    release_url = _take(raw, "release_url", str)
    if release_url is not None:
        try:
            release_url.format(version="")
        except (KeyError, IndexError, ValueError):
            raise SpecError("release_url can only contain {version}") from None
    dispatcher = _dispatcher(
        DISPATCHER,
        _take(raw, "dispatcher", dict) or {},
        where=DISPATCHER,
        default_package=DISPATCHER,
        default_binary=DISPATCHER,
        default_memory=3538,
    )
    combiners = []
    for combiner, table in (_take(raw, "combiners", dict) or {}).items():
        if not isinstance(table, dict):
            raise SpecError(f"combiners.{combiner} must be a table")
        name = f"{DISPATCHER}-{combiner}"
        combiners.append(
            _dispatcher(
                name,
                table,
                where=f"combiners.{combiner}",
                default_package=name,
                default_binary=name,
                default_memory=dispatcher.memory,
                default_timeout=dispatcher.timeout,
            )
        )

    functions = []
    for name, table in (_take(raw, "functions", dict) or {}).items():
        if name == DISPATCHER:
            raise SpecError("a function can't be called 'dispatcher'")
        if not isinstance(table, dict):
            raise SpecError(f"functions.{name} must be a table")
        functions.append(_function(name, table))
    if not functions:
        raise SpecError("the spec has no functions")
    _refuse_unknown(raw, "the spec")

    spec = Spec(
        project=project,
        dispatcher=dispatcher,
        functions=tuple(functions),
        combiners=tuple(combiners),
        region=region,
        workspace=None if workspace is None else (base / workspace).resolve(),
        release_url=release_url,
        distribution=distribution,
    )
    for function in (*spec.dispatchers, *spec.functions):
        for deployed, _ in function.deployed(project):
            if len(deployed) > LAMBDA_MAX_NAME:
                raise SpecError(
                    f"{deployed!r} is longer than Lambda's {LAMBDA_MAX_NAME} characters"
                )
    return spec


def _dispatcher(name: str, table: dict[str, Any], *, where: str, **defaults: Any) -> Function:
    dispatcher = _function(name, table, where=where, **defaults)
    if dispatcher.sizes:
        raise SpecError(f"{where}: a dispatcher takes one memory size")
    if dispatcher.bucket_access:
        raise SpecError(f"{where}: a dispatcher can't have bucket_access: only workers touch data")
    return dispatcher


def _function(
    name: str,
    table: dict[str, Any],
    *,
    where: str | None = None,
    default_package: str | None = None,
    default_binary: str | None = None,
    default_memory: int | None = None,
    default_timeout: int = LAMBDA_MAX_TIMEOUT,
) -> Function:
    where = where or f"functions.{name}"
    if not _NAME.match(name):
        raise SpecError(f"{where}: names must be lowercase letters, digits and hyphens")
    table = dict(table)
    package = _take(table, "package", str) or default_package or name
    binary = _take(table, "binary", str) or default_binary or name

    memory = table.pop("memory", default_memory)
    if memory is None:
        raise SpecError(f"{where}: memory is required")
    sizes = tuple(memory) if isinstance(memory, list) else (memory,)
    if not sizes or not all(isinstance(m, int) and m in LAMBDA_MEMORY_RANGE for m in sizes):
        raise SpecError(f"{where}: memory must be MB from 128 to 10240, or a list of them")
    if len(set(sizes)) != len(sizes):
        raise SpecError(f"{where}: memory has a size twice")

    timeout = table.pop("timeout", default_timeout)
    if not isinstance(timeout, int) or not 1 <= timeout <= LAMBDA_MAX_TIMEOUT:
        raise SpecError(f"{where}: timeout must be seconds from 1 to {LAMBDA_MAX_TIMEOUT}")

    access = []
    for statement in table.pop("bucket_access", []):
        statement = dict(statement)
        action = statement.pop("action", None)
        actions = (action,) if isinstance(action, str) else tuple(action or ())
        resource = _take(statement, "resource", str, required=True)
        if not actions:
            raise SpecError(f"{where}.bucket_access: action is required")
        if "{bucket}" not in resource:
            raise SpecError(f"{where}.bucket_access: resource must contain {{bucket}}")
        _refuse_unknown(statement, f"{where}.bucket_access")
        access.append(BucketAccess(actions, resource))
    _refuse_unknown(table, where)

    return Function(
        name=name,
        package=package,
        binary=binary,
        memory=sizes[0],
        sizes=sizes if isinstance(memory, list) else (),
        timeout=timeout,
        bucket_access=tuple(access),
    )


def _take(table: dict[str, Any], key: str, kind: type, *, required: bool = False) -> Any:
    value = table.pop(key, None)
    if value is None:
        if required:
            raise SpecError(f"{key} is required")
        return None
    if not isinstance(value, kind):
        raise SpecError(f"{key} must be a {kind.__name__}")
    return value


def _refuse_unknown(table: dict[str, Any], where: str) -> None:
    """Refused, so a misspelt setting fails rather than silently taking its default."""
    if table:
        raise SpecError(f"{where}: unknown {', '.join(sorted(table))}")
