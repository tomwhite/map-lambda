"""
A release's zips: written for a release to attach, with their checksums beside the spec
for the wheel to carry, and downloaded again by a deploy that doesn't build from source.
"""

import hashlib
import importlib.metadata
import shutil
import urllib.error
import urllib.request
from pathlib import Path

from .aws import DeployError, step
from .spec import Spec, SpecError

_TIMEOUT_SECONDS = 60


def write(spec: Spec, zips: dict[str, Path], out: Path) -> list[Path]:
    """
    Each binary's zip as `<binary>.zip` in `out`, and their checksums in the spec's
    `sums` file, which a wheel built afterwards carries.
    """
    if spec.sums is None:
        raise SpecError("a release needs a spec file, for its checksums to go beside")
    out.mkdir(parents=True, exist_ok=True)
    written = []
    for binary, path in sorted(zips.items()):
        dest = out / f"{binary}.zip"
        shutil.copyfile(path, dest)
        written.append(dest)
    spec.sums.write_text("".join(f"{_sha256(path)}  {path.name}\n" for path in written))
    return [*written, spec.sums]


def url(spec: Spec) -> str:
    """The release matching the installed version of the distribution holding the spec."""
    if spec.release_url is None:
        raise SpecError(f"{spec.project} has no release_url: deploy with --from-source")
    if spec.distribution is None:
        raise SpecError("a spec outside an installed package has no release: use --from-source")
    version = importlib.metadata.version(spec.distribution)
    return spec.release_url.format(version=version).rstrip("/")


def download(spec: Spec, dest: Path) -> dict[str, Path]:
    """Each binary's zip from the release, checked against the checksums installed with it."""
    base = url(spec)
    if spec.sums is None or not spec.sums.exists():
        raise SpecError(
            f"this install of {spec.distribution} has no checksums of its release's zips, "
            "as it wasn't built by a release: deploy with --from-source"
        )
    sums = {}
    for line in spec.sums.read_text().splitlines():
        digest, _, name = line.partition("  ")
        sums[name] = digest
    zips = {}
    for binary in spec.binaries:
        name = f"{binary}.zip"
        if name not in sums:
            raise SpecError(f"{spec.sums} has no checksum for {name}")
        path = dest / name
        with step(f"Downloading {name}"):
            _fetch(f"{base}/{name}", path)
        if _sha256(path) != sums[name]:
            raise DeployError(f"{name} from {base} isn't the one this install was built with")
        zips[binary] = path
    return zips


def _fetch(url: str, path: Path) -> None:
    try:
        with (
            urllib.request.urlopen(url, timeout=_TIMEOUT_SECONDS) as response,
            open(path, "wb") as f,
        ):
            shutil.copyfileobj(response, f)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            raise DeployError(f"{url} doesn't exist: deploy with --from-source") from None
        raise DeployError(f"downloading {url} failed: {e.code} {e.reason}") from None
    except urllib.error.URLError as e:
        raise DeployError(f"downloading {url} failed: {e.reason}") from None


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()
