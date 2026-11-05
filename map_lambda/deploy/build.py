"""Building a spec's zips from source, and naming them by their contents."""

import hashlib
import shutil
import subprocess
import zipfile
from pathlib import Path

from .spec import Spec, SpecError


def build(spec: Spec, lambda_dir: Path) -> dict[str, Path]:
    """
    Each binary's zip, built with `cargo lambda` into `lambda_dir`. Needs Rust, Zig and
    cargo lambda.
    """
    if spec.workspace is None or not (spec.workspace / "Cargo.toml").exists():
        raise SpecError(
            "building from source needs the project's Cargo workspace: "
            "run from a checkout, with `workspace` set in the spec"
        )
    if shutil.which("cargo-lambda") is None:
        raise SpecError("building from source needs cargo lambda: https://www.cargo-lambda.info")
    packages = sorted(set(spec.binaries.values()))
    subprocess.run(
        [
            "cargo",
            "lambda",
            "build",
            "--release",
            "--arm64",
            "--output-format",
            "zip",
            "--lambda-dir",
            str(lambda_dir),
            *(arg for package in packages for arg in ("--package", package)),
        ],
        cwd=spec.workspace,
        check=True,
    )
    zips = {binary: lambda_dir / binary / "bootstrap.zip" for binary in spec.binaries}
    for binary, path in zips.items():
        if not path.exists():
            raise SpecError(f"the build made no {binary!r} binary: check the spec's package")
    return zips


def content_hash(path: Path) -> str:
    """
    A hash of the files in a zip, ignoring the timestamps a rebuild changes, so an
    unchanged binary keeps its key and CloudFormation leaves its functions alone.
    """
    digest = hashlib.sha256()
    with zipfile.ZipFile(path) as archive:
        for info in sorted(archive.infolist(), key=lambda i: i.filename):
            digest.update(info.filename.encode() + b"\0")
            digest.update(f"{info.external_attr}\0".encode())
            digest.update(archive.read(info))
    return digest.hexdigest()


def code_key(project: str, binary: str, path: Path) -> str:
    return f"{project}/{binary}-{content_hash(path)[:16]}.zip"
