"""
Deploying a project's lambdas as one CloudFormation stack.

A project ships a spec (see `spec`) and an entry point calling `main` with it. Its
client then opens a `Session` with `project(spec_path)`, which names the functions
the way the stack does.
"""

from pathlib import Path

from . import spec
from .cli import commands, group, main


def project(spec_path: Path) -> str:
    """The project a spec deploys, for a client's `Session(project=...)`."""
    return spec.load(spec_path).project


__all__ = ["commands", "group", "main", "project"]
