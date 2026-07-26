"""Run Mercury's complete, cross-platform source quality gate."""

from __future__ import annotations

import shlex
import subprocess
from collections.abc import Iterable, Sequence

CHECK_COMMANDS: tuple[tuple[str, ...], ...] = (
    ("uv", "lock", "--check"),
    ("uv", "run", "ruff", "check", "--no-cache", "."),
    ("uv", "run", "ruff", "format", "--check", "--no-cache", "."),
    ("uv", "run", "mypy", "."),
    ("uv", "run", "pytest"),
    (
        "uv",
        "export",
        "--frozen",
        "--no-dev",
        "--no-emit-project",
        "--no-hashes",
        "--format",
        "requirements-txt",
        "--output-file",
        ".audit-requirements.txt",
    ),
    ("uv", "run", "pip-audit", "--requirement", ".audit-requirements.txt"),
    ("uv", "build"),
)


def run_checks(commands: Iterable[Sequence[str]] = CHECK_COMMANDS) -> None:
    """Run each check in order and stop on the first failure."""
    for command in commands:
        print(f"$ {shlex.join(command)}", flush=True)
        subprocess.run(command, check=True)


def main() -> None:
    run_checks()


if __name__ == "__main__":
    main()
