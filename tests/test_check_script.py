from __future__ import annotations

import subprocess
from collections.abc import Sequence

import pytest

from scripts.check import CHECK_COMMANDS, run_checks


def test_check_commands_cover_the_release_gate() -> None:
    rendered = {" ".join(command) for command in CHECK_COMMANDS}

    assert "uv lock --check" in rendered
    assert "uv run ruff check --no-cache ." in rendered
    assert "uv run ruff format --check --no-cache ." in rendered
    assert "uv run mypy ." in rendered
    assert "uv run pytest" in rendered
    assert any(command.startswith("uv export --frozen --no-dev") for command in rendered)
    assert "uv run pip-audit --requirement .audit-requirements.txt" in rendered
    assert "uv build" in rendered


def test_run_checks_stops_at_the_first_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    commands: tuple[tuple[str, ...], ...] = (("first",), ("failing",), ("never",))
    calls: list[tuple[str, ...]] = []

    def fake_run(command: Sequence[str], *, check: bool) -> None:
        calls.append(tuple(command))
        assert check is True
        if command[0] == "failing":
            raise subprocess.CalledProcessError(1, command)

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(subprocess.CalledProcessError):
        run_checks(commands)

    assert calls == [("first",), ("failing",)]
