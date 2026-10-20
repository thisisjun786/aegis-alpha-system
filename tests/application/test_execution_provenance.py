"""Behavioral source coverage: simulate revisions without editing installed code."""

from __future__ import annotations

from pathlib import Path

import pytest

from aegis_alpha.application import backtest_prepare
from aegis_alpha.application.backtest_prepare import calculation_identity, research_source_identity


@pytest.mark.parametrize(
    "relative",
    [
        "application/backtest_cli.py",
        "application/research_run.py",
        "application/run_backtest.py",
        "application/run_research.py",
        "application/run_staging.py",
        "storage/publication.py",
        "storage/runs.py",
        "data/serialization.py",
        "engine/execution.py",
    ],
)
def test_execution_identity_detects_result_owners(
    relative: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    before = calculation_identity()
    preparation = research_source_identity()
    original = Path.read_bytes

    def revision(path: Path) -> bytes:
        raw = original(path)
        return raw + b"\n# synthetic revision\n" if path.as_posix().endswith(relative) else raw

    monkeypatch.setattr(Path, "read_bytes", revision)
    after = calculation_identity()
    assert after != before, f"execution provenance omitted {relative}"
    assert after["execution_source_hash"] != before["execution_source_hash"]
    assert calculation_identity() == after
    if relative in {"application/backtest_cli.py", "storage/publication.py", "storage/runs.py"}:
        # The old, narrower digests keep their meaning; only the v2 field covers these.
        assert after["calculation_source_hash"] == before["calculation_source_hash"]
        assert research_source_identity() == preparation


def test_unrelated_source_changes_do_not_rename_an_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    before = calculation_identity()
    original = Path.read_bytes

    def revision(path: Path) -> bytes:
        raw = original(path)
        return raw + b"\n# synthetic revision\n" if path.name == "provider_cli.py" else raw

    monkeypatch.setattr(Path, "read_bytes", revision)
    assert calculation_identity() == before


def test_missing_execution_source_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    original = Path.read_bytes

    def missing(path: Path) -> bytes:
        if path.name == "backtest_cli.py":
            raise FileNotFoundError("synthetic missing source")
        return original(path)

    monkeypatch.setattr(Path, "read_bytes", missing)
    with pytest.raises(FileNotFoundError, match="synthetic missing source"):
        calculation_identity()


def test_execution_source_format_has_a_frozen_digest(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(Path, "read_bytes", lambda _: b"synthetic code")
    assert (
        backtest_prepare._source_hash(  # noqa: SLF001 -- frozen hash format vector
            ("aegis_alpha.application.backtest_cli", "aegis_alpha.storage.publication"),
            "aas-execution-sources-v1",
        )
        == "b96227787caa80cae26fc103fd1bd576f9213c5fc746a6bdf1f6cf6df161b5b5"
    )
