"""``aas db compact``: rebuild into a new root, verify there, never touch the original."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pyarrow as pa
import pytest

from aegis_alpha.storage import compaction
from aegis_alpha.storage.backup import backup
from aegis_alpha.storage.compaction import compact
from aegis_alpha.storage.paths import read_json
from aegis_alpha.storage.promotion.engine import promote
from aegis_alpha.storage.runs import RunResult, commit_run, open_run, read_run
from aegis_alpha.storage.source_library import retired_sources
from aegis_alpha.storage.source_retirement import retire_sources
from aegis_alpha.storage.verification import verify_workspace
from aegis_alpha.storage.workspace import initialize, open_workspace
from tests.storage.promotion_support import add_source, at, bar, register_symbols
from tests.storage.promotion_support import spec as promotion_spec
from tests.storage.retirement_support import ROWS, commit, group, other_device, spec
from tests.storage.test_runs import BUDGET as RUN_BUDGET
from tests.storage.test_runs import RESULT, intent, prepared

if TYPE_CHECKING:
    from aegis_alpha.compute_resources import ComputeBudget
    from aegis_alpha.storage.workspace import Workspace

_ROOT = Path(__file__).resolve().parents[2]
_WIDE = pa.schema([("symbol", pa.string()), ("payload", pa.string())])


def _files(home: Path) -> dict[str, str]:
    """Every file under the installation with its content hash (SQLite side files aside)."""
    return {
        str(path.relative_to(home)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(home.rglob("*"))
        if path.is_file() and not path.name.endswith(("-shm", "-wal"))
    }


def _installation(home: Path) -> None:
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        commit(workspace, "export")
    (home / "secrets" / "provider.token").write_text("synthetic")
    (home / "secrets" / "provider.token").chmod(0o600)


def test_compaction_reclaims_retired_space_and_verifies_the_same(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    home = tmp_path / "aas"
    _installation(home)
    # Hex digests barely compress, so the dropped copy leaves megabytes of free blocks.
    wide = [
        (f"S{number:06d}", hashlib.sha256(str(number).encode()).hexdigest()[:40])
        for number in range(60_000)
    ]
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        bulky = commit(workspace, "bulky", wide, schema=_WIDE)
        copy = commit(workspace, "copy", wide[::-1], schema=_WIDE)
    backup(home, tmp_path / "backup")
    other_device(monkeypatch, tmp_path / "backup")
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        applied = retire_sources(
            workspace,
            spec(group([copy], [bulky], columns=["symbol", "payload"])),
            backup_root=tmp_path / "backup",
            apply=True,
        )
        assert applied["retired"] == [copy]
        workspace.market.execute("CHECKPOINT")
        before = verify_workspace(workspace)
    original = _files(home)
    report = compact(home, tmp_path / "compacted")
    assert report["compacted"] is True
    sizes_before = cast("dict[str, int]", report["bytes_before"])
    sizes_after = cast("dict[str, int]", report["bytes_after"])
    assert sizes_after["market"] < sizes_before["market"]
    assert report["verification"] == before
    assert _files(home) == original
    target = tmp_path / "compacted"
    assert read_json(target / "installation.json")["phase"] == "ready"
    paths = cast("dict[str, str]", read_json(target / "runtime.json")["paths"])
    assert paths["market"] == "market.duckdb"
    assert (target / "secrets" / "provider.token").read_text() == "synthetic"
    with open_workspace(target) as rebuilt:
        assert verify_workspace(rebuilt) == before
        assert set(retired_sources(rebuilt)) == {copy}
        assert read_json(target / "installation.json")["installation_id"] == (
            rebuilt.installation_id
        )


def test_compaction_copies_rows_that_reference_other_rows(tmp_path: Path) -> None:
    """Promoted generation chains and run results hold foreign keys; parents land first."""
    fx = prepared(tmp_path / "source")
    home = fx.home
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        identity = register_symbols(workspace, commit(workspace, "identity"))
        parent = None
        for number in range(3):
            day = date(2025, 1, 2 + number)
            pin = add_source(
                workspace,
                [bar("AAA.KO", day, 100.0 + number, retrieved=at("2025-01-10T00:00:00"))],
                tag=f"p{number}",
            )
            raw, digest = promotion_spec([pin], identity, parent=parent)
            parent = str(promote(workspace, raw, digest, apply=True)["generation_id"])
        handle = open_run(workspace, intent(fx, "run-compacted"))
        commit_run(workspace, handle, RunResult(RESULT), budget=RUN_BUDGET)
        before = verify_workspace(workspace, budget=RUN_BUDGET)
        run = read_run(workspace, "run-compacted", budget=RUN_BUDGET)
        counts = {
            table: workspace.market.execute(f"SELECT count(*) FROM {table}").fetchone()  # noqa: S608 -- fixed names
            for table in ("market_generations", "prices", "result_commits")
        }
    assert counts["market_generations"] == (3,)
    report = compact(home, tmp_path / "compacted", budget=RUN_BUDGET)
    assert report["verification"] == before
    with open_workspace(tmp_path / "compacted") as rebuilt:
        assert read_run(rebuilt, "run-compacted", budget=RUN_BUDGET) == run
        for table, count in counts.items():
            assert rebuilt.market.execute(f"SELECT count(*) FROM {table}").fetchone() == count  # noqa: S608 -- fixed names


def test_interrupted_compaction_preserves_original(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    home = tmp_path / "aas"
    _installation(home)
    with open_workspace(home) as workspace:
        before = verify_workspace(workspace)
    original = _files(home)
    target = tmp_path / "compacted"
    with monkeypatch.context() as patched:

        def stop(*_args: object) -> None:
            raise RuntimeError("stopped while copying raw/")

        patched.setattr(compaction, "copy_tree", stop)
        with pytest.raises(RuntimeError, match="stopped"):
            compact(home, target)
    assert _files(home) == original
    with open_workspace(home) as workspace:
        assert verify_workspace(workspace) == before
    assert read_json(target / "installation.json")["phase"] == "restore-incomplete"
    with pytest.raises(ValueError, match="incomplete"), open_workspace(target):
        pass
    with pytest.raises(ValueError, match="new nonexistent root"):
        compact(home, target)
    report = compact(home, tmp_path / "second")
    assert report["verification"] == before


def test_compaction_root_must_be_new_and_outside_the_installation(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    _installation(home)
    for inside in (home / "raw" / "new", home / "runs" / "new", home):
        with pytest.raises(ValueError, match=r"nonexistent|outside"):
            compact(home, inside)
    (tmp_path / "taken").mkdir()
    with pytest.raises(ValueError, match="nonexistent"):
        compact(home, tmp_path / "taken")


def test_compact_command_reports_the_new_root(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    _installation(home)
    result = subprocess.run(  # noqa: S603 -- fixed interpreter, temporary synthetic home
        [sys.executable, "-m", "aegis_alpha", "db", "compact", "--to", str(tmp_path / "new")],
        env={**os.environ, "AAS_HOME": str(home), "PYTHONPATH": str(_ROOT / "src")},
        cwd=tmp_path,
        text=True,
        capture_output=True,
        check=False,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    assert report["home"] == str(tmp_path / "new")
    assert report["original_unchanged"] is True
    assert (
        len(ROWS)
        == cast("dict[str, dict[str, int]]", report["verification"])["source_library"]["rows"]
    )


@pytest.mark.parametrize("deep", [False, True])
def test_compaction_rehashes_the_rewritten_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, deep: bool
) -> None:
    """The original is verified as asked; the rewritten root is always verified deep."""
    home = tmp_path / "aas"
    _installation(home)
    modes: list[bool] = []

    def recorded(
        workspace: Workspace, *, budget: ComputeBudget | None = None, deep: bool = False
    ) -> dict[str, object]:
        modes.append(deep)
        return verify_workspace(workspace, budget=budget, deep=deep)

    monkeypatch.setattr(compaction, "verify_workspace", recorded)
    assert compact(home, tmp_path / "compacted", deep=deep)["compacted"] is True
    assert modes == [deep, True]
