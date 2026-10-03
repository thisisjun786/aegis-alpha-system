"""``aas data promote`` and ``aas data promotions`` through the installed entry point."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import date
from pathlib import Path
from typing import cast

from aegis_alpha.storage.workspace import initialize, open_workspace
from tests.storage.promotion_support import add_source, at, bar, register_symbols, spec

_ROOT = Path(__file__).resolve().parents[2]


def _data(*args: str, home: Path) -> dict[str, object]:
    result = subprocess.run(  # noqa: S603 -- fixed interpreter, temporary synthetic home
        [sys.executable, "-m", "aegis_alpha", "data", *args],
        env={**os.environ, "AAS_HOME": str(home), "PYTHONPATH": str(_ROOT / "src")},
        cwd=home.parent,
        text=True,
        capture_output=True,
        check=False,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def test_promote_plan_writes_nothing_and_apply_publishes(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        pin = add_source(
            workspace,
            [bar("AAA.KO", date(2025, 1, 2), 100.0, retrieved=at("2025-01-10T00:00:00"))],
            tag="a",
        )
        identity = register_symbols(workspace, pin["source_id"])
    raw, sha256 = spec([pin], identity)
    path = tmp_path / "spec.json"
    path.write_bytes(raw)
    state = (home / "state.sqlite3").read_bytes()
    planned = _data("promote", "--spec", str(path), "--sha256", sha256, "--plan", home=home)
    assert planned["mode"] == "plan"
    assert planned["published"] is False
    assert planned["operations"] == {"ASSERT": 1}
    assert (home / "state.sqlite3").read_bytes() == state
    assert _data("promotions", home=home) == {"promotions": []}
    applied = _data("promote", "--spec", str(path), "--sha256", sha256, home=home)
    assert applied["published"] is True
    (listed,) = cast("list[dict[str, object]]", _data("promotions", home=home)["promotions"])
    assert listed["phase"] == "COMPLETED"
    assert listed["generation_id"] == applied["generation_id"]
    assert listed["dataset_id"] == "prices.kr.eodhd"
    assert listed["row_count"] == 1
