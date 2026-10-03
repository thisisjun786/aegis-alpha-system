"""``aas data promote`` and ``aas data promotions`` through the installed entry point."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from datetime import date
from pathlib import Path
from typing import cast

from aegis_alpha.storage.workspace import initialize, open_workspace
from tests.storage.promotion_support import (
    add_bulk_source,
    add_source,
    at,
    bar,
    bulk_row,
    publish_calendar,
    register_symbols,
    spec,
    us,
)

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


def _tree(home: Path) -> dict[str, str]:
    """Every file under the installation with its content hash.

    A read-only SQLite reader may leave its shared-memory index and an empty WAL behind;
    neither holds data, so the index is skipped and an empty WAL counts as absent.
    """
    return {
        str(path.relative_to(home)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(home.rglob("*"))
        if path.is_file()
        and not path.name.endswith("-shm")
        and not (path.name.endswith("-wal") and path.stat().st_size == 0)
    }


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
    before = _tree(home)
    planned = _data("promote", "--spec", str(path), "--sha256", sha256, "--plan", home=home)
    assert planned["mode"] == "plan"
    assert planned["published"] is False
    assert planned["operations"] == {"ASSERT": 1}
    # No file appears, disappears or changes: state, market, its WAL and raw/ alike.
    assert _tree(home) == before
    assert _data("promotions", home=home) == {"promotions": []}
    applied = _data("promote", "--spec", str(path), "--sha256", sha256, home=home)
    assert applied["published"] is True
    (listed,) = cast("list[dict[str, object]]", _data("promotions", home=home)["promotions"])
    assert listed["phase"] == "COMPLETED"
    assert listed["generation_id"] == applied["generation_id"]
    assert listed["dataset_id"] == "prices.kr.eodhd"
    assert listed["row_count"] == 1


def test_kr_prices_plan_writes_nothing_and_apply_publishes(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    initialize(home)
    day, next_day = date(2025, 1, 2), date(2025, 1, 3)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        pin = add_source(
            workspace, [bar("AAA.KO", day, 100.0, retrieved=at("2025-01-10T00:00:00"))], tag="a"
        )
        register_symbols(workspace, pin["source_id"])
        add_bulk_source(
            workspace,
            [bulk_row("AAA", "KO", next_day, 101)],
            tag="b",
            linked=at("2025-01-20T00:00:00"),
        )
        publish_calendar(
            workspace,
            {
                session: (None, us(at(f"{session.isoformat()}T06:30:00")))
                for session in (day, next_day)
            },
        )
    command = (
        "kr-prices",
        "--identity-snapshot",
        "kr",
        "--lag-us",
        "0",
        "--history-lineage",
        "synthetic-kr-bars",
        "--bulk-lineage",
        "synthetic-kr-bulk",
    )
    before = _tree(home)
    planned = _data(*command, "--plan", home=home)
    assert planned["mode"] == "plan"
    assert [step["published"] for step in cast("list[dict[str, object]]", planned["steps"])] == [
        False,
        False,
    ]
    assert _tree(home) == before
    applied = _data(*command, home=home)
    steps = cast("list[dict[str, object]]", applied["steps"])
    assert [(step["kind"], step["published"]) for step in steps] == [
        ("history", True),
        ("bulk", True),
    ]
    assert steps[1]["flags"] == {"provider_reported_partial": 1}
