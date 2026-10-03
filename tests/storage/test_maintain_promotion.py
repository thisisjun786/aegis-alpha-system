"""Maintenance promotion: routes, templates and the fields a maintenance spec advances.

Sources are synthetic EODHD daily-bar tables (``tests.storage.promotion_support``) under
a test route; every symbol and value is made up.
"""

# ruff: noqa: PLR2004 -- synthetic counts are the expected values
from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import date
from pathlib import Path
from typing import cast

import pytest

from aegis_alpha.storage import maintain_promotion
from aegis_alpha.storage.maintain_promotion import Route, maintenance_spec, promote_datasets
from aegis_alpha.storage.promotion.engine import dataset_head, promote
from aegis_alpha.storage.workspace import Workspace, initialize, open_workspace
from tests.storage.promotion_support import add_source, at, bar, register_symbols, spec

DAY1, DAY2, DAY3 = date(2026, 9, 14), date(2026, 9, 15), date(2026, 9, 16)
KR = "(provider_symbol LIKE '%.KO' OR provider_symbol LIKE '%.KQ')"
ROUTE = Route("prices.kr.eodhd", "eodhd.bars@1", "synthetic-kr-bars-", "bars", KR)


@pytest.fixture
def ws(tmp_path: Path) -> Iterator[Workspace]:
    root = tmp_path / "aas"
    initialize(root)
    with open_workspace(root, writable=True, strategy_write=True) as workspace:
        yield workspace


def _datasets(report: dict[str, object]) -> dict[tuple[str, str], dict[str, object]]:
    return {
        (str(item["dataset_id"]), str(item["mapper"])): item
        for item in cast("list[dict[str, object]]", report["datasets"])
    }


def test_new_sources_continue_the_chain_from_the_template(ws: Workspace) -> None:
    first = add_source(ws, [bar("AAA.KO", DAY1, 100.0, retrieved=at("2026-09-14T08:00:00"))],
                       tag="day1", linked=at("2026-09-14T09:00:00"))  # fmt: skip
    identity = register_symbols(ws, first["source_id"])
    template = spec([first], identity, partition={"from": DAY1.isoformat(), "to": DAY2.isoformat()})
    head = str(promote(ws, *template, apply=True)["generation_id"])
    second = add_source(
        ws,
        [bar("AAA.KO", DAY2, 101.0, retrieved=at("2026-09-15T08:00:00")),
         bar("BBB.KQ", DAY2, 50.0, retrieved=at("2026-09-15T08:00:00"))],
        tag="day2", linked=at("2026-09-15T09:00:00"),
    )  # fmt: skip
    add_source(ws, [bar("AAA.KO", DAY3, 1.0, retrieved=at("2026-09-16T08:00:00")),
                    bar("ZZZ.US", DAY3, 2.0, retrieved=at("2026-09-16T08:00:00"))],
               tag="mixed", linked=at("2026-09-16T09:00:00"))  # fmt: skip
    add_source(ws, [], tag="empty", linked=at("2026-09-16T09:30:00"))
    report = promote_datasets(ws, apply=True, now_us=1, routes=[ROUTE])
    run = _datasets(report)[("prices.kr.eodhd", "eodhd.bars@1")]
    assert run["status"] == "advanced"
    assert run["skipped"] == {"done": 0, "empty": 1, "mixed": 1}
    sources = cast("list[dict[str, object]]", run["sources"])
    # The operator's own source is planned once more and found unchanged; the new one is
    # the head's child over its own dates.
    assert [(item["source_id"], item["status"]) for item in sources] == [
        (first["source_id"], "unchanged"),
        (second["source_id"], "promoted"),
    ]
    step = cast("list[dict[str, object]]", sources[1]["steps"])[0]
    assert step["partition"] == {"from": DAY2.isoformat(), "to": DAY3.isoformat()}
    assert step["operations"] == {"ASSERT": 2}
    row = ws.state.execute(
        "SELECT parent_generation_id, transform_hash FROM dataset_versions WHERE generation_id=?",
        (dataset_head(ws, "prices.kr.eodhd"),),
    ).fetchone()
    assert row[0] == head
    advanced = json.loads((ws.paths.raw / row[1][:2] / row[1]).read_bytes())
    original = json.loads(template[0])
    # Only the maintenance fields differ from the template.
    changed = {key for key in original if advanced[key] != original[key]}
    assert changed == {"target", "sources", "partition"}
    assert advanced["tombstone_policy"] == {"mode": "never"}
    # Both sources are done: the next pass plans nothing and publishes nothing.
    again = _datasets(promote_datasets(ws, apply=True, now_us=2, routes=[ROUTE]))
    repeated = again[("prices.kr.eodhd", "eodhd.bars@1")]
    assert (repeated["status"], repeated["sources"]) == ("current", [])
    assert cast("dict[str, int]", repeated["skipped"])["done"] == 2


def test_a_route_without_a_head_or_template_reports_and_starts_nothing(ws: Workspace) -> None:
    pin = add_source(ws, [bar("AAA.KO", DAY1, 100.0, retrieved=at("2026-09-14T08:00:00"))],
                     tag="day1", linked=at("2026-09-14T09:00:00"))  # fmt: skip
    adjusted = Route("prices.kr.eodhd", "eodhd.bars_adjusted@1", "synthetic-kr-bars-", "bars", KR)
    report = _datasets(promote_datasets(ws, apply=True, now_us=1, routes=[ROUTE]))
    assert report[("prices.kr.eodhd", "eodhd.bars@1")]["status"] == "no_head"
    identity = register_symbols(ws, pin["source_id"])
    promote(ws, *spec([pin], identity), apply=True)
    report = _datasets(promote_datasets(ws, apply=False, now_us=1, routes=[adjusted]))
    assert report[("prices.kr.eodhd", "eodhd.bars_adjusted@1")]["status"] == "no_template"
    assert ws.state.execute("SELECT count(*) FROM dataset_versions").fetchone()[0] == 1


def test_generation_pins_advance_and_the_identity_replaces_only_a_pinned_snapshot(
    ws: Workspace,
) -> None:
    pin = add_source(ws, [bar("AAA.KO", DAY1, 100.0, retrieved=at("2026-09-14T08:00:00"))],
                     tag="day1", linked=at("2026-09-14T09:00:00"))  # fmt: skip
    identity = register_symbols(ws, pin["source_id"])
    promote(ws, *spec([pin], identity), apply=True)
    current = maintain_promotion.head_pin(ws, "prices.kr.eodhd")
    assert current is not None
    stale = {**current, "version": "0", "generation_id": "prm-old"}
    template = json.loads(spec([pin], identity)[0])
    template["quality_rules"] = [{"rule": "synthetic@1", "args": {"reference": stale}}]
    template["tombstone_policy"] = {"mode": "absent_in_full_snapshot"}
    other = {"snapshot_id": "maintain-x", "content_hash": "0" * 64}
    raw = maintenance_spec(ws, template, parent="prm-head", pin=pin,
                           step=maintain_promotion.Step(None), identity=other)  # fmt: skip
    advanced = json.loads(raw)
    assert advanced["quality_rules"][0]["args"]["reference"] == current
    assert advanced["identity_snapshot"] == other
    assert advanced["target"]["parent"] == "prm-head"
    assert advanced["tombstone_policy"] == {"mode": "never"}
    template["identity_snapshot"] = None
    unpinned = json.loads(maintenance_spec(ws, template, parent=None, pin=pin,
                                           step=maintain_promotion.Step(None),
                                           identity=other))  # fmt: skip
    assert unpinned["identity_snapshot"] is None
