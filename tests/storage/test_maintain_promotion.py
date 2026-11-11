"""Maintenance promotion: routes, templates and the fields a maintenance spec advances.

Sources are synthetic EODHD daily-bar tables (``tests.storage.promotion_support``) under
a test route; every symbol and value is made up.
"""

# ruff: noqa: PLR2004 -- synthetic counts are the expected values
from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest

from aegis_alpha.storage import kr_prices, maintain_promotion
from aegis_alpha.storage.dataset_catalog import CATALOG
from aegis_alpha.storage.identity import mint_instrument
from aegis_alpha.storage.maintain_promotion import (
    KeyMembers,
    Route,
    maintenance_spec,
    promote_datasets,
)
from aegis_alpha.storage.market import publish_generation
from aegis_alpha.storage.promotion.engine import dataset_head, promote
from aegis_alpha.storage.state import atomic
from aegis_alpha.storage.workspace import Workspace, initialize, open_workspace
from tests.storage.promotion_support import (
    PARTIAL,
    add_bulk_source,
    add_source,
    at,
    bar,
    bulk_row,
    full_snapshot,
    publish_calendar,
    register_symbols,
    spec,
    us,
)

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


# --- KR daily bulk: the first template derived from the history, and the value check ----

H1, B1, B2 = date(2026, 9, 4), date(2026, 9, 7), date(2026, 9, 8)
LAG = 3_600_000_000


def _kr_route(dataset: str, name: str) -> Route:
    """The shipped route with the synthetic bulk lineage's prefix."""
    (found,) = [
        item
        for item in maintain_promotion.ROUTES
        if (item.dataset_id, item.mapper) == (dataset, name)
    ]
    return replace(found, prefix="synthetic-kr-bulk-")


BULK = _kr_route(kr_prices.DATASET, "eodhd.bulk_quarantine@1")
BULK_REF = _kr_route(kr_prices.REFERENCE_DATASET, "eodhd.bulk_quarantine_adjusted@1")
KR_CHAINS = pytest.mark.parametrize(
    ("dataset", "route"),
    [(kr_prices.DATASET, BULK), (kr_prices.REFERENCE_DATASET, BULK_REF)],
    ids=["canonical", "reference"],
)


def _kr_step(kind: str, day: date, name: str, pin: dict[str, str]) -> kr_prices.Step:
    if kind == "history":
        return kr_prices.Step(kind, date(day.year, 1, 1), date(day.year + 1, 1, 1), name, (pin,))
    return kr_prices.Step(kind, day, day + timedelta(days=1), name, (pin,))


def _kr_promote(ws: Workspace, dataset: str, step: kr_prices.Step,
                pins: dict[str, dict[str, str]]) -> bytes:  # fmt: skip
    """Promote one ``aas data kr-prices`` step as an operator would; return its spec."""
    raw = kr_prices.step_spec(step, dataset=dataset, parent=dataset_head(ws, dataset),
                              identity=pins["identity"], calendar=pins["calendar"],
                              lag_us=LAG)  # fmt: skip
    assert promote(ws, raw, hashlib.sha256(raw).hexdigest(), apply=True)["published"] is True
    return raw


def _kr_history(ws: Workspace, dataset: str) -> tuple[dict[str, dict[str, str]], bytes]:
    """A chain the history backfill alone built, as ``aas data kr-prices`` writes it."""
    retrieved = at("2026-09-04T08:00:00")
    history = add_source(
        ws,
        [bar("AAA.KO", H1, 100.0, retrieved=retrieved),
         bar("BBB.KQ", H1, 50.0, retrieved=retrieved),
         bar("CCC.KO", H1, 7.0, retrieved=retrieved)],
        tag="history", linked=at("2026-09-04T09:00:00"),
    )  # fmt: skip
    sessions = {day: (us(at(f"{day}T00:00:00")), us(at(f"{day}T06:30:00")))
                for day in (H1, B1, B2)}  # fmt: skip
    pins = {"identity": register_symbols(ws, history["source_id"]),
            "calendar": publish_calendar(ws, sessions)}  # fmt: skip
    name = "eodhd.bars_adjusted@1" if dataset == kr_prices.REFERENCE_DATASET else "eodhd.bars@1"
    return pins, _kr_promote(ws, dataset, _kr_step("history", H1, name, history), pins)


def _bulk_day(ws: Workspace, day: date, *extra: tuple[str, str], tag: str) -> dict[str, str]:
    rows = [bulk_row("AAA", "KO", day, 101), bulk_row("BBB", "KQ", day, 51), *extra]
    return add_bulk_source(ws, rows, tag=tag, linked=at(f"{day}T09:00:00"))


def _retained(ws: Workspace, generation: str | None) -> tuple[str, str, str, bytes]:
    """(mapper, parent, transform hash, retained spec) of one committed generation."""
    name, parent, digest = ws.state.execute(
        "SELECT normalizer_version, parent_generation_id, transform_hash FROM dataset_versions "
        "WHERE generation_id=?",
        (generation,),
    ).fetchone()
    return name, parent, digest, (ws.paths.raw / digest[:2] / digest).read_bytes()


@KR_CHAINS
def test_a_warned_kr_bulk_day_continues_a_chain_only_the_history_built(
    ws: Workspace, dataset: str, route: Route
) -> None:
    _, history = _kr_history(ws, dataset)
    parent = dataset_head(ws, dataset)
    pin = _bulk_day(ws, B1, tag="b1")
    report = promote_datasets(ws, apply=True, now_us=1, routes=[route])
    run = _datasets(report)[(dataset, route.mapper)]
    assert (run["status"], run["derived_from"]) == ("advanced", route.derive_from)
    # The template is the history's spec, which the derived one came from.
    assert run["template_spec_sha256"] == hashlib.sha256(history).hexdigest()
    (source,) = cast("list[dict[str, object]]", run["sources"])
    assert (source["source_id"], source["status"]) == (pin["source_id"], "promoted")
    (step,) = cast("list[dict[str, object]]", source["steps"])
    assert step["partition"] == {"from": B1.isoformat(), "to": B2.isoformat()}
    generation = dataset_head(ws, dataset)
    name, linked, _, _ = _retained(ws, generation)
    assert (name, linked) == (route.mapper, parent)
    flags = ws.market.execute(
        "SELECT count(*) FROM quality_flags WHERE generation_id=? AND flag=?",
        [generation, PARTIAL],
    ).fetchone()
    assert flags == (2,)  # both warned rows, each a new revision
    # Two rows against the three of the complete history date.
    check = cast("dict[str, object]", step["partition_row_count"])
    assert check["result"] == "below_reference"
    recorded = ws.state.execute(
        "SELECT result FROM quality_checks WHERE dataset_id=? AND rule_id='partition_row_count'",
        (dataset,),
    ).fetchall()
    assert [tuple(row) for row in recorded] == [("below_reference",)]
    versions = tuple(ws.state.execute("SELECT count(*) FROM dataset_versions").fetchone())
    again = _datasets(promote_datasets(ws, apply=True, now_us=2, routes=[route]))
    repeated = again[(dataset, route.mapper)]
    assert (repeated["status"], repeated["sources"]) == ("current", [])
    assert cast("dict[str, int]", repeated["skipped"])["done"] == 1
    assert tuple(ws.state.execute("SELECT count(*) FROM dataset_versions").fetchone()) == versions


@KR_CHAINS
def test_a_derived_bulk_spec_is_the_one_kr_prices_writes_for_that_day(
    ws: Workspace, dataset: str, route: Route
) -> None:
    pins, _ = _kr_history(ws, dataset)
    parent = dataset_head(ws, dataset)
    pin = _bulk_day(ws, B1, tag="b1")
    promote_datasets(ws, apply=True, now_us=1, routes=[route])
    _, _, _, retained = _retained(ws, dataset_head(ws, dataset))
    expected = kr_prices.step_spec(_kr_step("bulk", B1, route.mapper, pin), dataset=dataset,
                                   parent=parent, identity=pins["identity"],
                                   calendar=pins["calendar"], lag_us=LAG)  # fmt: skip
    derived, written = json.loads(retained), json.loads(expected)
    for key in ("mapper", "decimal_rule", "time_rules", "quality_rules", "identity_snapshot"):
        assert derived[key] == written[key], key
    assert derived["mapper"]["args"]["currencies"] == {"KO": "KRW", "KQ": "KRW"}
    # Every other field the maintenance spec advances lands where the backfill puts it.
    assert retained == expected


@KR_CHAINS
def test_a_chain_with_a_bulk_generation_continues_from_it_without_deriving(
    ws: Workspace, dataset: str, route: Route
) -> None:
    pins, _ = _kr_history(ws, dataset)
    first = _bulk_day(ws, B1, tag="b1")
    bulk = _kr_promote(ws, dataset, _kr_step("bulk", B1, route.mapper, first), pins)
    second = _bulk_day(ws, B2, tag="b2")
    run = _datasets(promote_datasets(ws, apply=True, now_us=1, routes=[route]))[
        (dataset, route.mapper)
    ]
    assert run["derived_from"] is None
    assert run["template_spec_sha256"] == hashlib.sha256(bulk).hexdigest()
    sources = cast("list[dict[str, object]]", run["sources"])
    assert [(item["source_id"], item["status"]) for item in sources] == [
        (first["source_id"], "unchanged"),
        (second["source_id"], "promoted"),
    ]


# The manifest SHA-256 of one KR bulk day promoted with ``eodhd.bulk_quarantine@1`` after the
# history year, recorded on the code before ``eodhd.bulk_quarantine@2`` existed.
V1_BULK_MANIFEST = "63edbc92f9443f124918bddb5c16b15f48a76343e7eb0dca7918eac8fe921242"


def test_a_bulk_quarantine_v1_generation_keeps_its_manifest(ws: Workspace) -> None:
    pins, _ = _kr_history(ws, kr_prices.DATASET)
    held = _bulk_day(ws, B1, tag="b1")
    _kr_promote(ws, kr_prices.DATASET, _kr_step("bulk", B1, BULK.mapper, held), pins)
    generation = dataset_head(ws, kr_prices.DATASET)
    recorded = ws.state.execute(
        "SELECT normalizer_version, manifest_hash FROM dataset_versions WHERE generation_id=?",
        (generation,),
    ).fetchone()
    assert tuple(recorded) == ("eodhd.bulk_quarantine@1", V1_BULK_MANIFEST)


NEW_YORK = "America/New_York"
US_RULE = {"rule": "local_day_end@1", "basis": "record", "input": "session_date",
           "args": {"timezone": NEW_YORK}}  # fmt: skip
US_BULK = replace(
    next(item for item in maintain_promotion.ROUTES
         if (item.dataset_id, item.mapper) == ("prices.us.eodhd", "eodhd.bulk_quarantine@2")),
    prefix="synthetic-kr-bulk-",
)  # fmt: skip
IDENTITY = "explicit instrument identity is required"


def _us_chain(ws: Workspace) -> tuple[dict[str, str], tuple[bytes, str]]:
    """A ``prices.us.eodhd`` chain one US history day built, AAA.US and BBB.US registered."""
    retrieved = at("2026-09-09T08:00:00")
    daily = add_source(ws, [bar("AAA.US", B1, 10.0, retrieved=retrieved, currency="USD"),
                            bar("BBB.US", B1, 20.0, retrieved=retrieved, currency="USD")],
                       tag="us-daily", linked=retrieved)  # fmt: skip
    pin = register_symbols(ws, daily["source_id"], {"AAA.US": "900001", "BBB.US": "900002"},
                           name="us")  # fmt: skip
    floats = dict.fromkeys(("open", "high", "low", "close", "volume"), "float_shortest@1")
    history = spec([daily], pin, dataset="prices.us.eodhd", decimals=floats,
                   rules={"available_at_us": US_RULE, "revision_known_at_us": US_RULE},
                   partition={"from": B1.isoformat(), "to": B2.isoformat()},
                   mapper={"name": "eodhd.bars@1", "args": {"timezone": NEW_YORK}})  # fmt: skip
    assert promote(ws, *history, apply=True)["published"] is True
    return daily, history


def _us_row(code: str, close: float, reason: str = PARTIAL, **fields: object) -> tuple[str, str]:
    """A held US row on B2 whose JSON fields ``fields`` replace."""
    _, text = bulk_row(code, "US", B2, close)
    return reason, json.dumps({**json.loads(text), **fields}, sort_keys=True)


def _us_run(ws: Workspace, identity: dict[str, str] | None = None) -> dict[str, object]:
    report = promote_datasets(ws, apply=True, now_us=1, routes=[US_BULK], identity=identity)
    return _datasets(report)[("prices.us.eodhd", US_BULK.mapper)]


def _closes(ws: Workspace, generation: str | None) -> list[tuple[str, float]]:
    """(Norgate asset ID, close) of a generation's rows; the IDs are minted from the anchor."""
    anchors = {mint_instrument("norgate_assetid", token): token
               for token in ("900001", "900002", "900003")}  # fmt: skip
    rows = ws.market.execute(
        "SELECT instrument_id, close FROM prices WHERE generation_id=? ORDER BY close",
        [generation],
    ).fetchall()
    return [(anchors[str(instrument)], float(close)) for instrument, close in rows]


def test_a_warned_us_bulk_day_continues_the_us_chain_with_the_v2_mapper(ws: Workspace) -> None:
    _, history = _us_chain(ws)
    parent = dataset_head(ws, "prices.us.eodhd")
    warned = add_bulk_source(ws, [_us_row("AAA", 11), _us_row("BBB", 21)],
                             tag="us-warned", linked=at("2026-09-09T09:00:00"))  # fmt: skip
    run = _us_run(ws)
    assert (run["status"], run["derived_from"]) == ("advanced", "eodhd.bars@1")
    assert run["template_spec_sha256"] == history[1]
    (source,) = cast("list[dict[str, object]]", run["sources"])
    assert (source["source_id"], source["status"]) == (warned["source_id"], "promoted")
    generation = dataset_head(ws, "prices.us.eodhd")
    name, linked, _, retained = _retained(ws, generation)
    assert (name, linked) == ("eodhd.bulk_quarantine@2", parent)
    assert json.loads(retained)["mapper"]["args"] == {"timezone": NEW_YORK,
                                                      "currencies": {"US": "USD"}}  # fmt: skip
    rows = ws.market.execute(
        "SELECT currency, close FROM prices WHERE generation_id=? ORDER BY close", [generation]
    ).fetchall()
    assert [(currency, float(close)) for currency, close in rows] == [("USD", 11.0),
                                                                      ("USD", 21.0)]  # fmt: skip
    flags = ws.market.execute(
        "SELECT count(*) FROM quality_flags WHERE generation_id=? AND flag=?",
        [generation, PARTIAL],
    ).fetchone()
    assert flags == (2,)


def test_identity_held_us_rows_promote_by_the_reason_table_and_wait_for_their_identity(
    ws: Workspace, monkeypatch: pytest.MonkeyPatch
) -> None:
    daily, _ = _us_chain(ws)
    held = add_bulk_source(
        ws,
        [_us_row("AAA", 11, IDENTITY),
         _us_row("ZZZ", 31, IDENTITY),  # no registered identity yet
         _us_row("BBB", 21, IDENTITY, low=0),  # values fail the bar checks
         _us_row("BBB", 22, "provider identity or currency differs"),
         _us_row("BBB", 23, "inconsistent_ohlc")],
        tag="us-identity", linked=at("2026-09-09T09:30:00"),
    )  # fmt: skip
    # A table whose every row is held for a reason @2 does not map stays unmapped.
    add_bulk_source(ws, [_us_row("BBB", 24, "provider identity or currency differs")],
                    tag="us-differs", linked=at("2026-09-09T09:45:00"))  # fmt: skip
    run = _us_run(ws)
    assert cast("dict[str, int]", run["skipped"]) == {"done": 0}
    (source,) = cast("list[dict[str, object]]", run["sources"])
    assert (source["source_id"], source["status"]) == (
        held["source_id"],
        maintain_promotion.UNRESOLVED,
    )
    (step,) = cast("list[dict[str, object]]", source["steps"])
    assert step["source_outcomes"] == {
        f"mapped:{IDENTITY}": 2,
        f"value_check:{IDENTITY}": 1,
        "held:provider identity or currency differs": 1,
        "held:inconsistent_ohlc": 1,
    }
    assert step["unresolved_token_count"] == 1
    # Only the row the pinned snapshot resolves is promoted; nothing is made up for ZZZ.US.
    first = dataset_head(ws, "prices.us.eodhd")
    assert _closes(ws, first) == [("900001", 11.0)]
    # A clean response row held for its identity carries no partial-response flag.
    assert ws.market.execute(
        "SELECT count(*) FROM quality_flags WHERE generation_id=? AND flag=?", [first, PARTIAL]
    ).fetchone() == (0,)
    standing = maintain_promotion.standing_unresolved(ws)["prices.us.eodhd"]
    assert (standing["sources"], standing["unresolved_tokens"]) == (1, 1)
    shipped = [(dataset, "synthetic-kr-bulk-" if table == "quarantine" else prefix, table, accept)
               for dataset, prefix, table, accept in maintain_promotion.UNMAPPED
               if dataset == "prices.us.eodhd"]  # fmt: skip
    monkeypatch.setattr(maintain_promotion, "UNMAPPED", tuple(shipped))
    assert maintain_promotion.unmapped(ws) == {"prices.us.eodhd": 1}
    # Once ZZZ.US is registered, the waiting source is planned again and its row promoted.
    later = register_symbols(ws, daily["source_id"],
                             {"AAA.US": "900001", "BBB.US": "900002", "ZZZ.US": "900003"},
                             name="us-later")  # fmt: skip
    again = _us_run(ws, identity=later)
    (source,) = cast("list[dict[str, object]]", again["sources"])
    assert source["status"] == "promoted"
    assert _closes(ws, dataset_head(ws, "prices.us.eodhd")) == [("900003", 31.0)]


def test_rows_held_for_their_values_keep_no_valid_row_of_a_warned_us_table_out(
    ws: Workspace,
) -> None:
    _us_chain(ws)
    warned = add_bulk_source(
        ws,
        [_us_row("AAA", 11),
         _us_row("BBB", 21, "inconsistent_ohlc", high=20),
         _us_row("BBB", 21, "invalid_date", date="2026-9-8"),
         (PARTIAL, '{"code": "BBB", "exchange_short_name": "US", "close": '),
         _us_row("BBB", 21, PARTIAL, volume=-1)],
        tag="us-warned-mixed", linked=at("2026-09-09T09:00:00"),
    )  # fmt: skip
    run = _us_run(ws)
    assert cast("dict[str, int]", run["skipped"]) == {"done": 0}
    (source,) = cast("list[dict[str, object]]", run["sources"])
    assert (source["source_id"], source["status"]) == (warned["source_id"], "promoted")
    (step,) = cast("list[dict[str, object]]", source["steps"])
    assert step["source_outcomes"] == {
        f"mapped:{PARTIAL}": 1,
        f"value_check:{PARTIAL}": 2,
        "held:inconsistent_ohlc": 1,
        "held:invalid_date": 1,
    }
    assert _closes(ws, dataset_head(ws, "prices.us.eodhd")) == [("900001", 11.0)]


def test_a_chain_without_a_head_derives_nothing(ws: Workspace) -> None:
    _bulk_day(ws, B1, tag="b1")
    run = _datasets(promote_datasets(ws, apply=True, now_us=1, routes=[BULK]))[
        (kr_prices.DATASET, BULK.mapper)
    ]
    assert (run["status"], run["derived_from"]) == ("no_head", None)
    assert ws.state.execute("SELECT count(*) FROM dataset_versions").fetchone()[0] == 0


def _held(fields: dict[str, object]) -> tuple[str, str]:
    """A warned KO row of CCC on B2 whose JSON fields ``fields`` replace."""
    reason, text = bulk_row("CCC", "KO", B2, 7)
    return reason, json.dumps({**json.loads(text), **fields}, sort_keys=True)


_INVALID = {
    "zero_price": _held({"low": 0}),
    "low_above_high": _held({"open": 7, "close": 7, "low": 7, "high": 6}),
    "high_below_close": _held({"close": 8}),
    "negative_volume": _held({"volume": -1}),
    "text_number": _held({"close": "7"}),
    "null_value": _held({"adjusted_close": None}),
    "broken_json": (PARTIAL, '{"code": "CCC", "exchange_short_name": "KO", "close": '),
    "bad_date": _held({"date": "2026-09-31"}),
}


@pytest.mark.parametrize("invalid", list(_INVALID.values()), ids=list(_INVALID))
@KR_CHAINS
def test_one_invalid_value_keeps_its_whole_bulk_table_out(
    ws: Workspace, dataset: str, route: Route, invalid: tuple[str, str]
) -> None:
    _kr_history(ws, dataset)
    clean = _bulk_day(ws, B1, tag="clean")
    held = _bulk_day(ws, B2, invalid, tag="invalid")
    run = _datasets(promote_datasets(ws, apply=True, now_us=1, routes=[route]))[
        (dataset, route.mapper)
    ]
    assert run["skipped"] == {"done": 0, "value_check": 1}
    sources = cast("list[dict[str, object]]", run["sources"])
    assert [(item["source_id"], item["status"]) for item in sources] == [
        (clean["source_id"], "promoted")
    ]
    # Nothing of the held table reached the chain, not even its valid rows.
    days = ws.market.execute("SELECT DISTINCT session_date FROM prices ORDER BY 1").fetchall()
    assert days == [(H1,), (B1,)]
    recorded = ws.state.execute(
        "SELECT count(*) FROM quality_checks WHERE rule_id='maintain_source' AND reason LIKE ?",
        (f"%{held['source_id']}%",),
    ).fetchone()
    assert recorded[0] == 0


def test_the_kr_check_judges_only_accepted_rows_and_leaves_other_holds_mixed(
    ws: Workspace,
) -> None:
    # A row the import held for its own failing values is not checked as a partial row: the
    # @1 route cannot leave it, so the table stays out as mixed, not as a failed value check.
    _kr_history(ws, kr_prices.DATASET)
    _, text = _held({"low": 0})
    _bulk_day(ws, B2, ("invalid_price_or_volume", text), tag="own-reason")
    run = _datasets(promote_datasets(ws, apply=True, now_us=1, routes=[BULK]))[
        (kr_prices.DATASET, BULK.mapper)
    ]
    assert (run["skipped"], run["sources"]) == ({"done": 0, "mixed": 1}, [])


def _instant(moment: datetime) -> int:
    return (moment - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(microseconds=1)


def test_a_scoped_key_covers_every_instant_of_its_dates_and_nothing_two_days_on() -> None:
    # A one-day partition of 2026-09-16. Mappers may resolve at a row's own instant: a KIND
    # list retrieved at 23:30 in Seoul (14:30 UTC) or a US row at 23:30 in New York
    # (03:30 UTC the next day). A short claim starting just before either must change the
    # source's scoped key; one starting two New York days later must not.
    day = date(2026, 9, 16)
    hour = 3_600_000_000
    base: list[tuple[str, str, int, int | None]] = [("AAA.KO", "ins-a", -(2**63), None)]

    def scoped(*extra: tuple[str, str, int, int | None]) -> str:
        return KeyMembers("s", sorted([*base, *extra])).scoped_sha256(day, day + timedelta(days=1))

    unchanged = scoped()
    seoul = _instant(datetime(2026, 9, 16, 14, 15, tzinfo=UTC))
    new_york = _instant(datetime(2026, 9, 17, 3, 15, tzinfo=UTC))
    later = _instant(datetime(2026, 9, 18, 4, tzinfo=UTC))
    assert scoped(("NEW.KO", "ins-b", seoul, seoul + 24 * hour)) != unchanged
    assert scoped(("NEW.US", "ins-c", new_york, new_york + 24 * hour)) != unchanged
    assert scoped(("LATE.US", "ins-d", later, later + 24 * hour)) == unchanged


def test_a_scoped_key_does_not_depend_on_the_order_of_its_rows() -> None:
    day = date(2026, 9, 16)
    hour = 3_600_000_000
    start = _instant(datetime(2026, 9, 16, 4, tzinfo=UTC))
    rows: list[tuple[str, str, int, int | None]] = [
        ("AAA.US", "ins-a", -(2**63), None),
        ("BBB.US", "ins-b", -(2**63), start),
        ("CCC.US", "ins-c", start, start + 24 * hour),
        ("DDD.US", "ins-d", start - 24 * hour, start),
    ]

    def scoped(members: list[tuple[str, str, int, int | None]]) -> str:
        return KeyMembers("s", members).scoped_sha256(day, day + timedelta(days=1))

    assert scoped(rows) == scoped(list(reversed(rows))) == scoped(rows[1:] + rows[:1])


# A fixture for the frozen key digests: a long unbounded row, a long bounded row, a short
# row inside the one-day window of 2026-09-16, a short row outside it, and a long row with
# a start and no end; in the engine's order (token, instrument, from, to).
_UNBOUNDED = -(2**63)
_FROZEN: list[tuple[str, str, int, int | None]] = [
    ("AAA.US", "ins-a", _UNBOUNDED, None),
    ("BBB.US", "ins-b", _UNBOUNDED, 1788926400000000),  # 2026-09-09T04:00Z
    ("CCC.US", "ins-c", 1789531200000000, 1789617600000000),  # 2026-09-16 New York day
    ("DDD.US", "ins-d", 1789704000000000, 1789790400000000),  # 2026-09-18 New York day
    ("EEE.US", "ins-e", 1788235200000000, None),  # from 2026-09-01T04:00Z
]


def test_the_key_digests_are_frozen() -> None:
    # aas-maintain-key-scope-v1: sha256 of canonical JSON
    # ["aas-maintain-key-scope-v1", sha256(canonical(sorted long rows)), sorted inside rows];
    # the whole key: sha256 of canonical JSON of every row in the engine's order. The long
    # rows are given in reverse so the scoped digest cannot depend on the caller's order.
    scoped = KeyMembers("s", list(reversed(_FROZEN))).scoped_sha256(date(2026, 9, 16),
                                                                    date(2026, 9, 17))  # fmt: skip
    assert scoped == "8ce88adbad30b77cd720ed8bbeef836afef7fbcb41b7b237e23445954a9c9cd1"
    assert KeyMembers("s", _FROZEN).sha256() == (
        "dc93aa5cb1c7124afe9cd864d8b171151e1b5fe26f4437d048971187d8c53cd6"
    )


# Tuesday 2026-07-28, 12:00 in Seoul: that day's XKRX session is still open, so the latest
# session a run can expect is Monday 07-27.
JULY_RUN = datetime(2026, 7, 28, 3, tzinfo=UTC)
_FRESHNESS = ("data_through", "expected_through", "lag_sessions", "lag_unit", "stale_after",
              "freshness", "stale_reasons")  # fmt: skip


def _bars_downloaded_late(ws: Workspace, dataset: str) -> None:
    """A chain whose only bar is for 2026-07-01 but whose source was collected on 07-27."""
    pin = add_source(ws, [bar("AAA.KO", date(2026, 7, 1), 100.0,
                              retrieved=at("2026-07-27T08:00:00"))],
                     tag=dataset, linked=at("2026-07-27T09:00:00"))  # fmt: skip
    promote(ws, *spec([pin], register_symbols(ws, pin["source_id"]), dataset=dataset), apply=True)


def _heads(ws: Workspace, now: datetime) -> dict[str, dict[str, object]]:
    return {str(item["dataset_id"]): item for item in maintain_promotion.dataset_heads(ws, now=now)}


def test_a_recent_watermark_over_weeks_old_data_is_stale_by_its_session_lag(
    ws: Workspace,
) -> None:
    _bars_downloaded_late(ws, "prices.kr.eodhd")
    head = _heads(ws, JULY_RUN)["prices.kr.eodhd"]
    # The watermark is the collection time and looks current...
    (watermark,) = cast("dict[str, dict[str, int]]", head["watermarks"]).values()
    assert watermark["through_us"] == us(at("2026-07-27T08:00:00"))
    # ...but the data ends on 07-01: XKRX sessions 07-02..03, 06..10, 13..16, 20..24 and 27
    # (07-17, Constitution Day, is closed from 2026) are 17 sessions behind.
    assert {key: head[key] for key in _FRESHNESS} == {
        "data_through": "2026-07-01",
        "expected_through": "2026-07-27",
        "lag_sessions": 17,
        "lag_unit": "sessions",
        "stale_after": maintain_promotion.FRESHNESS["prices.kr.eodhd"].stale_after,
        "freshness": "stale",
        "stale_reasons": ["lag"],
    }
    # The next morning in Seoul the same chain is current: 07-01 is the last closed session.
    fresh = _heads(ws, datetime(2026, 7, 2, 3, tzinfo=UTC))["prices.kr.eodhd"]
    assert (fresh["expected_through"], fresh["lag_sessions"], fresh["freshness"]) == (
        "2026-07-01", 0, "current")  # fmt: skip


def test_a_tombstoned_newest_bar_does_not_count_as_data(ws: Workspace) -> None:
    stamp = at("2026-07-27T08:00:00")
    first = add_source(
        ws,
        [bar("AAA.KO", date(2026, 7, 1), 100.0, retrieved=stamp),
         bar("AAA.KO", date(2026, 7, 27), 101.0, retrieved=stamp)],
        tag="first",
        linked=at("2026-07-27T09:00:00"),
    )  # fmt: skip
    identity = register_symbols(ws, first["source_id"])
    head = promote(ws, *spec([first], identity), apply=True)
    assert head["published"] is True, head.get("refusals")
    assert _heads(ws, JULY_RUN)["prices.kr.eodhd"]["data_through"] == "2026-07-27"
    # A full snapshot without the 07-27 bar deletes it: the readable data ends on 07-01.
    second = add_source(
        ws,
        [bar("AAA.KO", date(2026, 7, 1), 100.0, retrieved=at("2026-07-27T10:00:00"))],
        tag="second",
        linked=at("2026-07-27T11:00:00"),
    )  # fmt: skip
    cleaned = promote(
        ws,
        *spec([second], identity, parent=str(head["generation_id"]),
              tombstone=full_snapshot(second, "2026-07-01", "2026-07-28")),
        apply=True,
    )  # fmt: skip
    assert cleaned["published"] is True, cleaned.get("refusals")
    assert cast("dict[str, int]", cleaned["operations"]).get("TOMBSTONE") == 1
    found = _heads(ws, JULY_RUN)["prices.kr.eodhd"]
    assert (found["data_through"], found["freshness"]) == ("2026-07-01", "stale")


def test_a_frozen_provider_dataset_is_frozen_not_stale(ws: Workspace) -> None:
    _bars_downloaded_late(ws, "prices.us.norgate")
    head = _heads(ws, JULY_RUN)["prices.us.norgate"]
    assert {key: head[key] for key in _FRESHNESS} == {
        "data_through": "2026-07-01",
        "expected_through": None,
        "lag_sessions": None,
        "lag_unit": None,
        "stale_after": None,
        "freshness": "frozen",
        "stale_reasons": [],
    }


def test_every_catalog_dataset_reports_its_freshness_fields(ws: Workspace) -> None:
    _bars_downloaded_late(ws, "prices.kr.eodhd")
    heads = _heads(ws, JULY_RUN)
    assert {entry.dataset_id for entry in CATALOG} <= set(heads)
    for entry in CATALOG:
        head = heads[entry.dataset_id]
        assert set(_FRESHNESS) | {"stale_series", "zero_promotion_runs"} <= set(head)
        if entry.dataset_id != "prices.kr.eodhd":
            # No generation yet: listed, never silently absent.
            assert (head["version"], head["freshness"]) == (None, "no_head"), entry.dataset_id
        # Every catalog dataset is frozen or has an expectation (none counts) of its own.
        frozen = maintain_promotion.frozen(entry.dataset_id)
        assert frozen or entry.dataset_id in maintain_promotion.FRESHNESS, entry.dataset_id
    # Norgate and FMP stopped; the catalog's own flag marks only some of their datasets.
    stopped = {entry.dataset_id for entry in CATALOG if maintain_promotion.frozen(entry.dataset_id)}
    assert stopped == {
        "prices.us.norgate", "prices.us.norgate.ref", "prices.us.fmp.ref", "prices.ref.norgate",
        "actions.us.norgate", "actions.us.fmp.ref", "status.us.norgate", "fx.usdkrw.norgate",
        "classifications.us.norgate"}  # fmt: skip


def _publish_macro(ws: Workspace, dataset: str, series: dict[str, list[date]]) -> None:
    """One committed macro generation holding ``series``' observation periods."""
    rows: list[dict[str, object]] = [
        {"revision_id": f"{name}-{day}", "supersedes_revision_id": None, "op": "ASSERT",
         "available_at_us": 1, "revision_known_at_us": 1, "ingested_at_us": 2,
         "source_snapshot_id": "synthetic",
         "source_row_hash": hashlib.sha256(f"{name}-{day}".encode()).hexdigest(),
         "series_id": name, "observation_period": day, "unit": "pct",
         "source_vintage_start": None, "source_vintage_end": None, "value": "1",
         "value_state": "present"}
        for name, days in series.items() for day in days
    ]  # fmt: skip
    marker = publish_generation(ws.market, dataset_id=dataset, version="1", generation_id="m-1",
                                operation_id="op-m-1", request_hash="0" * 64, parent_id=None,
                                domain="macro_observations", rows=rows)  # fmt: skip
    with atomic(ws.state):
        ws.state.execute("INSERT INTO datasets VALUES (?,'macro_observations',?,'test')",
                         (dataset, "aas-market-rowset-v1"))  # fmt: skip
        ws.state.execute(
            "INSERT INTO dataset_versions VALUES (?,'1','m-1',NULL,1,?,?,?,'synthetic',?,NULL,"
            "NULL,?,'all','committed')",
            (dataset, marker["chain_hash"], "0" * 64, "aas-market-rowset-v1", "0" * 64,
             marker["row_count"]),
        )  # fmt: skip


def _weekdays(first: date, last: date) -> list[date]:
    days = [first + timedelta(days=offset) for offset in range((last - first).days + 1)]
    return [day for day in days if day.weekday() < 5]


def test_macro_series_lag_by_their_own_publication_period(ws: Workspace) -> None:
    # A daily series through Monday 11-09 and a monthly one whose last period is 2026-06.
    _publish_macro(ws, "macro.us.alfred", {
        "SYNDAILY": _weekdays(date(2026, 10, 1), date(2026, 11, 9)),
        "SYNMONTHLY": [date(2026, month, 1) for month in range(1, 7)]})  # fmt: skip
    head = _heads(ws, datetime(2026, 11, 11, 3, tzinfo=UTC))["macro.us.alfred"]
    # On Wednesday 11-11 a daily period is expected once two weekdays have passed it
    # (11-09); a month once the month after it has passed (09-01). The monthly series misses
    # 07, 08 and 09: three periods, above the threshold of two.
    assert {key: head[key] for key in (*_FRESHNESS, "stale_series")} == {
        "data_through": "2026-11-09",
        "expected_through": "2026-11-09",
        "lag_sessions": 3,
        "lag_unit": "periods",
        "stale_after": 2,
        "freshness": "stale",
        "stale_reasons": ["lag"],
        "stale_series": ["SYNMONTHLY"],
    }
