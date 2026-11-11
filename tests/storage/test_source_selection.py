"""Versioned source-selection rules and the reads that apply them (#153).

Every store here is synthetic: two US price datasets under the rule's dataset names, with
explicit bars on either side of the rule's cutover, and corporate actions for the derived
adjustment. Nothing reads an installation or calls a provider.
"""

from __future__ import annotations

import hashlib
from datetime import date
from decimal import Decimal
from fractions import Fraction
from typing import Final

import duckdb
import pytest

from aegis_alpha.compute_resources import ComputeBudget
from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.storage import market
from aegis_alpha.storage.market_inputs import GenerationPin
from aegis_alpha.storage.market_schema import NATURAL_KEYS
from aegis_alpha.storage.read_heads import RECORDED_TIMES, HeadQuery, QualityFlag, TimeRules
from aegis_alpha.storage.source_selection import (
    RULES,
    SelectedRead,
    Selection,
    SelectionRule,
    SelectionSegment,
    overlap_close_differences,
    read_selected,
    read_selected_adjusted,
    seam_factor_continuity,
    selection_rule,
)

BUDGET: Final = ComputeBudget(Fraction(1), 256 * 1024 * 1024)
NORGATE: Final = "prices.us.norgate"
EODHD: Final = "prices.us.eodhd"
# Norgate's last session and EODHD's first under select.prices.us@1.
SEP03, SEP04, SEP08, SEP09, SEP10 = (date(2026, 9, day) for day in (3, 4, 8, 9, 10))
MISMATCH: Final = QualityFlag("cross_provider_mismatch", "1", "cross_provider_mismatch", "close")
type Row = dict[str, object]
type Bar = tuple[str, date, Decimal]


def _bars(dataset: str, bars: list[Bar]) -> list[Row]:
    rows = []
    for instrument, day, close in bars:
        revision = f"{dataset}:{instrument}:{day.isoformat()}"
        row: Row = {
            "instrument_id": instrument,
            "session_date": day,
            "interval": "1d",
            "bar_end_us": (day - date(1970, 1, 1)).days * 86_400_000_000,
            "basis": "unadjusted",
            "currency": "USD",
            "open": close,
            "high": close,
            "low": close,
            "close": close,
            "volume": Decimal(1000),
            "price_role": "canonical",
            "value_state": "present",
            "revision_id": revision,
            "supersedes_revision_id": None,
            "op": "ASSERT",
            "available_at_us": 1,
            "revision_known_at_us": 1,
            "ingested_at_us": 1,
            "source_snapshot_id": "synthetic-source",
            "source_row_hash": hashlib.sha256(revision.encode()).hexdigest(),
        }
        row["record_id"] = market.record_identity(
            "prices", [row[k] for k in NATURAL_KEYS["prices"]]
        )
        rows.append(row)
    return rows


def _actions(dataset: str, specs: list[tuple[str, date, Decimal]]) -> list[Row]:
    """Split ASSERTs: (instrument, ex-date, ratio), all known at time 1."""
    rows = []
    for instrument, day, ratio in specs:
        revision = f"{dataset}:{instrument}:{day.isoformat()}"
        row: Row = {
            "instrument_id": instrument,
            "action_id": f"split:{day.isoformat()}",
            "action_type": "split",
            "ex_date": day,
            "record_date": None,
            "pay_date": None,
            "effective_date": day,
            "amount": None,
            "ratio": ratio,
            "currency": None,
            "value_state": "present",
            "revision_id": revision,
            "supersedes_revision_id": None,
            "op": "ASSERT",
            "available_at_us": 1,
            "revision_known_at_us": 1,
            "ingested_at_us": 1,
            "source_snapshot_id": "synthetic-source",
            "source_row_hash": hashlib.sha256(revision.encode()).hexdigest(),
        }
        row["record_id"] = market.record_identity(
            "corporate_actions", [row[k] for k in NATURAL_KEYS["corporate_actions"]]
        )
        rows.append(row)
    return rows


def _publish(
    connection: duckdb.DuckDBPyConnection, dataset: str, domain: str, rows: list[Row]
) -> GenerationPin:
    generation = f"{dataset}-g1"
    market.publish_generation(
        connection,
        dataset_id=dataset,
        version="1",
        generation_id=generation,
        operation_id=f"op-{generation}",
        request_hash=hashlib.sha256(generation.encode()).hexdigest(),
        parent_id=None,
        domain=domain,
        rows=rows,
    )
    marker = market.marker_for(connection, generation)
    return GenerationPin(
        dataset, "1", generation, str(marker["chain_hash"]), str(marker["request_hash"])
    )


# Norgate holds A through 09-08 and nothing for B; EODHD overlaps on 09-04 and 09-08
# (its 09-08 close disagrees and carries the promotion's mismatch flag), holds B on 09-03,
# where Norgate has no bar, and continues A after the cutover, where a 2:1 split takes
# effect on 09-09.
NORGATE_BARS: Final[list[Bar]] = [
    ("A", SEP03, Decimal(100)),
    ("A", SEP04, Decimal(100)),
    ("A", SEP08, Decimal(101)),
    ("C", SEP08, Decimal(20)),
]
EODHD_BARS: Final[list[Bar]] = [
    ("A", SEP04, Decimal(100)),
    ("A", SEP08, Decimal(105)),
    ("A", SEP09, Decimal(50)),
    ("A", SEP10, Decimal(51)),
    ("B", SEP03, Decimal(7)),
    ("C", SEP08, Decimal(20)),
    ("C", SEP09, Decimal(20)),
]


@pytest.fixture
def store() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    market.initialize_market(connection, "synthetic")
    _publish(connection, NORGATE, "prices", _bars(NORGATE, NORGATE_BARS))
    eodhd_rows = _bars(EODHD, EODHD_BARS)
    _publish(connection, EODHD, "prices", eodhd_rows)
    flagged = next(
        r for r in eodhd_rows if r["instrument_id"] == "A" and r["session_date"] == SEP08
    )
    connection.execute(
        "INSERT INTO quality_flags VALUES (?,?,?,?,?,?,?)",
        [
            f"{EODHD}-g1",
            flagged["record_id"],
            flagged["revision_id"],
            MISMATCH.rule_id,
            MISMATCH.rule_version,
            MISMATCH.flag,
            MISMATCH.detail,
        ],
    )
    # The same split in both action datasets: only the dataset the rule selects for the
    # ex-date may apply it, so it is never applied twice.
    _publish(
        connection,
        "actions.us.norgate",
        "corporate_actions",
        _actions(
            "actions.us.norgate", [("A", SEP09, Decimal(2)), ("Z", date(2026, 7, 1), Decimal(3))]
        ),
    )
    _publish(
        connection,
        "actions.us.eodhd",
        "corporate_actions",
        _actions("actions.us.eodhd", [("A", SEP09, Decimal(2))]),
    )
    return connection


def _pins(connection: duckdb.DuckDBPyConnection, *datasets: str) -> tuple[GenerationPin, ...]:
    pins = []
    for dataset in datasets:
        marker = market.marker_for(connection, f"{dataset}-g1")
        pins.append(
            GenerationPin(
                dataset,
                "1",
                f"{dataset}-g1",
                str(marker["chain_hash"]),
                str(marker["request_hash"]),
            )
        )
    return tuple(pins)


def _rules(connection: duckdb.DuckDBPyConnection) -> dict[str, TimeRules]:
    generations = connection.execute("SELECT generation_id FROM market_generations").fetchall()
    return {str(generation): RECORDED_TIMES for (generation,) in generations}


def _prices(connection: duckdb.DuckDBPyConnection, rule: SelectionRule | None = None) -> Selection:
    return Selection(
        rule or selection_rule("select.prices.us@1"), _pins(connection, NORGATE, EODHD)
    )


def _read(
    connection: duckdb.DuckDBPyConnection, selection: Selection, query: HeadQuery | None = None
) -> SelectedRead:
    return read_selected(
        connection,
        selection,
        query or HeadQuery(cutoff_us=10, subjects=("A", "B", "C")),
        time_rules=_rules(connection),
        budget=BUDGET,
    )


def _by_cell(read: SelectedRead) -> dict[tuple[str, object], tuple[str, object]]:
    return {
        (str(row.values["instrument_id"]), row.values["session_date"]): (
            row.source,
            row.values["close"],
        )
        for row in read.rows
    }


def test_each_session_reads_the_one_source_the_rule_selects(
    store: duckdb.DuckDBPyConnection,
) -> None:
    read = _read(store, _prices(store))
    cells = _by_cell(read)
    # A Norgate-only day, an EODHD-only day, and overlapping days where Norgate wins.
    assert cells[("A", SEP03)] == (NORGATE, Decimal(100))
    assert cells[("A", SEP09)] == (EODHD, Decimal(50))
    assert cells[("A", SEP08)] == (NORGATE, Decimal(101))
    assert cells[("A", SEP04)] == (NORGATE, Decimal(100))
    # No implicit fallback: B@09-03 exists only in EODHD, which the rule does not select
    # for 09-03, so it is not read.
    assert ("B", SEP03) not in cells
    assert sorted(cells) == [
        ("A", SEP03),
        ("A", SEP04),
        ("A", SEP08),
        ("A", SEP09),
        ("A", SEP10),
        ("C", SEP08),
        ("C", SEP09),
    ]
    grid = _read(store, _prices(store), HeadQuery(cutoff_us=10, subjects=("B",), grid=(SEP03,)))
    assert grid.heads.coverage is not None
    assert [cell.reasons for cell in grid.heads.coverage.cells] == [("missing_price",)]
    assert read.receipt["rule"] == "select.prices.us@1"
    assert read.receipt["rule_hash"] == selection_rule("select.prices.us@1").rule_hash
    assert read.receipt["heads_hash"] == read.heads.receipt_hash
    assert read.receipt_hash == hashlib.sha256(canonical_json_bytes(dict(read.receipt))).hexdigest()


def test_a_mismatch_flag_on_the_overlap_is_visible_in_the_selected_read(
    store: duckdb.DuckDBPyConnection,
) -> None:
    read = _read(store, _prices(store))
    found = {(row.values["instrument_id"], row.values["session_date"]): row for row in read.rows}
    flagged = found[("A", SEP08)]
    # Norgate's value is served, and EODHD's flag on the same bar comes with it.
    assert flagged.source == NORGATE
    assert flagged.values["close"] == Decimal(101)
    assert flagged.witness_flags == ((EODHD, MISMATCH),)
    assert found[("A", SEP04)].witness_flags == ()
    assert found[("A", SEP09)].witness_flags == ()
    # The seam audit helper sees the same pairs: three overlapping bars, one flagged.
    report = overlap_close_differences(read, tolerance=Decimal("0.0001"))
    assert (report.pairs, report.flagged, report.over_tolerance) == (3, 1, 1)
    assert report.max_abs_relative == Decimal(4) / Decimal(101)
    assert report.witness_only == 1  # B@09-03: EODHD has it, the selected source has not
    assert report.sessions == ((SEP04, 1), (SEP08, 2))


def test_adjustment_factors_stay_continuous_across_the_source_cutover(
    store: duckdb.DuckDBPyConnection,
) -> None:
    actions = Selection(
        selection_rule("select.actions.us@1"),
        _pins(store, "actions.us.norgate", "actions.us.eodhd"),
        granted_rules=("exdate_open@1",),
    )
    read = read_selected_adjusted(
        store,
        _prices(store),
        actions,
        HeadQuery(cutoff_us=10, subjects=("A", "C")),
        basis="split_adjusted",
        time_rules=_rules(store),
        budget=BUDGET,
    )
    closes = {
        (row.values["instrument_id"], row.values["session_date"]): (row.source, row.values["close"])
        for row in read.rows
    }
    # The split selected from EODHD halves every Norgate bar before it, once.
    assert closes[("A", SEP03)] == (NORGATE, Decimal(50))
    assert closes[("A", SEP08)] == (NORGATE, Decimal("50.5"))
    assert closes[("A", SEP09)] == (EODHD, Decimal(50))
    checks = {
        check.instrument_id: check
        for check in seam_factor_continuity(read, tolerance=Decimal("0.05"))
    }
    a = checks["A"]
    assert (a.before, a.after) == ((SEP08, NORGATE), (SEP09, EODHD))
    assert a.factor_step == Decimal("0.5")
    assert a.actions_between == 1
    assert a.status == "explained_step"
    assert not a.price_jump
    c = checks["C"]
    assert (c.factor_step, c.actions_between, c.status) == (Decimal(1), 0, "continuous")
    assert read.receipt["prices_rule"] == "select.prices.us@1"
    assert read.receipt["actions_rule"] == "select.actions.us@1"


def test_a_new_rule_version_leaves_the_earlier_version_and_its_reads_unchanged(
    store: duckdb.DuckDBPyConnection,
) -> None:
    first = _read(store, _prices(store))
    # A later version that moves the cutover a day earlier is a separate rule.
    later = SelectionRule(
        "select.prices.us",
        2,
        "prices",
        (
            SelectionSegment(NORGATE, None, SEP08),
            SelectionSegment(EODHD, SEP08, None),
        ),
        witnesses=(SelectionSegment(EODHD, None, SEP08),),
        surfaced_flags=("cross_provider_mismatch",),
    )
    moved = _read(store, _prices(store, later))
    assert _by_cell(moved)[("A", SEP08)] == (EODHD, Decimal(105))
    assert moved.receipt["rule"] == "select.prices.us@2"
    again = _read(store, _prices(store))
    assert again.receipt_hash == first.receipt_hash
    assert _by_cell(again)[("A", SEP08)] == (NORGATE, Decimal(101))
    # Registered rules are immutable: their documents hash to frozen digests.
    assert {ref: rule.rule_hash for ref, rule in RULES.items()} == FROZEN_RULE_HASHES
    assert (
        hashlib.sha256(canonical_json_bytes(PRICES_US_1)).hexdigest()
        == FROZEN_RULE_HASHES["select.prices.us@1"]
    )
    with pytest.raises(KeyError, match="unregistered"):
        selection_rule("select.prices.us@2")
    with pytest.raises(ValueError, match="name@version"):
        selection_rule("select.prices.us")


def test_rules_bind_their_datasets_with_exact_cutovers(store: duckdb.DuckDBPyConnection) -> None:
    binding = _prices(store).binding
    assert [(p.pin.dataset_id, p.from_date, p.to_date) for p in binding.pins] == [
        (NORGATE, None, SEP09),
        (EODHD, SEP09, None),
    ]
    actions = selection_rule("select.actions.us@1")
    assert [(s.dataset_id, s.from_date, s.to_date) for s in actions.segments] == [
        ("actions.us.norgate", None, date(2026, 7, 28)),
        ("actions.us.eodhd", date(2026, 7, 28), None),
    ]
    fx = selection_rule("select.fx.usdkrw@1")
    assert fx.domain == "fx_rates"
    assert [(s.dataset_id, s.from_date, s.to_date) for s in fx.segments] == [
        ("fx.usdkrw.norgate", None, date(2026, 9, 10)),
        ("fx.usdkrw.eodhd", date(2026, 9, 10), None),
    ]
    for ref, dataset in (
        ("select.prices.kr@1", "prices.kr.eodhd"),
        ("select.actions.kr@1", "actions.kr.eodhd"),
    ):
        assert [(s.dataset_id, s.from_date, s.to_date) for s in selection_rule(ref).segments] == [
            (dataset, None, None)
        ]
    rule = selection_rule("select.prices.us@1")
    norgate, eodhd = _pins(store, NORGATE, EODHD)
    with pytest.raises(ValueError, match="exactly one pin"):
        Selection(rule, (norgate,))
    with pytest.raises(ValueError, match="exactly one pin"):
        Selection(rule, (norgate, eodhd, eodhd))
    stray = GenerationPin("prices.us.fmp.ref", "1", "g", "a" * 64, "b" * 64)
    with pytest.raises(ValueError, match="not in rule"):
        Selection(rule, (norgate, eodhd, stray))


FROZEN_RULE_HASHES: Final[dict[str, str]] = {
    "select.actions.kr@1": "d5e17edae8cc05bd8760d85c5c067f3319bfb9a88601b3f35369711d7471e6b6",
    "select.actions.us@1": "3712f56b417acfc5508918f5c2b88d99b96a4220c4a14be94b4436ebd8babf09",
    "select.fx.usdkrw@1": "c76dbc9e4c52d955e771822a6e5e29679ee6dd555b5a2eabaf8037d20b1e4378",
    "select.prices.kr@1": "c3b20508f84cadad8391fd07bc5c1b4353641f83b61b9c444d90843614f6c52b",
    "select.prices.us@1": "e29202e2df9044ae5aaa7fd40a6045b18407608958542e004db4a99df2b5e557",
}
# The document select.prices.us@1 hashes, written out by hand.
PRICES_US_1: Final = {
    "schema": "aas-source-selection-rule-v1",
    "rule": "select.prices.us@1",
    "domain": "prices",
    "segments": [
        {"dataset_id": "prices.us.norgate", "from": None, "to": "2026-09-09"},
        {"dataset_id": "prices.us.eodhd", "from": "2026-09-09", "to": None},
    ],
    "witnesses": [{"dataset_id": "prices.us.eodhd", "from": None, "to": "2026-09-09"}],
    "surfaced_flags": ["cross_provider_mismatch"],
}
