# ruff: noqa: PLR2004, S311 -- seeded synthetic fixtures, test-owned SQL
"""``read_heads`` projects exactly what ``market.project_heads`` projects, pushed into DuckDB.

The independent expectation is ``project_heads`` over the verified Python chain read, with
rule masking and flag exclusion applied to the input rows the way the contract states them.
Every chain is a seeded random revision history over a small key space, so records are
asserted, superseded, tombstoned and re-asserted with known, unknown and late times.
"""

from __future__ import annotations

import hashlib
import json
import random
from collections.abc import Callable, Iterator, Mapping
from dataclasses import replace
from datetime import date, timedelta
from decimal import Decimal
from fractions import Fraction
from pathlib import Path
from typing import TYPE_CHECKING, Final, cast

import duckdb
import pytest

from aegis_alpha.compute_resources import ComputeBudget, ComputeResourceError
from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.storage import market, market_inputs, publication
from aegis_alpha.storage import read_heads as heads_module
from aegis_alpha.storage.adjusted_prices import read_adjusted_prices
from aegis_alpha.storage.import_document import parse_import
from aegis_alpha.storage.market_inputs import (
    GenerationPin,
    generation_time_rules,
    load_pinned_heads,
)
from aegis_alpha.storage.market_schema import NATURAL_KEYS
from aegis_alpha.storage.raw import put_raw
from aegis_alpha.storage.read_heads import (
    RECORDED_TIMES,
    HeadBinding,
    HeadPin,
    HeadQuery,
    HeadRead,
    TimeRules,
    read_heads,
)
from aegis_alpha.storage.workspace import initialize, open_workspace
from tests.storage.test_market_inputs import pin as legacy_pin
from tests.storage.test_market_inputs import prices
from tests.storage.test_publication import document as import_document
from tests.storage.test_research_inputs import _source_row

if TYPE_CHECKING:
    from aegis_alpha.storage.workspace import Workspace

BUDGET: Final = ComputeBudget(Fraction(1), 256 * 1024 * 1024)
DAY0: Final = date(2026, 1, 5)
DAYS: Final = tuple(DAY0 + timedelta(days=offset) for offset in range(4))
LAG: Final = "session_close_plus_lag@1"
DAY_END: Final = "local_day_end@1"
type Row = dict[str, object]
type Factory = Callable[[random.Random, str, date, str], Row]


def _store(path: Path) -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect(str(path))
    market.initialize_market(connection, "synthetic")
    return connection


def _price(rng: random.Random, subject: str, day: date, role: str) -> Row:
    present = rng.random() < 0.85
    close = Decimal(rng.randrange(1, 10_000)) / 100 if present else None
    return {
        "instrument_id": subject,
        "session_date": day,
        "interval": "1d",
        "bar_end_us": (day - date(1970, 1, 1)).days * 86_400_000_000,
        "basis": "unadjusted",
        "currency": "USD",
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "volume": Decimal(rng.randrange(1, 1000)) if present else None,
        "price_role": role,
        "value_state": "present" if present else "missing",
    }


def _session(rng: random.Random, subject: str, day: date, _role: str) -> Row:
    opened = rng.random() < 0.8
    base = (day - date(1970, 1, 1)).days * 86_400_000_000
    return {
        "calendar_id": subject,
        "venue": "SYNTHETIC",
        "session_date": day,
        "open_at_us": base + 1 if opened else None,
        "close_at_us": base + 2 + rng.randrange(5) if opened else None,
        "status": "open" if opened else "closed",
        "timezone_version": "synthetic-v1",
    }


def _action(rng: random.Random, subject: str, day: date, role: str) -> Row:
    # The effective date is not part of the natural key, so a revision can move it.
    moved = day + timedelta(days=rng.choice([-1, 0, 0, 1]))
    return {
        "instrument_id": subject,
        "action_id": f"{role}-{day.isoformat()}",
        "action_type": "dividend",
        "ex_date": moved,
        "record_date": None,
        "pay_date": None,
        "effective_date": moved,
        "amount": Decimal(rng.randrange(1, 100)) / 10,
        "ratio": None,
        "currency": "USD",
        "value_state": "present",
    }


_FACTORIES: Final[dict[str, Factory]] = {
    "prices": _price,
    "calendar_sessions": _session,
    "corporate_actions": _action,
}


def _time(rng: random.Random, floor: object) -> int | None:
    if rng.random() < 0.15:
        return None
    value = rng.randrange(0, 100)
    return max(value, int(str(floor))) if floor is not None else value


def _chain(  # noqa: PLR0913 -- one seeded history and its publication identity
    connection: duckdb.DuckDBPyConnection,
    rng: random.Random,
    *,
    domain: str,
    dataset: str,
    generations: int = 4,
    subjects: tuple[str, ...] = ("A", "B"),
    roles: tuple[str, ...] = ("canonical", "reference"),
) -> list[str]:
    """Publish a random revision history and return its generation IDs, oldest first."""
    heads: dict[tuple[str, date, str], Row] = {}
    names = []
    factory = _FACTORIES[domain]
    for sequence in range(1, generations + 1):
        rows = []
        for subject in subjects:
            for day in DAYS:
                for role in roles if domain != "calendar_sessions" else ("canonical",):
                    key = (subject, day, role)
                    prior = heads.get(key)
                    draw = rng.random()
                    if prior is None and draw < 0.55:
                        op = "ASSERT"
                    elif prior is not None and draw < 0.35:
                        op = "SUPERSEDE"
                    elif prior is not None and prior["op"] != "TOMBSTONE" and draw < 0.5:
                        op = "TOMBSTONE"
                    else:
                        continue
                    values = factory(rng, subject, day, role)
                    if prior is not None:
                        # A revision keeps its record: the natural key is unchanged.
                        values.update({name: prior[name] for name in NATURAL_KEYS[domain]})
                    available = _time(rng, prior["available_at_us"] if prior else None)
                    known = _time(rng, prior["revision_known_at_us"] if prior else None)
                    row = {
                        **values,
                        "revision_id": f"{dataset}-r{sequence}-{subject}-{day}-{role}",
                        "supersedes_revision_id": None if prior is None else prior["revision_id"],
                        "op": op,
                        "available_at_us": available,
                        "revision_known_at_us": known,
                        "ingested_at_us": max(
                            [rng.randrange(0, 130), *(t for t in (available, known) if t)]
                        ),
                        "source_snapshot_id": "synthetic-source",
                        "source_row_hash": f"{rng.getrandbits(256):064x}",
                    }
                    row["record_id"] = market.record_identity(
                        domain, [row[name] for name in NATURAL_KEYS[domain]]
                    )
                    rows.append(row)
                    heads[key] = row
        if not rows:
            continue
        generation = f"{dataset}-g{len(names) + 1}"
        market.publish_generation(
            connection,
            dataset_id=dataset,
            version=str(len(names) + 1),
            generation_id=generation,
            operation_id=f"op-{generation}",
            request_hash=hashlib.sha256(generation.encode()).hexdigest(),
            parent_id=names[-1] if names else None,
            domain=domain,
            rows=[dict(row) for row in rows],
        )
        names.append(generation)
    return names


def _pin(connection: duckdb.DuckDBPyConnection, generation: str) -> GenerationPin:
    marker = market.marker_for(connection, generation)
    return GenerationPin(
        str(marker["dataset_id"]),
        str(marker["version"]),
        generation,
        str(marker["chain_hash"]),
        str(marker["request_hash"]),
    )


def _read(  # noqa: PLR0913 -- binding, query and provenance under test
    connection: duckdb.DuckDBPyConnection,
    binding: HeadBinding,
    query: HeadQuery | None = None,
    rules: Mapping[str, TimeRules] | None = None,
    *,
    budget: ComputeBudget = BUDGET,
    rehash: bool = False,
) -> HeadRead:
    if rules is None:
        generations = [
            str(marker["generation_id"])
            for item in binding.pins
            for marker in market.generation_chain(connection, item.pin.generation_id)
        ]
        rules = dict.fromkeys(generations, RECORDED_TIMES)
    return read_heads(
        connection,
        binding,
        query or HeadQuery(),
        time_rules=rules,
        budget=budget,
        rehash=rehash,
    )


def _day(value: object) -> date:
    assert isinstance(value, date)
    return value


def _binding(read: HeadRead) -> Mapping[str, object]:
    binding = read.receipt["binding"]
    assert isinstance(binding, Mapping)
    return binding


def _values(read: HeadRead) -> list[Row]:
    return [dict(row.values) for row in read.rows]


def _history(connection: duckdb.DuckDBPyConnection, generation: str) -> list[Row]:
    return [dict(row) for row in market.read_chain_rows(connection, generation, budget=BUDGET)]


def _cutoffs(rng: random.Random, draws: int) -> Iterator[tuple[int | None, int | None]]:
    yield None, None
    yield 1_000, None
    for _ in range(draws):
        yield rng.choice([None, rng.randrange(0, 110)]), rng.choice([None, rng.randrange(0, 140)])


@pytest.fixture
def store(tmp_path: Path) -> Iterator[duckdb.DuckDBPyConnection]:
    connection = _store(tmp_path / "market.duckdb")
    try:
        yield connection
    finally:
        connection.close()


def test_read_heads_matches_project_heads(store: duckdb.DuckDBPyConnection) -> None:
    for seed, domain in enumerate(["prices", "calendar_sessions", "corporate_actions"] * 2):
        rng = random.Random(seed)
        chain = _chain(store, rng, domain=domain, dataset=f"d{seed}-{domain}")
        history = _history(store, chain[-1])
        binding = HeadBinding(domain, (HeadPin(_pin(store, chain[-1])),))
        day_column = "effective_date" if domain == "corporate_actions" else "session_date"
        subject_column = "calendar_id" if domain == "calendar_sessions" else "instrument_id"
        for cutoff, ingested in _cutoffs(rng, 8):
            expected = market.project_heads(history, cutoff_us=cutoff, ingestion_cutoff_us=ingested)
            query = HeadQuery(cutoff_us=cutoff, ingestion_cutoff_us=ingested)
            assert _values(_read(store, binding, query)) == expected, (seed, cutoff, ingested)
            # Filters select the same heads as filtering the full projection afterwards,
            # including a non-key date that a later revision moved.
            subject = rng.choice(["A", "B"])
            low, high = sorted(rng.sample([*DAYS, DAYS[-1] + timedelta(days=1)], 2))
            filtered = replace(query, subjects=(subject,), from_date=low, to_date=high)
            assert _values(_read(store, binding, filtered)) == [
                row
                for row in expected
                if row[subject_column] == subject and low <= _day(row[day_column]) < high
            ], (seed, cutoff, ingested)
            if domain == "prices":
                roles = HeadQuery(cutoff_us=cutoff, price_roles=("reference",))
                assert _values(_read(store, binding, roles)) == [
                    row
                    for row in market.project_heads(history, cutoff_us=cutoff)
                    if row["price_role"] == "reference"
                ]


def test_research_known_ceiling_matches_snapshot_candidates(
    store: duckdb.DuckDBPyConnection,
) -> None:
    """A research read under a known ceiling drops exactly the revisions known after it.

    Rows with no recorded knowledge time stay readable, as ``_Visibility.candidates`` keeps
    them for observed-snapshot research; the ceiling enters the query document only when
    set, so every read without one keeps its receipt bytes.
    """
    for seed, domain in enumerate(["prices", "calendar_sessions", "corporate_actions"]):
        rng = random.Random(200 + seed)
        chain = _chain(store, rng, domain=domain, dataset=f"ceiling{seed}-{domain}")
        history = _history(store, chain[-1])
        binding = HeadBinding(domain, (HeadPin(_pin(store, chain[-1])),))
        for ceiling in (0, 30, 60, 90, 1_000):
            candidates = [
                row
                for row in history
                if row["revision_known_at_us"] is None
                or cast("int", row["revision_known_at_us"]) <= ceiling
            ]
            read = _read(store, binding, HeadQuery(known_ceiling_us=ceiling))
            assert _values(read) == market.project_heads(candidates), (seed, ceiling)
            assert cast("Row", read.receipt["query"])["known_ceiling_us"] == ceiling
    assert "known_ceiling_us" not in HeadQuery().document()
    with pytest.raises(ValueError, match="research read only"):
        HeadQuery(cutoff_us=1, known_ceiling_us=1)


def _mask(history: list[Row], rules: Mapping[str, TimeRules], grants: tuple[str, ...]) -> list[Row]:
    """Null every time a strict read may not use: a rule that is neither recorded nor granted."""
    masked = []
    for row in history:
        provenance = rules[str(row["generation_id"])]
        copy = dict(row)
        for column, rule in (
            ("available_at_us", provenance.available),
            ("revision_known_at_us", provenance.known),
        ):
            if rule not in {"source_column@1", "unknown_null@1", *grants}:
                copy[column] = None
        masked.append(copy)
    return masked


def test_ungranted_rule_rows_excluded_from_strict(store: duckdb.DuckDBPyConnection) -> None:
    choices = ("source_column@1", LAG, DAY_END, "unknown_null@1")
    for seed in range(4):
        rng = random.Random(100 + seed)
        chain = _chain(store, rng, domain="prices", dataset=f"rules{seed}")
        rules = {
            generation: TimeRules(rng.choice(choices), rng.choice(choices)) for generation in chain
        }
        history = _history(store, chain[-1])
        for grants in ((), (LAG,), (DAY_END,), (DAY_END, LAG)):
            binding = HeadBinding("prices", (HeadPin(_pin(store, chain[-1])),), grants)
            for cutoff, ingested in _cutoffs(rng, 4):
                query = HeadQuery(cutoff_us=cutoff, ingestion_cutoff_us=ingested)
                read = _read(store, binding, query, rules)
                if cutoff is None:
                    # Inspection reads ignore grants.
                    expected = market.project_heads(history, ingestion_cutoff_us=ingested)
                else:
                    expected = market.project_heads(
                        _mask(history, rules, grants),
                        cutoff_us=cutoff,
                        ingestion_cutoff_us=ingested,
                    )
                assert _values(read) == expected, (seed, grants, cutoff, ingested)
                present = {rule for item in rules.values() for rule in (item.available, item.known)}
                present -= {"source_column@1", "unknown_null@1"}
                strict = cutoff is not None
                assert _binding(read)["granted_rules"] == sorted(grants)
                assert read.receipt["applied_rules"] == (
                    sorted(present & set(grants)) if strict else []
                )
                assert read.receipt["withheld_rules"] == (
                    sorted(present - set(grants)) if strict else []
                )


def _within(row: Row, domain: str, low: date | None, high: date | None) -> bool:
    day = _day(row["effective_date" if domain == "corporate_actions" else "session_date"])
    return (low is None or low <= day) and (high is None or day < high)


def test_multi_pin_reads_match_each_pin_projection(store: duckdb.DuckDBPyConnection) -> None:
    """Each pin serves exactly its own chain's heads inside its own interval."""
    # Six seeds cover every (domain, shape) pair.
    for seed in range(6):
        rng = random.Random(300 + seed)
        domain = ("prices", "corporate_actions")[seed % 2]
        shape = ("providers", "same_dataset", "a_b_a")[seed % 3]
        first = _chain(store, rng, domain=domain, dataset=f"multi{seed}a")
        cut, later = sorted(rng.sample(DAYS[1:], 2))
        if shape == "same_dataset":
            # An early generation up to the cut, the head of the same chain from it: the two
            # pins share every ancestor generation.
            spans = [(first[0], None, cut), (first[-1], cut, None)]
        else:
            second = _chain(store, rng, domain=domain, dataset=f"multi{seed}b")
            spans = [(first[-1], None, cut), (second[-1], cut, None)]
            if shape == "a_b_a":
                spans = [(first[-1], None, cut), (second[-1], cut, later), (first[-1], later, None)]
        binding = HeadBinding(
            domain,
            tuple(HeadPin(_pin(store, generation), low, high) for generation, low, high in spans),
        )
        for cutoff, ingested in _cutoffs(rng, 5):
            expected = [
                (ordinal, row)
                for ordinal, (generation, low, high) in enumerate(spans)
                for row in market.project_heads(
                    _history(store, generation), cutoff_us=cutoff, ingestion_cutoff_us=ingested
                )
                if _within(row, domain, low, high)
            ]
            read = _read(store, binding, HeadQuery(cutoff_us=cutoff, ingestion_cutoff_us=ingested))
            assert [(row.pin, dict(row.values)) for row in read.rows] == expected, (
                seed,
                shape,
                cutoff,
                ingested,
            )


def test_coverage_reasons_match_market_inputs(store: duckdb.DuckDBPyConnection) -> None:
    """A single-pin strict read reports the reasons ``market_inputs._absence`` reports."""
    absence = market_inputs._absence  # noqa: SLF001 -- the reader this one replaces
    decision = market_inputs._Decision  # noqa: SLF001
    for seed in range(8):
        rng = random.Random(200 + seed)
        role = ("canonical", "reference")[seed % 2]
        chain = _chain(store, rng, domain="prices", dataset=f"parity{seed}", roles=(role,))
        history = _history(store, chain[-1])
        binding = HeadBinding("prices", (HeadPin(_pin(store, chain[-1])),))
        for cutoff, ingested in _cutoffs(rng, 8):
            if cutoff is None:
                continue
            query = HeadQuery(
                cutoff_us=cutoff, ingestion_cutoff_us=ingested, subjects=("A", "B"), grid=DAYS
            )
            report = _read(store, binding, query).coverage
            assert report is not None
            heads = {
                (row["instrument_id"], row["session_date"]): row
                for row in market.project_heads(
                    history, cutoff_us=cutoff, ingestion_cutoff_us=ingested
                )
            }
            for cell in report.cells:
                head = heads.get((cell.instrument_id, cell.session_date))
                if head is None:
                    candidates = tuple(
                        row
                        for row in history
                        if (row["instrument_id"], row["session_date"])
                        == (cell.instrument_id, cell.session_date)
                    )
                    expected = (
                        absence(candidates, decision(cutoff, "strict_pit", ingested), "price"),
                    )
                else:
                    state = str(head["value_state"])
                    expected = () if state == "present" else (state,)
                assert cell.reasons == expected, (seed, cutoff, ingested, cell)
                assert cell.present == (head is not None and head["value_state"] == "present")


def test_coverage_cells_report_why_they_are_not_served() -> None:
    # A retained revision whose availability is unknown or later is not a plain data gap.
    connection = duckdb.connect()
    market.initialize_market(connection, "synthetic")
    late, unknown = _rows(
        "prices",
        [("A", DAYS[0], "late", 10, Decimal(5)), ("A", DAYS[1], "unknown", 10, Decimal(5))],
    )
    late["available_at_us"] = late["ingested_at_us"] = 50
    unknown["available_at_us"] = None
    pin = _publish(connection, "kr", [late, unknown])
    query = HeadQuery(cutoff_us=20, subjects=("A",), grid=DAYS[:2])
    report = _read(connection, HeadBinding("prices", (HeadPin(pin),)), query).coverage
    assert report is not None
    assert [cell.reasons for cell in report.cells] == [
        ("price_unavailable",),
        ("unknown_price_evidence",),
    ]
    # A skipped reference price beside a canonical head does not make the cell incomplete.
    canonical, reference = _rows(
        "prices", [("A", DAYS[2], "canon", 10, Decimal(5)), ("A", DAYS[2], "ref", 10, Decimal(6))]
    )
    reference["price_role"] = "reference"
    reference["record_id"] = market.record_identity(
        "prices", [reference[k] for k in NATURAL_KEYS["prices"]]
    )
    both = _publish(connection, "kr.both", [canonical, reference])
    query = HeadQuery(cutoff_us=20, subjects=("A",), grid=(DAYS[2],))
    report = _read(connection, HeadBinding("prices", (HeadPin(both),)), query).coverage
    assert report is not None
    assert report.complete
    assert report.present_count == 1
    # A closed session is a reason, not a covered cell, as in market_inputs.
    sessions = []
    for day, status in ((DAYS[0], "open"), (DAYS[1], "closed")):
        row = {
            **_session(random.Random(0), "XKRX", day, "canonical"),
            "status": status,
            "revision_id": f"session-{day}",
            "supersedes_revision_id": None,
            "op": "ASSERT",
            "available_at_us": 10,
            "revision_known_at_us": 10,
            "ingested_at_us": 10,
            "source_snapshot_id": "synthetic-source",
            "source_row_hash": hashlib.sha256(str(day).encode()).hexdigest(),
        }
        if status == "closed":
            row.update(open_at_us=None, close_at_us=None)
        else:
            row.update(open_at_us=1, close_at_us=2)
        row["record_id"] = market.record_identity(
            "calendar_sessions", [row[k] for k in NATURAL_KEYS["calendar_sessions"]]
        )
        sessions.append(row)
    market.publish_generation(
        connection,
        dataset_id="calendar.kr",
        version="1",
        generation_id="calendar.kr-g1",
        operation_id="op-calendar.kr-g1",
        request_hash=hashlib.sha256(b"calendar.kr-g1").hexdigest(),
        parent_id=None,
        domain="calendar_sessions",
        rows=sessions,
    )
    calendar = HeadBinding("calendar_sessions", (HeadPin(_pin(connection, "calendar.kr-g1")),))
    query = HeadQuery(cutoff_us=20, subjects=("XKRX",), grid=DAYS[:2])
    report = _read(connection, calendar, query).coverage
    assert report is not None
    assert [(cell.present, cell.reasons) for cell in report.cells] == [
        (True, ()),
        (False, ("session_closed",)),
    ]


def _rows(domain: str, specs: list[tuple[str, date, str, int, Decimal | None]]) -> list[Row]:
    """Explicit price ASSERTs: (instrument, day, revision, known/available time, close)."""
    rows = []
    for instrument, day, revision, at, close in specs:
        row = {
            **_price(random.Random(0), instrument, day, "canonical"),
            "close": close,
            "open": close,
            "high": close,
            "low": close,
            "volume": Decimal(1),
            "value_state": "present",
            "revision_id": revision,
            "supersedes_revision_id": None,
            "op": "ASSERT",
            "available_at_us": at,
            "revision_known_at_us": at,
            "ingested_at_us": at,
            "source_snapshot_id": "synthetic-source",
            "source_row_hash": hashlib.sha256(revision.encode()).hexdigest(),
        }
        row["record_id"] = market.record_identity(domain, [row[k] for k in NATURAL_KEYS[domain]])
        rows.append(row)
    return rows


def _publish(
    connection: duckdb.DuckDBPyConnection,
    dataset: str,
    rows: list[Row],
    *,
    version: str = "1",
    parent: str | None = None,
) -> GenerationPin:
    generation = f"{dataset}-g{version}"
    market.publish_generation(
        connection,
        dataset_id=dataset,
        version=version,
        generation_id=generation,
        operation_id=f"op-{generation}",
        request_hash=hashlib.sha256(generation.encode()).hexdigest(),
        parent_id=parent,
        domain="prices",
        rows=rows,
    )
    return _pin(connection, generation)


def test_rule_grant_changes_strict_reads_only() -> None:
    connection = duckdb.connect()
    market.initialize_market(connection, "synthetic")
    pin = _publish(connection, "kr", _rows("prices", [("A", DAYS[0], "r1", 10, Decimal(5))]))
    rules = {pin.generation_id: TimeRules(LAG, LAG)}
    query = HeadQuery(cutoff_us=20, subjects=("A",), grid=(DAYS[0],))
    withheld = _read(connection, HeadBinding("prices", (HeadPin(pin),)), query, rules)
    assert withheld.rows == ()
    assert withheld.coverage is not None
    assert withheld.coverage.cells[0].reasons == ("ungranted_time_rule",)
    granted = _read(connection, HeadBinding("prices", (HeadPin(pin),), (LAG,)), query, rules)
    assert [row.values["close"] for row in granted.rows] == [Decimal(5)]
    assert granted.coverage is not None
    assert granted.coverage.complete
    assert granted.receipt["applied_rules"] == [LAG]
    assert granted.receipt["time_rules"] == [[0, pin.generation_id, LAG, LAG]]
    inspection = _read(connection, HeadBinding("prices", (HeadPin(pin),)), HeadQuery(), rules)
    assert len(inspection.rows) == 1
    assert withheld.receipt["binding_hash"] != granted.receipt["binding_hash"]
    # A correction known only by an ungranted rule leaves the earlier head served, and the
    # cell says a grant held something back.
    first = _publish(connection, "us", _rows("prices", [("A", DAYS[0], "u1", 10, Decimal(5))]))
    original = _rows("prices", [("A", DAYS[0], "u1", 10, Decimal(5))])[0]
    correction = {
        **original,
        "revision_id": "u2",
        "supersedes_revision_id": "u1",
        "op": "SUPERSEDE",
        "close": Decimal(7),
        "available_at_us": 20,
        "revision_known_at_us": 20,
        "ingested_at_us": 20,
    }
    second = _publish(connection, "us", [correction], version="2", parent=first.generation_id)
    stale = _read(
        connection,
        HeadBinding("prices", (HeadPin(second),)),
        HeadQuery(cutoff_us=30, subjects=("A",), grid=(DAYS[0],)),
        {first.generation_id: RECORDED_TIMES, second.generation_id: TimeRules(LAG, LAG)},
    )
    assert [row.values["revision_id"] for row in stale.rows] == ["u1"]
    assert stale.coverage is not None
    assert [(cell.present, cell.reasons) for cell in stale.coverage.cells] == [
        (True, ("ungranted_time_rule",))
    ]


def test_cutover_gap_is_reported_not_filled() -> None:
    connection = duckdb.connect()
    market.initialize_market(connection, "synthetic")
    # Pin A serves dates before DAYS[2] and lacks A@DAYS[1]; pin B has every date.
    first = _publish(
        connection,
        "prices.us.first",
        _rows(
            "prices",
            [
                ("A", DAYS[0], "a0", 1, Decimal(10)),
                ("A", DAYS[2], "a2", 1, Decimal(12)),
            ],
        ),
    )
    second = _publish(
        connection,
        "prices.us.second",
        _rows(
            "prices",
            [("A", day, f"b{index}", 1, Decimal(20 + index)) for index, day in enumerate(DAYS)],
        ),
    )
    binding = HeadBinding(
        "prices", (HeadPin(first, to_date=DAYS[2]), HeadPin(second, from_date=DAYS[2]))
    )
    query = HeadQuery(cutoff_us=5, subjects=("A",), grid=DAYS)
    read = _read(connection, binding, query)
    # Each date comes from exactly one pin; A@DAYS[1] is not filled from the second pin,
    # and the first pin's DAYS[2] bar is outside its interval.
    assert [(row.pin, row.values["session_date"], row.values["close"]) for row in read.rows] == [
        (0, DAYS[0], Decimal(10)),
        (1, DAYS[2], Decimal(22)),
        (1, DAYS[3], Decimal(23)),
    ]
    assert read.coverage is not None
    assert [(cell.session_date, cell.present, cell.reasons) for cell in read.coverage.cells] == [
        (DAYS[0], True, ()),
        (DAYS[1], False, ("missing_price",)),
        (DAYS[2], True, ()),
        (DAYS[3], True, ()),
    ]
    # A date no pin covers is reported outside the cutover rather than read.
    bounded = HeadBinding("prices", (HeadPin(second, from_date=DAYS[1], to_date=DAYS[3]),))
    report = _read(connection, bounded, query).coverage
    assert report is not None
    assert [cell.reasons for cell in report.cells] == [
        ("outside_cutover",),
        (),
        (),
        ("outside_cutover",),
    ]
    # Cutovers are part of the binding hash.
    shifted = HeadBinding(
        "prices", (HeadPin(first, to_date=DAYS[1]), HeadPin(second, from_date=DAYS[1]))
    )
    assert shifted.binding_hash != binding.binding_hash
    for pins in (
        (HeadPin(first, to_date=DAYS[2]), HeadPin(second, from_date=DAYS[3])),
        (HeadPin(first, to_date=DAYS[2]), HeadPin(second, from_date=DAYS[1])),
        (HeadPin(first), HeadPin(second)),
    ):
        with pytest.raises(ValueError, match="contiguous"):
            HeadBinding("prices", pins)
    # One generation may serve two separate intervals around another provider's.
    returning = HeadBinding(
        "prices",
        (
            HeadPin(second, to_date=DAYS[1]),
            HeadPin(first, from_date=DAYS[1], to_date=DAYS[3]),
            HeadPin(second, from_date=DAYS[3]),
        ),
    )
    read = _read(connection, returning, query)
    assert [(row.pin, row.values["session_date"], row.values["close"]) for row in read.rows] == [
        (0, DAYS[0], Decimal(20)),
        (1, DAYS[2], Decimal(12)),
        (2, DAYS[3], Decimal(23)),
    ]
    assert read.coverage is not None
    assert [cell.reasons for cell in read.coverage.cells] == [(), ("missing_price",), (), ()]


def _flag(
    connection: duckdb.DuckDBPyConnection, generation: str, row: Row, flag: str, detail: str
) -> None:
    connection.execute(
        "INSERT INTO quality_flags VALUES (?,?,?,?,?,?,?)",
        [generation, row["record_id"], row["revision_id"], "krw_tick", "1", flag, detail],
    )


def test_flag_exclusions_enter_bundle_hash_and_receipt() -> None:
    connection = duckdb.connect()
    market.initialize_market(connection, "synthetic")
    first_rows = _rows(
        "prices", [("A", DAYS[0], "r1", 10, Decimal(5)), ("B", DAYS[0], "s1", 10, Decimal(7))]
    )
    first = _publish(connection, "kr", first_rows)
    correction = {
        **first_rows[0],
        "revision_id": "r2",
        "supersedes_revision_id": "r1",
        "op": "SUPERSEDE",
        "close": Decimal("5.000000000001"),
        "available_at_us": 30,
        "revision_known_at_us": 30,
        "ingested_at_us": 30,
    }
    second = _publish(connection, "kr", [correction], version="2", parent=first.generation_id)
    _flag(connection, first.generation_id, first_rows[1], "time_clamped_to_ingestion", "late")
    _flag(connection, second.generation_id, correction, "provider_float_reconstructed", "5.0")
    plain = HeadBinding("prices", (HeadPin(second),))
    excluding = HeadBinding(
        "prices", (HeadPin(second),), excluded_flags=("provider_float_reconstructed",)
    )
    assert plain.binding_hash != excluding.binding_hash
    # Without the exclusion the corrected value is read with its flag.
    read = _read(connection, plain, HeadQuery(cutoff_us=40))
    assert [(row.values["close"], [f.flag for f in row.flags]) for row in read.rows] == [
        (Decimal("5.000000000001"), ["provider_float_reconstructed"]),
        (Decimal(7), ["time_clamped_to_ingestion"]),
    ]
    # Before the flagged correction is known the original head stands; once it is known
    # the excluded revision removes that head instead of serving a superseded value.
    query = HeadQuery(cutoff_us=20, subjects=("A", "B"), grid=(DAYS[0],))
    before = _read(connection, excluding, query)
    assert [row.values["revision_id"] for row in before.rows] == ["r1", "s1"]
    after = _read(connection, excluding, replace(query, cutoff_us=40))
    assert [row.values["revision_id"] for row in after.rows] == ["s1"]
    assert after.coverage is not None
    assert [cell.reasons for cell in after.coverage.cells] == [("flag_excluded",), ()]
    observed = _read(connection, excluding, HeadQuery())
    assert [row.values["revision_id"] for row in observed.rows] == ["s1"]
    # An excluded initial revision creates no head in a research read either.
    clamped = replace(excluding, excluded_flags=("time_clamped_to_ingestion",))
    assert [row.values["revision_id"] for row in _read(connection, clamped).rows] == ["r2"]
    for result in (before, after, observed):
        assert _binding(result)["excluded_flags"] == ["provider_float_reconstructed"]
        assert result.receipt["binding_hash"] == excluding.binding_hash
    assert after.receipt_hash == hashlib.sha256(canonical_json_bytes(after.receipt)).hexdigest()
    assert (
        after.receipt["heads_hash"]
        == hashlib.sha256(
            canonical_json_bytes([[0, row.values["record_id"], "s1"] for row in after.rows])
        ).hexdigest()
    )


def test_head_binding_and_receipt_formats_are_frozen() -> None:
    pin = GenerationPin("prices.kr.eodhd", "1", "g1", "a" * 64, "b" * 64)
    other = GenerationPin("prices.kr.qveris", "1", "g2", "c" * 64, "d" * 64)
    binding = HeadBinding(
        "prices",
        (HeadPin(pin, to_date=date(2026, 9, 7)), HeadPin(other, from_date=date(2026, 9, 7))),
        granted_rules=(LAG, "exdate_open@1"),
        excluded_flags=("provider_reported_partial",),
    )
    assert canonical_json_bytes(binding.document()) == (
        b'{"domain":"prices","excluded_flags":["provider_reported_partial"],'
        b'"granted_rules":["exdate_open@1","session_close_plus_lag@1"],"pins":['
        b'{"chain_hash":"' + b"a" * 64 + b'","dataset_id":"prices.kr.eodhd",'
        b'"from":null,"generation_id":"g1","manifest_hash":"' + b"b" * 64 + b'",'
        b'"to":"2026-09-07","version":"1"},'
        b'{"chain_hash":"' + b"c" * 64 + b'","dataset_id":"prices.kr.qveris",'
        b'"from":"2026-09-07","generation_id":"g2","manifest_hash":"' + b"d" * 64 + b'",'
        b'"to":null,"version":"1"}],"schema":"aas-head-binding-v1"}'
    )
    assert binding.binding_hash == (
        "d2894de2b1cb4ded660203b9fe74ed5059b03714dc133cefac587b5219c7e1b4"
    )
    # The read receipt over a fixed chain and query is fixed as well.
    connection = duckdb.connect()
    market.initialize_market(connection, "synthetic")
    fixed = _publish(connection, "kr", _rows("prices", [("A", DAYS[0], "r1", 10, Decimal(5))]))
    read = _read(
        connection,
        HeadBinding("prices", (HeadPin(fixed),), granted_rules=(LAG,)),
        HeadQuery(cutoff_us=20, subjects=("A",)),
        {fixed.generation_id: TimeRules(LAG, "source_column@1")},
    )
    assert sorted(read.receipt) == [
        "applied_rules",
        "binding",
        "binding_hash",
        "heads",
        "heads_hash",
        "mode",
        "query",
        "rehashed",
        "schema",
        "time_rules",
        "withheld_rules",
    ]
    assert read.receipt["schema"] == "aas-head-read-v1"
    assert read.receipt["rehashed"] is False
    assert read.receipt_hash == ("03b0d9322c8be4ae6ab0098f75f398f275df075e6f1b3837092fc70b6298b7db")


def test_pins_are_verified_before_rows_are_read(store: duckdb.DuckDBPyConnection) -> None:
    chain = _chain(store, random.Random(7), domain="prices", dataset="verified")
    pin = _pin(store, chain[-1])
    binding = HeadBinding("prices", (HeadPin(pin),))
    assert _read(store, binding, rehash=True).receipt["rehashed"] is True
    for wrong in (
        replace(pin, chain_hash="0" * 64),
        replace(pin, manifest_hash="0" * 64),
        replace(pin, version="9"),
    ):
        with pytest.raises(ValueError, match="does not match its marker"):
            _read(store, HeadBinding("prices", (HeadPin(wrong),)))
    with pytest.raises(ValueError, match="incompatible dataset/domain"):
        _read(store, HeadBinding("calendar_sessions", (HeadPin(pin),)))
    with pytest.raises(ValueError, match="time-rule provenance"):
        _read(store, binding, rules={pin.generation_id: RECORDED_TIMES})
    # A broken link in an intermediate marker fails before any row is read, while every
    # generation's row count still matches.
    linked = _chain(store, random.Random(8), domain="prices", dataset="linked")
    linked_binding = HeadBinding("prices", (HeadPin(_pin(store, linked[-1])),))
    assert _read(store, linked_binding).rows
    store.execute(
        "UPDATE market_generations SET delta_hash = ? WHERE generation_id = ?",
        ["f" * 64, linked[1]],
    )
    with pytest.raises(ValueError, match="chain link mismatch"):
        _read(store, linked_binding)
    # A value edited in place passes the link and count checks and fails the rehash.
    store.execute(
        "UPDATE prices SET source_row_hash = ? WHERE generation_id = ? AND revision_id = "
        "(SELECT min(revision_id) FROM prices WHERE generation_id = ?)",
        ["e" * 64, chain[0], chain[0]],
    )
    with pytest.raises(ValueError, match="hash/count mismatch"):
        _read(store, binding, rehash=True)
    # A removed row fails the recount without a rehash.
    store.execute(
        "DELETE FROM prices WHERE rowid = (SELECT min(rowid) FROM prices WHERE generation_id = ?)",
        [chain[-1]],
    )
    with pytest.raises(ValueError, match="hash/count mismatch"):
        _read(store, binding)


def test_head_read_is_admitted_before_rows_are_fetched(
    store: duckdb.DuckDBPyConnection, monkeypatch: pytest.MonkeyPatch
) -> None:
    chain = _chain(store, random.Random(11), domain="prices", dataset="budget")
    binding = HeadBinding("prices", (HeadPin(_pin(store, chain[-1])),))
    tiny = ComputeBudget(Fraction(1), 32 * 1024 * 1024)
    narrow = HeadQuery(subjects=("A",), from_date=DAYS[0], to_date=DAYS[1])
    assert _values(_read(store, binding, narrow, budget=tiny)) == [
        row
        for row in market.project_heads(_history(store, chain[-1]))
        if row["instrument_id"] == "A" and row["session_date"] == DAYS[0]
    ]

    def fetched(*_: object) -> object:
        pytest.fail("rows were fetched before the read was admitted")

    monkeypatch.setattr(heads_module, "_fetch", fetched)
    with pytest.raises(ComputeResourceError, match="head read memory estimate"):
        _read(store, binding, budget=replace(tiny, reserved_bytes=8 * 1024 * 1024 - 70_000))
    # A coverage grid is charged for every cell it will build, even when no row matches.
    grid = tuple(DAY0 + timedelta(days=offset) for offset in range(1_000))
    subjects = tuple(f"ABSENT{index:05d}" for index in range(1_000))
    with pytest.raises(ComputeResourceError, match="head read memory estimate"):
        _read(store, binding, HeadQuery(subjects=subjects, grid=grid), budget=tiny)


def _spec(rules: tuple[str, str]) -> bytes:
    return json.dumps(
        {
            "schema_version": "aas-promotion-v1",
            "time_rules": {
                "available_at_us": {"rule": rules[0], "basis": "record", "args": {"lag_us": 0}},
                "revision_known_at_us": {"rule": rules[1], "basis": "record", "args": {}},
            },
        }
    ).encode()


def _catalog(workspace: Workspace, pin: GenerationPin, transform: str) -> None:
    marker = market.marker_for(workspace.market, pin.generation_id)
    workspace.state.execute(
        "INSERT OR IGNORE INTO datasets VALUES (?, 'prices', 'aas-market-rowset-v1', 'synthetic')",
        (pin.dataset_id,),
    )
    workspace.state.execute(
        "INSERT INTO dataset_versions(dataset_id, version, generation_id, parent_generation_id, "
        "sequence, chain_hash, manifest_hash, record_schema, normalizer_version, transform_hash, "
        "identity_snapshot_hash, authority_policy_hash, row_count, coverage, status) "
        "VALUES (?,?,?,?,?,?,?,'aas-market-rowset-v1','synthetic',?,NULL,NULL,?,'unverified',"
        "'committed')",
        (
            pin.dataset_id,
            pin.version,
            pin.generation_id,
            marker["parent_id"],
            marker["sequence"],
            pin.chain_hash,
            pin.manifest_hash,
            transform,
            marker["row_count"],
        ),
    )
    workspace.state.commit()


def test_time_rule_provenance_comes_from_retained_evidence(tmp_path: Path) -> None:
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        # A registered research transform maps recorded source times.
        prices(workspace, tmp_path, [_source_row()], "1")
        legacy = legacy_pin(workspace)
        assert generation_time_rules(workspace, legacy.generation_id, budget=BUDGET) == (
            RECORDED_TIMES
        )
        query = HeadQuery(cutoff_us=10**15, subjects=("ASSET_A",))
        read = load_pinned_heads(
            workspace, HeadBinding("prices", (HeadPin(legacy),)), query, budget=BUDGET
        )
        assert [row.values["record_id"] for row in read.rows] == [_source_row()["record_id"]]
        assert read.receipt["time_rules"] == [
            [0, legacy.generation_id, "source_column@1", "source_column@1"]
        ]
        # A promotion generation's rules come from its retained spec, so an ungranted rule
        # withholds its rows from strict reads.
        spec = _spec((LAG, DAY_END))
        _, digest, _ = put_raw(workspace.paths.raw, spec)
        promoted = _publish(
            workspace.market,
            "prices.kr.eodhd",
            _rows("prices", [("ASSET_A", DAYS[0], "p1", 5, Decimal(3))]),
        )
        _catalog(workspace, promoted, digest)
        assert generation_time_rules(workspace, promoted.generation_id, budget=BUDGET) == TimeRules(
            LAG, DAY_END
        )
        binding = HeadBinding("prices", (HeadPin(promoted),))
        strict = HeadQuery(cutoff_us=10)
        assert load_pinned_heads(workspace, binding, strict, budget=BUDGET).rows == ()
        granted = replace(binding, granted_rules=(DAY_END, LAG))
        assert len(load_pinned_heads(workspace, granted, strict, budget=BUDGET).rows) == 1
        # A generation whose transform and manifest are not retained has no provenance.
        opaque = _publish(
            workspace.market,
            "prices.kr.opaque",
            _rows("prices", [("ASSET_A", DAYS[0], "o1", 5, Decimal(3))]),
        )
        _catalog(workspace, opaque, "9" * 64)
        with pytest.raises(ValueError, match="provenance is not retained"):
            load_pinned_heads(
                workspace, HeadBinding("prices", (HeadPin(opaque),)), strict, budget=BUDGET
            )
        # A generation published from a sealed aas-market-import-v1 document has recorded times.
        body = json.loads(import_document())
        body.update(
            dataset_id="prices.us.sealed", generation_id="sealed-g1", operation_id="sealed-op1"
        )
        body["rows"][0]["revision_id"] = "sealed-r1"
        publication.publish_document(workspace, parse_import(json.dumps(body).encode()))
        sealed = _pin(workspace.market, "sealed-g1")
        assert generation_time_rules(workspace, sealed.generation_id, budget=BUDGET) == (
            RECORDED_TIMES
        )
        imported = load_pinned_heads(
            workspace,
            HeadBinding("prices", (HeadPin(sealed),)),
            HeadQuery(cutoff_us=100),
            budget=BUDGET,
        )
        assert [row.values["revision_id"] for row in imported.rows] == ["sealed-r1"]
        # A retained document that no longer matches its address is corruption, and one too
        # large to decode within the allocation is a resource refusal; neither is "missing".
        tiny = replace(BUDGET, reserved_bytes=BUDGET.available_bytes - 1024)
        with pytest.raises(ComputeResourceError, match="provenance document"):
            generation_time_rules(workspace, promoted.generation_id, budget=tiny)
        path = workspace.paths.raw / digest[:2] / digest
        path.chmod(0o600)
        path.write_bytes(_spec((DAY_END, LAG)))
        with pytest.raises(ValueError, match="provenance document hash mismatch"):
            generation_time_rules(workspace, promoted.generation_id, budget=BUDGET)
        # A pin the catalog does not hold is refused before any read.
        unlisted = _publish(
            workspace.market,
            "prices.kr.unlisted",
            _rows("prices", [("ASSET_A", DAYS[0], "u1", 5, Decimal(3))]),
        )
        with pytest.raises(ValueError, match="catalog and market generation disagree"):
            load_pinned_heads(
                workspace, HeadBinding("prices", (HeadPin(unlisted),)), strict, budget=BUDGET
            )


def _actions(specs: list[tuple[str, str, date, Decimal, int]]) -> list[Row]:
    """Corporate action ASSERTs: (instrument, type, ex-date, amount or ratio, known time)."""
    rows = []
    for instrument, kind, day, value, at in specs:
        row: Row = {
            "instrument_id": instrument,
            "action_id": f"{kind}:{day.isoformat()}",
            "action_type": kind,
            "ex_date": day,
            "record_date": None,
            "pay_date": None,
            "effective_date": day,
            "amount": value if kind == "dividend" else None,
            "ratio": value if kind != "dividend" else None,
            "currency": "USD" if kind == "dividend" else None,
            "value_state": "present",
            "revision_id": f"{kind}-{day.isoformat()}",
            "supersedes_revision_id": None,
            "op": "ASSERT",
            "available_at_us": at,
            "revision_known_at_us": at,
            "ingested_at_us": at,
            "source_snapshot_id": "synthetic-source",
            "source_row_hash": hashlib.sha256(f"{kind}{day}".encode()).hexdigest(),
        }
        row["record_id"] = market.record_identity(
            "corporate_actions", [row[k] for k in NATURAL_KEYS["corporate_actions"]]
        )
        rows.append(row)
    return rows


def test_adjustment_ignores_actions_after_cutoff() -> None:
    connection = duckdb.connect()
    market.initialize_market(connection, "synthetic")
    bars = _publish(
        connection,
        "prices.us.synthetic",
        _rows(
            "prices",
            [
                ("A", DAYS[0], "b0", 1, Decimal(100)),
                ("A", DAYS[1], "b1", 2, Decimal(100)),
                ("A", DAYS[2], "b2", 3, Decimal(50)),
                ("A", DAYS[3], "b3", 4, Decimal(49)),
            ],
        ),
    )
    market.publish_generation(
        connection,
        dataset_id="actions.us.synthetic",
        version="1",
        generation_id="actions-g1",
        operation_id="op-actions-g1",
        request_hash=hashlib.sha256(b"actions-g1").hexdigest(),
        parent_id=None,
        domain="corporate_actions",
        # A 2:1 split known at 30; a dividend of 1 known only at 80.
        rows=_actions(
            [("A", "split", DAYS[2], Decimal(2), 30), ("A", "dividend", DAYS[3], Decimal(1), 80)]
        ),
    )
    actions = _pin(connection, "actions-g1")
    exdate = "exdate_open@1"
    rules = {bars.generation_id: RECORDED_TIMES, actions.generation_id: TimeRules(exdate, exdate)}
    prices_binding = HeadBinding("prices", (HeadPin(bars),))

    def closes(cutoff: int, basis: str, grants: tuple[str, ...] = (exdate,)) -> list[object]:
        read = read_adjusted_prices(
            connection,
            prices_binding,
            HeadBinding("corporate_actions", (HeadPin(actions),), grants),
            HeadQuery(cutoff_us=cutoff),
            basis=basis,
            time_rules=rules,
            budget=BUDGET,
        )
        ordered = sorted(read.rows, key=lambda row: _day(row.values["session_date"]))
        return [row.values["close"] for row in ordered]

    # Before the split is known, every bar stays as traded.
    assert closes(20, "total_return") == [Decimal(100), Decimal(100), Decimal(50), Decimal(49)]
    # The split is known, the dividend is not: earlier bars halve and nothing more.
    assert closes(60, "total_return") == [Decimal(50), Decimal(50), Decimal(50), Decimal(49)]
    # Once the dividend is known it is reinvested at the close before its ex-date, 50, so
    # earlier bars take the factor 49/50.
    assert closes(90, "total_return") == [Decimal(49), Decimal(49), Decimal(49), Decimal(49)]
    assert closes(90, "split_adjusted") == [Decimal(50), Decimal(50), Decimal(50), Decimal(49)]
    # Without a grant for the action rule, a strict read knows no action at all.
    assert closes(90, "split_adjusted", grants=()) == [
        Decimal(100),
        Decimal(100),
        Decimal(50),
        Decimal(49),
    ]
