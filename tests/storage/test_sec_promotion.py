"""SEC filings and company facts through the promotion engine on synthetic sources.

Expected issuers come from ``mint_issuer``, dimensions from ``formats.dimensions_hash``
and instants are spelled in UTC, independently of the mappers' SQL.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Final, cast

import pyarrow as pa
import pytest

from aegis_alpha.storage import source_library
from aegis_alpha.storage.identity import mint_issuer
from aegis_alpha.storage.legacy_import.public import SUBMISSION_FILINGS
from aegis_alpha.storage.promotion import formats
from aegis_alpha.storage.promotion.engine import promote, verify_promotion
from aegis_alpha.storage.promotion.spec import parse_spec
from aegis_alpha.storage.raw import put_raw
from aegis_alpha.storage.source_identity import SourceContent, SourceFile
from aegis_alpha.storage.workspace import Workspace, initialize, open_workspace
from tests.storage.promotion_support import publish_calendar, us

ISSUER: Final = mint_issuer("sec_cik", "0000000001")
OTHER: Final = mint_issuer("sec_cik", "0000000002")
FACT_SCHEMA: Final = pa.schema(
    [
        ("cik", pa.string()),
        ("taxonomy", pa.string()),
        ("tag", pa.string()),
        ("unit", pa.string()),
        ("fp", pa.string()),
        ("form", pa.string()),
        ("accession_number", pa.string()),
        ("value", pa.string()),
        ("period_start", pa.date32()),
        ("period_end", pa.date32()),
        ("filed", pa.date32()),
        ("retrieved_at", pa.timestamp("us", tz="UTC")),
    ]
)
SOURCE_COLUMN: Final = {"rule": "source_column@1", "basis": "revision", "input": "accepted_at"}
RULES: Final = {
    "available_at_us": {**SOURCE_COLUMN, "args": {}},
    "revision_known_at_us": {**SOURCE_COLUMN, "args": {}},
}
COLLECTED: Final = datetime(2026, 9, 6, 2, 24, 26, tzinfo=UTC)
A1: Final = "0000000001-25-000001"
A2: Final = "0000000001-26-000001"
A3: Final = "0000000001-26-000002"


@pytest.fixture
def ws(tmp_path: Path) -> Iterator[Workspace]:
    root = tmp_path / "aas"
    initialize(root)
    with open_workspace(root, writable=True, strategy_write=True) as workspace:
        yield workspace


def _commit(
    workspace: Workspace, schema: pa.Schema, table: str, rows: list[tuple[object, ...]], tag: str
) -> dict[str, str]:
    _, digest, size = put_raw(workspace.paths.raw, f"synthetic-sec-{tag}".encode())
    content = SourceContent("synthetic", f"sec-{table}", 1, (SourceFile(digest, size),))
    columns = list(zip(*rows, strict=True))
    arrow = pa.table(
        {name: list(column) for name, column in zip(schema.names, columns, strict=True)},
        schema=schema,
    )
    result = source_library.import_content_arrow(workspace, content, table, arrow.to_reader())
    tables = cast("list[dict[str, object]]", result["tables"])
    return {
        "source_id": content.source_id,
        "source_sha256": content.sha256,
        "table": table,
        "digest": str(tables[0]["digest"]),
    }


def _filing(cik: str, accession: str, filed: str, accepted: str) -> tuple[object, ...]:
    values = {
        "member": f"CIK{cik}.json",
        "cik": cik,
        "accessionNumber": accession,
        "filingDate": filed,
        "reportDate": "",
        "acceptanceDateTime": accepted,
        "form": "10-K",
    }
    return tuple(values.get(name) for name in SUBMISSION_FILINGS.schema().names)


def _fact(  # noqa: PLR0913 -- one synthetic fact spells the columns a test varies
    accession: str,
    value: str,
    *,
    tag: str = "Assets",
    filed: date = date(2025, 2, 10),
    period_end: date = date(2024, 12, 31),
    cik: str = "0000000001",
) -> tuple[object, ...]:
    return (
        cik,
        "us-gaap",
        tag,
        "USD",
        "FY",
        "10-K",
        accession,
        value,
        None,
        period_end,
        filed,
        COLLECTED,
    )


def _spec(  # noqa: PLR0913 -- every spec field a test varies
    sources: list[dict[str, str]],
    *,
    domain: str,
    dataset: str,
    parent: str | None = None,
    filings: dict[str, str] | None = None,
    partition: dict[str, str] | None = None,
    rules: dict[str, object] | None = None,
) -> tuple[bytes, str]:
    facts = domain == "fundamentals"
    document = {
        "schema_version": "aas-promotion-v1",
        "target": {"domain": domain, "dataset_id": dataset, "parent": parent},
        "sources": sources,
        "mapper": {
            "name": "sec.companyfacts@1" if facts else "sec.submissions@1",
            "args": {"filings": filings} if facts else {},
        },
        "partition": partition,
        "time_rules": rules or RULES,
        "decimal_rule": {"value": "decimal_text@1"} if facts else {},
        "quality_rules": [],
        "tombstone_policy": {"mode": "never"},
        "identity_snapshot": None,
    }
    raw = json.dumps(document, sort_keys=True).encode()
    return raw, hashlib.sha256(raw).hexdigest()


def _pin(workspace: Workspace, generation_id: str) -> dict[str, str]:
    row = workspace.state.execute(
        "SELECT dataset_id, version, generation_id, chain_hash, manifest_hash "
        "FROM dataset_versions WHERE generation_id=?",
        (generation_id,),
    ).fetchone()
    assert row is not None
    names = ("dataset_id", "version", "generation_id", "chain_hash", "manifest_hash")
    return dict(zip(names, (str(value) for value in row), strict=True))


def _apply(workspace: Workspace, document: tuple[bytes, str]) -> dict[str, object]:
    return promote(workspace, document[0], document[1], apply=True)


def _filings(
    workspace: Workspace, rows: list[tuple[object, ...]], tag: str, parent: str | None = None
) -> tuple[dict[str, object], dict[str, str]]:
    pin = _commit(workspace, SUBMISSION_FILINGS.schema(), "filings", rows, tag)
    document = _spec([pin], domain="filings", dataset="filings.us.sec", parent=parent)
    result = _apply(workspace, document)
    return result, _pin(workspace, str(result["generation_id"]))


def _fundamentals(workspace: Workspace, generation_id: str) -> list[dict[str, object]]:
    cursor = workspace.market.execute(
        "SELECT issuer_id, instrument_id, concept, fiscal_period, dimensions_hash, accession, "
        "accepted_at_us, value, value_state, op, available_at_us, revision_known_at_us "
        "FROM fundamentals WHERE generation_id=? ORDER BY accession, concept",
        [generation_id],
    )
    names = [item[0] for item in cursor.description]
    return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


def test_filings_take_the_recorded_acceptance_instant(ws: Workspace) -> None:
    result, _ = _filings(
        ws,
        [
            _filing("0000000001", A1, "2025-02-10", "2025-02-10T21:30:00.000Z"),
            # A co-registrant lists the same accession: the issuer is part of the key.
            _filing("0000000002", A1, "2025-02-10", "2025-02-10T21:30:00.000Z"),
            # A date-only filing (local midnight in New York) has no instant.
            _filing("0000000001", "0000000001-99-000001", "1999-01-04", "1999-01-04T05:00:00.000Z"),
        ],
        "f1",
    )
    assert result["operations"] == {"ASSERT": 3}
    assert result["rows"] == {"ok": 3}
    rows = ws.market.execute(
        "SELECT issuer_id, filing_id, filed_date, accepted_at_us, available_at_us, "
        "revision_known_at_us FROM filings ORDER BY filed_date, issuer_id"
    ).fetchall()
    accepted = us(datetime(2025, 2, 10, 21, 30, tzinfo=UTC))
    assert rows == [
        (ISSUER, "0000000001-99-000001", date(1999, 1, 4), None, None, None),
        *sorted(
            [
                (ISSUER, A1, date(2025, 2, 10), accepted, accepted, accepted),
                (OTHER, A1, date(2025, 2, 10), accepted, accepted, accepted),
            ]
        ),
    ]
    # Issuer-level mappers resolve no instrument, so a spec pins no identity snapshot.
    pin = _commit(
        ws,
        SUBMISSION_FILINGS.schema(),
        "filings",
        [_filing("0000000001", A1, "2025-02-10", "")],
        "f9",
    )
    document = json.loads(_spec([pin], domain="filings", dataset="filings.us.sec")[0])
    assert parse_spec(*_encoded(document)).identity_snapshot is None
    document["identity_snapshot"] = {"snapshot_id": "s", "content_hash": "0" * 64}
    with pytest.raises(ValueError, match="others pin none"):
        parse_spec(*_encoded(document))


def _encoded(document: dict[str, object]) -> tuple[bytes, str]:
    raw = json.dumps(document, sort_keys=True).encode()
    return raw, hashlib.sha256(raw).hexdigest()


def test_facts_are_known_from_their_filing_acceptance(ws: Workspace) -> None:
    _, filings = _filings(
        ws,
        [
            _filing("0000000001", A1, "2025-02-10", "2025-02-10T21:30:00.000Z"),
            _filing("0000000001", A2, "2026-02-09", "2026-02-09T21:00:00.000Z"),
        ],
        "f1",
    )
    facts = _commit(
        ws,
        FACT_SCHEMA,
        "facts",
        [
            _fact(A1, "100"),
            # The 2026 10-K reports the same concept and period again: its own record.
            _fact(A2, "100", filed=date(2026, 2, 9)),
            _fact(A2, "7.5", tag="Revenues", filed=date(2026, 2, 9), period_end=date(2025, 12, 31)),
            # An accession the filings generation lacks has no acceptance and no time.
            _fact(A3, "9", filed=date(2026, 5, 1), period_end=date(2026, 3, 31)),
        ],
        "c1",
    )
    plan = promote(
        ws,
        *_spec([facts], domain="fundamentals", dataset="fundamentals.us.sec", filings=filings),
        apply=False,
    )
    assert plan["rows"] == {"ok": 4}
    time_rules = cast("dict[str, dict[str, int]]", plan["time_rules"])
    assert time_rules["available_at_us"]["null"] == 1
    result = _apply(
        ws, _spec([facts], domain="fundamentals", dataset="fundamentals.us.sec", filings=filings)
    )
    assert result["operations"] == {"ASSERT": 4}
    generation = str(result["generation_id"])
    rows = _fundamentals(ws, generation)
    first = us(datetime(2025, 2, 10, 21, 30, tzinfo=UTC))
    second = us(datetime(2026, 2, 9, 21, tzinfo=UTC))
    assert [
        (
            row["accession"],
            row["concept"],
            row["accepted_at_us"],
            row["available_at_us"],
            row["value"],
        )
        for row in rows
    ] == [
        (A1, "us-gaap:Assets", first, first, Decimal(100)),
        (A2, "us-gaap:Assets", second, second, Decimal(100)),
        (A2, "us-gaap:Revenues", second, second, Decimal("7.5")),
        (A3, "us-gaap:Assets", None, None, Decimal(9)),
    ]
    assert {row["issuer_id"] for row in rows} == {ISSUER}
    assert {row["instrument_id"] for row in rows} == {None}
    assert rows[0]["dimensions_hash"] == formats.dimensions_hash({"accession": A1})
    assert rows[0]["fiscal_period"] == "instant"
    assert verify_promotion(ws, generation)["verified"] is True
    again = _apply(
        ws,
        _spec(
            [facts],
            domain="fundamentals",
            dataset="fundamentals.us.sec",
            filings=filings,
            parent=generation,
        ),
    )
    assert again["published"] is False
    assert again["empty_delta"] is True


def test_a_later_filings_generation_completes_unmatched_facts(ws: Workspace) -> None:
    first, filings = _filings(
        ws, [_filing("0000000001", A1, "2025-02-10", "2025-02-10T21:30:00.000Z")], "f1"
    )
    _, unrelated = _filings_other(ws)
    facts = _commit(ws, FACT_SCHEMA, "facts", [_fact(A1, "100"), _fact(A3, "9")], "c1")
    created = _apply(
        ws, _spec([facts], domain="fundamentals", dataset="fundamentals.us.sec", filings=filings)
    )
    parent = str(created["generation_id"])
    # The reference may move only along its own chain, never back or to another dataset.
    with pytest.raises(ValueError, match="descendant"):
        promote(
            ws,
            *_spec(
                [facts],
                domain="fundamentals",
                dataset="fundamentals.us.sec",
                filings=unrelated,
                parent=parent,
            ),
            apply=False,
        )
    _, later = _filings(
        ws,
        [
            _filing("0000000001", A1, "2025-02-10", "2025-02-10T21:30:00.000Z"),
            _filing("0000000001", A3, "2025-03-03", "2025-03-03T13:00:00.000Z"),
        ],
        "f2",
        parent=str(first["generation_id"]),
    )
    result = _apply(
        ws,
        _spec(
            [facts],
            domain="fundamentals",
            dataset="fundamentals.us.sec",
            filings=later,
            parent=parent,
        ),
    )
    # The fact now known by its filing's acceptance is a SUPERSEDE of the timeless one.
    assert result["operations"] == {"SUPERSEDE": 1}
    assert result["unchanged"] == 1
    (row,) = _fundamentals(ws, str(result["generation_id"]))
    accepted = us(datetime(2025, 3, 3, 13, tzinfo=UTC))
    assert (row["accession"], row["op"], row["accepted_at_us"], row["available_at_us"]) == (
        A3,
        "SUPERSEDE",
        accepted,
        accepted,
    )


def _filings_other(workspace: Workspace) -> tuple[dict[str, object], dict[str, str]]:
    pin = _commit(
        workspace,
        SUBMISSION_FILINGS.schema(),
        "filings",
        [_filing("0000000001", A3, "2025-03-03", "2025-03-03T13:00:00.000Z")],
        "other",
    )
    result = _apply(workspace, _spec([pin], domain="filings", dataset="filings.us.sec.alt"))
    return result, _pin(workspace, str(result["generation_id"]))


def test_a_changed_value_of_one_accession_supersedes_and_partitions_select_by_filing(
    ws: Workspace,
) -> None:
    _, filings = _filings(
        ws,
        [
            _filing("0000000001", A1, "2025-02-10", "2025-02-10T21:30:00.000Z"),
            _filing("0000000001", A2, "2026-02-09", "2026-02-09T21:00:00.000Z"),
        ],
        "f1",
    )
    facts = _commit(
        ws,
        FACT_SCHEMA,
        "facts",
        [_fact(A1, "100"), _fact(A2, "100", filed=date(2026, 2, 9))],
        "c1",
    )
    year = {"from": "2025-01-01", "to": "2026-01-01"}
    created = _apply(
        ws,
        _spec(
            [facts],
            domain="fundamentals",
            dataset="fundamentals.us.sec",
            filings=filings,
            partition=year,
        ),
    )
    assert created["operations"] == {"ASSERT": 1}
    # A later collection states another value for the same accession.
    recollected = _commit(ws, FACT_SCHEMA, "facts", [_fact(A1, "101")], "c2")
    result = _apply(
        ws,
        _spec(
            [recollected],
            domain="fundamentals",
            dataset="fundamentals.us.sec",
            filings=filings,
            partition=year,
            parent=str(created["generation_id"]),
        ),
    )
    assert result["operations"] == {"SUPERSEDE": 1}
    (row,) = _fundamentals(ws, str(result["generation_id"]))
    first = us(datetime(2025, 2, 10, 21, 30, tzinfo=UTC))
    # The revision basis keeps the filing's acceptance as the time of the correction.
    assert (row["value"], row["available_at_us"]) == (Decimal(101), first)


def test_a_filings_reference_must_pin_a_filings_generation(ws: Workspace) -> None:
    calendar = publish_calendar(ws, {date(2025, 1, 2): (None, 1)}, dataset="filings.us.fake")
    facts = _commit(ws, FACT_SCHEMA, "facts", [_fact(A1, "100")], "c1")
    with pytest.raises(ValueError, match="must pin a filings dataset generation"):
        promote(
            ws,
            *_spec([facts], domain="fundamentals", dataset="fundamentals.us.sec", filings=calendar),
            apply=False,
        )


def test_a_repeated_listing_is_read_once(ws: Workspace) -> None:
    listing = _filing("0000000001", A1, "2025-02-10", "2025-02-10T21:30:00.000Z")
    paged = (
        "CIK0000000001-submissions-001.json",
        *listing[1:],
    )
    result, _ = _filings(ws, [listing, paged], "f1")
    assert (result["source_rows"], result["operations"]) == (2, {"ASSERT": 1})
    # Listings of one accession that state different fields are both mapped and refused.
    changed = _filing("0000000001", A1, "2025-02-11", "2025-02-10T21:30:00.000Z")
    pin = _commit(ws, SUBMISSION_FILINGS.schema(), "filings", [listing, changed], "f2")
    with pytest.raises(ValueError, match="natural keys repeat"):
        _apply(ws, _spec([pin], domain="filings", dataset="filings.us.sec.alt"))
