# ruff: noqa: PLR2004, S311, S608, SLF001 -- seeded synthetic fixtures, test-owned SQL
"""Bulk generations keep byte parity with the Python aas-rowset-v1 path.

The expected values come from the independent Python route: ``market.publish_generation``
for markers and ``rowset.encode_row`` for single cells. Fixtures are seeded random rows
over every domain, so each run checks the same wide set of shapes.
"""

from __future__ import annotations

import math
import random
import struct
import tracemalloc
from collections.abc import Iterator
from datetime import date, timedelta
from decimal import Decimal
from fractions import Fraction
from pathlib import Path
from typing import Final

import duckdb
import pytest

from aegis_alpha.compute_resources import ComputeBudget, ComputeResourceError
from aegis_alpha.storage import bulk_generation, market
from aegis_alpha.storage.bulk_generation import (
    BulkRequest,
    ParentChangedError,
    PlanChangedError,
    plan_generation_bulk,
    publish_generation_bulk,
    verify_generation_bulk,
)
from aegis_alpha.storage.market_schema import COMMON, DOMAINS, NATURAL_KEYS
from aegis_alpha.storage.rowset import RowsetStream, encode_row, rowset_hash

BUDGET: Final = ComputeBudget(Fraction(1), 512 * 1024 * 1024)
_TEXT: Final = "aZ09-_.:/é한字😀\t \x7f\u0085\u00a0"
# normalize_rows compares abs() under the default 28-digit context, so a magnitude that
# rounds up to 10**26 there is refused; stay just under it so both routes admit the rows.
_LARGEST: Final = 10**38 - 10**10
_FIRST_DAY: Final = date(1, 1, 1)
_LAST_DAY: Final = date(9999, 12, 31)


def _store(path: Path, *, version: int | None = None) -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect(str(path))
    market.initialize_market(connection, "synthetic", version=version)
    return connection


def _text(rng: random.Random) -> str:
    return rng.choice("abcXYZ") + "".join(rng.choice(_TEXT) for _ in range(rng.randrange(12)))


def _decimal(rng: random.Random, *, signed: bool) -> Decimal:
    """An exact DECIMAL(38,12) value, built from digits so no context precision rounds it."""
    shape = rng.randrange(5)
    if shape == 0:
        unscaled = 0
    elif shape == 1:
        unscaled = _LARGEST
    elif shape == 2:
        unscaled = rng.randrange(1, 10**6)
    else:
        unscaled = rng.randrange(_LARGEST)
    sign = 1 if signed and unscaled and rng.random() < 0.5 else 0
    return Decimal((sign, tuple(int(digit) for digit in str(unscaled)), -12))


def _float(rng: random.Random) -> float:
    shape = rng.randrange(6)
    if shape == 0:
        return rng.choice(
            [0.0, -0.0, 5e-324, -5e-324, 2.2250738585072014e-308, 1.7976931348623157e308]
        )
    while True:
        bits = rng.getrandbits(64)
        if shape == 1:
            bits &= 0x800FFFFFFFFFFFFF  # subnormal
        value = struct.unpack(">d", bits.to_bytes(8, "big"))[0]
        if math.isfinite(value):
            return value


def _cell(rng: random.Random, name: str, kind: str, domain: str) -> object:  # noqa: PLR0911 -- one draw per column type
    base = kind.rstrip("?")
    if kind.endswith("?") and rng.random() < 0.25:
        return None
    if base == "VARCHAR":
        return _text(rng)
    if base == "DATE":
        return _FIRST_DAY + timedelta(days=rng.randrange((_LAST_DAY - _FIRST_DAY).days + 1))
    if base == "DOUBLE":
        return _float(rng)
    if base == "DECIMAL(38,12)":
        return _decimal(rng, signed=domain != "prices")
    if name.endswith("_us"):
        return rng.randrange(2**63)
    return rng.randrange(-(2**63), 2**63)


def _domain_row(rng: random.Random, domain: str, ordinal: int) -> dict[str, object]:
    row: dict[str, object] = {}
    for name, kind in DOMAINS[domain]:
        row[name] = _cell(rng, name, kind, domain)
    # Natural keys carry the ordinal so every record is distinct.
    first = NATURAL_KEYS[domain][0]
    row[first] = f"k{ordinal}-{_text(rng)}"
    if domain == "classifications" and row["effective_to"] is not None:
        start = row["effective_from"]
        assert isinstance(start, date)
        row["effective_to"] = None if start == _LAST_DAY else start + timedelta(days=1)
    if domain == "prices":
        row["basis"] = rng.choice(["unadjusted", "unadjusted", "split_adjusted"])
        row["price_role"] = (
            "reference" if row["basis"] != "unadjusted" else rng.choice(["canonical", "reference"])
        )
    if "value_state" in row:
        value = "close" if domain == "prices" else "rate" if domain == "fx_rates" else "value"
        if value in row:
            row["value_state"] = (
                "present" if row[value] is not None else rng.choice(["missing", "invalid"])
            )
        else:
            row["value_state"] = rng.choice(["present", "missing", "not_collected"])
    return row


def _common(rng: random.Random, revision: str) -> dict[str, object]:
    ingested = rng.randrange(1_000, 1_000_000)
    return {
        "revision_id": revision,
        "supersedes_revision_id": None,
        "op": "ASSERT",
        "available_at_us": rng.choice([None, rng.randrange(ingested + 1)]),
        "revision_known_at_us": rng.choice([None, rng.randrange(ingested + 1)]),
        "ingested_at_us": ingested,
        "source_snapshot_id": _text(rng),
        "source_row_hash": f"{rng.getrandbits(256):064x}",
    }


def _identify(domain: str, row: dict[str, object]) -> dict[str, object]:
    natural = [row[name] for name in NATURAL_KEYS[domain]]
    return {**row, "record_id": market.record_identity(domain, natural)}


def _first(rng: random.Random, domain: str, count: int) -> list[dict[str, object]]:
    return [
        _identify(domain, {**_common(rng, f"r1-{index}"), **_domain_row(rng, domain, index)})
        for index in range(count)
    ]


def _second(
    rng: random.Random, domain: str, first: list[dict[str, object]]
) -> list[dict[str, object]]:
    """Supersede, tombstone and newly assert records against the first delta's heads."""
    rows = []
    for index, prior in enumerate(first):
        choice = rng.randrange(3)
        if choice == 0:
            continue
        common = _common(rng, f"r2-{index}")
        # Knowledge never moves backwards, and ingestion still follows it.
        ingested = int(str(common["ingested_at_us"]))
        for field in ("available_at_us", "revision_known_at_us"):
            then, now = prior[field], common[field]
            if then is not None and now is not None:
                common[field] = max(int(str(now)), int(str(then)))
                ingested = max(ingested, int(str(common[field])))
        common["ingested_at_us"] = ingested
        common.update(
            op="SUPERSEDE" if choice == 1 else "TOMBSTONE",
            supersedes_revision_id=prior["revision_id"],
        )
        replacement = _domain_row(rng, domain, index)
        natural = {name: prior[name] for name in NATURAL_KEYS[domain]}
        row = {**common, **replacement, **natural, "record_id": prior["record_id"]}
        if domain == "classifications":
            row["effective_to"] = None
        rows.append(row)
    rows.extend(
        _identify(
            domain, {**_common(rng, f"r2-new-{index}"), **_domain_row(rng, domain, 10_000 + index)}
        )
        for index in range(3)
    )
    return rows


def _stage(
    connection: duckdb.DuckDBPyConnection,
    domain: str,
    rows: list[dict[str, object]],
    *,
    name: str = "staged",
    fields: bool = False,
) -> str:
    columns = [(n, k.rstrip("?")) for n, k in COMMON + DOMAINS[domain] if n != "generation_id"]
    if fields:
        columns.append(("fields", "VARCHAR"))
    connection.execute(f'DROP TABLE IF EXISTS "{name}"')
    connection.execute(
        f'CREATE TEMP TABLE "{name}" (' + ", ".join(f'"{n}" {k}' for n, k in columns) + ")"
    )
    if rows:
        connection.executemany(
            f'INSERT INTO "{name}" VALUES (' + ",".join("?" for _ in columns) + ")",
            [[row.get(n, "ohlcv" if n == "fields" else None) for n, _ in columns] for row in rows],
        )
    return name


def _request(
    version: str, *, parent: str | None, domain: str, staged: str = "staged"
) -> BulkRequest:
    return BulkRequest(
        dataset_id="synthetic",
        version=version,
        generation_id="g" + version,
        operation_id="op" + version,
        request_hash=version * 64,
        parent_id=parent,
        domain=domain,
        staged=staged,
    )


def _python(
    connection: duckdb.DuckDBPyConnection,
    rows: list[dict[str, object]],
    version: str,
    *,
    parent: str | None,
    domain: str,
) -> dict[str, object]:
    return market.publish_generation(
        connection,
        dataset_id="synthetic",
        version=version,
        generation_id="g" + version,
        operation_id="op" + version,
        request_hash=version * 64,
        parent_id=parent,
        domain=domain,
        rows=[dict(row) for row in rows],
    )


def _bulk(  # noqa: PLR0913 -- mirrors _python plus the staged shape
    connection: duckdb.DuckDBPyConnection,
    rows: list[dict[str, object]],
    version: str,
    *,
    parent: str | None,
    domain: str,
    fields: bool = False,
) -> dict[str, object]:
    _stage(connection, domain, rows, fields=fields)
    return publish_generation_bulk(
        connection, _request(version, parent=parent, domain=domain), budget=BUDGET
    )


@pytest.fixture
def stores(tmp_path: Path) -> Iterator[tuple[duckdb.DuckDBPyConnection, duckdb.DuckDBPyConnection]]:
    python = _store(tmp_path / "python.duckdb")
    bulk = _store(tmp_path / "bulk.duckdb")
    try:
        yield python, bulk
    finally:
        python.close()
        bulk.close()


def test_streaming_hash_matches_python_rowset(
    stores: tuple[duckdb.DuckDBPyConnection, duckdb.DuckDBPyConnection],
) -> None:
    python, bulk = stores
    for seed, domain in enumerate(sorted(DOMAINS)):
        rng = random.Random(seed)
        first = _first(rng, domain, 24)
        second = _second(rng, domain, first)
        dataset = f"synthetic-{domain}"
        expected = []
        actual = []
        for rows, version, parent in ((first, "1", None), (second, "2", "g1")):
            expected.append(
                market.publish_generation(
                    python,
                    dataset_id=dataset,
                    version=version,
                    generation_id=f"{domain}-g{version}",
                    operation_id=f"{domain}-op{version}",
                    request_hash=version * 64,
                    parent_id=None if parent is None else f"{domain}-{parent}",
                    domain=domain,
                    rows=[dict(row) for row in rows],
                )
            )
            _stage(bulk, domain, rows)
            actual.append(
                publish_generation_bulk(
                    bulk,
                    BulkRequest(
                        dataset_id=dataset,
                        version=version,
                        generation_id=f"{domain}-g{version}",
                        operation_id=f"{domain}-op{version}",
                        request_hash=version * 64,
                        parent_id=None if parent is None else f"{domain}-{parent}",
                        domain=domain,
                        staged="staged",
                    ),
                    budget=BUDGET,
                )
            )
        assert actual == expected, domain
        # Each route's store verifies under the other route's verifier.
        assert market.verify_generation(bulk, f"{domain}-g2") == expected[-1]
        assert (
            verify_generation_bulk(python, f"{domain}-g2", budget=BUDGET, deep=True)
            == (expected[-1])
        )


def test_sql_cell_encoding_matches_python_codec() -> None:
    connection = duckdb.connect()
    rng = random.Random(7)
    kinds = {
        "text": ("VARCHAR", lambda: _text(rng)),
        "int": (
            "BIGINT",
            lambda: rng.choice([-(2**63), 2**63 - 1, 0, -1, rng.randrange(-(2**63), 2**63)]),
        ),
        "utc_us": ("BIGINT", lambda: rng.randrange(2**63)),
        "date": ("DATE", lambda: _FIRST_DAY + timedelta(days=rng.randrange(3_652_059))),
        "decimal": ("DECIMAL(38,12)", lambda: _decimal(rng, signed=True)),
        "float": ("DOUBLE", lambda: _float(rng)),
    }
    for rowset_type, (sql_type, draw) in kinds.items():
        connection.execute(f"CREATE OR REPLACE TABLE cells (v {sql_type})")
        values = [None, *(draw() for _ in range(2_000))]
        connection.executemany("INSERT INTO cells VALUES (?)", [[value] for value in values])
        encoded = bulk_generation._encoded("v", rowset_type)
        for stored, blob in connection.execute(f"SELECT v, {encoded} FROM cells").fetchall():
            assert blob == encode_row((("v", rowset_type),), {"v": stored}), (rowset_type, stored)


def test_rowset_stream_refuses_order_and_count_mismatch() -> None:
    schema = (("v", "text"),)
    rows: list[dict[str, object]] = [{"v": "b"}, {"v": "a"}, {"v": "a"}]
    encoded = sorted(encode_row(schema, row) for row in rows)
    stream = RowsetStream(schema, len(rows))
    for blob in encoded:
        stream.update(blob)
    assert stream.hexdigest() == rowset_hash(schema, rows)
    unordered = RowsetStream(schema, 2)
    unordered.update(encoded[-1])
    with pytest.raises(ValueError, match="ascending byte order"):
        unordered.update(encoded[0])
    short = RowsetStream(schema, 2)
    short.update(encoded[0])
    with pytest.raises(ValueError, match="fewer rows"):
        short.hexdigest()
    with pytest.raises(ValueError, match="more rows"):
        RowsetStream(schema, 0).update(encoded[0])


def test_close_only_and_v1_prices_keep_parity(tmp_path: Path) -> None:
    rng = random.Random(11)
    rows = _first(rng, "prices", 12)
    close_rows = [
        {**row, "fields": "close", "price_role": "reference", "open": None, "high": None,
         "low": None, "volume": None}
        if index % 3 == 0 else row
        for index, row in enumerate(rows)
    ]  # fmt: skip
    python = _store(tmp_path / "python.duckdb")
    bulk = _store(tmp_path / "bulk.duckdb")
    expected = _python(python, close_rows, "1", parent=None, domain="prices")
    assert _bulk(bulk, close_rows, "1", parent=None, domain="prices", fields=True) == expected
    assert market.verify_generation(bulk, "g1") == expected
    # An all-OHLCV delta staged with a fields column hashes in its v1 shape.
    old_python = _store(tmp_path / "v1-python.duckdb", version=1)
    old_bulk = _store(tmp_path / "v1-bulk.duckdb", version=1)
    expected = _python(old_python, rows, "1", parent=None, domain="prices")
    assert _bulk(old_bulk, rows, "1", parent=None, domain="prices", fields=True) == expected
    _stage(old_bulk, "prices", close_rows, fields=True)
    with pytest.raises(ValueError, match="aas db migrate --to 2"):
        plan_generation_bulk(old_bulk, _request("2", parent="g1", domain="prices"), budget=BUDGET)
    for connection in (python, bulk, old_python, old_bulk):
        connection.close()


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"currency": " \t　"}, "market text must be nonempty"),
        ({"currency": "U\x00SD"}, "market text must be nonempty"),
        ({"instrument_id": None}, "required market field is null"),
        ({"source_row_hash": "A" * 64}, "invalid source row hash"),
        ({"ingested_at_us": 2**62}, "ingestion timestamp cannot be in the future"),
        ({"available_at_us": 2_000_000}, "ingestion cannot precede source knowledge"),
        ({"close": Decimal(-1), "value_state": "present"}, "cannot be negative"),
        ({"close": None, "value_state": "present"}, "missing state disagree"),
        ({"value_state": "estimated"}, "unknown market value state"),
        ({"basis": "split_adjusted", "price_role": "canonical"}, "remain reference data"),
        ({"price_role": "primary"}, "unknown market price role"),
        ({"record_id": "0" * 64}, "record identity does not match"),
    ],
)
def test_row_rules_match_normalize_rows(
    tmp_path: Path, change: dict[str, object], message: str
) -> None:
    rng = random.Random(3)
    rows = _first(rng, "prices", 4)
    changed = {**rows[2], **change}
    if "record_id" not in change and changed["instrument_id"] is not None:
        changed = _identify("prices", changed)
    rows[2] = changed
    with pytest.raises(ValueError, match=message):
        market.normalize_rows("prices", "g1", [dict(row) for row in rows])
    connection = _store(tmp_path / "market.duckdb")
    _stage(connection, "prices", rows)
    with pytest.raises(ValueError, match=message):
        plan_generation_bulk(connection, _request("1", parent=None, domain="prices"), budget=BUDGET)
    assert connection.execute("SELECT count(*) FROM market_generations").fetchone() == (0,)
    connection.close()


def test_revision_rules_match_python(tmp_path: Path) -> None:
    rng = random.Random(5)
    first = _first(rng, "prices", 3)
    first[0].update(available_at_us=10, revision_known_at_us=10)
    python = _store(tmp_path / "python.duckdb")
    bulk = _store(tmp_path / "bulk.duckdb")
    _python(python, first, "1", parent=None, domain="prices")
    _bulk(bulk, first, "1", parent=None, domain="prices")
    head = first[0]
    cases = [
        ([{**head, "revision_id": "again"}], "ambiguous repeated ASSERT"),
        ([{**head, "revision_id": "x", "op": "SUPERSEDE", "supersedes_revision_id": "nope"}],
         "current ancestor"),
        ([{**head, "op": "SUPERSEDE", "supersedes_revision_id": head["revision_id"]}],
         "duplicate market revision"),
        ([{**head, "revision_id": "x", "op": "SUPERSEDE",
           "supersedes_revision_id": head["revision_id"], "available_at_us": 0,
           "revision_known_at_us": 0}], "knowledge cannot move backwards"),
        ([{**head, "revision_id": "x", "op": "SUPERSEDE",
           "supersedes_revision_id": head["revision_id"]},
          {**head, "revision_id": "y", "op": "SUPERSEDE",
           "supersedes_revision_id": head["revision_id"]}], "only one revision per natural record"),
    ]  # fmt: skip
    for rows, message in cases:
        with pytest.raises(ValueError, match=message):
            _python(python, rows, "2", parent="g1", domain="prices")
        _stage(bulk, "prices", rows)
        with pytest.raises(ValueError, match=message):
            plan_generation_bulk(bulk, _request("2", parent="g1", domain="prices"), budget=BUDGET)
    python.close()
    bulk.close()


def test_parent_cas_mismatch_requires_replan(tmp_path: Path) -> None:
    rng = random.Random(13)
    first = _first(rng, "prices", 4)
    connection = _store(tmp_path / "market.duckdb")
    _bulk(connection, first, "1", parent=None, domain="prices")
    _stage(connection, "prices", _first(rng, "prices", 2)[:1], name="planned")
    request = _request("2", parent="g1", domain="prices", staged="planned")
    plan = plan_generation_bulk(connection, request, budget=BUDGET)
    assert connection.execute("SELECT count(*) FROM market_generations").fetchone() == (1,)
    # A competing request publishes on the same parent first.
    competing = [_identify("prices", {**row, "instrument_id": "other"}) for row in first[:1]]
    _stage(connection, "prices", competing, name="competing")
    publish_generation_bulk(
        connection, _request("3", parent="g1", domain="prices", staged="competing"), budget=BUDGET
    )
    with pytest.raises(ParentChangedError, match="plan again"):
        publish_generation_bulk(connection, request, budget=BUDGET, plan=plan)
    replanned_request = _request("2", parent="g3", domain="prices", staged="planned")
    replanned = plan_generation_bulk(connection, replanned_request, budget=BUDGET)
    assert replanned.marker["parent_id"] == "g3"
    assert replanned.marker["chain_hash"] != plan.marker["chain_hash"]
    # A stale reviewed plan cannot commit under the new parent either.
    with pytest.raises(PlanChangedError):
        publish_generation_bulk(connection, replanned_request, budget=BUDGET, plan=plan)
    marker = publish_generation_bulk(connection, replanned_request, budget=BUDGET, plan=replanned)
    assert marker == dict(replanned.marker)
    assert market.verify_generation(connection, "g2") == marker
    connection.close()


def test_leftover_generation_is_not_adopted(tmp_path: Path) -> None:
    rng = random.Random(17)
    rows = _first(rng, "prices", 4)
    connection = _store(tmp_path / "market.duckdb")
    marker = _bulk(connection, rows, "1", parent=None, domain="prices")
    # The same request again reuses the committed generation and writes nothing.
    plan = plan_generation_bulk(
        connection, _request("1", parent=None, domain="prices"), budget=BUDGET
    )
    assert plan.reused
    assert dict(plan.marker) == marker
    assert _bulk(connection, rows, "1", parent=None, domain="prices") == marker
    assert connection.execute("SELECT count(*) FROM prices").fetchone() == (4,)
    # Other content under the leftover generation's IDs is refused, not adopted.
    _stage(connection, "prices", rows[:3])
    with pytest.raises(ValueError, match="conflicts with existing content"):
        publish_generation_bulk(
            connection, _request("1", parent=None, domain="prices"), budget=BUDGET
        )
    other = BulkRequest("synthetic", "9", "g9", "op1", "9" * 64, "g1", "prices", "staged")
    with pytest.raises(ValueError, match="conflicts with existing content"):
        publish_generation_bulk(connection, other, budget=BUDGET)
    # A request planned before the leftover landed fails the parent CAS.
    with pytest.raises(ParentChangedError):
        publish_generation_bulk(
            connection, _request("2", parent=None, domain="prices"), budget=BUDGET
        )
    assert connection.execute("SELECT count(*) FROM market_generations").fetchone() == (1,)
    connection.close()


def test_incremental_verify_checks_links_and_leaf_rows(tmp_path: Path) -> None:
    rng = random.Random(19)
    first = _first(rng, "prices", 6)
    connection = _store(tmp_path / "market.duckdb")
    _bulk(connection, first, "1", parent=None, domain="prices")
    _bulk(connection, _second(rng, "prices", first), "2", parent="g1", domain="prices")
    assert verify_generation_bulk(connection, "g2", budget=BUDGET)["generation_id"] == "g2"
    # Ancestor rows are rehashed only when the verification is deep.
    connection.execute(
        "UPDATE prices SET currency='XXX' WHERE generation_id='g1' AND record_id=?",
        [first[0]["record_id"]],
    )
    verify_generation_bulk(connection, "g2", budget=BUDGET)
    with pytest.raises(ValueError, match="hash/count mismatch"):
        verify_generation_bulk(connection, "g2", budget=BUDGET, deep=True)
    with pytest.raises(ValueError, match="hash/count mismatch"):
        market.verify_generation(connection, "g2")
    connection.execute(
        "UPDATE prices SET currency=? WHERE generation_id='g1' AND record_id=?",
        [first[0]["currency"], first[0]["record_id"]],
    )
    verify_generation_bulk(connection, "g2", budget=BUDGET, deep=True)
    # A broken link anywhere in the chain fails the shallow verification.
    connection.execute(
        "UPDATE market_generations SET request_hash=? WHERE generation_id='g1'", ["f" * 64]
    )
    with pytest.raises(ValueError, match="hash/count mismatch"):
        verify_generation_bulk(connection, "g2", budget=BUDGET)
    connection.close()


def test_batches_fit_the_default_budget_at_ten_million_rows() -> None:
    # The widest prices row this vertical stages: digests, ids and full decimals.
    columns = market.delta_columns("prices", fields=True)
    widths = {"generation_id": 69, "record_id": 69, "revision_id": 69, "source_row_hash": 69}
    row_bytes = sum(
        widths.get(name, 5 + 64) if kind == "text" else bulk_generation._FIXED_WIDTH[kind]
        for name, kind in market.rowset_schema(columns)
    )
    stats = bulk_generation._Stats(
        count=10_000_000, close_rows=False, row_bytes=row_bytes, identity_characters=320
    )
    hash_rows, identity_rows = bulk_generation._batch_rows(stats, BUDGET)
    assert hash_rows >= 10_000
    assert identity_rows >= 10_000
    hash_charge = hash_rows * (2 * row_bytes + bulk_generation._ROW_OBJECT_BYTES) + row_bytes
    assert hash_charge <= BUDGET.available_bytes
    # The charge depends on the widest row, never on the row count.
    assert bulk_generation._batch_rows(
        bulk_generation._Stats(
            count=10, close_rows=False, row_bytes=row_bytes, identity_characters=320
        ),
        BUDGET,
    ) == (hash_rows, identity_rows)
    tight = ComputeBudget(Fraction(1), 4 * 1024 * 1024)
    assert 0 < bulk_generation._batch_rows(stats, tight)[0] < hash_rows
    with pytest.raises(ComputeResourceError, match="exceeds the admitted"):
        bulk_generation._batch_rows(
            bulk_generation._Stats(
                count=1, close_rows=False, row_bytes=tight.available_bytes, identity_characters=0
            ),
            tight,
        )


def test_streaming_memory_stays_within_the_admitted_allowance(tmp_path: Path) -> None:
    connection = _store(tmp_path / "market.duckdb")
    rows = 100_000
    connection.execute(
        f"""CREATE TABLE staged AS
        SELECT sha256(json_array('aas-record-v1', 'prices', json_array(
                 json_array('instrument_id', 'I' || i), json_array('session_date', '2026-01-02'),
                 json_array('interval', '1d'), json_array('bar_end_us', 20),
                 json_array('basis', 'unadjusted'), json_array('currency', 'USD'),
                 json_array('price_role', 'canonical')))::VARCHAR) AS record_id,
               sha256('rev' || i) AS revision_id, NULL::VARCHAR AS supersedes_revision_id,
               'ASSERT' AS op, 10::BIGINT AS available_at_us, 10::BIGINT AS revision_known_at_us,
               20::BIGINT AS ingested_at_us, 'synthetic' AS source_snapshot_id,
               sha256('src' || i) AS source_row_hash, 'I' || i AS instrument_id,
               DATE '2026-01-02' AS session_date, '1d' AS "interval", 20::BIGINT AS bar_end_us,
               'unadjusted' AS basis, 'USD' AS currency,
               (i / 7)::DECIMAL(38,12) AS "open", (i / 3)::DECIMAL(38,12) AS high,
               (i / 9)::DECIMAL(38,12) AS low, (i / 5)::DECIMAL(38,12) AS "close",
               i::DECIMAL(38,12) AS volume, 'canonical' AS price_role, 'present' AS value_state
        FROM range({rows}) t(i)"""
    )
    # Python keeps a quarter of the allocation, 32 MiB here, which is less than the
    # delta's encoded rows alone, so only a streamed digest can plan it. Planning does
    # every Python-side step of a publication: validation, identities and the digest.
    budget = ComputeBudget(Fraction(1), 128 * 1024 * 1024)
    encoded = bulk_generation._stats(
        connection, "prices", '"staged"', fields=False, generation_id="g1"
    )
    assert encoded.count * encoded.row_bytes > budget.available_bytes
    request = _request("1", parent=None, domain="prices")
    tracemalloc.start()
    try:
        plan = plan_generation_bulk(connection, request, budget=budget)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert plan.marker["row_count"] == rows
    assert peak <= budget.available_bytes
    # The constraint indexes the insert maintains are DuckDB's share, sized separately;
    # the lowered limit stays on a connection, so the larger allocation takes a new one.
    connection.close()
    connection = duckdb.connect(str(tmp_path / "market.duckdb"))
    marker = publish_generation_bulk(
        connection, request, budget=ComputeBudget(Fraction(1), 1024 * 1024 * 1024), plan=plan
    )
    assert market.verify_generation(connection, "g1") == marker
    connection.close()


def test_staged_relation_must_match_the_domain(tmp_path: Path) -> None:
    connection = _store(tmp_path / "market.duckdb")
    rows = _first(random.Random(23), "prices", 2)
    _stage(connection, "prices", rows)
    connection.execute("ALTER TABLE staged ADD COLUMN extra VARCHAR")
    with pytest.raises(ValueError, match="exactly the domain's typed columns"):
        plan_generation_bulk(connection, _request("1", parent=None, domain="prices"), budget=BUDGET)
    _stage(connection, "prices", [])
    with pytest.raises(ValueError, match="empty generation"):
        plan_generation_bulk(connection, _request("1", parent=None, domain="prices"), budget=BUDGET)
    with pytest.raises(ValueError, match="plain SQL names"):
        plan_generation_bulk(
            connection,
            _request("1", parent=None, domain="prices", staged='x"; DROP TABLE prices; --'),
            budget=BUDGET,
        )
    with pytest.raises(ValueError, match="outside the market tables"):
        plan_generation_bulk(
            connection, _request("1", parent=None, domain="prices", staged="prices"), budget=BUDGET
        )
    connection.close()


def test_sql_record_identity_matches_python() -> None:
    connection = duckdb.connect()
    rng = random.Random(29)
    plain_text = "".join(chr(point) for point in range(0x20, 0x7F))
    for domain in sorted(DOMAINS):
        rows = [_domain_row(rng, domain, index) for index in range(300)]
        for row in rows[::2]:
            # Half the keys are plain printable ASCII, quotes and backslashes included.
            for name in NATURAL_KEYS[domain]:
                if isinstance(row[name], str):
                    row[name] = "".join(rng.choice(plain_text) for _ in range(1 + rng.randrange(9)))
        columns = [(name, kind.rstrip("?")) for name, kind in DOMAINS[domain]]
        connection.execute(
            "CREATE OR REPLACE TABLE keys ("
            + ", ".join(f'"{name}" {kind}' for name, kind in columns)
            + ")"
        )
        connection.executemany(
            "INSERT INTO keys VALUES (" + ",".join("?" for _ in columns) + ")",
            [[row[name] for name, _ in columns] for row in rows],
        )
        expected, plain = bulk_generation._identity_sql(domain)
        names = ", ".join(f'"{name}"' for name in NATURAL_KEYS[domain])
        checked = connection.execute(
            f"SELECT {expected}, {names} FROM keys WHERE {plain}"
        ).fetchall()
        assert len(checked) >= 60, domain
        for identity, *natural in checked:
            assert identity == market.record_identity(domain, natural), (domain, natural)
        escaped = connection.execute(f"SELECT count(*) FROM keys WHERE NOT ({plain})").fetchone()
        assert escaped is not None
        if any(kind == "VARCHAR" for name, kind in columns if name in NATURAL_KEYS[domain]):
            assert escaped[0] > 0, domain
    connection.close()


@pytest.mark.parametrize("plain", [True, False])
def test_wrong_record_identity_is_refused_on_both_paths(tmp_path: Path, *, plain: bool) -> None:
    rows = _first(random.Random(31), "prices", 3)
    rows[1] = _identify("prices", {**rows[1], "instrument_id": "PLAIN" if plain else 'é"\\'})
    rows[1]["record_id"] = market.record_identity(
        "prices", ["other", *[rows[1][name] for name in NATURAL_KEYS["prices"][1:]]]
    )
    connection = _store(tmp_path / "market.duckdb")
    _stage(connection, "prices", rows)
    with pytest.raises(ValueError, match="record identity does not match"):
        plan_generation_bulk(connection, _request("1", parent=None, domain="prices"), budget=BUDGET)
    connection.close()
