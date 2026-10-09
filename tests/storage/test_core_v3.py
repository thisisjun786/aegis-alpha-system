# ruff: noqa: PLR2004, S311, S608, SLF001 -- seeded synthetic fixtures, test-owned SQL, private helpers
"""Core schema v3: the domain tables and quality_flags without their key indexes.

v3 rebuilds the eleven domain tables and ``quality_flags`` in one market transaction,
keeping every column, default, CHECK, NOT NULL and foreign key, and every row and
recorded hash. Fresh and migrated stores hold the same catalog. A publication's DuckDB
share then follows its own delta rather than every key the table has ever stored, which
is what let the rehearsal's COMMIT run out of memory. Every case uses a disposable
installation or store this module created.
"""

from __future__ import annotations

import json
import os
import random
import subprocess
import sys
from collections.abc import Callable
from decimal import Decimal
from fractions import Fraction
from pathlib import Path
from typing import Final, cast

import duckdb
import pytest

from aegis_alpha.compute_resources import ComputeBudget, ComputeResourceError
from aegis_alpha.storage import market, migration, publication, workspace
from aegis_alpha.storage.bulk_generation import (
    BulkPlan,
    BulkRequest,
    publish_generation_bulk,
    verify_generation_bulk,
)
from aegis_alpha.storage.compaction import compact
from aegis_alpha.storage.import_document import parse_import
from aegis_alpha.storage.market_integrity import _catalog, audit_market, check_core_catalog
from aegis_alpha.storage.market_schema import (
    DDL,
    DOMAINS,
    MIGRATIONS,
    V2_DDL,
    V3_DDL,
    domain_ddl,
)
from aegis_alpha.storage.migration import (
    CORE_VERSION,
    inspect_core_schema,
    migrate_core_schema,
    plan_core_migration,
    step_operation,
)
from aegis_alpha.storage.paths import load_paths
from aegis_alpha.storage.promotion import engine
from aegis_alpha.storage.promotion.engine import promote
from aegis_alpha.storage.publication import recover_operations
from aegis_alpha.storage.state import get_operation
from aegis_alpha.storage.verification import verify_workspace
from aegis_alpha.storage.workspace import initialize, open_workspace
from tests.storage.capacity_support import PIN_BLOCK, FailingCommit
from tests.storage.promotion_support import add_source, at, bar, register_symbols, spec
from tests.storage.test_bulk_generation import _first, _second, _stage
from tests.storage.test_migration import (
    RECORDED_MARKET,
    close_row,
    file_digest,
    price,
    v1_installation,
)
from tests.storage.test_migration import publish as publish_close
from tests.storage.test_migration_steps import pending_promotion
from tests.storage.test_publication import document

BUDGET: Final = ComputeBudget(Fraction(1), 512 * 1024 * 1024)
_KILLED: Final = 137
_ROOT: Final = Path(__file__).resolve().parents[2]
_REBUILT: Final = (*DOMAINS, "quality_flags")
_KEYS: Final = ("PRIMARY KEY", "UNIQUE")
_KEY_LINE: Final = (
    "\n PRIMARY KEY(generation_id,record_id,revision_id), UNIQUE(record_id,revision_id),"
)
# A tick-rounded close, so the promotion stores a quality flag.
_TICKED: Final = 2649999.947


def _core(connection: duckdb.DuckDBPyConnection) -> dict[str, object]:
    """The core-owned part of a store's catalog: the tables an empty store of v3 holds."""
    catalog = _catalog(connection)
    return {name: catalog.get(name) for name in _catalog(_connection())}


def _connection(version: int | None = None) -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect(config={"threads": 1})
    market.initialize_market(connection, "synthetic", version=version)
    return connection


# --- the schema ----------------------------------------------------------------------


def test_v1_and_v2_texts_stay_the_recorded_bytes() -> None:
    assert MIGRATIONS == (DDL, V2_DDL, V3_DDL)
    assert CORE_VERSION == len(MIGRATIONS) == 3
    assert market.MARKET_CHECKSUMS == RECORDED_MARKET
    # The keyed text is still exactly what v1 and v2 hold; v3 differs by the key line only.
    for name, columns in DOMAINS.items():
        keyed = domain_ddl(name, columns, fields=name == "prices")
        keyless = domain_ddl(name, columns, fields=name == "prices", keys=False)
        assert keyed in DDL + V2_DDL or name == "prices"
        assert keyed.replace(_KEY_LINE, "") == keyless
        assert keyless in V3_DDL
    # No table is rebuilt by copying a query result, which would drop its constraints.
    assert " AS SELECT" not in V3_DDL.upper()


def test_v3_drops_only_the_key_indexes() -> None:
    v2, v3 = _connection(2), _connection()
    assert market.market_version(v3) == 3
    before, after = _catalog(v2), _catalog(v3)
    assert before.keys() == after.keys()
    for table, (columns, constraints) in before.items():
        # Columns keep their order, types, nullability and defaults (prices.fields too).
        assert after[table][0] == columns, table
        if table in _REBUILT:
            assert any(item[0] in _KEYS for item in constraints), table
            assert after[table][1] == tuple(item for item in constraints if item[0] not in _KEYS)
            kinds = {item[0] for item in after[table][1]}
            assert {"FOREIGN KEY", "NOT NULL"} <= kinds, table
            assert table == "quality_flags" or "CHECK" in kinds
        else:
            # market_generations, the receipts and the result tables keep every key.
            assert after[table][1] == constraints, table
    columns = cast("tuple[tuple[object, ...], ...]", after["prices"][0])
    assert ("fields", "VARCHAR", False, "'ohlcv'") in [column[1:] for column in columns]
    v2.close()
    v3.close()


def test_a_v3_store_holds_every_rule_but_the_keys() -> None:
    connection = _connection()
    common = "'g','r','v',NULL,'ASSERT',NULL,NULL,0,'s','" + "c" * 64 + "'"
    for sql in (
        # The foreign key: no rows of a generation without its marker.
        f"INSERT INTO filings VALUES ({common},'i','f','10-K','2026-01-02',1,NULL)",
        "INSERT INTO quality_flags VALUES ('absent','r','v','rule','1','flag',NULL)",
    ):
        with pytest.raises(duckdb.ConstraintException, match="foreign key"):
            connection.execute(sql)
    connection.execute(
        "INSERT INTO market_generations VALUES ('g','d','1',NULL,1,'filings','s',?,?,1,'op',?)",
        ["a" * 64] * 3,
    )
    for sql in (
        f"INSERT INTO filings VALUES ({common},'i','f','10-K','2026-01-02',-1,NULL)",
        f"INSERT INTO filings VALUES ({common},'i','f',NULL,'2026-01-02',1,NULL)",
        (
            f"INSERT INTO filings VALUES ({common.replace('ASSERT', 'SUPERSEDE')},'i','f',"
            "'10-K','2026-01-02',1,NULL)"
        ),
    ):
        with pytest.raises(duckdb.ConstraintException):
            connection.execute(sql)
    # The same pair twice is not refused by the table any more; the writers refuse it.
    row = f"INSERT INTO filings VALUES ({common},'i','f','10-K','2026-01-02',1,NULL)"
    connection.execute(row)
    connection.execute(row)
    connection.close()


# --- migration ---------------------------------------------------------------------


def _bulk(
    connection: duckdb.DuckDBPyConnection,
    rows: list[dict[str, object]],
    domain: str,
    version: str,
    parent: str | None,
) -> None:
    _stage(connection, domain, rows)
    publish_generation_bulk(
        connection,
        BulkRequest(
            dataset_id=f"synthetic.{domain}",
            version=version,
            generation_id=f"{domain}-g{version}",
            operation_id=f"{domain}-op{version}",
            request_hash=version * 64,
            parent_id=None if parent is None else f"{domain}-g{parent}",
            domain=domain,
            staged="staged",
        ),
        budget=BUDGET,
    )
    connection.execute('DROP TABLE "staged"')


def _promotion(admitted: workspace.Workspace) -> dict[str, object]:
    """One promoted generation whose tick-rounded close leaves a quality flag."""
    day = at("2025-01-02T00:00:00").date()
    late = at("2025-01-10T00:00:00")
    pin = add_source(
        admitted, [bar("AAA.KO", day, _TICKED, retrieved=late)], tag="flagged", linked=late
    )
    identity = register_symbols(admitted, pin["source_id"])
    return promote(admitted, *spec([pin], identity), apply=True)


def _close_document() -> bytes:
    """An imported generation of one close-only reference price, a shape only v2 holds."""
    body = json.loads(document())
    body.update(
        dataset_id="close-prices",
        generation_id="close-only-generation",
        operation_id="close-only-import",
        rows=[
            price(
                "2026-01-08",
                basis="split_adjusted",
                price_role="reference",
                fields="close",
                open=None,
                high=None,
                low=None,
                volume=None,
                revision_id="close-only-r1",
            )
        ],
    )
    return json.dumps(body).encode()


def v2_installation(root: Path) -> Path:
    """A cataloged v2 installation: every price shape and a promotion with quality flags.

    The v1-era documents (present, missing and reference prices) were migrated to v2; a
    close-only price and the promotion were published on v2. Every generation is
    cataloged, so its backups and compaction admit it.
    """
    home = v1_installation(root)
    migrate_core_schema(home, to_version=2, backup_output=root / "v2-backup")
    with open_workspace(home, writable=True, strategy_write=True) as admitted:
        publication.publish_document(admitted, parse_import(_close_document()))
        _promotion(admitted)
    return home


def _stored(home: Path) -> dict[str, object]:
    """Every marker, every rebuilt table's rows as a multiset, and the deep verification."""
    with open_workspace(home) as admitted:
        connection = admitted.market
        return {
            "markers": connection.execute(
                "SELECT * FROM market_generations ORDER BY generation_id"
            ).fetchall(),
            "rows": {
                table: connection.execute(f'SELECT * FROM "{table}" ORDER BY ALL').fetchall()
                for table in _REBUILT
            },
            "verification": verify_workspace(admitted, deep=True),
        }


def test_migration_to_v3_keeps_every_row_and_hash(tmp_path: Path) -> None:
    home = v2_installation(tmp_path)
    before = _stored(home)
    rows = cast("dict[str, list[object]]", before["rows"])
    assert rows["prices"]
    assert rows["quality_flags"]
    plan = plan_core_migration(home, to_version=3)
    assert (plan["state"], plan["steps"]) == ("outdated", list(migration._STEPS))
    report = migrate_core_schema(home, to_version=3, backup_output=tmp_path / "v3-backup")
    assert (report["state"], report["market_version"], report["operation_id"]) == (
        "current",
        3,
        step_operation(3),
    )
    # Markers, rows and their recorded hashes are unchanged, and the deep verification,
    # which rehashes every delta and audits the new key checks, reports the same.
    assert _stored(home) == before
    with open_workspace(home, writable=True) as admitted:
        assert admitted.market.execute(
            "SELECT count(*) FROM duckdb_tables() WHERE ends_with(table_name, '_v2_rebuild')"
        ).fetchone() == (0,)
        check_core_catalog(admitted.market)


def test_the_v3_rebuild_keeps_every_domain_row_and_hash(tmp_path: Path) -> None:
    """Every domain, every op and a close-only price keep their rows and recorded hashes."""
    connection = duckdb.connect(str(tmp_path / "market.duckdb"), config={"threads": 1})
    market.initialize_market(connection, "synthetic", version=2)
    for seed, domain in enumerate(sorted(DOMAINS)):
        rng = random.Random(seed)
        first = _first(rng, domain, 12)
        _bulk(connection, first, domain, "1", None)
        _bulk(connection, _second(rng, domain, first), domain, "2", "1")
    publish_close(connection, close_row())
    connection.execute(
        "INSERT INTO quality_flags SELECT generation_id, record_id, revision_id, 'rule', '1', "
        "'flag', NULL FROM prices"
    )
    generations = [
        str(row[0])
        for row in connection.execute(
            "SELECT generation_id FROM market_generations ORDER BY 1"
        ).fetchall()
    ]
    assert {
        str(row[0]) for row in connection.execute("SELECT DISTINCT op FROM prices").fetchall()
    } == {"ASSERT", "SUPERSEDE", "TOMBSTONE"}

    def stored() -> tuple[object, ...]:
        return (
            connection.execute("SELECT * FROM market_generations ORDER BY ALL").fetchall(),
            {
                table: connection.execute(f'SELECT * FROM "{table}" ORDER BY ALL').fetchall()
                for table in _REBUILT
            },
            [
                verify_generation_bulk(connection, generation, budget=BUDGET, deep=True)
                for generation in generations
            ],
            [market.verify_generation(connection, generation) for generation in generations],
        )

    before = stored()
    market.upgrade_market(connection, "synthetic", 3)
    assert stored() == before
    check_core_catalog(connection)
    audit_market(connection, BUDGET, deep=True)
    # The next publication lands on the rebuilt tables, chained to a migrated head.
    rng = random.Random(99)
    _bulk(connection, _first(rng, "filings", 6), "filings", "3", "2")
    assert market.verify_generation(connection, "filings-g3")["parent_id"] == "filings-g2"
    connection.close()


def test_fresh_and_migrated_v3_stores_hold_the_same_catalog(tmp_path: Path) -> None:
    fresh = tmp_path / "fresh"
    initialize(fresh)
    from_v1 = v1_installation(tmp_path / "v1")
    migrate_core_schema(from_v1, to_version=3, backup_output=tmp_path / "v1-backups")
    from_v2 = v2_installation(tmp_path / "v2")
    migrate_core_schema(from_v2, to_version=3, backup_output=tmp_path / "v2-backup")
    catalogs = []
    for home in (fresh, from_v1, from_v2):
        with open_workspace(home) as admitted:
            check_core_catalog(admitted.market)
            catalogs.append(_core(admitted.market))
            assert inspect_core_schema(admitted).state == "current"
    assert catalogs[0] == catalogs[1] == catalogs[2]


def _value(column: str, replacement: str) -> Callable[[str], str]:
    """Copy ``column`` as ``replacement``: a value changes, every count stays."""

    def change(insert: str) -> str:
        head, select = insert.split(" SELECT ", 1)
        return head + " SELECT " + select.replace(f'"{column}"', replacement, 1)

    return change


def _lost(generation: str) -> Callable[[str], str]:
    """Leave out one row of ``generation``, keeping every other generation whole."""

    def change(insert: str) -> str:
        aside = insert.split(" FROM ", 1)[1].removesuffix(";")
        return (
            insert.removesuffix(";")
            + f' WHERE "record_id" <> (SELECT min("record_id") FROM {aside} '
            + f"WHERE generation_id = '{generation}');"
        )

    return change


@pytest.mark.parametrize(
    ("table", "change"),
    [
        ("prices", _value("close", '-"close"')),
        ("quality_flags", _value("detail", "coalesce(\"detail\", 'x')")),
        ("filings", _lost("filings-g1")),
    ],
)
def test_the_rebuild_refuses_a_copy_that_changed_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, table: str, change: Callable[[str], str]
) -> None:
    """The copy is compared with the old rows value by value, not by counts alone."""
    connection = duckdb.connect(str(tmp_path / "market.duckdb"), config={"threads": 1})
    market.initialize_market(connection, "synthetic", version=2)
    for seed, domain in enumerate(("prices", "filings")):
        rng = random.Random(seed)
        first = _first(rng, domain, 8)
        _bulk(connection, first, domain, "1", None)
        _bulk(connection, _second(rng, domain, first), domain, "2", "1")
    connection.execute(
        "INSERT INTO quality_flags SELECT generation_id, record_id, revision_id, 'rule', '1', "
        "'flag', NULL FROM prices"
    )
    insert = next(
        line for line in V3_DDL.splitlines() if line.startswith(f'INSERT INTO "{table}" ')
    )
    assert change(insert) != insert
    texts = (DDL, V2_DDL, V3_DDL.replace(insert, change(insert)))
    before = {
        name: connection.execute(f'SELECT * FROM "{name}" ORDER BY ALL').fetchall()
        for name in _REBUILT
    }
    monkeypatch.setattr(market, "MIGRATIONS", texts)
    with pytest.raises(duckdb.Error, match=f"v3 rebuild of {table} changed its rows"):
        market.upgrade_market(connection, "synthetic", 3)
    # The whole transaction rolled back: still v2, every row in place, nothing set aside.
    assert market.market_version(connection) == 2
    assert {
        name: connection.execute(f'SELECT * FROM "{name}" ORDER BY ALL').fetchall()
        for name in _REBUILT
    } == before
    assert connection.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE ends_with(table_name, '_v2_rebuild')"
    ).fetchone() == (0,)
    connection.close()


@pytest.mark.parametrize(
    "case",
    [
        # Two DECIMAL(38,12) values DuckDB hashes alike, so a count and hash sum agree.
        ("prices", "close", "0.000000000001", "18446744.073709551616"),
        # DuckDB's equality holds these equal, though they are different stored values.
        ("feature_values", "value", "0.0::DOUBLE", "-0.0::DOUBLE"),
    ],
)
def test_the_rebuild_compares_values_not_their_hashes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, case: tuple[str, str, str, str]
) -> None:
    domain, column, stored, copied = case
    connection = duckdb.connect(str(tmp_path / "market.duckdb"), config={"threads": 1})
    market.initialize_market(connection, "synthetic", version=2)
    _bulk(connection, _first(random.Random(3), domain, 8), domain, "1", None)
    connection.execute(
        f'UPDATE "{domain}" SET "{column}" = {stored} WHERE record_id = '
        f'(SELECT min(record_id) FROM "{domain}")'
    )
    if domain == "prices":
        assert connection.execute(
            f"SELECT hash({stored}::DECIMAL(38,12)) = hash({copied}::DECIMAL(38,12))"
        ).fetchone() == (True,)
    else:
        assert connection.execute(f"SELECT {stored} = {copied}").fetchone() == (True,)
    insert = next(
        line for line in V3_DDL.splitlines() if line.startswith(f'INSERT INTO "{domain}" ')
    )
    changed = _value(column, f'CASE WHEN "{column}" = {stored} THEN {copied} ELSE "{column}" END')(
        insert
    )
    monkeypatch.setattr(market, "MIGRATIONS", (DDL, V2_DDL, V3_DDL.replace(insert, changed)))
    with pytest.raises(duckdb.Error, match=f"v3 rebuild of {domain} changed its rows"):
        market.upgrade_market(connection, "synthetic", 3)
    assert market.market_version(connection) == 2
    connection.close()


def test_the_rebuild_refuses_a_connection_that_may_reorder_its_copy(tmp_path: Path) -> None:
    """The copy is compared row by row in order, so its order must be the old table's."""
    connection = duckdb.connect(str(tmp_path / "market.duckdb"), config={"threads": 1})
    market.initialize_market(connection, "synthetic", version=2)
    connection.execute("SET preserve_insertion_order = false")
    with pytest.raises(duckdb.Error, match="needs preserve_insertion_order"):
        market.upgrade_market(connection, "synthetic", 3)
    assert market.market_version(connection) == 2
    connection.close()


def _corrupted_close(home: Path) -> tuple[str, object]:
    """The record whose close the cases below change, and its stored close."""
    with duckdb.connect(str(load_paths(home).market), read_only=True) as connection:
        found = connection.execute(
            "SELECT record_id, close FROM prices WHERE close IS NOT NULL ORDER BY record_id LIMIT 1"
        ).fetchone()
    assert found is not None
    return str(found[0]), found[1]


def _set_close(connection: duckdb.DuckDBPyConnection, record: str, close: object) -> None:
    connection.execute("UPDATE prices SET close = ? WHERE record_id = ?", [close, record])


def test_a_landed_step_whose_rows_differ_stays_prepared(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The step completes only once every committed generation rehashes to its digest.

    Once in the run that commits the market, and again in a run that resumes after an
    earlier one's COMMIT: a value changed after the rebuild is refused both times, with
    no deep flag passed, and the step completes once the value is back.
    """
    home = v2_installation(tmp_path)
    before = _stored(home)
    record, close = _corrupted_close(home)

    def corrupted(connection: duckdb.DuckDBPyConnection, installation_id: str, target: int) -> int:
        landed = market.upgrade_market(connection, installation_id, target)
        _set_close(connection, record, Decimal(str(close)) + 1)
        return landed

    with monkeypatch.context() as patch:
        patch.setattr(migration, "upgrade_market", corrupted)
        with pytest.raises(ValueError, match="logical hash/count mismatch"):
            migrate_core_schema(home, to_version=3, backup_output=tmp_path / "v3-backup")
    plan = plan_core_migration(home, to_version=3)
    assert (plan["state"], plan["market_version"], plan["migration_operation"]) == (
        "incomplete",
        3,
        step_operation(3),
    )
    with pytest.raises(ValueError, match="logical hash/count mismatch"):
        migrate_core_schema(home, to_version=3, backup_output=None)
    assert plan_core_migration(home, to_version=3)["state"] == "incomplete"
    with duckdb.connect(str(load_paths(home).market)) as connection:
        _set_close(connection, record, close)
    report = migrate_core_schema(home, to_version=3, backup_output=None)
    assert report["state"] == "current"
    assert _stored(home) == before


def test_a_resumed_step_whose_rows_changed_after_commit_stays_prepared(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A run that died after the market's COMMIT leaves rows a resume must still prove."""
    home = v2_installation(tmp_path)
    record, close = _corrupted_close(home)

    def died(*_: object) -> None:
        raise KeyboardInterrupt

    with monkeypatch.context() as patch:
        patch.setattr(migration, "_upgrade_state", died)
        with pytest.raises(KeyboardInterrupt):
            migrate_core_schema(home, to_version=3, backup_output=tmp_path / "v3-backup")
    assert plan_core_migration(home, to_version=3)["market_version"] == 3
    with duckdb.connect(str(load_paths(home).market)) as connection:
        _set_close(connection, record, Decimal(str(close)) + 1)
    with pytest.raises(ValueError, match="logical hash/count mismatch"):
        migrate_core_schema(home, to_version=3, backup_output=None)
    plan = plan_core_migration(home, to_version=3)
    assert (plan["state"], plan["migration_operation"]) == ("incomplete", step_operation(3))
    with duckdb.connect(str(load_paths(home).market)) as connection:
        _set_close(connection, record, close)
    assert migrate_core_schema(home, to_version=3, backup_output=None)["state"] == "current"


def test_an_exhausted_v3_commit_is_a_budget_error_and_resumes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    home = v2_installation(tmp_path)
    before = _stored(home)
    commits: list[FailingCommit] = []

    def exhausted(connection: duckdb.DuckDBPyConnection, installation_id: str, target: int) -> int:
        failing = FailingCommit(connection)
        commits.append(failing)
        return market.upgrade_market(failing.borrowed, installation_id, target)

    budget = ComputeBudget(Fraction(1), 64 * 1024 * 1024)
    with monkeypatch.context() as patch:
        patch.setattr(migration, "upgrade_market", exhausted)
        with pytest.raises(ComputeResourceError, match="core schema migration") as caught:
            migrate_core_schema(
                home, to_version=3, backup_output=tmp_path / "v3-backup", budget=budget
            )
    assert str(caught.value.__cause__) == PIN_BLOCK
    assert [failing.commits for failing in commits] == [1]
    plan = plan_core_migration(home, to_version=3)
    assert (plan["state"], plan["market_version"], plan["migration_operation"]) == (
        "incomplete",
        2,
        step_operation(3),
    )
    with pytest.raises(ValueError, match="repeat aas db migrate --to 3"):
        open_workspace(home).__enter__()
    # A COMMIT failure that is not about memory keeps its own error.
    conflict = "TransactionContext Error: Failed to commit: Conflict on tuple deletion"
    with monkeypatch.context() as patch:
        patch.setattr(
            migration,
            "upgrade_market",
            lambda connection, installation_id, target: market.upgrade_market(
                FailingCommit(connection, conflict).borrowed, installation_id, target
            ),
        )
        with pytest.raises(duckdb.TransactionException, match="Conflict on tuple deletion"):
            migrate_core_schema(home, to_version=3, backup_output=None, budget=budget)
    report = migrate_core_schema(home, to_version=3, backup_output=None, budget=budget)
    assert (report["state"], report["backup_manifest_sha256"]) == (
        "current",
        file_digest(tmp_path / "v3-backup" / "backup.json"),
    )
    assert _stored(home) == before


def test_a_landed_step_with_another_catalog_stays_prepared(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The migration checks the rebuilt catalog itself, not only a later verification."""
    home = v2_installation(tmp_path)

    def widened(connection: duckdb.DuckDBPyConnection, installation_id: str, target: int) -> int:
        landed = market.upgrade_market(connection, installation_id, target)
        connection.execute('ALTER TABLE filings ADD COLUMN "extra" VARCHAR')
        return landed

    with monkeypatch.context() as patch:
        patch.setattr(migration, "upgrade_market", widened)
        with pytest.raises(ValueError, match="core table filings differs"):
            migrate_core_schema(home, to_version=3, backup_output=tmp_path / "v3-backup")
    plan = plan_core_migration(home, to_version=3)
    assert (plan["state"], plan["market_version"], plan["migration_operation"]) == (
        "incomplete",
        3,
        step_operation(3),
    )
    # Repeating the command checks again and still refuses to complete the step.
    with pytest.raises(ValueError, match="core table filings differs"):
        migrate_core_schema(home, to_version=3, backup_output=None)


_CHILD: Final = """
import os, sys
from pathlib import Path
from aegis_alpha.storage import backup, market, migration
from aegis_alpha.storage.market_schema import V3_DDL
home, step, output = sys.argv[1:4]
def die(*args, **kwargs):
    os._exit(137)
def rebuilt_then_die(connection, *args):
    # The whole rebuild ran in the open transaction; the process dies before COMMIT.
    connection.execute("BEGIN TRANSACTION")
    connection.execute(V3_DDL)
    os._exit(137)
if step == "rebuilt":
    migration.upgrade_market = rebuilt_then_die
elif step == "backup_workspace":
    backup.backup_workspace = die
else:
    setattr(migration, step, die)
migration.migrate_core_schema(Path(home), to_version=3, backup_output=Path(output))
os._exit(0)
"""


@pytest.mark.parametrize(
    "step",
    [
        "backup_workspace",
        "_upgrade_market",
        "rebuilt",
        "_upgrade_state",
        "_write_receipt",
        "complete_operation",
    ],
)
def test_a_kill_at_every_boundary_of_the_v3_step_is_finished(tmp_path: Path, step: str) -> None:
    home = v2_installation(tmp_path)
    before = _stored(home)
    output = tmp_path / "v3-backup"
    assert (
        subprocess.run(  # noqa: S603 -- fixed interpreter and script, disposable home
            [sys.executable, "-c", _CHILD, str(home), step, str(output)],
            check=False,
            cwd=_ROOT,
            env=os.environ.copy(),
        ).returncode
        == _KILLED
    )
    plan = plan_core_migration(home, to_version=3)
    if step == "backup_workspace":
        # Killed before its intent: the step needs its own new backup directory.
        assert (plan["state"], plan["backup_required"]) == ("outdated", True)
        report = migrate_core_schema(home, to_version=3, backup_output=tmp_path / "again")
        assert report["backup_root"] == str(tmp_path / "again")
    else:
        assert (plan["state"], plan["backup_required"]) == ("incomplete", False)
        assert plan["market_version"] == (2 if step in {"_upgrade_market", "rebuilt"} else 3)
        with pytest.raises(ValueError, match="repeat aas db migrate --to 3"):
            open_workspace(home).__enter__()
        report = migrate_core_schema(home, to_version=3, backup_output=tmp_path / "unused")
        assert not (tmp_path / "unused").exists()
        assert report["backup_manifest_sha256"] == file_digest(output / "backup.json")
    assert report["state"] == "current"
    assert _stored(home) == before
    with open_workspace(home) as admitted:
        check_core_catalog(admitted.market)


# --- publication on v3 -------------------------------------------------------------


def _flags_of(generation: str, *, revision: str = "revision_id", copies: int = 1) -> str:
    one = (
        f"SELECT generation_id, record_id, {revision}, 'rule', '1', 'flag', NULL "
        f"FROM prices WHERE generation_id = '{generation}'"
    )
    return "INSERT INTO quality_flags " + " UNION ALL ".join([one] * copies)


def _companion(sql: str) -> Callable[[duckdb.DuckDBPyConnection], None]:
    def companion(connection: duckdb.DuckDBPyConnection) -> None:
        connection.execute(sql)

    return companion


@pytest.mark.parametrize(
    ("flags", "message"),
    [
        (_flags_of("prices-g2", copies=2), "repeats its key"),
        (_flags_of("prices-g2", revision="'missing'"), "does not store"),
        (_flags_of("prices-g1"), "changed another generation's quality flags"),
    ],
)
def test_the_bulk_writer_checks_what_its_companion_stored(flags: str, message: str) -> None:
    """With no flag key in v3, the public writer refuses a bad companion before COMMIT."""
    connection = _connection()
    rng = random.Random(9)
    first = _first(rng, "prices", 6)
    _bulk(connection, first, "prices", "1", None)
    _stage(connection, "prices", _second(rng, "prices", first))
    request = BulkRequest(
        dataset_id="synthetic.prices",
        version="2",
        generation_id="prices-g2",
        operation_id="prices-op2",
        request_hash="2" * 64,
        parent_id="prices-g1",
        domain="prices",
        staged="staged",
    )
    with pytest.raises(ValueError, match=message):
        publish_generation_bulk(connection, request, budget=BUDGET, companion=_companion(flags))
    # Marker, rows and flags all rolled back together.
    for table in ("market_generations", "prices", "quality_flags"):
        assert connection.execute(
            f"SELECT count(*) FROM {table} WHERE generation_id = 'prices-g2'"
        ).fetchone() == (0,)
    assert connection.execute("SELECT count(*) FROM quality_flags").fetchone() == (0,)
    marker = publish_generation_bulk(
        connection,
        request,
        budget=BUDGET,
        companion=_companion(_flags_of("prices-g2")),
    )
    assert marker["generation_id"] == "prices-g2"
    assert connection.execute("SELECT count(*) FROM quality_flags").fetchone() == (
        connection.execute("SELECT count(*) FROM prices WHERE generation_id='prices-g2'").fetchone()
    )
    audit_market(connection, BUDGET, deep=True)
    connection.close()


def test_a_publication_reads_only_its_staged_delta_and_flags(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Apply and recovery release the plan's other temp tables before publishing."""
    seen: list[set[str]] = []
    actual = engine.publish_generation_bulk

    def observed(
        connection: duckdb.DuckDBPyConnection,
        request: BulkRequest,
        *,
        budget: ComputeBudget,
        plan: BulkPlan | None = None,
        companion: Callable[[duckdb.DuckDBPyConnection], None] | None = None,
    ) -> dict[str, object]:
        seen.append(
            {
                str(row[0])
                for row in connection.execute(
                    "SELECT table_name FROM duckdb_tables() WHERE temporary"
                ).fetchall()
            }
        )
        return actual(connection, request, budget=budget, plan=plan, companion=companion)

    root = tmp_path / "apply"
    initialize(root)
    # Apply: the plan, then only the stage and the flags while it publishes.
    with monkeypatch.context() as patch:
        patch.setattr(engine, "publish_generation_bulk", observed)
        with open_workspace(root, writable=True, strategy_write=True) as admitted:
            applied = _promotion(admitted)
    assert applied["published"] is True
    assert seen == [{"_aas_p_stage", "_aas_p_flags"}]
    # Recovery publishes through the same release.
    home, planned_failed = pending_promotion(tmp_path / "recover")
    migrate_core_schema(home, to_version=3, backup_output=tmp_path / "snapshot")
    seen.clear()
    with monkeypatch.context() as patch:
        patch.setattr(engine, "publish_generation_bulk", observed)
        with open_workspace(home, writable=True) as admitted:
            assert recover_operations(admitted)["recovered"] == [planned_failed["operation_id"]]
            assert admitted.market.execute(
                "SELECT count(*) FROM duckdb_tables() WHERE temporary"
            ).fetchone() == (0,)
    assert seen == [{"_aas_p_stage", "_aas_p_flags"}]


def test_a_v2_promotion_replays_on_v3_with_the_same_hashes(tmp_path: Path) -> None:
    """v2 publication, migration, next publication equals the same chain promoted on v3."""

    def chain(home: Path, *, migrate: bool) -> list[tuple[object, ...]]:
        with open_workspace(home, writable=True, strategy_write=True) as admitted:
            first_pin = add_source(
                admitted,
                [
                    bar(
                        "AAA.KO",
                        at("2025-01-02T00:00:00").date(),
                        _TICKED,
                        retrieved=at("2025-01-10T00:00:00"),
                    )
                ],
                tag="first",
                linked=at("2025-01-10T00:00:00"),
            )
            identity = register_symbols(admitted, first_pin["source_id"])
            first = promote(admitted, *spec([first_pin], identity), apply=True)
        if migrate:
            migrate_core_schema(home, to_version=3, backup_output=home.parent / "v3-backup")
        with open_workspace(home, writable=True, strategy_write=True) as admitted:
            second_pin = add_source(
                admitted,
                [
                    bar(
                        "AAA.KO",
                        at("2025-01-02T00:00:00").date(),
                        2650001.0,
                        retrieved=at("2025-01-11T00:00:00"),
                    )
                ],
                tag="second",
                linked=at("2025-01-11T00:00:00"),
            )
            promote(
                admitted,
                *spec([second_pin], identity, parent=str(first["generation_id"])),
                apply=True,
            )
            return (
                [
                    tuple(row)
                    for row in admitted.market.execute(
                        "SELECT generation_id, request_hash, delta_hash, chain_hash, row_count, "
                        "operation_id FROM market_generations ORDER BY sequence"
                    ).fetchall()
                ]
                + [
                    tuple(row)
                    for row in admitted.state.execute(
                        "SELECT generation_id, chain_hash, manifest_hash FROM dataset_versions "
                        "ORDER BY generation_id"
                    )
                ]
                + [
                    tuple(row)
                    for row in admitted.market.execute(
                        "SELECT * FROM quality_flags ORDER BY ALL"
                    ).fetchall()
                ]
            )

    migrated = tmp_path / "migrated" / "home"
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(workspace, "_INSTALL_VERSION", 2)
        initialize(migrated)
    fresh = tmp_path / "fresh" / "home"
    initialize(fresh)
    assert chain(migrated, migrate=True) == chain(fresh, migrate=False)


def test_a_migrated_v3_installation_compacts_and_verifies_deep(tmp_path: Path) -> None:
    home = v2_installation(tmp_path)
    migrate_core_schema(home, to_version=3, backup_output=tmp_path / "v3-backup")
    small = ComputeBudget(Fraction(1), 64 * 1024 * 1024)
    result = compact(home, tmp_path / "compacted", budget=small, deep=True)
    with open_workspace(home) as admitted:
        assert result["verification"] == verify_workspace(admitted, deep=True)
    with open_workspace(tmp_path / "compacted") as admitted:
        check_core_catalog(admitted.market)
        assert _core(admitted.market) == _core(_connection())
        assert get_operation(admitted.state, step_operation(3)) is not None
