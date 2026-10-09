# ruff: noqa: PLR2004, S311, S608, SLF001 -- seeded synthetic fixtures, test-owned SQL, private audit parts
"""Generation integrity holds on key-less tables exactly where the v2 constraints held it.

A v2 store declares ``PRIMARY KEY(generation_id, record_id, revision_id)`` and
``UNIQUE(record_id, revision_id)`` on every domain and a six-column key on
``quality_flags``; v3 declares none of them. Each check here is proved twice: on a v2
store, where the declared key refuses the same write, and on a store whose domain tables
are rebuilt without those keys (and, where a test needs it, without the foreign keys),
where only ``market_integrity`` stands between the writer and a duplicate.
"""

from __future__ import annotations

import functools
import itertools
import json
import random
import re
import sys
from collections.abc import Callable
from dataclasses import replace
from fractions import Fraction
from pathlib import Path
from types import CodeType, FrameType, FunctionType
from typing import Final, cast

import duckdb
import pytest

from aegis_alpha.compute_resources import ComputeBudget, ComputeResourceError
from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.storage import bulk_generation, market, market_integrity, publication
from aegis_alpha.storage.backup import backup, restore
from aegis_alpha.storage.bulk_generation import (
    BulkFlags,
    BulkPlan,
    BulkRequest,
    publish_generation_bulk,
)
from aegis_alpha.storage.compaction import compact
from aegis_alpha.storage.import_document import parse_import
from aegis_alpha.storage.market_integrity import (
    audit_duplicates,
    audit_flag_references,
    audit_generation_counts,
    audit_market,
    begin_publication,
    check_core_catalog,
    check_generation_flags,
    check_inserted_generation,
    check_new_generation,
    commit_publication,
)
from aegis_alpha.storage.market_schema import (
    DOMAIN_VERSIONS,
    DOMAINS,
    QUALITY_FLAGS_DDL,
    domain_ddl,
)
from aegis_alpha.storage.promotion import engine
from aegis_alpha.storage.rowset import rowset_hash
from aegis_alpha.storage.verification import verify_workspace
from aegis_alpha.storage.workspace import initialize, open_workspace, store_info
from tests.storage.test_bulk_generation import _first, _request, _stage, inside_publication
from tests.storage.test_publication import document

BUDGET: Final = ComputeBudget(Fraction(1), 512 * 1024 * 1024)
_KEYS: Final = " PRIMARY KEY(generation_id,record_id,revision_id), UNIQUE(record_id,revision_id),"
_FOREIGN: Final = " FOREIGN KEY(generation_id) REFERENCES market_generations(generation_id),"
_FLAG_KEY: Final = ",\n PRIMARY KEY(generation_id,record_id,revision_id,rule_id,rule_version,flag)"
_OPEN: Final = "another generation publication is open on this market store"


def _store(path: Path, *, keys: bool = True, foreign: bool = True) -> duckdb.DuckDBPyConnection:
    """A v2 market store; without ``keys`` every domain and the flags lose their keys."""
    connection = duckdb.connect(str(path))
    market.initialize_market(connection, "synthetic", version=2)
    if keys and foreign:
        return connection
    for name, columns in DOMAINS.items():
        if DOMAIN_VERSIONS[name] > 2:
            continue
        ddl = domain_ddl(name, columns, fields=name == "prices")
        assert _KEYS in ddl
        assert _FOREIGN in ddl
        if not keys:
            ddl = ddl.replace(_KEYS, "")
        if not foreign:
            ddl = ddl.replace(_FOREIGN, "")
        connection.execute(f'DROP TABLE "{name}"')
        connection.execute(ddl)
    flags = QUALITY_FLAGS_DDL
    assert _FLAG_KEY in flags
    if not keys:
        flags = flags.replace(_FLAG_KEY, "")
    if not foreign:
        flags = flags.replace(" REFERENCES market_generations(generation_id)", "")
    connection.execute("DROP TABLE quality_flags")
    connection.execute(flags)
    return connection


def _price_rows() -> list[dict[str, object]]:
    return [dict(row) for row in parse_import(document()).rows]


def _publish(
    connection: duckdb.DuckDBPyConnection,
    rows: list[dict[str, object]],
    *,
    dataset: str = "synthetic",
    generation: str = "g1",
    parent: str | None = None,
) -> dict[str, object]:
    return market.publish_generation(
        connection,
        dataset_id=dataset,
        version=generation,
        generation_id=generation,
        operation_id="op-" + generation,
        request_hash="a" * 64,
        parent_id=parent,
        domain="prices",
        rows=[dict(row) for row in rows],
    )


def _revised(revision: str) -> list[dict[str, object]]:
    """The synthetic rows under another revision ID, which no other dataset holds."""
    return [{**row, "revision_id": revision} for row in _price_rows()]


def _count(connection: duckdb.DuckDBPyConnection, sql: str) -> int:
    row = connection.execute(sql).fetchone()
    assert row is not None
    return int(row[0])


# --- the writers -----------------------------------------------------------------


@pytest.mark.parametrize("keys", [True, False])
def test_python_writer_refuses_a_revision_another_dataset_holds(
    tmp_path: Path, *, keys: bool
) -> None:
    """A supplied revision ID is global: the parent chain alone would never see this pair."""
    connection = _store(tmp_path / "market.duckdb", keys=keys)
    rows = _price_rows()
    _publish(connection, rows)
    expected = (duckdb.ConstraintException,) if keys else (ValueError,)
    with pytest.raises(expected, match="Duplicate key" if keys else "duplicate market revision"):
        _publish(connection, rows, dataset="other", generation="o1")
    assert _count(connection, "SELECT count(*) FROM market_generations") == 1
    assert _count(connection, "SELECT count(*) FROM prices") == 1
    connection.close()


@pytest.mark.parametrize("keys", [True, False])
def test_bulk_writer_refuses_a_revision_another_dataset_holds(
    tmp_path: Path, *, keys: bool
) -> None:
    connection = _store(tmp_path / "market.duckdb", keys=keys)
    _stage(connection, "prices", _first(random.Random(7), "prices", 4))
    publish_generation_bulk(connection, _request("1", parent=None, domain="prices"), budget=BUDGET)
    other = replace(
        _request("1", parent=None, domain="prices"),
        dataset_id="other",
        generation_id="o1",
        operation_id="op-o1",
    )
    expected = (duckdb.ConstraintException,) if keys else (ValueError,)
    with pytest.raises(expected, match="Duplicate key" if keys else "duplicate market revision"):
        publish_generation_bulk(connection, other, budget=BUDGET)
    assert _count(connection, "SELECT count(*) FROM market_generations") == 1
    assert _count(connection, "SELECT count(*) FROM prices") == 4
    connection.close()


def test_without_the_check_a_keyless_store_commits_the_duplicate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The counterpart of the test above: nothing else in the writer refuses the pair."""
    connection = _store(tmp_path / "market.duckdb", keys=False)
    monkeypatch.setattr(bulk_generation, "check_inserted_generation", lambda *_: None)
    _stage(connection, "prices", _first(random.Random(7), "prices", 4))
    publish_generation_bulk(connection, _request("1", parent=None, domain="prices"), budget=BUDGET)
    other = replace(
        _request("1", parent=None, domain="prices"),
        dataset_id="other",
        generation_id="o1",
        operation_id="op-o1",
    )
    publish_generation_bulk(connection, other, budget=BUDGET)
    assert _count(connection, "SELECT count(*) FROM prices") == 8
    with pytest.raises(ValueError, match="stored more than once"):
        audit_duplicates(connection)
    connection.close()


def test_inserted_generation_rules_on_keyless_rows(tmp_path: Path) -> None:
    """Planned count, one revision per record and no earlier row, each its own rule."""
    connection = _store(tmp_path / "market.duckdb", keys=False)
    rows = _price_rows()
    _publish(connection, rows)
    check_inserted_generation(connection, "prices", "g1", 1)
    with pytest.raises(ValueError, match="different row count"):
        check_inserted_generation(connection, "prices", "g1", 2)
    with pytest.raises(ValueError, match="already name this generation"):
        check_new_generation(connection, "g1")
    check_new_generation(connection, "g2")
    # A second revision of the same record inside the generation.
    connection.execute(
        "INSERT INTO prices SELECT * REPLACE ('r9' AS revision_id) FROM prices "
        "WHERE generation_id='g1'"
    )
    with pytest.raises(ValueError, match="one revision per natural record"):
        check_inserted_generation(connection, "prices", "g1", 2)
    connection.close()


def test_python_writer_plans_inside_its_transaction(tmp_path: Path) -> None:
    """The parent check sees the head at publication, so a moved head is refused."""
    connection = _store(tmp_path / "market.duckdb")
    rows = _price_rows()
    _publish(connection, rows)
    moved = {**rows[0], "instrument_id": "ASSET_B"}
    with pytest.raises(ValueError, match="parent changed"):
        _publish(connection, [moved], generation="g2")
    assert _count(connection, "SELECT count(*) FROM market_generations") == 1
    # The same request again returns the committed marker and writes nothing.
    assert _publish(connection, rows) == market.marker_for(connection, "g1")
    connection.close()


# --- the publication claim --------------------------------------------------------


def _claim_free(connection: duckdb.DuckDBPyConnection) -> bool:
    """Whether a publication could claim the store now; nothing is left open either way."""
    refused: ValueError | None = None
    try:
        begin_publication(connection)
    except ValueError as error:
        refused = error
    finally:
        market.rollback(connection)
    if refused is None:
        return True
    assert str(refused) == _OPEN
    assert isinstance(refused.__cause__, duckdb.TransactionException)
    return False


def _info(connection: duckdb.DuckDBPyConnection) -> list[tuple[object, ...]]:
    return connection.execute("SELECT * FROM store_info").fetchall()


@pytest.mark.parametrize("rival_kind", ["cursor", "connection"])
def test_the_claim_admits_one_publication_per_store(tmp_path: Path, rival_kind: str) -> None:
    """Real DuckDB: a second cursor or a second connection of this process is refused."""
    path = tmp_path / "market.duckdb"
    connection = _store(path)
    before = _info(connection)
    holder = connection.cursor()
    rival = connection.cursor() if rival_kind == "cursor" else duckdb.connect(str(path))
    begin_publication(holder)
    # Inside the claiming transaction every store_info value reads as it was.
    assert _info(holder) == before
    assert not _claim_free(rival)
    # Rolling back releases the claim.
    market.rollback(holder)
    assert _claim_free(rival)
    # A claim that committed after the rival's snapshot was taken still refuses it.
    rival.execute("BEGIN TRANSACTION")
    assert _info(rival) == before
    begin_publication(holder)
    commit_publication(holder)
    with pytest.raises(duckdb.TransactionException, match="Conflict on update"):
        rival.execute(market_integrity._CLAIM)
    market.rollback(rival)
    # A publication that begins after the commit claims normally.
    assert _claim_free(rival)
    assert _info(connection) == _info(rival) == before
    rival.close()
    holder.close()
    connection.close()


def test_an_update_to_the_value_held_is_no_claim(tmp_path: Path) -> None:
    """Why the claim negates twice: DuckDB records no write for a same-value UPDATE."""
    connection = _store(tmp_path / "market.duckdb")
    first, second = connection.cursor(), connection.cursor()
    for cursor in (first, second):
        cursor.execute("BEGIN TRANSACTION")
        cursor.execute("UPDATE store_info SET schema_version = schema_version")
    first.execute("COMMIT")
    second.execute("COMMIT")
    first.close()
    second.close()
    connection.close()


def test_the_judges_two_cursor_counterexample_cannot_commit_both(tmp_path: Path) -> None:
    """Both transactions open before either inserts; only the first gets past its claim."""
    connection = _store(tmp_path / "market.duckdb", keys=False)
    rows = _first(random.Random(13), "prices", 3)
    first, second = connection.cursor(), connection.cursor()
    _stage(first, "prices", rows, name="staged_first")
    _stage(second, "prices", rows, name="staged_second")
    begin_publication(first)
    with pytest.raises(ValueError, match=_OPEN):
        begin_publication(second)
    market.rollback(second)
    market.rollback(first)
    # The writers themselves, the second started inside the first's transaction.
    refused: list[BaseException] = []

    def rival(generation_id: str) -> None:
        if generation_id == "o1":
            return
        request = replace(
            _request("1", parent=None, domain="prices", staged="staged_second"),
            dataset_id="other",
            generation_id="o1",
            operation_id="op-o1",
        )
        try:
            publish_generation_bulk(second, request, budget=BUDGET)
        except ValueError as error:
            refused.append(error)

    request = _request("1", parent=None, domain="prices", staged="staged_first")
    with inside_publication(rival):
        publish_generation_bulk(first, request, budget=BUDGET)
    assert [str(error) for error in refused] == [_OPEN]
    assert _count(connection, "SELECT count(*) FROM prices") == 3
    audit_duplicates(connection)
    # Once the first has committed, the second sees its rows and the gate refuses the pair.
    with pytest.raises(ValueError, match="duplicate market revision"):
        publish_generation_bulk(
            second,
            replace(
                _request("1", parent=None, domain="prices", staged="staged_second"),
                dataset_id="other",
                generation_id="o1",
                operation_id="op-o1",
            ),
            budget=BUDGET,
        )
    assert _count(connection, "SELECT count(*) FROM market_generations") == 1
    first.close()
    second.close()
    connection.close()


def test_without_the_claim_both_cursors_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The counterexample the claim exists for: each transaction's check sees no rival."""

    def unclaimed(connection: duckdb.DuckDBPyConnection) -> None:
        connection.execute("BEGIN TRANSACTION")

    monkeypatch.setattr(market_integrity, "begin_publication", unclaimed)
    connection = _store(tmp_path / "market.duckdb", keys=False)
    rows = _first(random.Random(13), "prices", 3)
    first, second = connection.cursor(), connection.cursor()
    _stage(first, "prices", rows, name="staged_first")
    _stage(second, "prices", rows, name="staged_second")

    def rival(generation_id: str) -> None:
        if generation_id == "o1":
            return
        request = replace(
            _request("1", parent=None, domain="prices", staged="staged_second"),
            dataset_id="other",
            generation_id="o1",
            operation_id="op-o1",
        )
        publish_generation_bulk(second, request, budget=BUDGET)

    request = _request("1", parent=None, domain="prices", staged="staged_first")
    with inside_publication(rival):
        publish_generation_bulk(first, request, budget=BUDGET)
    assert _count(connection, "SELECT count(*) FROM prices") == 6
    with pytest.raises(ValueError, match="stored more than once"):
        audit_duplicates(connection)
    first.close()
    second.close()
    connection.close()


@pytest.mark.parametrize("rival_kind", ["cursor", "connection"])
def test_a_publication_cannot_start_inside_another_on_the_same_store(
    tmp_path: Path, rival_kind: str
) -> None:
    """A Python-writer publication from inside a bulk one is refused, then proceeds."""
    path = tmp_path / "market.duckdb"
    connection = _store(path, keys=False)
    _stage(connection, "prices", _first(random.Random(11), "prices", 2))
    rival = connection.cursor() if rival_kind == "cursor" else duckdb.connect(str(path))
    refused: list[BaseException] = []

    def nested(generation_id: str) -> None:
        if generation_id != "g1":
            return
        try:
            _publish(rival, _price_rows(), dataset="other", generation="o1")
        except ValueError as error:
            refused.append(error)
            raise

    with inside_publication(nested), pytest.raises(ValueError, match=_OPEN):
        publish_generation_bulk(
            connection, _request("1", parent=None, domain="prices"), budget=BUDGET
        )
    assert len(refused) == 1
    assert _count(connection, "SELECT count(*) FROM market_generations") == 0
    assert _count(connection, "SELECT count(*) FROM prices") == 0
    # Both transactions rolled back, so nothing holds the store.
    _publish(rival, _price_rows(), dataset="other", generation="o1")
    publish_generation_bulk(connection, _request("1", parent=None, domain="prices"), budget=BUDGET)
    assert _count(connection, "SELECT count(*) FROM market_generations") == 2
    rival.close()
    connection.close()


def test_closing_a_connection_releases_its_claim(tmp_path: Path) -> None:
    """An open claim lives only in its transaction: closing the cursor ends it."""
    connection = _store(tmp_path / "market.duckdb", keys=False)
    abandoned = connection.cursor()
    begin_publication(abandoned)
    with pytest.raises(ValueError, match=_OPEN):
        _publish(connection, _price_rows())
    abandoned.close()
    _publish(connection, _price_rows())
    assert _count(connection, "SELECT count(*) FROM market_generations") == 1
    connection.close()


def test_reusing_an_identical_generation_still_returns_it(tmp_path: Path) -> None:
    connection = _store(tmp_path / "market.duckdb", keys=False)
    before = _info(connection)
    rows = _price_rows()
    marker = _publish(connection, rows)
    assert _publish(connection, rows) == marker
    _stage(connection, "prices", _first(random.Random(3), "prices", 2))
    request = replace(
        _request("1", parent=None, domain="prices"), dataset_id="bulk", generation_id="b1"
    )
    bulk = publish_generation_bulk(connection, request, budget=BUDGET)
    assert publish_generation_bulk(connection, request, budget=BUDGET) == bulk
    assert _count(connection, "SELECT count(*) FROM market_generations") == 2
    assert _info(connection) == before
    connection.close()


@pytest.mark.parametrize(
    ("message", "refused"),
    [
        ("TransactionContext Error: Conflict on update!", True),
        ("TransactionContext Error: Conflict on tuple deletion!", True),
        ("TransactionContext Error: Failed to commit: Conflict on update!", True),
        # Exhausted memory at COMMIT is capacity, whatever follows DuckDB's prefix.
        ("TransactionContext Error: Failed to commit: could not allocate Conflict on ", False),
        # A constraint violation may quote a key holding the conflict's words.
        (
            (
                "TransactionContext Error: Failed to commit: PRIMARY KEY or UNIQUE constraint "
                'violation: duplicate key "Conflict on update!"'
            ),
            False,
        ),
        ("TransactionContext Error: cannot start a transaction within a transaction", False),
    ],
)
def test_only_a_write_conflict_at_commit_is_the_open_publication(
    tmp_path: Path, message: str, *, refused: bool
) -> None:
    """COMMIT's conflict is the refusal; any other failure keeps its error past ROLLBACK."""
    connection = _store(tmp_path / "market.duckdb", keys=False)
    failing = cast("duckdb.DuckDBPyConnection", _FailingStatement(connection, "COMMIT", message))
    with pytest.raises((ValueError, duckdb.TransactionException)) as caught:
        _publish(failing, _price_rows())
    if refused:
        assert type(caught.value) is ValueError
        assert str(caught.value) == _OPEN
        assert str(caught.value.__cause__) == message
    else:
        assert type(caught.value) is duckdb.TransactionException
        assert str(caught.value) == message
    # The wrapper never ran COMMIT; the writer's ROLLBACK ended the transaction.
    assert _count(connection, "SELECT count(*) FROM market_generations") == 0
    assert _claim_free(connection)
    connection.close()


def _interrupting(
    codes: set[CodeType], target: int
) -> tuple[Callable[..., object], list[CodeType]]:
    """A trace raising one ``KeyboardInterrupt`` at the ``target``-th call, line or return.

    Only frames of ``codes`` count: the writers', their transaction's and ROLLBACK's. A
    call event raises before the frame's first line, as a call that never starts; a return
    event raises as the frame returns, after its BEGIN, claim, COMMIT or ROLLBACK ran.
    Python unsets a trace that raised, so this one interrupt is the only one. The list
    holds the code of every event seen.
    """
    seen: list[CodeType] = []

    def tick(frame: FrameType) -> None:
        seen.append(frame.f_code)
        if len(seen) == target + 1:
            raise KeyboardInterrupt

    def local(frame: FrameType, event: str, _arg: object) -> Callable[..., object] | None:
        if event in {"line", "return"}:
            tick(frame)
        return local

    def trace(frame: FrameType, _event: str, _arg: object) -> Callable[..., object] | None:
        if frame.f_code not in codes:
            return None
        tick(frame)
        return local

    return trace, seen


def _nested(function: FunctionType) -> set[CodeType]:
    """The code of ``function`` and of the functions defined in it."""
    code = function.__code__
    return {code, *(const for const in code.co_consts if isinstance(const, CodeType))}


_TRANSACTION: Final = {
    market_integrity.run_publication.__code__,
    market_integrity.begin_publication.__code__,
    market_integrity.commit_publication.__code__,
    market_integrity._refuse_conflict.__code__,
    market.rollback.__code__,
}
_REFUSALS: Final = {
    "commits": None,
    "parent": "market parent changed or awaits catalog recovery",
    "duplicate": "duplicate market revision",
}


def _interrupted_publication(
    connection: duckdb.DuckDBPyConnection, writer: str, outcome: str
) -> Callable[[], dict[str, object]]:
    """The publication to interrupt: one that commits, or one its writer refuses."""
    parent = "g-missing" if outcome == "parent" else None
    if writer == "python":
        if outcome == "duplicate":
            _publish(connection, _price_rows())
        dataset, generation = ("other", "o1") if outcome == "duplicate" else ("synthetic", "g1")
        return functools.partial(
            _publish,
            connection,
            _price_rows(),
            dataset=dataset,
            generation=generation,
            parent=parent,
        )
    _stage(connection, "prices", _first(random.Random(7), "prices", 2))
    request = _request("1", parent=parent, domain="prices")
    if outcome == "duplicate":
        publish_generation_bulk(connection, request, budget=BUDGET)
        request = replace(request, dataset_id="other", generation_id="o1", operation_id="op-o1")
    return functools.partial(publish_generation_bulk, connection, request, budget=BUDGET)


def _run_traced(
    publish: Callable[[], object], trace: Callable[..., object]
) -> BaseException | None:
    """Run ``publish`` under ``trace``; the interrupt or refusal it raised, if any."""
    previous = sys.gettrace()
    sys.settrace(trace)
    try:
        publish()
    except (KeyboardInterrupt, ValueError) as error:
        return error
    finally:
        sys.settrace(previous)
    return None


def _check_released(
    path: Path,
    connection: duckdb.DuckDBPyConnection,
    publish: Callable[[], dict[str, object]],
    refusal: str | None,
    before: list[tuple[object, ...]],
) -> None:
    """Nothing is open or claimed, and the next publications run as they would have."""
    generations = _count(connection, "SELECT count(*) FROM market_generations")
    other = duckdb.connect(str(path))
    cursor = connection.cursor()
    assert _claim_free(connection)
    assert _claim_free(cursor)
    assert _claim_free(other)
    assert _info(connection) == before
    if refusal is None:
        # The same request then commits, or returns what the interrupted one committed.
        marker = publish()
        assert marker == market.marker_for(connection, str(marker["generation_id"]))
    else:
        # The refused request is refused for its own reason, not as an open publication,
        # and a valid one commits from another cursor and from another connection.
        with pytest.raises(ValueError, match=refusal):
            publish()
        assert _count(connection, "SELECT count(*) FROM market_generations") == generations
        _publish(cursor, _revised("r-next"), dataset="next", generation="n1")
        _publish(other, _revised("r-later"), dataset="later", generation="l1")
    assert _info(other) == before
    other.close()
    cursor.close()


@pytest.mark.parametrize("outcome", list(_REFUSALS))
@pytest.mark.parametrize("writer", ["python", "bulk"])
def test_an_interrupt_anywhere_leaves_the_next_publication_free(
    tmp_path: Path, writer: str, outcome: str
) -> None:
    """One ``KeyboardInterrupt`` at every point of either writer blocks nothing after it.

    The points include the error handler and ROLLBACK of a refused publication (a parent
    that is not the head, a revision another dataset holds): an interrupt there, before or
    inside the first ROLLBACK, is the one the second ROLLBACK in ``run_publication`` ends.
    """
    codes = {*_TRANSACTION}
    if writer == "python":
        codes |= _nested(market.publish_generation) | {market._insert_rows.__code__}
    else:
        codes |= _nested(bulk_generation.publish_generation_bulk)
    refusal = _REFUSALS[outcome]
    interrupted: set[CodeType] = set()
    for target in itertools.count():
        path = tmp_path / f"market-{target}.duckdb"
        connection = _store(path, keys=False)
        before = _info(connection)
        publish = _interrupted_publication(connection, writer, outcome)
        trace, seen = _interrupting(codes, target)
        raised = _run_traced(publish, trace)
        reached = len(seen) > target
        if reached:
            assert isinstance(raised, KeyboardInterrupt)
            interrupted.add(seen[-1])
        else:
            # Past the last point the publication commits or its refusal arrives.
            assert (raised is None) if refusal is None else re.search(refusal, str(raised))
        _check_released(path, connection, publish, refusal, before)
        connection.close()
        if not reached:
            break
    # Every frame of the writer and its transaction was a point of interruption, ROLLBACK
    # and the publication's cleanup included. No conflict arises alone, a refused
    # publication never reaches COMMIT, and a parent the plan refuses inserts nothing.
    unreached = {market_integrity._refuse_conflict.__code__}
    if refusal is not None:
        unreached.add(market_integrity.commit_publication.__code__)
    if (writer, outcome) == ("python", "parent"):
        unreached.add(market._insert_rows.__code__)
    assert codes - interrupted == unreached
    assert market.rollback.__code__ in interrupted


class _FailingStatement:
    """Forward everything, except that ``statement`` raises ``message`` instead of running."""

    def __init__(self, connection: duckdb.DuckDBPyConnection, statement: str, message: str) -> None:
        self.connection = connection
        self.statement = statement
        self.message = message

    def execute(self, query: str, parameters: list[object] | None = None) -> object:
        if query.startswith(self.statement):
            if "Out of Memory" in self.message:
                raise duckdb.OutOfMemoryException(self.message)
            raise duckdb.TransactionException(self.message)
        return self.connection.execute(query, parameters)

    def __getattr__(self, name: str) -> object:
        return getattr(self.connection, name)


@pytest.mark.parametrize("writer", ["python", "bulk"])
def test_the_claim_is_inside_the_budget_boundary(tmp_path: Path, writer: str) -> None:
    connection = _store(tmp_path / "market.duckdb", keys=False)
    exhausted = cast(
        "duckdb.DuckDBPyConnection",
        _FailingStatement(
            connection,
            market_integrity._CLAIM,
            "Out of Memory Error: failed to pin block",
        ),
    )
    if writer == "python":
        rows = _price_rows()

        def publish() -> object:
            return _publish(exhausted, rows)
    else:
        _stage(connection, "prices", _first(random.Random(5), "prices", 2))
        request = _request("1", parent=None, domain="prices")

        def publish() -> object:
            return publish_generation_bulk(exhausted, request, budget=BUDGET)

    with pytest.raises(ComputeResourceError, match="within admitted memory") as caught:
        publish()
    assert isinstance(caught.value.__cause__, duckdb.OutOfMemoryException)
    assert _count(connection, "SELECT count(*) FROM market_generations") == 0
    assert _claim_free(connection)
    connection.close()


def test_many_publications_leave_the_store_identity_as_admitted(tmp_path: Path) -> None:
    """store_info, admission, checkpoint, backup, restore and compact see no change."""
    home = _chain_home(tmp_path, generations=12)
    with open_workspace(home, writable=True) as workspace:
        admitted = store_info(workspace.market)
        assert admitted == workspace._market_info
        report = verify_workspace(workspace)
        assert report == verify_workspace(workspace, deep=True)
        with workspace.checkpointed_market():
            pass
        assert store_info(workspace.market) == admitted
    restored = restore(Path(str(backup(home)["backup_root"])), tmp_path / "restored")
    assert restored["verification"] == report
    compacted = compact(home, tmp_path / "compacted")
    assert compacted["verification"] == report
    for root in (home, tmp_path / "restored", tmp_path / "compacted"):
        with open_workspace(root) as workspace:
            assert store_info(workspace.market) == admitted
            assert verify_workspace(workspace) == report


# --- quality flags ---------------------------------------------------------------


def _flag(connection: duckdb.DuckDBPyConnection, revision: str, flag: str = "f") -> None:
    connection.execute(
        "INSERT INTO quality_flags SELECT generation_id, record_id, ?, 'rule', '1', ?, NULL "
        "FROM prices WHERE generation_id='g1'",
        [revision, flag],
    )


def test_flag_gate_refuses_repeats_orphans_and_unreviewed_flags(tmp_path: Path) -> None:
    connection = _store(tmp_path / "market.duckdb", keys=False)
    _publish(connection, _price_rows())
    _flag(connection, "r1")
    check_generation_flags(connection, "prices", "g1")
    connection.execute("DELETE FROM quality_flags")
    _flag(connection, "r-missing")
    with pytest.raises(ValueError, match="does not store"):
        check_generation_flags(connection, "prices", "g1")
    connection.execute("DELETE FROM quality_flags")
    # The writer holds the flags it inserts to their reviewed digest before COMMIT.
    _stage(connection, "prices", _first(random.Random(17), "prices", 2))
    connection.execute(
        "CREATE TEMP TABLE staged_flags AS SELECT record_id, revision_id, 'rule' AS rule_id, "
        "'1' AS rule_version, 'f' AS flag, NULL::VARCHAR AS detail FROM staged"
    )
    digest, rows = bulk_generation.flags_digest(
        connection, "SELECT * FROM staged_flags", [], BUDGET
    )
    request = replace(_request("1", parent=None, domain="prices"), dataset_id="other")
    request = replace(request, generation_id="o1", operation_id="op-o1")
    connection.execute("INSERT INTO staged_flags SELECT * FROM staged_flags LIMIT 1")
    with pytest.raises(ValueError, match="repeats its key"):
        publish_generation_bulk(
            connection,
            request,
            budget=BUDGET,
            flags=BulkFlags(
                "staged_flags",
                *bulk_generation.flags_digest(connection, "SELECT * FROM staged_flags", [], BUDGET),
            ),
        )
    connection.execute(
        "DELETE FROM staged_flags; INSERT INTO staged_flags SELECT record_id, revision_id, "
        "'rule', '1', 'f', NULL FROM staged"
    )
    connection.execute(
        "UPDATE staged_flags SET detail = 'changed' "
        "WHERE record_id = (SELECT min(record_id) FROM staged_flags)"
    )
    with pytest.raises(ValueError, match="differ from their reviewed digest"):
        publish_generation_bulk(
            connection, request, budget=BUDGET, flags=BulkFlags("staged_flags", digest, rows)
        )
    assert (
        _count(connection, "SELECT count(*) FROM market_generations WHERE generation_id='o1'") == 0
    )
    assert _count(connection, "SELECT count(*) FROM quality_flags") == 0
    connection.execute("UPDATE staged_flags SET detail = NULL")
    publish_generation_bulk(
        connection, request, budget=BUDGET, flags=BulkFlags("staged_flags", digest, rows)
    )
    assert _count(connection, "SELECT count(*) FROM quality_flags WHERE generation_id='o1'") == rows
    connection.close()


_WIDE_REVISION: Final = 256 * 1024
_WIDE_DETAIL: Final = 1024 * 1024


def _wide_flags(connection: duckdb.DuckDBPyConnection) -> None:
    """Two staged prices with wide revision IDs, and one flag each with a wide detail."""
    _stage(connection, "prices", _first(random.Random(23), "prices", 2))
    connection.execute(
        f"UPDATE staged SET revision_id = revision_id || repeat('v', {_WIDE_REVISION})"
    )
    connection.execute(
        "CREATE TEMP TABLE staged_flags AS SELECT record_id, revision_id, 'rule' AS rule_id, "
        f"'1' AS rule_version, 'f' AS flag, repeat('d', {_WIDE_DETAIL}) AS detail FROM staged"
    )


def _tight(available: int) -> ComputeBudget:
    """``BUDGET`` with all but ``available`` materialization bytes already held live."""
    return replace(BUDGET, reserved_bytes=BUDGET.available_bytes - available)


def test_flag_batches_are_sized_from_the_widest_encoded_row(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The flag digest measures its widest row before fetching, never a fixed width."""
    connection = _store(tmp_path / "market.duckdb", keys=False)
    _wide_flags(connection)
    actual = bulk_generation.stream_rowset
    batches: list[int] = []

    def recorded(  # noqa: PLR0913 -- stream_rowset's signature
        connection: duckdb.DuckDBPyConnection,
        schema: tuple[tuple[str, str], ...],
        cells: list[str],
        relation: str,
        parameters: list[object],
        *,
        count: int,
        batch_rows: int,
    ) -> str:
        batches.append(batch_rows)
        return actual(
            connection, schema, cells, relation, parameters, count=count, batch_rows=batch_rows
        )

    monkeypatch.setattr(bulk_generation, "stream_rowset", recorded)
    relation = "SELECT * FROM staged_flags"
    expected = bulk_generation.flags_digest(connection, relation, [], BUDGET)
    found = connection.execute(
        "SELECT max(strlen(record_id) + strlen(revision_id) + strlen(rule_id)"
        " + strlen(rule_version) + strlen(flag) + strlen(detail)) "
        "FROM staged_flags"
    ).fetchone()
    assert found is not None
    row_bytes = int(found[0]) + 6 * 5  # six text cells, each tagged and length-framed
    for available in (BUDGET.available_bytes, 8 * row_bytes):
        batches.clear()
        budget = _tight(available)
        assert bulk_generation.flags_digest(connection, relation, [], budget) == expected
        (batch,) = batches
        charge = batch * (2 * row_bytes + bulk_generation._ROW_OBJECT_BYTES) + row_bytes
        assert batch >= 1
        assert charge <= budget.available_bytes
    # A fixed 4096-byte row would admit 2048 rows here; one wide row fits only a few times.
    assert batches == [3]
    # Not even one row fits: refused before any row is fetched.
    batches.clear()
    with pytest.raises(ComputeResourceError, match="one quality flag row"):
        bulk_generation.flags_digest(connection, relation, [], _tight(2 * row_bytes))
    assert batches == []
    connection.close()


def test_an_empty_flag_relation_needs_no_row_allowance(tmp_path: Path) -> None:
    """No flag row is fetched, so even an allowance below one row digests the empty rowset."""
    connection = _store(tmp_path / "market.duckdb", keys=False)
    _wide_flags(connection)
    relation = "SELECT * FROM staged_flags WHERE false"
    expected = (rowset_hash(bulk_generation.FLAG_SCHEMA, []), 0)
    assert bulk_generation.flags_digest(connection, relation, [], BUDGET) == expected
    assert bulk_generation.flags_digest(connection, relation, [], _tight(1)) == expected
    connection.close()


def test_a_flag_row_wider_than_the_allowance_rolls_the_publication_back(
    tmp_path: Path,
) -> None:
    """A flag row that cannot be fetched refuses the whole publication, rows and marker."""
    connection = _store(tmp_path / "market.duckdb", keys=False)
    _wide_flags(connection)
    flags = BulkFlags(
        "staged_flags",
        *bulk_generation.flags_digest(connection, "SELECT * FROM staged_flags", [], BUDGET),
    )
    request = _request("1", parent=None, domain="prices")
    # The prices fit this allowance; a flag row, four times wider, does not.
    with pytest.raises(ComputeResourceError, match="one quality flag row"):
        publish_generation_bulk(connection, request, budget=_tight(2 * 1024 * 1024), flags=flags)
    assert _count(connection, "SELECT count(*) FROM market_generations") == 0
    assert _count(connection, "SELECT count(*) FROM prices") == 0
    assert _count(connection, "SELECT count(*) FROM quality_flags") == 0
    # Nothing was left open: the same publication commits once the allowance admits a row.
    publish_generation_bulk(connection, request, budget=BUDGET, flags=flags)
    assert _count(connection, "SELECT count(*) FROM quality_flags") == 2
    connection.close()


@pytest.mark.parametrize("staged", ["quality_flags", "prices", "market_generations"])
def test_flags_are_never_copied_from_a_market_table(tmp_path: Path, staged: str) -> None:
    """The writer's flag INSERT reads a staged relation only, never another generation's rows."""
    connection = _store(tmp_path / "market.duckdb", keys=False)
    _stage(connection, "prices", _first(random.Random(19), "prices", 2))
    with pytest.raises(ValueError, match="outside the market tables"):
        publish_generation_bulk(
            connection,
            _request("1", parent=None, domain="prices"),
            budget=BUDGET,
            flags=BulkFlags(staged, "0" * 64, 0),
        )
    assert _count(connection, "SELECT count(*) FROM market_generations") == 0
    assert _count(connection, "SELECT count(*) FROM prices") == 0
    connection.close()


@pytest.mark.parametrize(
    ("tamper", "message"),
    [
        # A v3 store has no flag key, so only the writer's flag check refuses the repeat.
        (
            "INSERT INTO temp._aas_p_flags SELECT * FROM temp._aas_p_flags LIMIT 1",
            "repeats its key",
        ),
        (
            (
                "INSERT INTO temp._aas_p_flags SELECT record_id, 'r-none', rule_id, "
                "rule_version, flag, detail FROM temp._aas_p_flags LIMIT 1"
            ),
            "does not store",
        ),
        (
            "DELETE FROM temp._aas_p_flags WHERE flag='time_precision_day'",
            "differ from their reviewed digest",
        ),
    ],
)
def test_promotion_flags_changed_after_planning_never_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, tamper: str, message: str
) -> None:
    from tests.storage.promotion_support import (  # noqa: PLC0415 -- promotion fixtures
        add_source,
        at,
        bar,
        register_symbols,
        spec,
    )

    root = tmp_path / "aas"
    initialize(root)
    actual = engine.publish_generation_bulk

    def tampered(
        connection: duckdb.DuckDBPyConnection,
        request: BulkRequest,
        *,
        budget: ComputeBudget,
        plan: BulkPlan | None = None,
        flags: BulkFlags | None = None,
    ) -> dict[str, object]:
        connection.execute(tamper)
        return actual(connection, request, budget=budget, plan=plan, flags=flags)

    monkeypatch.setattr(engine, "publish_generation_bulk", tampered)
    with open_workspace(root, writable=True, strategy_write=True) as workspace:
        day = at("2025-01-02T00:00:00").date()
        late = at("2025-01-10T00:00:00")
        pin = add_source(workspace, [bar("AAA.KO", day, 2649999.947, retrieved=late)], tag="a")
        identity = register_symbols(workspace, pin["source_id"])
        raw, sha = spec([pin], identity)
        with pytest.raises((ValueError, duckdb.ConstraintException), match=message):
            engine.promote(workspace, raw, sha, apply=True)
        assert workspace.market.execute("SELECT count(*) FROM market_generations").fetchone() == (
            0,
        )
        assert workspace.market.execute("SELECT count(*) FROM quality_flags").fetchone() == (0,)


# --- the at-rest audits ----------------------------------------------------------


def test_count_audit_compares_rows_and_markers_both_ways(tmp_path: Path) -> None:
    connection = _store(tmp_path / "market.duckdb", keys=False, foreign=False)
    rows = _price_rows()
    _publish(connection, rows)
    audit_generation_counts(connection)
    connection.execute("UPDATE market_generations SET row_count = 2")
    with pytest.raises(ValueError, match="row count differs from its marker"):
        audit_generation_counts(connection)
    connection.execute("UPDATE market_generations SET row_count = 1")
    connection.execute("DELETE FROM prices")
    with pytest.raises(ValueError, match="row count differs from its marker"):
        audit_generation_counts(connection)
    _stage(connection, "prices", _first(random.Random(3), "prices", 1))
    connection.execute("INSERT INTO prices BY NAME SELECT 'g1' AS generation_id, * FROM staged")
    audit_generation_counts(connection)
    connection.execute("INSERT INTO prices BY NAME SELECT 'ghost' AS generation_id, * FROM staged")
    with pytest.raises(ValueError, match="without a marker"):
        audit_generation_counts(connection)
    connection.execute("DELETE FROM prices WHERE generation_id='ghost'")
    actions = _first(random.Random(5), "corporate_actions", 1)
    _stage(connection, "corporate_actions", actions, name="actions")
    connection.execute(
        "INSERT INTO corporate_actions BY NAME SELECT 'g1' AS generation_id, * FROM actions"
    )
    with pytest.raises(ValueError, match="foreign domain"):
        audit_generation_counts(connection)
    connection.close()


def test_flag_reference_audit(tmp_path: Path) -> None:
    connection = _store(tmp_path / "market.duckdb", keys=False, foreign=False)
    _publish(connection, _price_rows())
    _flag(connection, "r1")
    audit_flag_references(connection)
    _flag(connection, "r-missing")
    with pytest.raises(ValueError, match="does not store"):
        audit_flag_references(connection)
    connection.execute("DELETE FROM quality_flags WHERE revision_id='r-missing'")
    connection.execute(
        "INSERT INTO quality_flags VALUES ('ghost', 'x', 'r1', 'rule', '1', 'f', NULL)"
    )
    with pytest.raises(ValueError, match="names no generation marker"):
        audit_flag_references(connection)
    connection.close()


def test_duplicate_audit_covers_domains_and_flag_keys(tmp_path: Path) -> None:
    connection = _store(tmp_path / "market.duckdb", keys=False)
    _publish(connection, _price_rows())
    _flag(connection, "r1")
    audit_duplicates(connection)
    audit_duplicates(connection, pass_bytes=400)
    _flag(connection, "r1")
    with pytest.raises(ValueError, match="quality flag is stored more than once"):
        audit_duplicates(connection)
    connection.close()


def _pairs(connection: duckdb.DuckDBPyConnection, values: list[tuple[str, str]]) -> None:
    connection.execute("CREATE OR REPLACE TABLE pairs (record_id VARCHAR, revision_id VARCHAR)")
    connection.executemany("INSERT INTO pairs VALUES (?, ?)", values)


def _width(values: list[tuple[str, str]]) -> int:
    return sum(
        market_integrity._AUDIT_ROW_BYTES + len(a.encode()) + len(b.encode()) for a, b in values
    )


def test_adaptive_audit_splits_skewed_wide_keys_by_measured_size() -> None:
    """Every record starts alike and revisions are long, so the prefix split alone fails."""
    connection = duckdb.connect()
    values = [("z" * 40 + f"{index:05}", "글" * 100 + str(index)) for index in range(2_000)]
    _pairs(connection, values)
    pass_bytes = 40_000
    found, sizes = market_integrity._duplicates(
        connection, "pairs", ("record_id", "revision_id"), pass_bytes
    )
    assert not found
    assert len(sizes) > 10
    assert all(size <= pass_bytes for size in sizes)
    assert sum(sizes) == _width(values)
    # One repeated pair anywhere is found, whatever pass it lands in.
    connection.execute("INSERT INTO pairs SELECT * FROM pairs WHERE record_id LIKE '%01234'")
    found, _ = market_integrity._duplicates(
        connection, "pairs", ("record_id", "revision_id"), pass_bytes
    )
    assert found
    connection.close()


def test_adaptive_audit_compares_full_framed_keys() -> None:
    connection = duckdb.connect()
    # The same concatenation, different pairs: neither the framing nor the comparison merges them.
    _pairs(connection, [("ab", "c"), ("a", "bc"), ("a", "b" + "c")])
    found, _ = market_integrity._duplicates(
        connection, "pairs", ("record_id", "revision_id"), 1_000
    )
    assert found
    connection.execute("DELETE FROM pairs WHERE rowid = 2")
    found, sizes = market_integrity._duplicates(
        connection, "pairs", ("record_id", "revision_id"), 140
    )
    assert len(sizes) == 2
    assert not found
    connection.close()


def test_adaptive_audit_finds_a_key_repeated_past_one_pass() -> None:
    """Copies of one key never split; at full digest depth they are the duplicate."""
    connection = duckdb.connect()
    _pairs(connection, [("same", "pair")] * 500 + [("other", "pair")])
    found, _ = market_integrity._duplicates(
        connection, "pairs", ("record_id", "revision_id"), 2_000
    )
    assert found
    connection.close()


def test_adaptive_audit_refuses_a_row_wider_than_a_pass() -> None:
    connection = duckdb.connect()
    _pairs(connection, [("wide", "x" * 5_000), ("narrow", "y")])
    with pytest.raises(ComputeResourceError, match="cannot be split"):
        market_integrity._duplicates(connection, "pairs", ("record_id", "revision_id"), 1_000)
    connection.close()


class _Exhausted:
    """Forward SQL, except that a grouping statement exhausts DuckDB's memory."""

    def __init__(self, connection: duckdb.DuckDBPyConnection) -> None:
        self.connection = connection

    def execute(self, query: str, parameters: list[object] | None = None) -> object:
        if "HAVING" in query:
            raise duckdb.OutOfMemoryException("Out of Memory Error: failed to pin block")
        return self.connection.execute(query, parameters)


def test_audit_exhaustion_is_a_budget_error(tmp_path: Path) -> None:
    connection = _store(tmp_path / "market.duckdb")
    _publish(connection, _price_rows())
    with pytest.raises(ComputeResourceError, match="within admitted memory") as caught:
        audit_duplicates(cast("duckdb.DuckDBPyConnection", _Exhausted(connection)))
    assert isinstance(caught.value.__cause__, duckdb.OutOfMemoryException)
    connection.close()


# --- the core catalog ------------------------------------------------------------

_CHANGES: Final[dict[str, Callable[[duckdb.DuckDBPyConnection], object]]] = {
    "column": lambda c: c.execute('ALTER TABLE prices ADD COLUMN "extra" VARCHAR'),
    "default": lambda c: c.execute("ALTER TABLE prices ALTER \"fields\" SET DEFAULT 'close'"),
    "nullable": lambda c: c.execute("ALTER TABLE filings ALTER form DROP NOT NULL"),
    "missing": lambda c: c.execute("DROP TABLE quality_flags"),
    "type": lambda c: c.execute("ALTER TABLE estimates ALTER analyst_count TYPE INTEGER"),
}


def test_core_catalog_matches_an_empty_store_of_its_version(tmp_path: Path) -> None:
    fresh = _store(tmp_path / "fresh.duckdb")
    check_core_catalog(fresh)
    # Tables the core does not own, and temporary ones, are not part of its shape.
    fresh.execute("CREATE TABLE sl_extra (x INTEGER)")
    fresh.execute("CREATE TEMP TABLE prices (x INTEGER)")
    check_core_catalog(fresh)
    fresh.close()
    old = duckdb.connect(str(tmp_path / "old.duckdb"))
    market.initialize_market(old, "synthetic", version=1)
    check_core_catalog(old)
    market.upgrade_market(old, "synthetic", 2)
    check_core_catalog(old)
    old.close()
    keyless = _store(tmp_path / "keyless.duckdb", keys=False)
    with pytest.raises(ValueError, match="differs from its schema version"):
        check_core_catalog(keyless)
    # Migrated to v3, whose own domain tables and flags are key-less, it matches again.
    market.upgrade_market(keyless, "synthetic", 3)
    check_core_catalog(keyless)
    keyless.close()


@pytest.mark.parametrize("change", sorted(_CHANGES))
def test_core_catalog_refuses_a_changed_table(tmp_path: Path, change: str) -> None:
    connection = _store(tmp_path / "market.duckdb")
    _CHANGES[change](connection)
    with pytest.raises(ValueError, match="differs from its schema version"):
        check_core_catalog(connection)
    connection.close()


def test_core_catalog_refuses_reordered_columns(tmp_path: Path) -> None:
    connection = _store(tmp_path / "market.duckdb")
    ddl = domain_ddl("filings", tuple(reversed(DOMAINS["filings"])))
    connection.execute("DROP TABLE filings")
    connection.execute(ddl)
    with pytest.raises(ValueError, match="core table filings differs"):
        check_core_catalog(connection)
    connection.close()


# --- verify --------------------------------------------------------------------


def _chain_home(tmp_path: Path, *, generations: int = 2) -> Path:
    home = tmp_path / "home"
    initialize(home)
    names = tuple(f"generation-{number}" for number in range(1, generations + 1))
    with open_workspace(home, writable=True) as workspace:
        for index, generation in enumerate(names):
            body = json.loads(document())
            body.update(
                version=str(index + 1),
                generation_id=generation,
                operation_id="operation-" + generation,
                parent_id=names[index - 1] if index else None,
            )
            body["rows"][0]["session_date"] = f"2026-01-{index + 2:02}"
            publication.publish_document(workspace, parse_import(canonical_json_bytes(body)))
    return home


def test_verify_counts_every_generation_not_only_the_catalog(tmp_path: Path) -> None:
    """A generation no catalog row names is still counted against its marker."""
    home = _chain_home(tmp_path)
    with open_workspace(home, writable=True) as workspace:
        row = {**_price_rows()[0], "instrument_id": "ASSET_B", "revision_id": "orphan-r1"}
        _publish(workspace.market, [row], dataset="uncatalogued", generation="orphan")
        report = verify_workspace(workspace)
        assert report["orphan_generations"] == ["orphan"]
        assert verify_workspace(workspace, deep=True) == report
        workspace.market.execute("DELETE FROM prices WHERE generation_id='orphan'")
    with open_workspace(home) as workspace, pytest.raises(ValueError, match="row count differs"):
        verify_workspace(workspace)


def test_verify_checks_the_core_catalog(tmp_path: Path) -> None:
    home = _chain_home(tmp_path)
    with open_workspace(home, writable=True) as workspace:
        workspace.market.execute('ALTER TABLE filings ADD COLUMN "extra" VARCHAR')
    with open_workspace(home) as workspace, pytest.raises(ValueError, match="core table filings"):
        verify_workspace(workspace)


def test_only_deep_verify_audits_duplicates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    home = _chain_home(tmp_path)
    calls: list[int | None] = []
    actual = market_integrity.audit_duplicates

    def counted(connection: duckdb.DuckDBPyConnection, *, pass_bytes: int | None = None) -> None:
        calls.append(pass_bytes)
        actual(connection, pass_bytes=pass_bytes)

    monkeypatch.setattr(market_integrity, "audit_duplicates", counted)
    with open_workspace(home) as workspace:
        verify_workspace(workspace)
        assert calls == []
        verify_workspace(workspace, deep=True)
        assert calls == [None]
        audit_market(workspace.market, BUDGET, deep=True)
        assert calls == [None, None]


# --- core names that resolve past the store ----------------------------------------

_CORE: Final = sorted(market_integrity._core_names())
_SHADOWED: Final = "resolves to an object other than the store's"
_KINDS: Final = ("temporary table", "temporary view", "attached table")


def test_the_core_names_are_every_core_table() -> None:
    assert {"store_info", "market_generations", "quality_flags", *DOMAINS} <= set(_CORE)
    assert set(_CORE) == set(market_integrity._expected_catalog(len(market.MIGRATIONS)))


def _shadow(connection: duckdb.DuckDBPyConnection, name: str, kind: str) -> str:
    """Make ``name`` resolvable past the store; return the statement that undoes it."""
    store = cast("tuple[str]", connection.execute("SELECT current_database()").fetchone())[0]
    stored = f'"{store}".main."{name}"'
    if kind == "temporary table":
        connection.execute(f'CREATE TEMP TABLE "{name}" AS SELECT * FROM {stored}')
        return f'DROP TABLE temp.main."{name}"'
    if kind == "temporary view":
        # DuckDB matches names without case, so an upper-case view shadows as well.
        connection.execute(f'CREATE TEMP VIEW "{name.upper()}" AS SELECT * FROM {stored}')
        return f'DROP VIEW temp.main."{name.upper()}"'
    connection.execute("ATTACH ':memory:' AS shadow_store")
    connection.execute(f'CREATE TABLE shadow_store.main."{name}" AS SELECT * FROM {stored}')
    return "DETACH shadow_store"


def _stored(connection: duckdb.DuckDBPyConnection) -> list[object]:
    """Every core table's rows, read on a fresh cursor that sees no temporary object."""
    reader = connection.cursor()
    try:
        return [reader.execute(f'SELECT * FROM "{name}" ORDER BY ALL').fetchall() for name in _CORE]
    finally:
        reader.close()


def _refused_everywhere(
    connection: duckdb.DuckDBPyConnection, request: BulkRequest, rows: list[dict[str, object]]
) -> None:
    """Both writers, new and reused, and the audit refuse before they read or write."""
    attempts: list[Callable[[], object]] = [
        lambda: _publish(connection, _revised("r9"), dataset="other", generation="g9"),
        lambda: _publish(connection, rows),
        lambda: publish_generation_bulk(
            connection,
            replace(request, dataset_id="other", generation_id="b9", operation_id="op-b9"),
            budget=BUDGET,
        ),
        lambda: publish_generation_bulk(connection, request, budget=BUDGET),
        lambda: audit_market(connection, None, deep=False),
        lambda: audit_market(connection, BUDGET, deep=True),
    ]
    for attempt in attempts:
        with pytest.raises(ValueError, match=_SHADOWED):
            attempt()


@pytest.mark.parametrize("kind", _KINDS)
def test_a_core_name_resolving_past_the_store_is_refused_with_nothing_stored(
    tmp_path: Path, kind: str
) -> None:
    connection = _store(tmp_path / "market.duckdb")
    rows = _price_rows()
    _publish(connection, rows)
    _stage(connection, "prices", _first(random.Random(3), "prices", 2))
    request = replace(
        _request("1", parent=None, domain="prices"), dataset_id="bulk", generation_id="b1"
    )
    publish_generation_bulk(connection, request, budget=BUDGET)
    audit_market(connection, BUDGET, deep=True)
    before = _stored(connection)
    for name in _CORE:
        undo = _shadow(connection, name, kind)
        _refused_everywhere(connection, request, rows)
        # The refusals came before BEGIN: nothing stored changed and no claim is held
        # (an attached database is shared by every cursor, so the rival asks once it is gone).
        assert _stored(connection) == before, name
        connection.execute(undo)
        rival = connection.cursor()
        assert _claim_free(rival), name
        rival.close()
    # With every shadow gone the same connection publishes and audits again.
    _publish(connection, _revised("r9"), dataset="other", generation="g9")
    audit_market(connection, BUDGET, deep=True)
    connection.close()


def test_two_cursors_with_a_temporary_store_info_both_refuse(tmp_path: Path) -> None:
    """The judge's race: each cursor's temporary store_info is refused before any claim."""
    path = tmp_path / "market.duckdb"
    connection = _store(path, keys=False)
    before = _stored(connection)
    other = duckdb.connect(str(path))
    cursors = [connection.cursor(), other]
    for cursor in cursors:
        _shadow(cursor, "store_info", "temporary table")
    for index, cursor in enumerate(cursors):
        with pytest.raises(ValueError, match=_SHADOWED):
            _publish(cursor, _revised(f"r{index}"), dataset=f"d{index}", generation=f"g{index}")
    assert _stored(connection) == before
    assert _claim_free(connection)
    for cursor in cursors:
        cursor.close()
    connection.close()


@pytest.mark.parametrize(
    ("setting", "message"),
    [
        ("SET search_path = 'temp.main'", "not a persistent catalog"),
        ("SET search_path = 'market.main'", "search path is not DuckDB's default"),
        ("SET schema = 'side'", "search path is not DuckDB's default"),
    ],
)
def test_a_changed_search_path_is_refused_with_nothing_stored(
    tmp_path: Path, setting: str, message: str
) -> None:
    connection = _store(tmp_path / "market.duckdb")
    rows = _price_rows()
    _publish(connection, rows)
    connection.execute("CREATE SCHEMA side")
    before = _stored(connection)
    cursor = connection.cursor()
    _stage(cursor, "prices", _first(random.Random(3), "prices", 2))
    cursor.execute(setting)
    request = replace(
        _request("1", parent=None, domain="prices"), dataset_id="bulk", generation_id="b1"
    )
    for attempt in (
        lambda: _publish(cursor, rows),
        lambda: _publish(cursor, _revised("r9"), dataset="other", generation="g9"),
        lambda: publish_generation_bulk(cursor, request, budget=BUDGET),
        lambda: begin_publication(cursor),
        lambda: audit_market(cursor, None, deep=True),
    ):
        with pytest.raises(ValueError, match=message):
            attempt()
    assert _stored(connection) == before
    assert _claim_free(connection)
    cursor.close()
    connection.close()


def test_verify_refuses_a_core_name_resolving_past_the_store(tmp_path: Path) -> None:
    home = _chain_home(tmp_path)
    with open_workspace(home, writable=True) as workspace:
        report = verify_workspace(workspace, deep=True)
        before = _stored(workspace.market)
        for kind in _KINDS:
            for name in _CORE:
                undo = _shadow(workspace.market, name, kind)
                for deep in (False, True):
                    with pytest.raises(ValueError, match=_SHADOWED):
                        verify_workspace(workspace, deep=deep)
                workspace.market.execute(undo)
        workspace.market.execute("SET search_path = 'temp.main'")
        with pytest.raises(ValueError, match="not a persistent catalog"):
            verify_workspace(workspace)
        workspace.market.execute("RESET search_path")
        assert _stored(workspace.market) == before
        assert verify_workspace(workspace, deep=True) == report


def test_promotion_reuse_refuses_a_core_name_resolving_past_the_store(tmp_path: Path) -> None:
    from tests.storage.promotion_support import (  # noqa: PLC0415 -- promotion fixtures
        add_source,
        at,
        bar,
        register_symbols,
        spec,
    )

    root = tmp_path / "aas"
    initialize(root)
    with open_workspace(root, writable=True, strategy_write=True) as workspace:
        day = at("2025-01-02T00:00:00").date()
        late = at("2025-01-10T00:00:00")
        pin = add_source(workspace, [bar("AAA.KO", day, 100.0, retrieved=late)], tag="a")
        identity = register_symbols(workspace, pin["source_id"])
        raw, sha = spec([pin], identity)
        first = engine.promote(workspace, raw, sha, apply=True)
        assert first["published"] is True
        before = _stored(workspace.market)
        for name in _CORE:
            undo = _shadow(workspace.market, name, "temporary table")
            for apply in (True, False):
                with pytest.raises(ValueError, match=_SHADOWED):
                    engine.promote(workspace, raw, sha, apply=apply)
            workspace.market.execute(undo)
        assert _stored(workspace.market) == before
        again = engine.promote(workspace, raw, sha, apply=True)
        assert again["reused"] is True
        assert again["generation_id"] == first["generation_id"]
