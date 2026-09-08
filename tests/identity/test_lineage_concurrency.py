"""Deterministic lock-ordering tests for the snapshot lineage interlock.

A concurrency test that merely runs two threads and checks the end state can
pass without either transaction ever contending: if the second one starts after
the first finished, nothing was proven. Every test here therefore

1. holds the first transaction open until the second has demonstrably started,
2. proves the second is genuinely waiting by polling ``pg_blocking_pids``,
3. requires the specific expected error rather than accepting any outcome,
4. asserts both worker threads terminated, and
5. asserts the exact final parent and child rows.

Both lock orderings are covered for both child triggers, because the
``FOR SHARE`` read and the parent-side guard protect different halves of the
race and only their combination makes drift unreachable.
"""

from __future__ import annotations

import threading
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Literal

import pytest
from sqlalchemy import text

from aegis_alpha.identity.records import Instrument, InstrumentKind, Issuer
from aegis_alpha.identity.registry import IdentityRegistry

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy import Engine
    from sqlalchemy.sql.elements import TextClause

_NOW = datetime(2026, 7, 31, tzinfo=UTC)
_START = datetime(2020, 1, 1, tzinfo=UTC)
_SNAPSHOT = "snap-lock"
_LINEAGE_FROZEN = "lineage is immutable"
_HANDSHAKE_TIMEOUT = 20.0
_BLOCK_TIMEOUT = 10.0
_BLOCK_POLL_INTERVAL = 0.02

_INSERT_MAPPING = text(
    "INSERT INTO identity_provider_mappings (mapping_id, schema_version, provider, "
    "namespace, provider_identifier, instrument_id, source_snapshot_id, effective_start, "
    "asserted_at_utc, evidence_json, mapping_sha256, source_provider, "
    "source_validation_status) VALUES ('m-lock', 1, 'norgate', 'us-equities', '31337', "
    "'ins-lock', :snapshot, :start, :now, '{}', :digest, 'claimed', 'claimed')"
)
_INSERT_ASSERTION = text(
    "INSERT INTO identity_identifier_assertions (assertion_id, schema_version, entity_type, "
    "entity_id, identifier_type, identifier_value, source_value, source_snapshot_id, "
    "effective_start, asserted_at_utc, evidence_json, assertion_sha256, "
    "source_validation_status) VALUES ('a-lock', 1, 'instrument', 'ins-lock', 'ticker', "
    "'AAPL', 'AAPL', :snapshot, :start, :now, '{}', :digest, 'claimed')"
)
_FLIP_PARENT = text(
    "UPDATE source_snapshots SET validation_status = 'BLOCKED' WHERE snapshot_id = :snapshot"
)

ChildKind = Literal["mapping", "assertion"]


def _child_statement(child: ChildKind) -> TextClause:
    return _INSERT_MAPPING if child == "mapping" else _INSERT_ASSERTION


def _child_params(child: ChildKind) -> dict[str, object]:
    digest = ("c" if child == "mapping" else "a") * 64
    return {"snapshot": _SNAPSHOT, "start": _START, "now": _NOW, "digest": digest}


def _seed(registry: IdentityRegistry, register_source_snapshot: Callable[..., None]) -> None:
    register_source_snapshot(_SNAPSHOT, provider="norgate")
    registry.register_issuer(Issuer("iss-lock", _NOW))
    registry.register_instrument(Instrument("ins-lock", "iss-lock", InstrumentKind.EQUITY, _NOW))


def _wait_until_blocked(engine: Engine, pid_holder: dict[str, int]) -> int:
    """Return how many backends block the follower, proving it actually waits.

    Polling ``pg_blocking_pids`` from a third connection is what separates a real
    lock-ordering test from one that would also pass if the two transactions
    never overlapped.
    """

    deadline = time.monotonic() + _BLOCK_TIMEOUT
    while time.monotonic() < deadline:
        pid = pid_holder.get("pid")
        if pid is not None:
            with engine.connect() as connection:
                blocking = connection.execute(
                    text("SELECT cardinality(pg_blocking_pids(:pid))"), {"pid": pid}
                ).scalar_one()
            if blocking:
                return int(blocking)
        time.sleep(_BLOCK_POLL_INTERVAL)
    return 0


def _final_state(engine: Engine, child: ChildKind) -> tuple[tuple[str, str], int]:
    table = "identity_provider_mappings" if child == "mapping" else "identity_identifier_assertions"
    with engine.connect() as connection:
        parent = connection.execute(
            text(
                "SELECT provider, validation_status FROM source_snapshots "
                "WHERE snapshot_id = :snapshot"
            ),
            {"snapshot": _SNAPSHOT},
        ).one()
        children = connection.execute(
            text(f"SELECT count(*) FROM {table} WHERE source_snapshot_id = :snapshot"),  # noqa: S608
            {"snapshot": _SNAPSHOT},
        ).scalar_one()
    return (parent[0], parent[1]), int(children)


@pytest.mark.parametrize("child", ["mapping", "assertion"])
def test_child_first_makes_parent_flip_wait_then_fail(
    identity_registry: IdentityRegistry,
    register_source_snapshot: Callable[..., None],
    clean_postgres: Engine,
    child: ChildKind,
) -> None:
    """A child insert holds the snapshot, so a concurrent parent flip is refused.

    The child trigger reads its snapshot ``FOR SHARE``. The parent update must
    therefore block until the child commits and then be rejected by the guard,
    leaving the snapshot lineage untouched and the child intact.
    """

    _seed(identity_registry, register_source_snapshot)
    child_holding = threading.Event()
    allow_child_commit = threading.Event()
    follower: dict[str, int] = {}
    outcome: dict[str, object] = {}

    def insert_child_and_hold() -> None:
        with clean_postgres.connect() as connection:
            connection.execute(text("BEGIN"))
            connection.execute(_child_statement(child), _child_params(child))
            child_holding.set()
            allow_child_commit.wait(timeout=_HANDSHAKE_TIMEOUT)
            connection.execute(text("COMMIT"))
            outcome["child"] = "committed"

    def flip_parent() -> None:
        child_holding.wait(timeout=_HANDSHAKE_TIMEOUT)
        with clean_postgres.connect() as connection:
            follower["pid"] = connection.execute(text("SELECT pg_backend_pid()")).scalar_one()
            connection.execute(text("BEGIN"))
            try:
                connection.execute(_FLIP_PARENT, {"snapshot": _SNAPSHOT})
                connection.execute(text("COMMIT"))
                outcome["parent"] = "committed"
            except Exception as error:  # noqa: BLE001
                outcome["parent"] = error

    holder = threading.Thread(target=insert_child_and_hold)
    flipper = threading.Thread(target=flip_parent)
    holder.start()
    flipper.start()
    blocking = _wait_until_blocked(clean_postgres, follower)
    allow_child_commit.set()
    holder.join(timeout=_HANDSHAKE_TIMEOUT)
    flipper.join(timeout=_HANDSHAKE_TIMEOUT)

    assert blocking >= 1, "the parent flip must genuinely wait on the child's snapshot lock"
    assert not holder.is_alive()
    assert not flipper.is_alive()
    assert outcome["child"] == "committed"
    parent_error = outcome["parent"]
    assert isinstance(parent_error, Exception), "the parent flip must be refused, not committed"
    assert _LINEAGE_FROZEN in str(parent_error)

    lineage, children = _final_state(clean_postgres, child)
    assert lineage == ("norgate", "PASS")
    assert children == 1


@pytest.mark.parametrize("child", ["mapping", "assertion"])
def test_parent_first_makes_child_insert_wait_then_fail(
    identity_registry: IdentityRegistry,
    register_source_snapshot: Callable[..., None],
    clean_postgres: Engine,
    child: ChildKind,
) -> None:
    """An in-flight lineage change blocks a child that would cite the old value.

    While no evidence references the snapshot the guard permits the update, so
    this ordering is guarded by the child's own ``FOR SHARE`` read: it waits for
    the parent transaction, then observes ``BLOCKED`` and is rejected. The child
    can never be admitted against the pre-update lineage.
    """

    _seed(identity_registry, register_source_snapshot)
    parent_holding = threading.Event()
    allow_parent_commit = threading.Event()
    follower: dict[str, int] = {}
    outcome: dict[str, object] = {}

    def flip_parent_and_hold() -> None:
        with clean_postgres.connect() as connection:
            connection.execute(text("BEGIN"))
            connection.execute(_FLIP_PARENT, {"snapshot": _SNAPSHOT})
            parent_holding.set()
            allow_parent_commit.wait(timeout=_HANDSHAKE_TIMEOUT)
            connection.execute(text("COMMIT"))
            outcome["parent"] = "committed"

    def insert_child() -> None:
        parent_holding.wait(timeout=_HANDSHAKE_TIMEOUT)
        with clean_postgres.connect() as connection:
            follower["pid"] = connection.execute(text("SELECT pg_backend_pid()")).scalar_one()
            connection.execute(text("BEGIN"))
            try:
                connection.execute(_child_statement(child), _child_params(child))
                connection.execute(text("COMMIT"))
                outcome["child"] = "committed"
            except Exception as error:  # noqa: BLE001
                outcome["child"] = error

    holder = threading.Thread(target=flip_parent_and_hold)
    inserter = threading.Thread(target=insert_child)
    holder.start()
    inserter.start()
    blocking = _wait_until_blocked(clean_postgres, follower)
    allow_parent_commit.set()
    holder.join(timeout=_HANDSHAKE_TIMEOUT)
    inserter.join(timeout=_HANDSHAKE_TIMEOUT)

    assert blocking >= 1, "the child insert must genuinely wait on the parent's row lock"
    assert not holder.is_alive()
    assert not inserter.is_alive()
    assert outcome["parent"] == "committed"
    child_error = outcome["child"]
    assert isinstance(child_error, Exception), "the child insert must be refused, not committed"
    assert "source_status_admissible" in str(child_error)

    lineage, children = _final_state(clean_postgres, child)
    assert lineage == ("norgate", "BLOCKED")
    assert children == 0
