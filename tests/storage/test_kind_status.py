"""KIND listed-company lists as ``status.kr.kind`` listing events.

Sources are synthetic KIND lists and EODHD KR symbol lists (``tests.storage.kr_identity_support``)
committed the way ``aas collect kind`` and ``aas identity kr-import`` commit them; the
instrument is reached only through the KR identity registry they build. Every company, code
and ISIN is made up.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest

from aegis_alpha.storage.identity import (
    decode_registry,
    mint_instrument,
    register_identities,
    snapshot_identities,
)
from aegis_alpha.storage.kr_identity import build_from_workspace, eodhd_unit, import_unit, kind_unit
from aegis_alpha.storage.maintain_promotion import ROUTES, promote_datasets, unmapped
from aegis_alpha.storage.promotion.engine import dataset_head, promote
from aegis_alpha.storage.promotion.mappers import IdentityKey, mapper
from aegis_alpha.storage.workspace import Workspace, initialize, open_workspace
from tests.storage.kr_identity_support import (
    commit_legacy_kind_listing,
    eodhd_job,
    isin,
    kind_listing,
    symbol,
)
from tests.storage.promotion_support import us

LISTED, UNLISTED = "100010", "100020"
RULE = {
    "rule": "local_day_end@1",
    "basis": "record",
    "input": "status_date",
    "args": {"timezone": "Asia/Seoul"},
}


@pytest.fixture
def ws(tmp_path: Path) -> Iterator[Workspace]:
    root = tmp_path / "aas"
    initialize(root)
    with open_workspace(root, writable=True, strategy_write=True) as workspace:
        yield workspace


def _kind(ws: Workspace, day: str) -> dict[str, str]:
    """A KOSPI list collected on ``day``: one company twice (two regions), one unregistered."""
    rows = [("합성전자", LISTED, "1975-06-11"), ("합성전자", LISTED, "1975-06-11"),
            ("합성바이오", UNLISTED, "2011-03-04")]  # fmt: skip
    cells = [(None, "서울특별시"), (None, "경기도")]
    unit = kind_unit(*kind_listing(rows, retrieved=f"{day}T06:05:48Z", cells=cells))
    committed = import_unit(ws, unit)
    return {
        "source_id": unit.content.source_id,
        "source_sha256": unit.content.sha256,
        "table": "listings",
        "digest": str(committed["digest"]),
    }


def _identity(ws: Workspace, kind: dict[str, str]) -> dict[str, str]:
    """Register the listed company through its EODHD ISIN and ``kind``; snapshot it."""
    job = eodhd_unit(*eodhd_job([symbol(LISTED, isin("710000100"))]))
    import_unit(ws, job)
    registry = build_from_workspace(ws, eodhd=[job.content.source_id], kind=[kind["source_id"]])
    document = decode_registry(registry.raw(), expected_file_sha256=registry.sha256())
    register_identities(ws.state, document, apply=True)
    snapshot = snapshot_identities(ws.state, "kr", created_at_us=5, apply=True)
    return {"snapshot_id": str(snapshot["snapshot_id"]),
            "content_hash": str(snapshot["content_hash"])}  # fmt: skip


def _spec(pin: dict[str, str], identity: dict[str, str], parent: str | None) -> tuple[bytes, str]:
    document = {
        "schema_version": "aas-promotion-v1",
        "target": {"domain": "instrument_status", "dataset_id": "status.kr.kind",
                   "parent": parent},
        "sources": [pin],
        "mapper": {"name": "kind.status@1", "args": {}},
        "partition": None,
        "time_rules": {"available_at_us": RULE, "revision_known_at_us": RULE},
        "decimal_rule": {},
        "quality_rules": [],
        "tombstone_policy": {"mode": "never"},
        "identity_snapshot": identity,
    }  # fmt: skip
    raw = json.dumps(document, sort_keys=True).encode()
    return raw, hashlib.sha256(raw).hexdigest()


def _seoul(day: date) -> int:
    """UTC microseconds of the Seoul midnight that starts ``day`` (UTC+9, no daylight time)."""
    return us(datetime(day.year, day.month, day.day, tzinfo=UTC) - timedelta(hours=9))


def test_kind_listings_promote_to_status_kr_kind(ws: Workspace) -> None:
    found = mapper("kind.status@1")
    assert (found.domain, found.source_prefixes) == ("instrument_status", ("kind-listings-",))
    assert found.identity({}) == IdentityKey("kind", "krx_short_code")
    assert set(found.source_columns()) == {"short_code", "listed_on", "retrieved_at_utc"}
    first = _kind(ws, "2026-09-06")
    identity = _identity(ws, first)
    published = promote(ws, *_spec(first, identity, None), apply=True)
    assert published["published"] is True, published.get("refusals")
    # The repeated row maps once; the code no list binds to an ISIN stays unresolved.
    assert published["unresolved_token_count"] == 1
    listed = date(1975, 6, 11)
    rows = ws.market.execute(
        "SELECT instrument_id, status_event_id, effective_from_us, effective_to_us, status, "
        "reason, available_at_us, revision_known_at_us, ingested_at_us FROM instrument_status"
    ).fetchall()
    seen = us(datetime(2026, 9, 6, 6, 5, 48, tzinfo=UTC))
    # Listed from the Seoul start of its listing date, known by the end of that date.
    assert rows == [
        (mint_instrument("krx_isin", isin("710000100")), "kind:listed", _seoul(listed), None,
         "listed", "kind_listed_on", _seoul(listed + timedelta(days=1)) - 1,
         _seoul(listed + timedelta(days=1)) - 1, seen),
    ]  # fmt: skip
    # The next day's list says the same: no revision.
    second = _kind(ws, "2026-09-07")
    again = promote(ws, *_spec(second, identity, dataset_head(ws, "status.kr.kind")), apply=True)
    assert again["published"] is False
    assert again.get("empty_delta") is True
    assert ws.market.execute("SELECT count(*) FROM instrument_status").fetchone() == (1,)


def test_the_status_route_starts_the_kind_chain_only_when_named(ws: Workspace) -> None:
    (route,) = [item for item in ROUTES if item.dataset_id == "status.kr.kind"]
    assert (route.mapper, route.prefix, route.table) == (
        "kind.status@1",
        "kind-listings-",
        "listings",
    )
    commit_legacy_kind_listing(ws, LISTED)
    first = _kind(ws, "2026-09-06")
    identity = _identity(ws, first)
    # Collected KIND lists are route candidates now, not unmapped; the legacy table is a shape.
    assert unmapped(ws) == {}
    (waiting,) = cast(
        "list[dict[str, object]]",
        promote_datasets(ws, apply=True, now_us=1, routes=[route], identity=identity)["datasets"],
    )
    assert (waiting["status"], waiting["started"]) == ("no_head", False)
    assert waiting["skipped"] == {"done": 0, "shape": 1, "no_head": 1}
    (run,) = cast(
        "list[dict[str, object]]",
        promote_datasets(ws, apply=True, now_us=2, routes=[route], identity=identity,
                         start=frozenset({"status.kr.kind"}))["datasets"],
    )  # fmt: skip
    assert (run["status"], run["started"]) == ("advanced", True)
    (source,) = cast("list[dict[str, object]]", run["sources"])
    # One partition per collection day in Seoul; the code nobody registered waits.
    (step,) = cast("list[dict[str, object]]", source["steps"])
    assert step["partition"] == {"from": "2026-09-06", "to": "2026-09-07"}
    assert source["status"] == "unresolved"
    head = dataset_head(ws, "status.kr.kind")
    assert head is not None
    rules = ws.state.execute(
        "SELECT normalizer_version FROM dataset_versions WHERE generation_id=?", (head,)
    ).fetchone()
    assert tuple(rules) == ("kind.status@1",)
    assert ws.market.execute("SELECT count(*) FROM instrument_status").fetchone() == (1,)
