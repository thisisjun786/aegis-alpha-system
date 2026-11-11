"""Qveris ``USDKRW.FOREX`` daily closes as ``fx.usdkrw.eodhd`` rates.

Sources are completed Qveris FX history jobs replayed through the scripted gateway
(``tests.data.qveris_support``) and committed the way ``aas collect qveris import`` commits
them. Every rate and session is made up.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from aegis_alpha.data.qveris_contracts import QverisJob
from aegis_alpha.data.qveris_native import read_completed_job
from aegis_alpha.storage.maintain_promotion import head_pin
from aegis_alpha.storage.promotion.engine import promote
from aegis_alpha.storage.promotion.mappers import mapper
from aegis_alpha.storage.qveris_import import build_unit, completions, import_unit
from aegis_alpha.storage.workspace import Workspace, initialize, open_workspace
from tests.data.qveris_support import complete, fx_job
from tests.storage.promotion_support import publish_calendar, us

# Serial: these tests take the host-wide Qveris account lease (an abstract Unix socket named by
# the account), so every file that takes it runs in one xdist worker.
pytestmark = pytest.mark.xdist_group("qveris-account-lease")

MON, TUE, WED = date(2026, 8, 31), date(2026, 9, 1), date(2026, 9, 2)
HOUR = 3_600 * 10**6
LAG = 4 * HOUR


@pytest.fixture
def ws(tmp_path: Path) -> Iterator[Workspace]:
    root = tmp_path / "aas"
    initialize(root)
    with open_workspace(root, writable=True, strategy_write=True) as workspace:
        yield workspace


def _utc(day: date, hours: float = 0) -> int:
    return us(datetime(day.year, day.month, day.day, tzinfo=UTC) + timedelta(hours=hours))


def _closes(*days: tuple[date, float]) -> list[dict[str, object]]:
    return [{"date": day.isoformat(), "open": value, "high": value, "low": value, "close": value,
             "adjusted_close": value, "volume": 0} for day, value in days]  # fmt: skip


def _replay(ws: Workspace, raw: Path, job: QverisJob, rows: list[dict[str, object]]) -> dict:
    complete(raw, job, rows)
    (completion,), missing = completions(raw, fingerprints=[job.fingerprint])
    assert missing == []
    history = read_completed_job(raw, completion.fingerprint, completion.completion_sha256)
    unit = build_unit(completion, history, None)
    report = import_unit(ws, unit)
    return {
        "source_id": unit.source_id,
        "source_sha256": unit.content.sha256,
        "table": unit.kind.table,
        "digest": str(report["digest"]),
    }


def _spec(pin: dict[str, str], calendar: dict[str, str], parent: str | None) -> tuple[bytes, str]:
    rule = {
        "rule": "session_close_plus_lag@1",
        "basis": "record",
        "input": "fixing_date",
        "args": {"calendar": calendar, "calendar_id": "XNYS", "venue": "XNYS", "lag_us": LAG},
    }
    document = {
        "schema_version": "aas-promotion-v1",
        "target": {"domain": "fx_rates", "dataset_id": "fx.usdkrw.eodhd", "parent": parent},
        "sources": [pin],
        "mapper": {"name": "eodhd.fx_history@1",
                   "args": {"base": "USD", "quote": "KRW", "timezone": "UTC"}},
        "partition": {"from": MON.isoformat(), "to": (WED + timedelta(days=1)).isoformat()},
        "time_rules": {"available_at_us": rule, "revision_known_at_us": rule},
        "decimal_rule": {"rate": "float_shortest@1"},
        "quality_rules": [],
        "tombstone_policy": {"mode": "never"},
        "identity_snapshot": None,
    }  # fmt: skip
    raw = json.dumps(document, sort_keys=True).encode()
    return raw, hashlib.sha256(raw).hexdigest()


def test_usdkrw_forex_history_promotes_to_fx_usdkrw_eodhd(ws: Workspace, tmp_path: Path) -> None:
    found = mapper("eodhd.fx_history@1")
    assert (found.domain, found.source_prefixes) == ("fx_rates", ("qveris-fx-history-bars-",))
    assert found.identity({}) is None
    assert found.time_inputs == {"fixing_date": "date"}
    for bad in ({"base": "USD", "quote": "USD", "timezone": "UTC"},
                {"base": "USD", "quote": "KRW"}):  # fmt: skip
        with pytest.raises(ValueError, match=r"eodhd\.fx_history@1"):
            found.check_args(bad)
    raw = tmp_path / "qveris"
    first = _replay(ws, raw, fx_job(), _closes((MON, 1385.25), (TUE, 1390.5), (WED, 1388.0)))
    # XNYS closes at 20:00 UTC on Monday and Wednesday and does not open on Tuesday.
    sessions = {MON: (_utc(MON, 13.5), _utc(MON, 20)), WED: (_utc(WED, 13.5), _utc(WED, 20))}
    calendar = publish_calendar(ws, sessions, dataset="sessions.xnys", calendar_id="XNYS")
    published = promote(ws, *_spec(first, calendar, None), apply=True)
    assert published["published"] is True, published.get("refusals")
    rows = ws.market.execute(
        "SELECT base_currency, quote_currency, fixing_at_us, rate, value_state, available_at_us, "
        "revision_known_at_us FROM fx_rates ORDER BY fixing_at_us"
    ).fetchall()
    # The fixing is the end of the UTC day; the rate is known at the XNYS close plus the lag,
    # and a day the calendar does not open keeps no time a strict read could use.
    assert rows == [
        ("USD", "KRW", _utc(TUE) - 1, Decimal("1385.25"), "present", _utc(MON, 20) + LAG,
         _utc(MON, 20) + LAG),
        ("USD", "KRW", _utc(WED) - 1, Decimal("1390.5"), "present", None, None),
        ("USD", "KRW", _utc(WED + timedelta(days=1)) - 1, Decimal(1388), "present",
         _utc(WED, 20) + LAG, _utc(WED, 20) + LAG),
    ]  # fmt: skip
    # A later download repeats the window: an unchanged close is no revision, a changed one
    # supersedes the earlier rate.
    later = replace(fx_job(), job_id="fx-USDKRW-later", observation_date=date(2026, 9, 7))
    second = _replay(ws, raw, later, _closes((MON, 1385.25), (TUE, 1390.5), (WED, 1389.75)))
    head = str(published["generation_id"])
    pin = head_pin(ws, "sessions.xnys")
    assert pin is not None
    corrected = promote(ws, *_spec(second, pin, head), apply=True)
    assert corrected["published"] is True, corrected.get("refusals")
    assert corrected["operations"] == {"SUPERSEDE": 1}
    changed = ws.market.execute(
        "SELECT rate, op FROM fx_rates WHERE generation_id=?", [corrected["generation_id"]]
    ).fetchall()
    assert changed == [(Decimal("1389.75"), "SUPERSEDE")]
