# ruff: noqa: S608 -- test-owned SQL
"""Qveris exchange-day splits and dividends as corporate actions (``actions.*.eodhd``).

Sources are completed Qveris jobs replayed through the scripted gateway
(``tests.data.qveris_support``) and committed the way ``aas collect qveris import`` commits
them. Every symbol, ratio, amount and session is made up.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator, Mapping
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import duckdb
import pyarrow as pa
import pytest

from aegis_alpha.data.qveris_contracts import QverisJob
from aegis_alpha.data.qveris_native import read_completed_job
from aegis_alpha.storage.adjusted_prices import CASH_ACTIONS, RATIO_ACTIONS
from aegis_alpha.storage.identity import mint_instrument
from aegis_alpha.storage.maintain_promotion import ROUTES, head_pin, promote_datasets, unmapped
from aegis_alpha.storage.promotion.engine import dataset_head, promote
from aegis_alpha.storage.promotion.mappers import IdentityKey, mapper
from aegis_alpha.storage.qveris_import import build_unit, completions, import_unit, parse_identity
from aegis_alpha.storage.workspace import Workspace, initialize, open_workspace
from tests.data.qveris_support import bulk_job, complete, fx_job, identity_bytes
from tests.storage.promotion_support import publish_calendar, register_symbols, us

# Serial: these tests take the host-wide Qveris account lease (an abstract Unix socket named by
# the account), so every file that takes it runs in one xdist worker.
pytestmark = pytest.mark.xdist_group("qveris-account-lease")

EX = date(2026, 8, 31)
NEXT = date(2026, 9, 1)
NEW_YORK, SEOUL = "America/New_York", "Asia/Seoul"
HOUR = 3_600 * 10**6
US_STOCKS = ("AAA", "BBB", "CCC", "DDD")
KR_STOCKS = (("123456", "KO"), ("654321", "KQ"))
ANCHORS = {f"{code}.US": f"20000{n}" for n, code in enumerate(US_STOCKS, 1)} | {
    f"{code}.{exchange}": f"30000{n}" for n, (code, exchange) in enumerate(KR_STOCKS, 1)
}


def _identity(symbol: str) -> dict[str, str]:
    exchange = symbol.rsplit(".", 1)[1]
    return {
        "instrument_id": "synthetic-" + symbol.lower(),
        "venue": exchange,
        "instrument_type": "Common Stock",
        "currency": "USD" if exchange == "US" else "KRW",
    }


IDENTITIES = {symbol: _identity(symbol) for symbol in ANCHORS}


@pytest.fixture
def ws(tmp_path: Path) -> Iterator[Workspace]:
    root = tmp_path / "aas"
    initialize(root)
    with open_workspace(root, writable=True, strategy_write=True) as workspace:
        yield workspace


def replay(ws: Workspace, raw: Path, job: QverisJob, rows: list[dict[str, object]]) -> dict:
    """Complete ``job`` with ``rows`` through the scripted gateway and import it; its pin."""
    complete(raw, job, rows)
    (completion,), missing = completions(raw, fingerprints=[job.fingerprint])
    assert missing == []
    history = read_completed_job(raw, completion.fingerprint, completion.completion_sha256)
    unit = build_unit(completion, history, parse_identity(identity_bytes(IDENTITIES)))
    report = import_unit(ws, unit)
    return {
        "source_id": unit.source_id,
        "source_sha256": unit.content.sha256,
        "table": unit.kind.table,
        "digest": str(report["digest"]),
    }


def opens(ws: Workspace, venue: str, days: tuple[date, ...]) -> dict[date, int]:
    """Publish ``sessions.<venue>`` with a 09:30 local open on ``days``; their opens."""
    zone = NEW_YORK if venue == "XNYS" else SEOUL
    found = {}
    for day in days:
        offset = 4 if zone == NEW_YORK else -9  # summer: New York is UTC-4, Seoul UTC+9
        found[day] = us_at(day, 9.5 + offset)
    publish_calendar(
        ws,
        {day: (opened, opened + 6 * HOUR) for day, opened in found.items()},
        dataset=f"sessions.{venue.lower()}",
        calendar_id=venue,
    )
    return found


def us_at(day: date, hours: float) -> int:
    """UTC microseconds ``hours`` after the UTC midnight starting ``day``."""
    start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    return us(start + timedelta(hours=hours))


def calendar_pin(ws: Workspace, venue: str) -> dict[str, str]:
    found = head_pin(ws, f"sessions.{venue.lower()}")
    assert found is not None
    return found


def action_spec(  # noqa: PLR0913 -- every spec field these tests vary
    ws: Workspace,
    dataset: str,
    name: str,
    pins: list[dict[str, str]],
    *,
    identity: dict[str, str],
    venue: str,
    parent: str | None,
) -> tuple[bytes, str]:
    rule = {
        "rule": "exdate_open@1",
        "basis": "record",
        "input": "ex_date",
        "args": {"calendar": calendar_pin(ws, venue), "calendar_id": venue, "venue": venue},
    }
    numeric = {"amount": "decimal_text@1"} if "dividends" in name else {"ratio": "float_shortest@1"}
    document = {
        "schema_version": "aas-promotion-v1",
        "target": {"domain": "corporate_actions", "dataset_id": dataset, "parent": parent},
        "sources": pins,
        "mapper": {"name": name, "args": {"timezone": NEW_YORK if venue == "XNYS" else SEOUL}},
        "partition": {"from": EX.isoformat(), "to": NEXT.isoformat()},
        "time_rules": {"available_at_us": rule, "revision_known_at_us": rule},
        "decimal_rule": numeric,
        "quality_rules": [],
        "tombstone_policy": {"mode": "never"},
        "identity_snapshot": identity,
    }
    raw = json.dumps(document, sort_keys=True).encode()
    return raw, hashlib.sha256(raw).hexdigest()


def stored(ws: Workspace, generation: str) -> list[tuple[Any, ...]]:
    return ws.market.execute(
        "SELECT instrument_id, action_id, action_type, ex_date, record_date, pay_date, "
        "effective_date, amount, ratio, currency, value_state, available_at_us, "
        "revision_known_at_us FROM corporate_actions WHERE generation_id=? "
        "ORDER BY instrument_id, action_id",
        [generation],
    ).fetchall()


def instrument(symbol: str) -> str:
    return mint_instrument("norgate_assetid", ANCHORS[symbol])


def _split(code: str, exchange: str, ratio: str) -> dict[str, object]:
    return {"code": code, "exchange": exchange, "date": EX.isoformat(), "split": ratio}


def _dividend(code: str, exchange: str, value: object, **extra: object) -> dict[str, object]:
    currency = "USD" if exchange == "US" else "KRW"
    return {"code": code, "exchange": exchange, "date": EX.isoformat(), "dividend": value,
            "currency": currency, **extra}  # fmt: skip


def test_us_splits_and_dividends_promote_to_actions_us_eodhd(ws: Workspace, tmp_path: Path) -> None:
    raw = tmp_path / "qveris"
    splits = replay(ws, raw, bulk_job("US", "splits", EX.isoformat()), [
        _split("AAA", "US", "1.000000/10.000000"),  # a 1-for-10 consolidation
        _split("BBB", "US", "3.000000/2.000000"),
    ])  # fmt: skip
    dividends = replay(ws, raw, bulk_job("US", "dividends", EX.isoformat()), [
        # The amount as paid is the unadjusted value when the provider states one.
        _dividend("AAA", "US", "0.25", unadjustedValue="2.5", recordDate="2026-09-01",
                  paymentDate="2026-09-15", declarationDate="2026-08-01", period="Quarterly"),
        # Without an unadjusted value the stated dividend is the amount; no date stays NULL.
        _dividend("BBB", "US", 0.069, recordDate="0000-00-00"),
        # The collector holds a dividend that is no positive decimal; it never reaches here.
        _dividend("CCC", "US", "-1"),
    ])  # fmt: skip
    identity = register_symbols(ws, splits["source_id"], ANCHORS, name="us")
    opened = opens(ws, "XNYS", (EX,))[EX]
    first = promote(
        ws,
        *action_spec(
            ws,
            "actions.us.eodhd",
            "eodhd.splits@1",
            [splits],
            identity=identity,
            venue="XNYS",
            parent=None,
        ),
        apply=True,
    )
    assert first["published"] is True, first.get("refusals")
    second = promote(
        ws,
        *action_spec(
            ws,
            "actions.us.eodhd",
            "eodhd.dividends@1",
            [dividends],
            identity=identity,
            venue="XNYS",
            parent=str(first["generation_id"]),
        ),
        apply=True,
    )  # fmt: skip
    assert second["published"] is True, second.get("refusals")
    known = (opened, opened)
    assert stored(ws, str(first["generation_id"])) == sorted([
        (instrument("AAA.US"), "split:2026-08-31", "split", EX, None, None, EX, None,
         Decimal("0.1"), None, "present", *known),
        (instrument("BBB.US"), "split:2026-08-31", "split", EX, None, None, EX, None,
         Decimal("1.5"), None, "present", *known),
    ])  # fmt: skip
    assert stored(ws, str(second["generation_id"])) == sorted([
        (instrument("AAA.US"), "dividend:2026-08-31", "dividend", EX, date(2026, 9, 1),
         date(2026, 9, 15), EX, Decimal("2.5"), None, "USD", "present", *known),
        (instrument("BBB.US"), "dividend:2026-08-31", "dividend", EX, None, None, EX,
         Decimal("0.069"), None, "USD", "present", *known),
    ])  # fmt: skip
    # The adjusted-price reader applies both action types.
    assert {"split"} <= RATIO_ACTIONS
    assert {"dividend"} <= CASH_ACTIONS


def test_kr_splits_and_dividends_promote_to_actions_kr_eodhd(ws: Workspace, tmp_path: Path) -> None:
    raw = tmp_path / "qveris"
    splits = replay(ws, raw, bulk_job("KO", "splits", EX.isoformat()),
                    [_split("123456", "KO", "5.000000/1.000000")])  # fmt: skip
    dividends = replay(ws, raw, bulk_job("KQ", "dividends", EX.isoformat()),
                       [_dividend("654321", "KQ", "361", unadjustedValue="361")])  # fmt: skip
    identity = register_symbols(ws, splits["source_id"], ANCHORS, name="kr")
    opened = opens(ws, "XKRX", (EX,))[EX]
    first = promote(
        ws,
        *action_spec(
            ws,
            "actions.kr.eodhd",
            "eodhd.splits@1",
            [splits],
            identity=identity,
            venue="XKRX",
            parent=None,
        ),
        apply=True,
    )
    assert first["published"] is True, first.get("refusals")
    second = promote(
        ws,
        *action_spec(
            ws,
            "actions.kr.eodhd",
            "eodhd.dividends@1",
            [dividends],
            identity=identity,
            venue="XKRX",
            parent=str(first["generation_id"]),
        ),
        apply=True,
    )  # fmt: skip
    assert second["published"] is True, second.get("refusals")
    assert stored(ws, str(first["generation_id"])) == [
        (instrument("123456.KO"), "split:2026-08-31", "split", EX, None, None, EX, None,
         Decimal(5), None, "present", opened, opened),
    ]  # fmt: skip
    assert stored(ws, str(second["generation_id"])) == [
        (instrument("654321.KQ"), "dividend:2026-08-31", "dividend", EX, None, None, EX,
         Decimal(361), None, "KRW", "present", opened, opened),
    ]  # fmt: skip
    # The instrument resolves at the start of the ex-date in Seoul.
    rows = _mapped(
        [{"provider_symbol": "123456.KO", "date": EX, "split": "5.000000/1.000000"}],
        "eodhd.splits@1",
        {"timezone": SEOUL},
        "_aas_id_token, _aas_id_at_us, _aas_t_ex_date",
    )
    assert rows == [("123456.KO", us_at(EX, -9), EX)]


# --- the mapper SQL alone --------------------------------------------------------------------

_TYPES = {"date": pa.date32(), "retrieved_at": pa.timestamp("us", tz="UTC")}


def _mapped(
    rows: list[dict[str, object]], name: str, args: Mapping[str, object], columns: str
) -> list[tuple[Any, ...]]:
    retrieved = datetime(2026, 9, 6, tzinfo=UTC)
    names = sorted({key for row in rows for key in row} | {"retrieved_at"})
    schema = pa.schema([(key, _TYPES.get(key, pa.string())) for key in names])
    table = pa.table(
        {key: [row.get(key, retrieved if key == "retrieved_at" else None) for row in rows]
         for key in names},
        schema=schema,
    )  # fmt: skip
    connection = duckdb.connect()
    connection.execute("SET TimeZone='UTC'")
    connection.register("_arrow", table)
    connection.execute(
        "CREATE TABLE src AS SELECT 0::INTEGER AS _aas_pin, (row_number() OVER () - 1)::BIGINT "
        "AS _aas_ordinal, 'h' || (row_number() OVER ()) AS _aas_row_hash, * FROM _arrow"
    )
    found = mapper(name)
    found.check_args(dict(args))
    return connection.execute(
        f"SELECT {columns} FROM ({found.select('src', dict(args))}) ORDER BY _aas_ordinal"
    ).fetchall()


def test_eodhd_action_mappers_read_the_qveris_columns() -> None:
    for name, kind, table in (("eodhd.splits@1", "split", "splits"),
                              ("eodhd.dividends@1", "dividend", "dividends")):  # fmt: skip
        found = mapper(name)
        assert found.domain == "corporate_actions"
        assert found.identity({"timezone": NEW_YORK}) == IdentityKey("eodhd", "eodhd_symbol")
        assert found.time_inputs == {"ex_date": "date"}
        assert found.source_prefixes == (f"qveris-{table}-",)
        assert kind in RATIO_ACTIONS | CASH_ACTIONS
        with pytest.raises(ValueError, match=name):
            found.check_args({"timezone": NEW_YORK, "revision": 1})
    day = {"provider_symbol": "AAA.US", "date": EX}
    dividends = _mapped(
        [
            {**day, "dividend": "0.25", "unadjusted_value": None, "dividend_currency": "USD",
             "record_date": "2026-09-01", "payment_date": "2026-9-15"},
            # A currency that is no ISO code, and an amount that is no positive decimal.
            {**day, "provider_symbol": "BBB.US", "dividend": "0.25", "dividend_currency": "usd"},
            {**day, "provider_symbol": "CCC.US", "dividend": "0", "dividend_currency": "USD"},
        ],
        "eodhd.dividends@1",
        {"timezone": NEW_YORK},
        "_aas_id_token, action_id, record_date, pay_date, amount, currency, value_state",
    )  # fmt: skip
    assert dividends == [
        ("AAA.US", "dividend:2026-08-31", date(2026, 9, 1), None, "0.25", "USD", "present"),
        ("BBB.US", "dividend:2026-08-31", None, None, None, None, "invalid"),
        ("CCC.US", "dividend:2026-08-31", None, None, None, "USD", "invalid"),
    ]
    splits = _mapped(
        [{**day, "split": "2/0"}, {**day, "provider_symbol": "BBB.US", "split": "1:2"}],
        "eodhd.splits@1",
        {"timezone": NEW_YORK},
        "_aas_id_token, action_type, ratio, value_state",
    )
    assert splits == [("AAA.US", "split", None, "invalid"), ("BBB.US", "split", None, "invalid")]


# --- maintenance: the new routes see the collected sources, and start only when allowed ------

NEW_DATASETS = ("actions.us.eodhd", "actions.kr.eodhd", "fx.usdkrw.eodhd")


def _runs(report: dict[str, object]) -> dict[tuple[str, str], dict[str, object]]:
    return {
        (str(item["dataset_id"]), str(item["mapper"])): item
        for item in cast("list[dict[str, object]]", report["datasets"])
    }


def _fx_rows(*days: tuple[str, float]) -> list[dict[str, object]]:
    return [{"date": day, "open": close, "high": close, "low": close, "close": close,
             "adjusted_close": close, "volume": 0} for day, close in days]  # fmt: skip


def test_the_new_routes_see_collected_sources_and_start_only_named_datasets(
    ws: Workspace, tmp_path: Path
) -> None:
    routes = [route for route in ROUTES if route.dataset_id in NEW_DATASETS]
    assert {(route.dataset_id, route.mapper) for route in routes} == {
        ("actions.us.eodhd", "eodhd.splits@1"),
        ("actions.us.eodhd", "eodhd.dividends@1"),
        ("actions.kr.eodhd", "eodhd.splits@1"),
        ("actions.kr.eodhd", "eodhd.dividends@1"),
        ("fx.usdkrw.eodhd", "eodhd.fx_history@1"),
    }
    raw = tmp_path / "qveris"
    us_splits = replay(ws, raw, bulk_job("US", "splits", EX.isoformat()),
                       [_split("AAA", "US", "2/1")])  # fmt: skip
    replay(ws, raw, bulk_job("US", "dividends", EX.isoformat()), [_dividend("BBB", "US", "0.5")])
    replay(ws, raw, bulk_job("KO", "dividends", EX.isoformat()),
           [_dividend("123456", "KO", "100")])  # fmt: skip
    # An exchange day without splits is a committed, empty table.
    replay(ws, raw, bulk_job("KQ", "splits", EX.isoformat()), [])
    replay(ws, raw, fx_job(), _fx_rows(("2026-08-31", 1385.25), ("2026-09-01", 1390.5)))
    # Collected sources are now route candidates; without a head nothing starts.
    planned = promote_datasets(ws, apply=True, now_us=1, routes=routes)
    assert unmapped(ws) == {}
    for key, run in _runs(planned).items():
        expected = 0 if key == ("actions.kr.eodhd", "eodhd.splits@1") else 1
        assert (run["status"], run["started"]) == ("no_head" if expected else "current", False), key
        assert cast("dict[str, int]", run["skipped"]).get("no_head", 0) == expected, key
    assert tuple(ws.state.execute("SELECT count(*) FROM dataset_versions").fetchone()) == (0,)

    # Starting needs the calendar the declared time rule pins and the run's identity snapshot.
    identity = register_symbols(ws, us_splits["source_id"], ANCHORS, name="maintain-synthetic")
    blocked = _runs(promote_datasets(ws, apply=True, now_us=2, routes=routes, identity=identity,
                                     start=frozenset(NEW_DATASETS)))  # fmt: skip
    assert blocked[("actions.us.eodhd", "eodhd.splits@1")]["status"] == "no_calendar"
    assert blocked[("fx.usdkrw.eodhd", "eodhd.fx_history@1")]["status"] == "no_calendar"
    opens(ws, "XNYS", (EX, NEXT))
    opens(ws, "XKRX", (EX,))
    unpinned = _runs(promote_datasets(ws, apply=False, now_us=3, routes=routes, identity=None,
                                      start=frozenset(NEW_DATASETS)))  # fmt: skip
    assert unpinned[("actions.us.eodhd", "eodhd.splits@1")]["status"] == "identity_unavailable"
    assert unpinned[("fx.usdkrw.eodhd", "eodhd.fx_history@1")]["status"] == "planned"

    started = _runs(promote_datasets(ws, apply=True, now_us=4, routes=routes, identity=identity,
                                     start=frozenset(NEW_DATASETS)))  # fmt: skip
    for key in (("actions.us.eodhd", "eodhd.splits@1"), ("actions.us.eodhd", "eodhd.dividends@1"),
                ("actions.kr.eodhd", "eodhd.dividends@1"), ("fx.usdkrw.eodhd",
                                                            "eodhd.fx_history@1")):  # fmt: skip
        run = started[key]
        assert (run["status"], run["started"]) == ("advanced", True), (key, run)
    # The dividends generation continues the chain the splits generation started.
    chain = ws.state.execute(
        "SELECT normalizer_version, parent_generation_id IS NULL FROM dataset_versions "
        "WHERE dataset_id='actions.us.eodhd' ORDER BY sequence"
    ).fetchall()
    assert [tuple(row) for row in chain] == [("eodhd.splits@1", 1), ("eodhd.dividends@1", 0)]
    actions = ws.market.execute(
        "SELECT action_id, ratio, amount, currency FROM corporate_actions ORDER BY 1, 4"
    ).fetchall()
    assert actions == [
        ("dividend:2026-08-31", None, Decimal(100), "KRW"),
        ("dividend:2026-08-31", None, Decimal("0.5"), "USD"),
        ("split:2026-08-31", Decimal(2), None, None),
    ]
    rates = ws.market.execute(
        "SELECT base_currency, quote_currency, fixing_at_us, rate, available_at_us "
        "FROM fx_rates ORDER BY fixing_at_us"
    ).fetchall()
    close = {day: opened + 6 * HOUR for day, opened in opens_of(ws).items()}
    assert rates == [
        ("USD", "KRW", us_at(EX, 24) - 1, Decimal("1385.25"), close[EX] + 4 * HOUR),
        ("USD", "KRW", us_at(NEXT, 24) - 1, Decimal("1390.5"), close[NEXT] + 4 * HOUR),
    ]
    # A started chain continues from its own generation: no start needed for the next source.
    versions = tuple(ws.state.execute("SELECT count(*) FROM dataset_versions").fetchone())
    again = _runs(promote_datasets(ws, apply=True, now_us=5, routes=routes, identity=identity))
    assert all(run["status"] == "current" for run in again.values()), again
    assert tuple(ws.state.execute("SELECT count(*) FROM dataset_versions").fetchone()) == versions
    replay(ws, raw, bulk_job("US", "splits", NEXT.isoformat()),
           [{**_split("AAA", "US", "3/1"), "date": NEXT.isoformat()}])  # fmt: skip
    later = _runs(promote_datasets(ws, apply=True, now_us=6, routes=routes, identity=identity))
    run = later[("actions.us.eodhd", "eodhd.splits@1")]
    assert (run["status"], run["started"]) == ("advanced", False)
    assert dataset_head(ws, "actions.us.eodhd") is not None


def opens_of(ws: Workspace) -> dict[date, int]:
    """The XNYS opens the test published, read back from the calendar head."""
    rows = ws.market.execute(
        "SELECT session_date, open_at_us FROM calendar_sessions WHERE calendar_id='XNYS'"
    ).fetchall()
    return {day: int(opened) for day, opened in rows}
