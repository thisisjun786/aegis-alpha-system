"""``aas maintain``: daily passes from synthetic provider answers to continued chains, offline.

The providers are the suites' fakes (``FakeFred``, ``FakeSec``, a scripted Qveris gateway
whose answers carry the test clock). Every series, filing, symbol and value is synthetic.
The first generation of every dataset is an operator's spec, as at a cutover; maintenance
only continues chains.
"""

# ruff: noqa: PLR2004 -- synthetic counts are the expected values
from __future__ import annotations

import hashlib
import json
import ssl
from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast

import duckdb
import pytest

from aegis_alpha.application.maintain import (
    Policies,
    Ports,
    plan_maintenance,
    promotion_streaks,
    run_maintenance,
)
from aegis_alpha.application.maintain_config import (
    DartSettings,
    FredSettings,
    MaintainConfig,
    QverisSettings,
    SecSettings,
)
from aegis_alpha.application.maintain_qveris import QverisCaps, QverisPolicy
from aegis_alpha.data import fred_collect as fred
from aegis_alpha.data import sec_collect as sec
from aegis_alpha.data.opendart import OpenDartClient
from aegis_alpha.data.opendart_cohort import CohortPolicy
from aegis_alpha.data.qveris_client import QverisResponse, QverisTransportError
from aegis_alpha.storage import maintain_identity, maintain_promotion
from aegis_alpha.storage.identity import mint_instrument
from aegis_alpha.storage.maintain_promotion import STALE_AFTER_ZERO_PROMOTION_RUNS
from aegis_alpha.storage.promotion.engine import promote
from aegis_alpha.storage.provider_collection import committed_tables
from aegis_alpha.storage.verification import verify_workspace
from aegis_alpha.storage.workspace import Workspace, initialize, open_workspace
from tests.data.opendart_support import KEY, FakeClock, FakeProvider, corp_archive, statements
from tests.data.qveris_support import ScriptedQveris, account_key, bar, identity_bytes
from tests.data.us_collect_support import (
    FRED_KEY,
    USER_AGENT,
    FakeFred,
    FakeSec,
    SecFact,
    SecFiling,
)
from tests.storage import dart_receipt_support as dart
from tests.storage.kr_identity_support import (
    commit_legacy_kind_listing,
    isin,
    kind_listing,
    symbol,
)
from tests.storage.promotion_support import add_source, register_symbols
from tests.storage.promotion_support import bar as kr_bar
from tests.storage.promotion_support import spec as price_spec
from tests.storage.test_macro_fx import _rule, _spec
from tests.storage.test_us_collection import POLICY, _history, _promote_alfred, _sec_spec

# Serial: the scripted Qveris gateway takes the host-wide account lease (an abstract Unix
# socket named by the synthetic account), so every file that takes it runs in one worker.
pytestmark = pytest.mark.xdist_group("qveris-account-lease")

CIK, OTHER = "0000000101", "0000000202"
MON, TUE, WED = date(2026, 9, 14), date(2026, 9, 15), date(2026, 9, 16)
Q2 = SecFiling(CIK, "0000000101-26-000002", "10-Q", MON, "2026-09-14T20:15:30.000Z", "2026-06-30")
EVENT = SecFiling(CIK, "0000000101-26-000003", "8-K", TUE, "2026-09-15T12:00:00.000Z")
LATE = SecFiling(OTHER, "0000000202-26-000001", "10-Q", WED, "2026-09-16T20:01:00.000Z",
                 "2026-06-30")  # fmt: skip
# Wednesday 2026-09-16, 06:00 in New York: FRED, EDGAR and UTC agree on the date. The
# packaged calendars are declared later, so these runs leave them to their own test.
DAY1 = datetime(2026, 9, 16, 10, tzinfo=UTC)
NEW_YORK = "America/New_York"
DAY_RULE = {"rule": "local_day_end@1", "basis": "record", "input": "session_date",
            "args": {"timezone": NEW_YORK}}  # fmt: skip
OBSERVED = {"rule": "source_column@1", "basis": "revision", "input": "observed_at", "args": {}}
FLOATS = dict.fromkeys(("open", "high", "low", "close", "volume"), "float_shortest@1")


class DatedQveris(ScriptedQveris):
    """A scripted gateway whose responses are dated by the test clock."""

    def __init__(self, rows: dict[str, object], key: str, clock: FakeClock) -> None:
        super().__init__(rows, key)
        self.clock = clock
        self.crash_on_settlement = False
        self.settlement_down = False
        self.preflight_drops = 0

    def request(
        self,
        path: str,
        *,
        body: dict[str, object] | None = None,
        query: dict[str, str | int] | None = None,
    ) -> QverisResponse:
        if path == "/tools/probe" and self.preflight_drops:
            self.preflight_drops -= 1
            self.calls.append(path)
            raise QverisTransportError(ssl.SSLEOFError("synthetic TLS drop"))
        if path == "/auth/usage/history/v2" and self.crash_on_settlement:
            self.crash_on_settlement = False
            raise KeyboardInterrupt  # the process dies after a paid call, before settling it
        if path == "/auth/usage/history/v2" and self.settlement_down:
            raise RuntimeError("synthetic usage history outage")
        response = super().request(path, body=body, query=query)
        now = self.clock.now
        return replace(response, requested_at_utc=now, retrieved_at_utc=now)


def _bulk_rows(*days: date, close: float = 11.0) -> dict[str, object]:
    rows: dict[str, object] = {}
    for day in days:
        parameters = {"date": day.isoformat(), "exchange": "US", "fmt": "json"}
        key = json.dumps(parameters, sort_keys=True, separators=(",", ":"))
        rows[key] = [{"code": "AAA", "exchange_short_name": "US",
                      **bar(day.isoformat(), close=close)}]  # fmt: skip
    return rows


class World:
    """The installation, the providers and the configuration of one scenario."""

    def __init__(self, tmp_path: Path) -> None:
        self.home = tmp_path / "aas"
        initialize(self.home)
        self.clock = FakeClock(DAY1)
        self.fred = FakeFred(DAY1.date())
        _history(self.fred)
        self.sec = FakeSec(
            filings=[Q2, EVENT],
            facts={CIK: [SecFact(Q2.accession, "Assets", "1000.50", date(2026, 6, 30), MON)]},
        )
        self.raw = tmp_path / "qveris"
        self.qveris = DatedQveris(_bulk_rows(MON, TUE), account_key(self.raw), self.clock)
        identity = tmp_path / "identity.json"
        identity.write_bytes(identity_bytes())
        identity.chmod(0o600)
        policy = QverisPolicy(exchanges=("US",), datasets=("prices",), since={"US": MON})
        caps = QverisCaps(max_calls=10, max_credits="100", request_interval=0)
        self.config = MaintainConfig(
            jobs_enabled=True,
            sec=SecSettings(tmp_path / "unused", issuers="all"),
            fred=FredSettings(tmp_path / "unused"),
            qveris=QverisSettings(tmp_path / "unused", self.raw, policy, caps, identity),
        )
        self.ports = Ports(
            sec=lambda: sec.SecClient(USER_AGENT, self.sec, self.clock),
            fred=lambda: fred.FredClient(FRED_KEY, self.fred, self.clock),
            qveris=lambda: self.qveris,
        )
        self.policies = Policies(sec_lookback_days=3, fred=POLICY, sleep=lambda _: None)

    def workspace(self) -> Iterator[Workspace]:
        with open_workspace(self.home, writable=True, strategy_write=True) as workspace:
            yield workspace

    def run(self) -> dict[str, object]:
        with open_workspace(self.home, writable=True, strategy_write=True) as workspace:
            return run_maintenance(
                workspace, self.config, self.ports, clock=self.clock, policies=self.policies
            )

    def calls(self) -> tuple[int, int, int]:
        """(SEC calls, FRED observations asked, Qveris paid executions) so far."""
        return len(self.sec.calls), len(self.fred.asked("observations")), self.qveris.execute_count


def _tables(ws: Workspace, prefix: str) -> list[dict[str, str]]:
    shas = {table.source_id: table.source_id.rsplit("-", 1)[-1] for table in
            committed_tables(ws, prefix)}  # fmt: skip
    return [
        {"source_id": table.source_id, "source_sha256": shas[table.source_id],
         "table": str(table.entry["name"]), "digest": str(table.entry["digest"])}
        for table in committed_tables(ws, prefix)
        if table.entry["rows"]
    ]  # fmt: skip


def _head(ws: Workspace, dataset: str) -> tuple[int, str] | None:
    row = ws.state.execute(
        "SELECT sequence, generation_id FROM dataset_versions WHERE dataset_id=? "
        "AND status='committed' ORDER BY sequence DESC LIMIT 1",
        (dataset,),
    ).fetchone()
    return None if row is None else (int(row[0]), str(row[1]))


def _generation_pin(ws: Workspace, dataset: str) -> dict[str, str]:
    pin = maintain_promotion.head_pin(ws, dataset)
    assert pin is not None
    return pin


def _cutover(ws: Workspace) -> dict[str, tuple[int, str]]:
    """The operator's first generation of each dataset, from the first day's sources."""
    (alfred,) = _tables(ws, "fred-alfred-observations-")
    _promote_alfred(ws, alfred, None)
    (csv,) = _tables(ws, "fred-series-csv-")
    fx = _spec(
        "fx_rates", "fx.usdkrw.fred", [csv], "fred.fx_series@1",
        {"series": "DEXKOUS", "base": "USD", "quote": "KRW", "timezone": NEW_YORK},
        _rule("unknown_null@1", "record", None, None), decimal={"rate": "decimal_text@1"},
    )  # fmt: skip
    assert promote(ws, *fx, apply=True)["published"] is True
    (filings,) = _tables(ws, "sec-submissions-filings-")
    assert promote(ws, *_sec_spec(filings), apply=True)["published"] is True
    (facts,) = _tables(ws, "sec-companyfacts-facts-")
    funded = _sec_spec(facts, facts=_generation_pin(ws, "filings.us.sec"))
    assert promote(ws, *funded, apply=True)["published"] is True
    bars = _tables(ws, "qveris-bulk-bars-")
    assert len(bars) == 2
    identity = register_symbols(ws, bars[0]["source_id"], {"AAA.US": "900001"}, name="us")
    prices = price_spec(
        bars, identity, dataset="prices.us.eodhd",
        rules={"available_at_us": DAY_RULE, "revision_known_at_us": DAY_RULE},
        partition={"from": MON.isoformat(), "to": WED.isoformat()}, decimals=FLOATS,
        mapper={"name": "eodhd.bars@1", "args": {"timezone": NEW_YORK}},
    )  # fmt: skip
    assert promote(ws, *prices, apply=True)["published"] is True
    datasets = ("macro.us.alfred", "fx.usdkrw.fred", "filings.us.sec", "fundamentals.us.sec",
                "prices.us.eodhd")  # fmt: skip
    return {name: cast("tuple[int, str]", _head(ws, name)) for name in datasets}


def _statuses(report: dict[str, object]) -> dict[str, str]:
    stage = cast("dict[str, object]", cast("dict[str, object]", report["stages"])["promote"])
    return {
        str(item["dataset_id"]): str(item["status"])
        for item in cast("list[dict[str, object]]", stage["datasets"])
    }


def _route_statuses(report: dict[str, object]) -> dict[tuple[str, str], str]:
    """Each route's status by (dataset, mapper): a dataset may be continued by two routes."""
    stage = cast("dict[str, object]", cast("dict[str, object]", report["stages"])["promote"])
    return {
        (str(item["dataset_id"]), str(item["mapper"])): str(item["status"])
        for item in cast("list[dict[str, object]]", stage["datasets"])
    }


def _advance(world: World) -> None:
    """The next day: one new session, one new filing with facts, one new ALFRED vintage."""
    world.clock.advance(days=1)
    world.fred.today = WED + timedelta(days=1)
    world.fred.publish("DGS10", TUE, "4.20", WED)
    world.fred.publish("DGS10", date(2026, 8, 20), "4.55", WED)
    world.fred.publish("DEXKOUS", TUE, "1390.25", WED)
    world.sec.filings.append(LATE)
    world.sec.facts[OTHER] = [SecFact(LATE.accession, "Assets", "77", date(2026, 6, 30), WED)]
    world.qveris.rows = {**world.qveris.rows, **_bulk_rows(WED)}


def test_qveris_preflight_transport_recovers_with_successful_maintain(tmp_path: Path) -> None:
    world = World(tmp_path)
    world.qveris.preflight_drops = 1
    pauses: list[float] = []
    world.policies = replace(world.policies, sleep=pauses.append)

    report = world.run()

    assert (report["status"], report["exit_code"]) == ("succeeded", 0)
    assert pauses.count(30.0) == 1
    assert world.qveris.execute_count == len(world.qveris.usage) == 2
    assert len(world.qveris.ledger) == 2
    assert world.qveris.calls.count("/tools/probe") == 3
    stages = cast("dict[str, dict[str, object]]", report["stages"])
    assert stages["qveris"]["status"] == "succeeded"
    assert stages["qveris"]["settled_after"] == {"succeeded": 2, "uncertain": 0, "released": 0}


def test_two_daily_runs_continue_every_chain_and_repeat_nothing(tmp_path: Path) -> None:
    world = World(tmp_path)
    first = world.run()
    assert first["exit_code"] == 0, first["failed_stages"]
    statuses = _route_statuses(first)
    # No dataset has a chain yet: maintenance continues chains, it never starts one.
    for route in (("macro.us.alfred", "fred.alfred@1"), ("filings.us.sec", "sec.submissions@1"),
                  ("prices.us.eodhd", "eodhd.bars@1")):  # fmt: skip
        assert statuses[route] == "no_head"
    collect = cast("dict[str, dict[str, object]]", first["stages"])
    assert collect["qveris"]["provider_calls"] == 2
    assert cast("dict[str, int]", collect["qveris"]["reasons"])["new"] == 2
    with open_workspace(world.home, writable=True, strategy_write=True) as ws:
        heads = _cutover(ws)
    # The same day again: every answered request is reused, the operator's sources are
    # planned once more and recorded unchanged, and no generation is published.
    calls = world.calls()
    world.clock.advance(hours=1)
    again = world.run()
    assert again["exit_code"] == 0, again["failed_stages"]
    assert world.calls() == calls  # SEC, FRED and Qveris alike
    with open_workspace(world.home) as ws:
        assert {name: _head(ws, name) for name in heads} == heads
        recorded = ws.state.execute(
            "SELECT result, count(*) FROM quality_checks WHERE rule_id='maintain_source' GROUP BY 1"
        ).fetchall()
    assert dict(recorded) == {"unchanged": 6}

    _advance(world)
    second = world.run()
    assert second["exit_code"] == 0, second["failed_stages"]
    stages = cast("dict[str, dict[str, object]]", second["stages"])
    assert stages["qveris"]["provider_calls"] == 1
    assert stages["qveris"]["covered"] == 2
    with open_workspace(world.home) as ws:
        for name, (sequence, generation) in heads.items():
            head = _head(ws, name)
            assert head is not None
            assert head[0] > sequence, name
            parents = ws.state.execute(
                "SELECT parent_generation_id FROM dataset_versions WHERE dataset_id=? "
                "AND sequence=?",
                (name, sequence + 1),
            ).fetchone()
            assert parents[0] == generation, name  # the chain continues from the head
        prices = ws.market.execute(
            "SELECT session_date FROM prices ORDER BY session_date"
        ).fetchall()
        assert prices == [(MON,), (TUE,), (WED,)]
        accepted = ws.market.execute(
            "SELECT accepted_at_us FROM fundamentals WHERE issuer_id IN (SELECT issuer_id "
            "FROM filings WHERE filing_id=?)",
            (LATE.accession,),
        ).fetchall()
        # The facts took their acceptance from the filings head the same run advanced.
        assert accepted == [(int(datetime(2026, 9, 16, 20, 1, tzinfo=UTC).timestamp() * 1e6),)]
        advanced = {name: _head(ws, name) for name in heads}
        assert verify_workspace(ws)["verified"] is True

    # The second day again: idempotent, no call is repeated and no head moves.
    calls = world.calls()
    world.clock.advance(hours=2)
    repeat = world.run()
    assert repeat["exit_code"] == 0, repeat["failed_stages"]
    assert world.calls() == calls
    with open_workspace(world.home) as ws:
        assert {name: _head(ws, name) for name in heads} == advanced


def test_a_us_symbol_list_is_collected_imported_and_read_by_the_identity_stage(
    tmp_path: Path,
) -> None:
    world = World(tmp_path)
    listed = json.dumps({"EXCHANGE_CODE": "US", "delisted": "0", "fmt": "json"},
                        sort_keys=True, separators=(",", ":"))  # fmt: skip
    delisted = json.dumps({"EXCHANGE_CODE": "US", "delisted": "1", "fmt": "json"},
                          sort_keys=True, separators=(",", ":"))  # fmt: skip
    world.qveris.rows = {**world.qveris.rows, listed: [
        symbol("AAA", isin("000000011", "US"), exchange="NYSE", currency="USD")],
        delisted: [symbol("OLD", None, exchange="NYSE", currency="USD")]}  # fmt: skip
    qveris = cast("QverisSettings", world.config.qveris)
    policy = replace(qveris.policy, symbol_lists=("US",))
    world.config = replace(world.config, qveris=replace(qveris, policy=policy))
    report = world.run()
    assert report["exit_code"] == 0, report["failed_stages"]
    stages = cast("dict[str, dict[str, object]]", report["stages"])
    # Two sessions and the US listed and delisted lists.
    assert stages["qveris"]["provider_calls"] == 4
    units = cast("list[dict[str, object]]", stages["import"]["symbol_lists"])
    assert len(units) == 2
    assert all(str(unit["source_id"]).startswith("qveris-eodhd-exchange-symbols-")
               for unit in units)  # fmt: skip
    identity = stages["identity"]
    # The KR build never reads a US list; the US increment does.
    assert identity["new_sources"] == {"eodhd": 0, "kind": 0, "dart": 0}
    us = cast("dict[str, object]", identity["us_symbols"])
    # Both lists are new sources; the delisted list's rows are skipped as no listed rows.
    # AAA is a new listing: its ISIN mints its instrument and claims its day.
    assert (us["new_sources"], us["claims"], us["registered_instruments"]) == (2, 1, 1)
    assert us["unresolved"] == {}
    # The same day again: the list is covered until tomorrow.
    world.clock.advance(hours=1)
    again = world.run()
    assert again["exit_code"] == 0, again["failed_stages"]
    assert cast("dict[str, dict[str, object]]", again["stages"])["qveris"]["provider_calls"] == 0


def _ledger(ws: Workspace, provider: str) -> dict[str, int]:
    return dict(
        ws.state.execute(
            "SELECT a.status, count(*) FROM collection_attempts a JOIN collection_jobs j "
            "ON j.job_id=a.job_id WHERE j.provider=? GROUP BY 1",
            (provider,),
        ).fetchall()
    )


def test_a_killed_run_is_recovered_without_asking_any_answered_request_again(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    world = World(tmp_path)
    assert world.run()["exit_code"] == 0
    with open_workspace(world.home, writable=True, strategy_write=True) as ws:
        heads = _cutover(ws)
    _advance(world)

    # Killed after the day's paid Qveris call, before its settlement was recorded.
    world.qveris.crash_on_settlement = True
    with pytest.raises(KeyboardInterrupt):
        world.run()
    calls = world.calls()
    assert calls[2] == 3  # two on the first day, one before the kill
    with open_workspace(world.home) as ws:
        assert _ledger(ws, "qveris") == {"succeeded": 2, "started": 1}
        assert {name: _head(ws, name) for name in heads} == heads
        # A read-only plan sees the day's collected sources waiting for their promotion.
        planned = plan_maintenance(ws, world.config, now=world.clock.now,
                                   policies=world.policies, promotions=True)  # fmt: skip
    assert (planned["provider_calls"], planned["failed_stages"]) == (0, [])
    waiting = _statuses(planned)
    assert {waiting[name] for name in ("macro.us.alfred", "filings.us.sec")} == {"planned"}

    # Killed again, right after the first promotion of the pass was published.
    real = maintain_promotion.promote
    published: list[str] = []

    def dies_after_publishing(*args: object, **kwargs: object) -> dict[str, object]:
        result = real(*args, **kwargs)  # ty: ignore[invalid-argument-type]
        if result.get("published") and not published:
            published.append(str(result["generation_id"]))
            raise KeyboardInterrupt
        return result

    monkeypatch.setattr(maintain_promotion, "promote", dies_after_publishing)
    with pytest.raises(KeyboardInterrupt):
        world.run()
    # The interrupted ask was finished on its own job: settled, verified, never executed.
    assert world.calls()[2] == calls[2]
    assert world.calls()[1] == calls[1]
    with open_workspace(world.home) as ws:
        assert _ledger(ws, "qveris") == {"succeeded": 3}
        assert not {"started", "reserved"} & set(_ledger(ws, "fred"))
    monkeypatch.setattr(maintain_promotion, "promote", real)

    final = world.run()
    assert final["exit_code"] == 0, final["failed_stages"]
    assert world.calls()[1:] == calls[1:]
    with open_workspace(world.home) as ws:
        for name, (sequence, _) in heads.items():
            head = _head(ws, name)
            # One generation per new day, the one published before the kill not repeated.
            assert head is not None
            assert head[0] == sequence + 1, name
        status = ws.state.execute(
            "SELECT status FROM dataset_versions WHERE generation_id=?", (published[0],)
        ).fetchone()
        assert tuple(status) == ("committed",)
        assert verify_workspace(ws)["verified"] is True


# --- KR: KIND, OpenDART, symbol lists and the identity increment ------------------------------

SEOUL_DAY1 = datetime(2026, 9, 16, 1, tzinfo=UTC)  # 10:00 in Seoul
KR_PARTIAL_RULE = {"rule": "local_day_end@1", "basis": "record", "input": "session_date",
                   "args": {"timezone": "Asia/Seoul"}}  # fmt: skip
KRW = {**dict.fromkeys(("open", "high", "low", "close"), "krw_tick@1"),
       "volume": "float_shortest@1"}  # fmt: skip


def _symbol_rows(codes: dict[str, str]) -> list[dict[str, object]]:
    return [symbol(code, isin(body)) for code, body in sorted(codes.items())]


def _kr_rows(days: list[date], codes: list[str]) -> dict[str, object]:
    rows: dict[str, object] = {}
    for day in days:
        parameters = {"date": day.isoformat(), "exchange": "KO", "fmt": "json"}
        key = json.dumps(parameters, sort_keys=True, separators=(",", ":"))
        rows[key] = [{"code": code, "exchange_short_name": "KO",
                      **bar(day.isoformat(), open=52100, high=52500, low=51900, close=52300)}
                     for code in codes]  # fmt: skip
    return rows


def _symbol_lists(codes: dict[str, str]) -> dict[str, object]:
    listed = {"EXCHANGE_CODE": "KO", "delisted": "0", "fmt": "json"}
    delisted = {**listed, "delisted": "1"}
    return {
        json.dumps(listed, sort_keys=True, separators=(",", ":")): _symbol_rows(codes),
        json.dumps(delisted, sort_keys=True, separators=(",", ":")): [],
    }


class KrWorld:
    def __init__(self, tmp_path: Path) -> None:
        self.home = tmp_path / "aas"
        initialize(self.home)
        self.clock = FakeClock(SEOUL_DAY1)
        self.codes = {"000101": "000010101"}
        self.listings = [("합성전자", "000101", "2001-01-02")]
        self.dart = FakeProvider(
            corp_codes=corp_archive([("00000101", "000101"), ("00000202", "000202")]),
            answers={("00000101", "2025", "11014", "CFS"):
                     (200, statements("00000101", "2025", "11014", "20251114000001"))},
        )  # fmt: skip
        self._kind()
        self.raw = tmp_path / "qveris"
        rows = {**_kr_rows([MON, TUE], ["000101"]), **_symbol_lists(self.codes)}
        self.qveris = DatedQveris(rows, account_key(self.raw), self.clock)
        # Every KR download is answered with a warning, at no charge (the gateway's quote).
        self.qveris.failure = "included"
        self.qveris.price = Decimal(0)
        identity = tmp_path / "identity.json"
        identity.write_bytes(identity_bytes())
        identity.chmod(0o600)
        policy = QverisPolicy(exchanges=("KO",), datasets=("prices",), since={"KO": MON},
                              symbol_lists=("KO",), symbol_list_days=1)  # fmt: skip
        caps = QverisCaps(max_calls=10, max_credits="100", request_interval=0)
        self.config = MaintainConfig(
            jobs_enabled=True,
            kind=True,
            dart=DartSettings(tmp_path / "unused", max_calls=50),
            qveris=QverisSettings(tmp_path / "unused", self.raw, policy, caps, identity),
        )
        self.ports = Ports(
            kind=lambda: self.dart,
            dart=lambda: OpenDartClient(KEY, self.dart, self.clock),
            qveris=lambda: self.qveris,
        )
        self.policies = Policies(dart=CohortPolicy(first_year=2025, list_lookback_days=2),
                                 sleep=lambda _: None)  # fmt: skip

    def _kind(self) -> None:
        industries = {code: "반도체 제조업" for _, code, _ in self.listings}
        _, kospi = kind_listing(self.listings, list_id="kind-kospi", industries=industries)
        _, kosdaq = kind_listing([("합성바이오", "000909", "2011-03-04")], list_id="kind-kosdaq")
        self.dart.kind = {"stockMkt": (200, kospi), "kosdaqMkt": (200, kosdaq)}

    def run(self) -> dict[str, object]:
        with open_workspace(self.home, writable=True, strategy_write=True) as workspace:
            return run_maintenance(
                workspace, self.config, self.ports, clock=self.clock, policies=self.policies
            )


def _kr_cutover(ws: Workspace, identity: dict[str, str]) -> dict[str, tuple[int, str]]:
    listings = _tables(ws, "kind-listings-")
    industry = {
        "schema_version": "aas-promotion-v1",
        "target": {"domain": "classifications", "dataset_id": "classifications.kr.kind",
                   "parent": None},
        "sources": listings,
        "mapper": {"name": "kind.industry@2", "args": {}},
        "partition": None,
        "time_rules": {"available_at_us": OBSERVED, "revision_known_at_us": OBSERVED},
        "decimal_rule": {},
        "quality_rules": [],
        "tombstone_policy": {"mode": "never"},
        "identity_snapshot": identity,
    }  # fmt: skip
    raw = json.dumps(industry, sort_keys=True).encode()
    classified = promote(ws, raw, hashlib.sha256(raw).hexdigest(), apply=True)
    assert classified["published"] is True, {
        k: classified.get(k)
        for k in ("rows", "unresolved_tokens", "source_rows", "mapped_rows", "sources")
    }
    receipts = _tables(ws, "opendart-receipts-")
    statements_result = promote(ws, *dart.spec(receipts[:1]), apply=True)
    assert statements_result["published"] is True, statements_result
    (first, *_) = _tables(ws, "qveris-bulk-quarantine-")
    prices = price_spec(
        [first], identity, rules={"available_at_us": KR_PARTIAL_RULE,
                                  "revision_known_at_us": KR_PARTIAL_RULE},
        partition={"from": MON.isoformat(), "to": WED.isoformat()}, decimals=KRW,
        mapper={"name": "eodhd.bulk_quarantine@1",
                "args": {"timezone": "Asia/Seoul", "currencies": {"KO": "KRW", "KQ": "KRW"}}},
    )  # fmt: skip
    assert promote(ws, *prices, apply=True)["published"] is True
    names = ("classifications.kr.kind", "fundamentals.kr.dart", "prices.kr.eodhd")
    return {name: cast("tuple[int, str]", _head(ws, name)) for name in names}


def test_new_kr_listings_advance_identity_and_every_kr_chain(tmp_path: Path) -> None:
    world = KrWorld(tmp_path)
    first = world.run()
    assert first["exit_code"] == 0, first["failed_stages"]
    stages = cast("dict[str, dict[str, object]]", first["stages"])
    identity = stages["identity"]
    snapshot = cast("dict[str, str]", identity["snapshot"])
    assert snapshot["snapshot_id"].startswith("maintain-")
    assert stages["qveris"]["provider_calls"] == 4  # two sessions, two symbol lists
    with open_workspace(world.home, writable=True, strategy_write=True) as ws:
        heads = _kr_cutover(ws, snapshot)

    # The next day KIND and EODHD list a new company, and both KR sessions trade it.
    world.clock.advance(days=1)
    world.codes["000777"] = "000077707"
    world.listings.append(("합성신규", "000777", "2026-09-16"))
    world._kind()  # noqa: SLF001 -- the fake's next answers
    world.qveris.rows = {**world.qveris.rows, **_kr_rows([WED], ["000101", "000777"]),
                         **_symbol_lists(world.codes)}  # fmt: skip
    second = world.run()
    assert second["exit_code"] == 0, second["failed_stages"]
    stages = cast("dict[str, dict[str, object]]", second["stages"])
    advanced = cast("dict[str, str]", stages["identity"]["snapshot"])
    assert advanced["snapshot_id"] != snapshot["snapshot_id"]
    listed = mint_instrument("krx_isin", isin("000077707"))
    with open_workspace(world.home) as ws:
        for name, (sequence, _) in heads.items():
            head = _head(ws, name)
            assert head is not None
            if name == "fundamentals.kr.dart":
                continue  # a day without statements records coverage, not a generation
            assert head[0] > sequence, name
        new = ws.market.execute(
            "SELECT session_date, value_state FROM prices WHERE instrument_id=?", (listed,)
        ).fetchall()
        assert new == [(WED, "present")]
        classified = ws.market.execute(
            "SELECT count(*) FROM classifications WHERE subject_id=?", (listed,)
        ).fetchone()
        assert classified == (1,)
        flags = ws.market.execute(
            "SELECT DISTINCT flag FROM quality_flags WHERE flag='provider_reported_partial'"
        ).fetchall()
        assert flags == [("provider_reported_partial",)]
        assert verify_workspace(ws)["verified"] is True
        state = {name: _head(ws, name) for name in heads}
    world.clock.advance(hours=1)
    executed = world.qveris.execute_count
    repeat = world.run()
    assert repeat["exit_code"] == 0, repeat["failed_stages"]
    assert world.qveris.execute_count == executed
    with open_workspace(world.home) as ws:
        assert {name: _head(ws, name) for name in heads} == state


def test_warned_kr_bulk_days_continue_a_history_only_chain_and_the_store_verifies(
    tmp_path: Path,
) -> None:
    world = KrWorld(tmp_path)
    first = world.run()
    assert first["exit_code"] == 0, first["failed_stages"]
    snapshot = cast("dict[str, dict[str, object]]", first["stages"])["identity"]["snapshot"]
    friday = MON - timedelta(days=3)
    with open_workspace(world.home, writable=True, strategy_write=True) as ws:
        # The operator's history backfill: bars only, no bulk generation in the chain.
        history = add_source(ws, [kr_bar("000101.KO", friday, 52000.0, retrieved=SEOUL_DAY1)],
                             tag="history")  # fmt: skip
        rules = {"available_at_us": KR_PARTIAL_RULE, "revision_known_at_us": KR_PARTIAL_RULE}
        backfill = price_spec([history], cast("dict[str, str]", snapshot), rules=rules,
                              partition={"from": "2026-01-01", "to": "2027-01-01"},
                              decimals=KRW)  # fmt: skip
        assert promote(ws, *backfill, apply=True)["published"] is True
    world.clock.advance(days=1)
    world.qveris.rows = {**world.qveris.rows, **_kr_rows([WED], ["000101"])}
    second = world.run()
    assert second["exit_code"] == 0, second["failed_stages"]
    stage = cast("dict[str, object]", cast("dict[str, object]", second["stages"])["promote"])
    (bulk,) = [item for item in cast("list[dict[str, object]]", stage["datasets"])
               if (item["dataset_id"], item["mapper"])
               == ("prices.kr.eodhd", "eodhd.bulk_quarantine@1")]  # fmt: skip
    assert (bulk["status"], bulk["derived_from"]) == ("advanced", "eodhd.bars@1")
    assert [item["status"] for item in cast("list[dict[str, object]]", bulk["sources"])] == [
        "promoted"
    ] * 3  # Monday's, Tuesday's and Wednesday's warned downloads
    listed = mint_instrument("krx_isin", isin("000010101"))
    with open_workspace(world.home) as ws:
        days = ws.market.execute(
            "SELECT session_date FROM prices WHERE instrument_id=? ORDER BY 1", (listed,)
        ).fetchall()
        assert days == [(friday,), (MON,), (TUE,), (WED,)]
        flagged = ws.market.execute(
            "SELECT count(*) FROM quality_flags WHERE flag='provider_reported_partial'"
        ).fetchone()
        assert flagged == (3,)
        assert verify_workspace(ws)["verified"] is True


def _held_reasons(ws: Workspace, day: date) -> list[str]:
    found: list[str] = []
    for table in committed_tables(ws, "qveris-bulk-quarantine-"):
        rows = ws.market.execute(
            f'SELECT reason, source_row_json FROM "{table.entry["target"]}" ORDER BY ordinal'  # noqa: S608 -- quoted
        ).fetchall()
        found += [str(reason) for reason, row in rows if json.loads(row)["date"] == str(day)]
    return found


def test_a_warned_day_with_a_bad_row_is_partial_held_by_reason_and_asked_again_within_caps(
    tmp_path: Path,
) -> None:
    world = KrWorld(tmp_path)
    bad = {"code": "000202", "exchange_short_name": "KO",
           **bar(TUE.isoformat(), open=52100, high=52500, low=52600, close=52300)}  # fmt: skip
    (tuesday,) = _kr_rows([TUE], ["000101"])
    world.qveris.rows = {**world.qveris.rows, tuesday: [
        *cast("list[object]", _kr_rows([TUE], ["000101"])[tuesday]), bad]}  # fmt: skip
    first = world.run()
    assert first["exit_code"] == 0, first["failed_stages"]
    stages = cast("dict[str, dict[str, object]]", first["stages"])
    assert cast("dict[str, object]", stages["qveris"]["cohort"])["warned"] == 4
    snapshot = cast("dict[str, str]", stages["identity"]["snapshot"])
    friday = MON - timedelta(days=3)
    with open_workspace(world.home, writable=True, strategy_write=True) as ws:
        # Only Tuesday's low-above-open row carries its own reason; the others stay partial.
        assert _held_reasons(ws, TUE) == ["provider_reported_partial", "inconsistent_ohlc"]
        assert _held_reasons(ws, MON) == ["provider_reported_partial"]
        history = add_source(ws, [kr_bar("000101.KO", friday, 52000.0, retrieved=SEOUL_DAY1)],
                             tag="history")  # fmt: skip
        rules = {"available_at_us": KR_PARTIAL_RULE, "revision_known_at_us": KR_PARTIAL_RULE}
        backfill = price_spec([history], snapshot, rules=rules,
                              partition={"from": "2026-01-01", "to": "2027-01-01"},
                              decimals=KRW)  # fmt: skip
        assert promote(ws, *backfill, apply=True)["published"] is True

    # The next day the caps cover Wednesday and the two symbol lists, not Tuesday's retry.
    world.clock.advance(days=1)
    world.qveris.rows = {**world.qveris.rows, **_kr_rows([WED], ["000101"])}
    qveris = cast("QverisSettings", world.config.qveris)
    world.config = replace(world.config, qveris=replace(
        qveris, caps=replace(qveris.caps, max_calls=3)))  # fmt: skip
    executed = world.qveris.execute_count
    second = world.run()
    assert second["exit_code"] == 0, second["failed_stages"]
    stage = cast("dict[str, dict[str, object]]", second["stages"])["qveris"]
    assert cast("dict[str, int]", stage["reasons"])["partial_retry"] == 1
    assert stage["partial"] == 1  # Monday: every row passed, so it is not asked again
    warnings = cast("dict[str, object]", stage["warnings"])
    # The two warned KR symbol lists have no value checks: unjudged, refreshed by their rule.
    assert (warnings["held_rows"], warnings["unjudged"]) == (1, 2)
    assert world.qveris.execute_count - executed == 3
    assert cast("dict[str, int]", stage["settled_after"])["released"] == 1
    listed = mint_instrument("krx_isin", isin("000010101"))
    with open_workspace(world.home) as ws:
        days = ws.market.execute(
            "SELECT session_date FROM prices WHERE instrument_id=? ORDER BY 1", (listed,)
        ).fetchall()
        # Tuesday's table holds a failing row, so the maintenance route keeps it whole.
        assert days == [(friday,), (MON,), (WED,)]

    # With room under the caps, Tuesday is asked again; its clean answer is promoted.
    world.clock.advance(days=1)
    thursday = WED + timedelta(days=1)
    world.qveris.rows = {**world.qveris.rows, **_kr_rows([TUE, thursday], ["000101"])}
    world.config = replace(world.config, qveris=qveris)
    third = world.run()
    assert third["exit_code"] == 0, third["failed_stages"]
    stage = cast("dict[str, dict[str, object]]", third["stages"])["qveris"]
    assert cast("dict[str, int]", stage["reasons"])["partial_retry"] == 1
    with open_workspace(world.home) as ws:
        days = ws.market.execute(
            "SELECT session_date FROM prices WHERE instrument_id=? ORDER BY 1", (listed,)
        ).fetchall()
        assert days == [(friday,), (MON,), (TUE,), (WED,), (thursday,)]
        assert verify_workspace(ws)["verified"] is True
    planned = cast("dict[str, dict[str, object]]", world.run()["stages"])["qveris"]
    assert cast("dict[str, int]", planned["reasons"])["partial_retry"] == 0


def test_a_plan_skips_legacy_shaped_kind_listings(tmp_path: Path) -> None:
    world = KrWorld(tmp_path)
    first = world.run()
    assert first["exit_code"] == 0, first["failed_stages"]
    stages = cast("dict[str, dict[str, object]]", first["stages"])
    snapshot = cast("dict[str, str]", stages["identity"]["snapshot"])
    with open_workspace(world.home, writable=True, strategy_write=True) as ws:
        _kr_cutover(ws, snapshot)
        # The legacy importer's kind-listings source shares the prefix and table name.
        legacy = commit_legacy_kind_listing(ws)
        kind = [table.source_id for table in committed_tables(ws, "kind-listings-")]
    listed = sorted(set(kind) - {legacy})
    assert len(listed) == 2  # the KOSPI and KOSDAQ lists
    world.clock.advance(hours=1)
    with open_workspace(world.home) as ws:
        planned = plan_maintenance(ws, world.config, now=world.clock.now,
                                   policies=world.policies, promotions=True)  # fmt: skip
    assert planned["failed_stages"] == []
    stages = cast("dict[str, dict[str, object]]", planned["stages"])
    assert stages["identity"]["skipped_sources"] == {"kind": [legacy]}
    runs = cast("list[dict[str, object]]", stages["promote"]["datasets"])
    (run,) = [item for item in runs if item["dataset_id"] == "classifications.kr.kind"]
    assert cast("dict[str, int]", run["skipped"])["shape"] == 1
    # The kr-import lists the cutover pinned are still read, and planned once more.
    sources = cast("list[dict[str, object]]", run["sources"])
    assert sorted((str(item["source_id"]), item["status"]) for item in sources) == [
        (source_id, "planned") for source_id in listed
    ]
    # The status route sees both lists as candidates of a chain no run has started yet.
    assert "status.kr.kind" not in cast("dict[str, int]", stages["promote"]["unmapped"])
    (status,) = [item for item in runs if item["dataset_id"] == "status.kr.kind"]
    assert status["status"] == "no_head"
    assert cast("dict[str, int]", status["skipped"]) == {"done": 0, "shape": 1, "no_head": 2}


def test_an_unsettled_paid_page_stops_every_run_until_its_settlement(tmp_path: Path) -> None:
    world = World(tmp_path)
    assert world.run()["exit_code"] == 0
    with open_workspace(world.home, writable=True, strategy_write=True) as ws:
        _cutover(ws)
    _advance(world)
    world.qveris.crash_on_settlement = True
    with pytest.raises(KeyboardInterrupt):
        world.run()
    calls = world.calls()

    # The usage history stays down: the resume cannot settle the paid page.
    world.qveris.settlement_down = True
    world.clock.advance(hours=1)
    stopped = world.run()
    assert stopped["exit_code"] == 2, stopped["stages"]
    assert stopped["stopped_providers"] == ["qveris"]
    qveris = cast("dict[str, object]", cast("dict[str, object]", stopped["stages"])["qveris"])
    assert qveris["status"] == "stopped"
    assert len(cast("list[object]", qveris["held"])) == 1
    with open_workspace(world.home) as ws:
        # The attempt stays open; nothing is settled against the evidence.
        assert _ledger(ws, "qveris") == {"succeeded": 2, "started": 1}

    world.qveris.settlement_down = False
    world.clock.advance(hours=1)
    settled = world.run()
    assert settled["exit_code"] == 0, settled["failed_stages"]
    assert world.calls() == calls  # the page was settled, never executed again
    with open_workspace(world.home) as ws:
        assert _ledger(ws, "qveris") == {"succeeded": 3}


def test_a_database_error_in_one_stage_leaves_the_later_stages_and_the_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    world = World(tmp_path)
    world.config = replace(world.config, jobs_enabled=False)

    def out_of_memory(*args: object, **kwargs: object) -> dict[str, object]:
        del args, kwargs
        raise duckdb.OutOfMemoryException("synthetic: the compute budget is spent")

    monkeypatch.setattr(maintain_promotion, "promote_datasets", out_of_memory)
    report = world.run()
    assert report["exit_code"] == 1
    assert report["failed_stages"] == ["promote"]
    stages = cast("dict[str, dict[str, object]]", report["stages"])
    assert stages["promote"]["error_type"] == "OutOfMemoryException"
    assert "datasets" in stages["heads"]
    stored = json.loads((world.home / "runtime" / "maintain-report.json").read_text())
    assert stored["failed_stages"] == ["promote"]


def test_every_stage_reports_its_elapsed_time_and_memory(tmp_path: Path) -> None:
    world = World(tmp_path)
    world.config = replace(world.config, jobs_enabled=False)
    report = world.run()
    resources = cast("dict[str, dict[str, object]]", report["stage_resources"])
    # Jobs are off, so no provider stage runs.
    assert list(resources) == [
        "recover", "runtime", "calendars", "import", "identity", "promote", "heads"]  # fmt: skip
    for numbers in resources.values():
        assert set(numbers) == {
            "elapsed_seconds", "process_peak_rss_bytes", "cgroup_peak_bytes",
            "cgroup_current_bytes", "cgroup_anon_bytes", "cgroup_file_bytes",
            "cgroup_swap_bytes", "cgroup_swap_peak_bytes",
        }  # fmt: skip
        assert cast("float", numbers["elapsed_seconds"]) >= 0
    stored = json.loads((world.home / "runtime" / "maintain-report.json").read_text())
    assert stored["stage_resources"] == resources


def test_the_heads_stage_reads_the_previous_report_for_its_streaks(tmp_path: Path) -> None:
    world = World(tmp_path)
    world.config = replace(world.config, jobs_enabled=False)
    stages = cast("dict[str, dict[str, object]]", world.run()["stages"])
    assert stages["heads"]["previous_report"] == "missing"
    heads = cast("list[dict[str, object]]", stages["heads"]["datasets"])
    # Every catalog dataset is listed; none has a head in this installation.
    assert {item["freshness"] for item in heads} == {"no_head"}
    again = cast("dict[str, dict[str, object]]", world.run()["stages"])
    assert again["heads"]["previous_report"] == "read"


def _streak_head(sequence: int, *, lag: int | None = 1) -> dict[str, object]:
    return {"dataset_id": "prices.kr.eodhd", "sequence": sequence, "lag_sessions": lag,
            "freshness": "current", "stale_reasons": [], "zero_promotion_runs": None}  # fmt: skip


def _streak(
    previous: dict[str, object] | None, head: dict[str, object], collected: frozenset[str]
) -> dict[str, object]:
    known = {} if previous is None else {"prices.kr.eodhd": previous}
    promotion_streaks([head], known, collected)
    return head


def test_runs_that_collect_but_never_promote_a_lagging_head_mark_it_stale() -> None:
    qveris = frozenset({"qveris"})
    head = _streak(None, _streak_head(3), qveris)  # nothing to compare with: no count
    counts = [head["zero_promotion_runs"]]
    for _ in range(STALE_AFTER_ZERO_PROMOTION_RUNS):
        assert head["freshness"] == "current"
        head = _streak(head, _streak_head(3), qveris)
        counts.append(head["zero_promotion_runs"])
    assert counts == list(range(STALE_AFTER_ZERO_PROMOTION_RUNS + 1))
    assert (head["freshness"], head["stale_reasons"]) == ("stale", ["zero_promotion"])
    # A run that collected nothing for it carries the count; a promotion resets it.
    carried = _streak(head, _streak_head(3), frozenset({"fred"}))
    assert (carried["zero_promotion_runs"], carried["freshness"]) == (
        STALE_AFTER_ZERO_PROMOTION_RUNS, "stale")  # fmt: skip
    advanced = _streak(head, _streak_head(4), qveris)
    assert (advanced["zero_promotion_runs"], advanced["freshness"]) == (0, "current")
    # Data already current needs no promotion: the count resets.
    current = _streak(head, _streak_head(3, lag=0), qveris)
    assert (current["zero_promotion_runs"], current["freshness"]) == (0, "current")


def _kr_new_listing(world: KrWorld, *, symbol_list: bool) -> None:
    """KIND lists a new company and both days' KO sessions trade it; EODHD lists it if asked."""
    world.listings.append(("합성신규", "000777", "2026-09-16"))
    world._kind()  # noqa: SLF001 -- the fake's next answers
    if symbol_list:
        world.codes["000777"] = "000077707"
    world.qveris.rows = {**world.qveris.rows, **_kr_rows([WED], ["000101", "000777"]),
                         **_symbol_lists(world.codes)}  # fmt: skip


def _new_listing_rows(world: KrWorld) -> tuple[list[tuple[object, ...]], object]:
    listed = mint_instrument("krx_isin", isin("000077707"))
    with open_workspace(world.home) as ws:
        prices = ws.market.execute(
            "SELECT session_date FROM prices WHERE instrument_id=?", (listed,)
        ).fetchall()
        classified = ws.market.execute(
            "SELECT count(*) FROM classifications WHERE subject_id=?", (listed,)
        ).fetchone()
    return prices, classified


def test_a_failed_identity_stage_holds_identity_chains_until_it_recovers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    world = KrWorld(tmp_path)
    first = world.run()
    snapshot = cast("dict[str, dict[str, object]]", first["stages"])["identity"]["snapshot"]
    with open_workspace(world.home, writable=True, strategy_write=True) as ws:
        _kr_cutover(ws, cast("dict[str, str]", snapshot))
    world.clock.advance(days=1)
    _kr_new_listing(world, symbol_list=True)
    real = maintain_identity.advance

    def broken(*args: object, **kwargs: object) -> dict[str, object]:
        del args, kwargs
        raise RuntimeError("synthetic identity outage")

    monkeypatch.setattr(maintain_identity, "advance", broken)
    failed = world.run()
    assert (failed["exit_code"], failed["failed_stages"]) == (1, ["identity"])
    statuses = _statuses(failed)
    assert statuses["prices.kr.eodhd"] == "identity_unavailable"
    assert statuses["classifications.kr.kind"] == "identity_unavailable"
    assert _new_listing_rows(world) == ([], (0,))

    monkeypatch.setattr(maintain_identity, "advance", real)
    world.clock.advance(hours=1)
    recovered = world.run()
    assert recovered["exit_code"] == 0, recovered["failed_stages"]
    assert _new_listing_rows(world) == ([(WED,)], (1,))


def test_rows_an_older_snapshot_left_unresolved_are_promoted_once_identity_catches_up(
    tmp_path: Path,
) -> None:
    world = KrWorld(tmp_path)
    first = world.run()
    snapshot = cast("dict[str, dict[str, object]]", first["stages"])["identity"]["snapshot"]
    with open_workspace(world.home, writable=True, strategy_write=True) as ws:
        _kr_cutover(ws, cast("dict[str, str]", snapshot))
    # The new listing trades before the EODHD symbol list carries it: identity lags.
    world.clock.advance(days=1)
    _kr_new_listing(world, symbol_list=False)
    lagging = world.run()
    assert lagging["exit_code"] == 0, lagging["failed_stages"]
    promote = cast("dict[str, object]", cast("dict[str, object]", lagging["stages"])["promote"])
    assert "prices.kr.eodhd" in cast("list[str]", promote["unresolved"])
    assert _new_listing_rows(world)[0] == []
    with open_workspace(world.home) as ws:
        recorded = ws.state.execute(
            "SELECT count(*) FROM quality_checks WHERE rule_id='maintain_source' "
            "AND result='unresolved' AND dataset_id='prices.kr.eodhd'"
        ).fetchone()
    assert recorded[0] >= 1
    # The same identity again: the unresolved sources are not planned again.
    world.clock.advance(hours=1)
    again = world.run()
    assert again["exit_code"] == 0, again["failed_stages"]
    assert "prices.kr.eodhd" not in cast(
        "list[str]",
        cast("dict[str, object]", again["stages"])["promote"]["unresolved"],  # ty: ignore[not-subscriptable]
    )

    # The next day's symbol list carries the listing: the waiting rows are promoted.
    world.clock.advance(days=1)
    world.codes["000777"] = "000077707"
    thu = WED + timedelta(days=1)
    world.qveris.rows = {**world.qveris.rows, **_kr_rows([thu], ["000101", "000777"]),
                         **_symbol_lists(world.codes)}  # fmt: skip
    caught_up = world.run()
    assert caught_up["exit_code"] == 0, caught_up["failed_stages"]
    prices, classified = _new_listing_rows(world)
    assert (WED,) in prices
    with open_workspace(world.home) as ws:
        effective = ws.market.execute(
            "SELECT DISTINCT effective_from FROM classifications WHERE subject_id=? ORDER BY 1",
            (mint_instrument("krx_isin", isin("000077707")),),
        ).fetchall()
        assert verify_workspace(ws)["verified"] is True
    # The lagging day's KIND list is classified too, not only the next day's.
    assert classified == (2,)
    assert len(effective) == 2
