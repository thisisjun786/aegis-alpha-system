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
from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest

from aegis_alpha.application.maintain import Policies, Ports, plan_maintenance, run_maintenance
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
from aegis_alpha.data.qveris_client import QverisResponse
from aegis_alpha.storage import maintain_promotion
from aegis_alpha.storage.identity import mint_instrument
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
from tests.storage.kr_identity_support import isin, kind_listing, symbol
from tests.storage.promotion_support import register_symbols
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

    def request(
        self,
        path: str,
        *,
        body: dict[str, object] | None = None,
        query: dict[str, str | int] | None = None,
    ) -> QverisResponse:
        if path == "/auth/usage/history/v2" and self.crash_on_settlement:
            self.crash_on_settlement = False
            raise KeyboardInterrupt  # the process dies after a paid call, before settling it
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


def test_two_daily_runs_continue_every_chain_and_repeat_nothing(tmp_path: Path) -> None:
    world = World(tmp_path)
    first = world.run()
    assert first["exit_code"] == 0, first["failed_stages"]
    statuses = _statuses(first)
    # No dataset has a chain yet: maintenance continues chains, it never starts one.
    for name in ("macro.us.alfred", "filings.us.sec", "prices.us.eodhd"):
        assert statuses[name] == "no_head"
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
    assert world.calls()[2] == calls[2]
    assert world.calls()[1] == calls[1]
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
    assert world.calls()[1:] == calls[1:]
    with open_workspace(world.home) as ws:
        assert {name: _head(ws, name) for name in heads} == advanced


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
        "mapper": {"name": "kind.industry@1", "args": {}},
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
