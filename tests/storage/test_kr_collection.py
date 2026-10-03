"""KR collection into an installation: the rolling OpenDART cohort and KIND lists, offline.

The provider is ``tests.data.opendart_support.FakeProvider``; every company is synthetic.
"""

# ruff: noqa: PLR2004 -- synthetic counts are the expected values
from __future__ import annotations

import json
import os
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pytest

from aegis_alpha.application.cli import main
from aegis_alpha.data.opendart import HttpAnswer, OpenDartClient
from aegis_alpha.data.opendart_cohort import CohortPolicy
from aegis_alpha.storage import kr_collection
from aegis_alpha.storage.promotion.engine import promote
from aegis_alpha.storage.source_library import list_tables
from aegis_alpha.storage.workspace import Workspace, initialize, open_workspace
from tests.data.opendart_support import (
    KEY,
    FakeClock,
    FakeProvider,
    corp_archive,
    filing,
    list_page,
    statements,
    status,
)
from tests.data.test_opendart_cohort import _legacy_root
from tests.storage import dart_receipt_support as dart
from tests.storage.kr_identity_support import kind_listing

POLICY = CohortPolicy(first_year=2025, list_lookback_days=2)
A, B, UNLISTED = "00000101", "00000202", "00000303"
Q1 = "20260515000001"


@pytest.fixture
def ws(tmp_path: Path) -> Iterator[Workspace]:
    root = tmp_path / "aas"
    initialize(root)
    with open_workspace(root, writable=True, strategy_write=True) as workspace:
        yield workspace


def _provider() -> FakeProvider:
    return FakeProvider(
        corp_codes=corp_archive([(A, "000101"), (B, "000202"), (UNLISTED, "")]),
        lists={
            "20260515/1": list_page(
                [filing(A, "분기보고서 (2026.03)", Q1, "20260515")], page=1, total=2
            ),
            "20260515/2": list_page(
                [filing(B, "[기재정정]반기보고서 (2025.06)", "20260515000002", "20260515", "K")],
                page=2,
                total=2,
            ),
        },
        answers={(A, "2026", "11013", "CFS"): (200, statements(A, "2026", "11013", Q1))},
    )


def _clock() -> FakeClock:
    # 10:00 on 2026-05-16 in Seoul.
    return FakeClock(datetime(2026, 5, 16, 1, 0, tzinfo=UTC))


def _run(
    ws: Workspace,
    provider: FakeProvider,
    clock: FakeClock,
    policy: CohortPolicy = POLICY,
    **bounds: int,
) -> dict[str, object]:
    client = OpenDartClient(KEY, provider, clock)
    return kr_collection.collect_dart(
        ws, client, policy=policy, clock=clock, sleep=lambda _: None, **bounds
    )


def _statement_calls(provider: FakeProvider) -> list[tuple[str, str, str, str]]:
    return [
        (p["corp_code"], p["bsns_year"], p["reprt_code"], p["fs_div"])
        for p in provider.asked("fnlttSinglAcntAll.json")
    ]


def _ledger(ws: Workspace) -> tuple[dict[str, int], dict[str, int]]:
    attempts = dict(
        ws.state.execute("SELECT status,count(*) FROM collection_attempts GROUP BY status")
    )
    usage = dict(ws.state.execute("SELECT kind,count(*) FROM usage_events GROUP BY kind"))
    return attempts, usage


def _pins(ws: Workspace, sources: list[dict[str, object]]) -> list[dict[str, str]]:
    pins = []
    for source in sources:
        source_id = str(source["source_id"])
        (table,) = list_tables(ws, source_id)
        pins.append(
            {
                "source_id": source_id,
                "source_sha256": source_id.rsplit("-", 1)[-1],
                "table": "receipts",
                "digest": str(table["digest"]),
            }
        )
    return pins


def test_a_run_asks_by_phase_and_commits_receipts_the_dart_mappers_read(ws: Workspace) -> None:
    provider, clock = _provider(), _clock()
    result = _run(ws, provider, clock, batch_size=3)
    # Corp codes, the two days' list pages (a first page adds the page it counts), then the
    # statements of the listed corps for every ended period, newest first.
    assert [name for name, _ in provider.calls] == [
        "corpCode.xml",
        "list.json",
        "list.json",
        "list.json",
        *["fnlttSinglAcntAll.json"] * 10,
    ]
    assert [(p["bgn_de"], p["page_no"]) for p in provider.asked("list.json")] == [
        ("20260514", "1"),
        ("20260515", "1"),
        ("20260515", "2"),
    ]
    assert _statement_calls(provider) == [
        (corp, year, report, "CFS")
        for year, report in (("2026", "11013"), ("2025", "11011"), ("2025", "11014"),
                             ("2025", "11012"), ("2025", "11013"))
        for corp in (A, B)
    ]  # fmt: skip
    assert result["provider_calls"] == 14
    assert result["outcomes"] == {"COMPLETED": 4, "NO_DATA": 10}
    assert result["asked"] == {"corp_codes_refresh": 1, "list_page": 3, "never_asked": 10}
    assert result["stopped"] is None
    sources = cast("list[dict[str, object]]", result["sources"])
    assert [source["rows"] for source in sources] == [3, 3, 3, 3, 2]
    assert all(str(s["source_id"]).startswith("opendart-receipts-") for s in sources)
    assert _ledger(ws) == ({"succeeded": 14}, {"charged": 14, "reserved": 14})
    # The key reaches the provider and nothing the installation retains.
    for path in ws.paths.raw.rglob("*"):
        assert not path.is_file() or KEY.encode() not in path.read_bytes()
    # The committed batches are what dart.fnltt@1 reads.
    document = dart.spec(_pins(ws, sources), args={"december_year_end": [A, B]})
    plan = promote(ws, *document, apply=False)
    assert plan["source_outcomes"] == {"completed": 1, "no_data": 9, "other_endpoint": 4}
    assert plan["refusals"] == []


def test_the_next_day_asks_what_the_answers_made_due_and_nothing_else(ws: Workspace) -> None:
    provider, clock = _provider(), _clock()
    _run(ws, provider, clock)
    clock.advance(days=1)
    again = FakeProvider(corp_codes=provider.corp_codes)
    result = _run(ws, again, clock)
    # The separate statements behind each consolidated NO_DATA, and the new day's list.
    assert [(p["bgn_de"], p["page_no"]) for p in again.asked("list.json")] == [("20260516", "1")]
    reports = ("11011", "11012", "11013", "11014")
    assert sorted(_statement_calls(again)) == sorted(
        [(A, "2025", report, "OFS") for report in reports]
        + [(B, "2025", report, "OFS") for report in reports]
        + [(B, "2026", "11013", "OFS")]
    )
    assert again.asked("corpCode.xml") == []
    assert result["known"] == {
        "sources": 1,
        "rows": 14,
        "unreadable_rows": 0,
        "listed_corps": 2,
        "kind_filter": False,
    }


def test_the_daily_quota_counts_the_ledger_across_runs(ws: Workspace) -> None:
    provider, clock = _provider(), _clock()
    first = _run(ws, provider, clock, daily_quota=4)
    assert (first["provider_calls"], first["stopped"]) == (4, "budget")
    assert first["quota"] == {"daily": 4, "used_before": 0, "budget": 4}
    second = _run(ws, provider, clock, daily_quota=4)
    assert (second["provider_calls"], second["stopped"]) == (0, "budget")
    pending = cast("dict[str, dict[str, int]]", second["pending"])
    assert pending["by_reason"] == {"never_asked": 10}
    clock.advance(days=1)
    third = _run(ws, provider, clock, daily_quota=4, max_calls=2)
    assert (third["provider_calls"], third["quota"]) == (
        2,
        {"daily": 4, "used_before": 0, "budget": 2},
    )


class _InterruptedError(Exception):
    pass


def test_an_interrupted_run_is_settled_and_its_receipts_committed_next(ws: Workspace) -> None:
    provider, clock = _provider(), _clock()

    def interrupt(
        method: str, url: str, body: bytes | None, headers: Mapping[str, str]
    ) -> HttpAnswer:
        if len(provider.calls) == 3:
            raise _InterruptedError
        return provider(method, url, body, headers)

    with pytest.raises(_InterruptedError):
        kr_collection.collect_dart(
            ws, OpenDartClient(KEY, interrupt, clock), policy=POLICY, clock=clock,
            sleep=lambda _: None,
        )  # fmt: skip
    assert _ledger(ws) == ({"started": 1, "succeeded": 3}, {"charged": 3, "reserved": 4})
    assert kr_collection.load_known(ws).sources == 0
    result = _run(ws, provider, clock)
    assert result["recovered_attempts"] == {"released": 0, "uncertain": 1}
    assert result["recovered_receipts"] == 3
    sources = cast("list[dict[str, object]]", result["sources"])
    assert sources[0]["rows"] == 3 + cast("int", result["provider_calls"])
    attempts, usage = _ledger(ws)
    assert attempts["uncertain"] == 1
    assert usage["uncertain"] == 1


def test_a_refused_key_stops_the_run_and_keeps_the_answer(ws: Workspace) -> None:
    provider, clock = _provider(), _clock()
    provider.corp_codes = status("020")
    result = _run(ws, provider, clock)
    assert (result["provider_calls"], result["stopped"]) == (1, "provider_refused:020")
    assert result["outcomes"] == {"FAILED": 1}
    assert len(cast("list[object]", result["sources"])) == 1


def test_transport_failures_are_uncertain_and_stop_after_three(ws: Workspace) -> None:
    provider, clock = _provider(), _clock()
    provider.fail.add("list.json")
    result = _run(ws, provider, clock, CohortPolicy(first_year=2025, list_lookback_days=5))
    assert (result["provider_calls"], result["uncertain"], result["stopped"]) == (
        4,
        3,
        "transport_failures",
    )
    assert _ledger(ws) == (
        {"succeeded": 1, "uncertain": 3},
        {"charged": 1, "reserved": 4, "uncertain": 3},
    )
    # Unanswered calls count against the quota and wait a day before they are asked again.
    clock.advance(hours=1)
    provider.fail.clear()
    provider.calls.clear()
    later = _run(ws, provider, clock, CohortPolicy(first_year=2025, list_lookback_days=5))
    assert later["quota"] == {"daily": 19_000, "used_before": 4, "budget": 2_000}
    assert [p["bgn_de"] for p in provider.asked("list.json")] == ["20260514", "20260515",
                                                                 "20260515"]  # fmt: skip


def _kind_provider() -> FakeProvider:
    _, kospi = kind_listing([("합성전자", "000101", "2001-01-02")], list_id="kind-kospi")
    _, kosdaq = kind_listing([("합성바이오", "000909", "2011-03-04")], list_id="kind-kosdaq")
    return FakeProvider(kind={"stockMkt": (200, kospi), "kosdaqMkt": (200, kosdaq)})


def test_kind_lists_commit_as_listing_sources_and_narrow_the_cohort(ws: Workspace) -> None:
    kind, clock = _kind_provider(), _clock()
    result = kr_collection.collect_kind(ws, kind, clock=clock)
    lists = cast("list[dict[str, object]]", result["lists"])
    assert [(item["list"], item["status"], item["rows"]) for item in lists] == [
        ("kind-kosdaq", "committed", 1),
        ("kind-kospi", "committed", 1),
    ]
    assert all(str(item["source_id"]).startswith("kind-listings-") for item in lists)
    assert kr_collection.kind_codes(ws) == frozenset({"000101", "000909"})
    assert _ledger(ws) == ({"succeeded": 2}, {"charged": 2, "reserved": 2})
    provider = _provider()
    provider.lists.clear()
    _run(ws, provider, clock)
    # B's stock code is in neither KIND list, and no KOSPI or KOSDAQ filing names B.
    assert {call[0] for call in _statement_calls(provider)} == {A}


def test_a_kind_answer_that_is_not_the_listing_table_is_refused(ws: Workspace) -> None:
    kind = _kind_provider()
    kind.kind["kosdaqMkt"] = (200, "<html><table></table></html>".encode("euc-kr"))
    result = kr_collection.collect_kind(ws, kind, clock=_clock())
    lists = cast("list[dict[str, object]]", result["lists"])
    assert [item["status"] for item in lists] == ["refused", "committed"]
    assert kr_collection.kind_codes(ws) is None


def test_cli_plans_without_calls_and_runs_with_a_private_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    root = tmp_path / "aas"
    initialize(root)
    assert main(["collect", "dart", "plan", "--home", str(root), "--today", "2026-05-16"]) == 0
    plan = json.loads(capsys.readouterr().out)
    assert (plan["provider_calls"], plan["corp_codes_due"], plan["listed_corps"]) == (0, True, 0)
    legacy = tmp_path / "opendart"
    _legacy_root(legacy)
    assert main(["collect", "dart", "plan", "--legacy-root", str(legacy),
                 "--today", "2026-10-03"]) == 0  # fmt: skip
    replayed = json.loads(capsys.readouterr().out)
    assert replayed["legacy"]["requests"] == 4
    assert "2026/11014/CFS" in replayed["statements"]["by_period"]
    provider = _provider()
    monkeypatch.setattr("aegis_alpha.data.opendart.urllib_transport", lambda: provider)
    key = tmp_path / "opendart-key"
    key.write_text(KEY + "\n")
    os.chmod(key, 0o600)  # noqa: PTH101 -- explicit owner-only secret mode
    code = main(["collect", "dart", "run", "--home", str(root), "--key-file", str(key),
                 "--max-calls", "1"])  # fmt: skip
    run = json.loads(capsys.readouterr().out)
    assert (code, run["provider_calls"], run["stopped"], run["exit_code"]) == (0, 1, "budget", 0)
    monkeypatch.setattr("aegis_alpha.data.opendart.urllib_transport", _kind_provider)
    assert main(["collect", "kind", "run", "--home", str(root)]) == 0
    assert json.loads(capsys.readouterr().out)["exit_code"] == 0
