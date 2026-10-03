"""KR collection into an installation: the rolling OpenDART cohort and KIND lists, offline.

The provider is ``tests.data.opendart_support.FakeProvider``; every company is synthetic.
"""

# ruff: noqa: PLR2004 -- synthetic counts are the expected values
from __future__ import annotations

import base64
import hashlib
import json
import os
from collections.abc import Iterator, Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Final, cast

import pyarrow as pa
import pytest

from aegis_alpha.application.cli import main
from aegis_alpha.data.kind import KindResponse
from aegis_alpha.data.opendart import DartRequest, DartResponse, HttpAnswer, OpenDartClient
from aegis_alpha.data.opendart_cohort import CohortPolicy, Knowledge, plan_financials
from aegis_alpha.storage import collection_ledger as ledger
from aegis_alpha.storage import kr_collection
from aegis_alpha.storage.promotion.engine import promote
from aegis_alpha.storage.raw import put_raw
from aegis_alpha.storage.source_identity import SourceContent, SourceFile
from aegis_alpha.storage.source_library import import_content_arrow, list_tables
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
# Frozen digests of aas-opendart-receipt-v1, aas-opendart-batch-v1 and aas-kind-receipt-v1.
RECEIPT_SHA256: Final = "bdbdefd81710247c4282351e961f5afad2565740bc5c5806db468d0d77e15db8"
BATCH_SHA256: Final = "3244fc85c575d916c554f2a428a8ef4c2e7fad6fc79cf67e5112a16ecd2c554b"
SOURCE_SHA256: Final = "ca260bfbb35fe47bf5c19a47bd7b7d632672df86de14ae8b1c0b6b9a38b81361"
KIND_SHA256: Final = "f62b9e840744f756ec213b6b6e91d44bc8ff833849e9af4d7335e60c6c136712"


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
    answered = list(provider.calls)
    plan = kr_collection.plan_dart(ws, today=date(2026, 5, 16), policy=POLICY)
    assert (plan["corp_codes_due"], plan["listed_corps"]) == (False, 2)
    assert cast("dict[str, int]", plan["known"])["uncommitted_receipts"] == 3
    provider.calls.clear()
    result = _run(ws, provider, clock)
    assert result["recovered_attempts"] == {"released": 0, "uncertain": 1}
    assert result["recovered_receipts"] == 3
    # The interrupted run's answers are known before planning, so none is asked again.
    assert [call for call in provider.calls if call in answered] == []
    assert provider.asked("corpCode.xml") == []
    assert [(p["bgn_de"], p["page_no"]) for p in provider.asked("list.json")] == []
    sources = cast("list[dict[str, object]]", result["sources"])
    assert sources[0]["rows"] == 3 + cast("int", result["provider_calls"])
    attempts, usage = _ledger(ws)
    assert attempts["uncertain"] == 1
    assert usage["uncertain"] == 1


def _endpoints(ws: Workspace, source: dict[str, object]) -> list[str]:
    ((store, entry),) = kr_collection._receipt_tables(ws, str(source["source_id"]), "receipts")  # noqa: SLF001 -- the collector's own table reader
    rows = kr_collection._select(ws, store, entry, ("endpoint",), "ORDER BY _aas_ordinal")  # noqa: SLF001
    return [str(row[0]) for row in rows]


def test_a_batch_holds_at_most_one_completed_corp_code_list(ws: Workspace) -> None:
    provider, clock = _provider(), _clock()

    def interrupt(
        method: str, url: str, body: bytes | None, headers: Mapping[str, str]
    ) -> HttpAnswer:
        if len(provider.calls) == 1:
            raise _InterruptedError
        return provider(method, url, body, headers)

    with pytest.raises(_InterruptedError):
        kr_collection.collect_dart(
            ws, OpenDartClient(KEY, interrupt, clock), policy=POLICY, clock=clock,
            sleep=lambda _: None,
        )  # fmt: skip
    # A week later the list is due again while the first answer is still uncommitted.
    clock.advance(days=8)
    provider.calls.clear()
    result = _run(ws, provider, clock)
    assert len(provider.asked("corpCode.xml")) == 1
    sources = cast("list[dict[str, object]]", result["sources"])
    lists = [_endpoints(ws, source).count("corp_codes") for source in sources]
    assert lists[:2] == [1, 1]
    assert max(lists) == 1


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


def test_a_commit_left_without_its_completion_is_finished_not_committed_again(
    ws: Workspace, monkeypatch: pytest.MonkeyPatch
) -> None:
    provider, clock = _provider(), _clock()

    def interrupt(*_args: object) -> None:
        raise _InterruptedError

    # The batch marker commits and the run stops before its operation completes.
    with monkeypatch.context() as patch:
        patch.setattr("aegis_alpha.storage.source_library.complete_operation", interrupt)
        with pytest.raises(_InterruptedError):
            _run(ws, provider, clock)
    assert kr_collection.load_known(ws).sources == 0
    clock.advance(days=1)
    result = _run(ws, FakeProvider(corp_codes=provider.corp_codes), clock)
    assert result["recovered_receipts"] == 0
    assert cast("dict[str, int]", result["known"])["sources"] == 1
    assert cast("dict[str, int]", result["known"])["rows"] == 14


_LEGACY_SHAPES: Final = {
    "raw_json": ("fingerprint", "outcome", "endpoint", "request_json", "receipt_json",
                 "raw_json", "raw_sha256", "retrieved_at_utc"),
    "raw_base64": ("fingerprint", "outcome", "endpoint", "request_json", "receipt_json",
                   "raw_base64", "raw_sha256", "retrieved_at_utc"),
    "validated": ("fingerprint", "outcome", "original_outcome", "validation_json", "endpoint",
                  "request_json", "receipt_json", "raw_base64", "raw_sha256",
                  "retrieved_at_utc"),
}  # fmt: skip


def _legacy_row(  # noqa: PLR0913, PLR0917 -- one legacy row spells every receipt field
    shape: str, endpoint: str, outcome: str, request: str, raw: bytes, retrieved: str
) -> dict[str, str]:
    row = {
        "fingerprint": hashlib.sha256(request.encode()).hexdigest(),
        "outcome": outcome,
        "original_outcome": "FAILED",
        "validation_json": json.dumps({"validated_outcome": outcome}),
        "endpoint": endpoint,
        "request_json": request,
        "receipt_json": "{}",
        "raw_json": raw.decode(errors="replace"),
        "raw_base64": base64.b64encode(raw).decode(),
        "raw_sha256": hashlib.sha256(raw).hexdigest(),
        "retrieved_at_utc": retrieved,
    }
    return {name: row[name] for name in _LEGACY_SHAPES[shape]}


def _commit_legacy(ws: Workspace, shape: str, rows: list[dict[str, str]]) -> None:
    _, digest, size = put_raw(ws.paths.raw, f"synthetic-legacy-{shape}".encode())
    content = SourceContent("opendart", "native", 1, (SourceFile(digest, size),))
    columns = _LEGACY_SHAPES[shape]
    table = pa.table(
        {name: [row[name] for row in rows] for name in columns},
        schema=pa.schema([(name, pa.string()) for name in columns]),
    )
    import_content_arrow(ws, content, "receipts", table.to_reader())


def test_legacy_receipts_tables_of_every_shape_are_read(ws: Workspace) -> None:
    corp_request = json.dumps({"endpoint": "corp_codes", "parameters_json": "{}"})
    answer = statements(A, "2025", "11011", "20250515000001")
    _commit_legacy(ws, "raw_json", [
        _legacy_row("raw_json", "financials", "COMPLETED", dart.request(A, "2025", "11011"),
                    answer, "2026-05-02T00:00:00.000000Z"),
    ])  # fmt: skip
    _commit_legacy(ws, "raw_base64", [
        _legacy_row("raw_base64", "corp_codes", "COMPLETED", corp_request,
                    corp_archive([(A, "000101")]), "2026-05-01T00:00:00.000000Z"),
    ])  # fmt: skip
    # The validated outcome is the one read; the provider's original outcome was FAILED.
    _commit_legacy(ws, "validated", [
        _legacy_row("validated", "financials", "COMPLETED", dart.request(B, "2025", "11012"),
                    statements(B, "2025", "11012", "20250814000001"),
                    "2026-05-03T00:00:00.000000Z"),
        _legacy_row("validated", "corp_codes", "COMPLETED", corp_request,
                    corp_archive([(A, "000101"), (B, "000202")]),
                    "2026-05-04T00:00:00.000000Z"),
    ])  # fmt: skip
    known = kr_collection.load_known(ws)
    assert (known.sources, known.rows, known.unreadable) == (3, 4, 0)
    assert known.knowledge.corps == (A, B)
    for corp, report in ((A, "11011"), (B, "11012")):
        seen = known.knowledge.seen(DartRequest.financials(corp, 2025, report, "CFS"))
        assert seen is not None
        assert seen.outcome == "COMPLETED"
    planned = plan_financials(known.knowledge, date(2026, 5, 16), POLICY)
    asked = {(p.request.parameters["corp_code"], p.request.parameters["reprt_code"])
             for p in planned if p.request.parameters["bsns_year"] == "2025"}  # fmt: skip
    assert not asked & {(A, "11011"), (B, "11012")}
    assert {(A, "11012"), (B, "11011")} <= asked


def test_receipt_batch_and_kind_receipt_formats_are_frozen() -> None:
    at = datetime(2026, 5, 16, 1, 0, tzinfo=UTC)
    request = DartRequest.financials(A, 2026, "11013", "CFS")
    response = DartResponse(200, (("content-type", "application/json"),),
                            statements(A, "2026", "11013", Q1), at, at)  # fmt: skip
    receipt = kr_collection.receipt_bytes(
        request, response, outcome="COMPLETED", provider_status="000",
        attempt=ledger.Attempt("opendart:" + request.fingerprint, 1),
    )  # fmt: skip
    assert hashlib.sha256(receipt).hexdigest() == RECEIPT_SHA256
    batch = kr_collection.Batch.of([kr_collection.Retained(receipt, response.body)])
    assert hashlib.sha256(batch.manifest).hexdigest() == BATCH_SHA256
    assert batch.content.source_id == "opendart-receipts-" + SOURCE_SHA256
    kind = KindResponse("kind-kospi", HttpAnswer(200, (), b"synthetic listing"), at, at)
    assert hashlib.sha256(kind.receipt()).hexdigest() == KIND_SHA256


def test_a_kind_list_with_a_malformed_code_never_narrows_the_cohort(ws: Workspace) -> None:
    kind = _kind_provider()
    _, malformed = kind_listing([("합성바이오", "90 9", "2011-03-04")], list_id="kind-kosdaq")
    kind.kind["kosdaqMkt"] = (200, malformed)
    result = kr_collection.collect_kind(ws, kind, clock=_clock())
    lists = cast("list[dict[str, object]]", result["lists"])
    assert [item["status"] for item in lists] == ["committed", "committed"]
    assert kr_collection.kind_codes(ws) is None


def test_a_completed_list_row_without_its_page_is_unreadable() -> None:
    knowledge = Knowledge()
    request = DartRequest.list_page(date(2026, 5, 15), 1)
    row = ("list", "COMPLETED", json.dumps(request.document), "2026-05-16T01:00:00.000000Z")
    assert not kr_collection.observe_row(knowledge, row, None)
    assert knowledge.list_pages == {}


def test_a_batch_ends_at_its_byte_budget(ws: Workspace, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(kr_collection, "MAX_BATCH_BYTES", 1)
    result = _run(ws, _provider(), _clock())
    sources = cast("list[dict[str, object]]", result["sources"])
    assert [source["rows"] for source in sources] == [1] * 14
