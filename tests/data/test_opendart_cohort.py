"""The rolling OpenDART cohort and the legacy ledger replay, on synthetic knowledge."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING

import pytest

from aegis_alpha.data.opendart import COMPLETED, FAILED, NO_DATA, DartRequest, Filing
from aegis_alpha.data.opendart_cohort import (
    CohortPolicy,
    Knowledge,
    Observation,
    list_gaps,
    plan_corp_codes,
    plan_financials,
)
from aegis_alpha.data.opendart_legacy import replay
from aegis_alpha.data.serialization import canonical_json_bytes
from tests.data.opendart_support import corp_archive

if TYPE_CHECKING:
    from pathlib import Path

POLICY = CohortPolicy(first_year=2025)
CORP = "00000101"


def _at(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=UTC)


def _knowledge(*corps: str) -> Knowledge:
    knowledge = Knowledge()
    knowledge.listed({corp: corp[-6:] for corp in corps}, _at("2026-10-01T00:00:00"))
    return knowledge


def _statement(year: int, report: str, fs_div: str = "CFS", corp: str = CORP) -> DartRequest:
    return DartRequest.financials(corp, year, report, fs_div)


def _plan(knowledge: Knowledge, today: date) -> dict[tuple[int, str, str], str]:
    return {
        (int(item.request.parameters["bsns_year"]), item.request.parameters["reprt_code"],
         item.request.parameters["fs_div"]): item.reason
        for item in plan_financials(knowledge, today, POLICY)
    }  # fmt: skip


def test_policy_hash_is_frozen() -> None:
    assert CohortPolicy().sha256 == (
        "ffdcf295bc8fb0f13e99799ec2311d5fb4667954283bab6688432b619baf1be8"
    )
    with pytest.raises(ValueError, match="season_retry_days"):
        CohortPolicy(season_retry_days=0)


def test_a_quarter_enters_the_cohort_when_its_period_ends() -> None:
    knowledge = _knowledge(CORP)
    # 2026-09-30 is the third quarter's end: on that day it is not yet due.
    assert (2026, "11014", "CFS") not in _plan(knowledge, date(2026, 9, 30))
    plan = _plan(knowledge, date(2026, 10, 1))
    assert plan[(2026, "11014", "CFS")] == "never_asked"
    # Every ended period of the policy's years, never the annual report of 2026.
    assert sorted(plan) == sorted(
        [(2025, report, "CFS") for report in ("11011", "11012", "11013", "11014")]
        + [(2026, report, "CFS") for report in ("11012", "11013", "11014")]
    )
    order = [item.request for item in plan_financials(knowledge, date(2026, 10, 1), POLICY)]
    assert order[0] == _statement(2026, "11014")


def test_no_data_is_asked_again_in_season_weekly_and_after_it_quarterly() -> None:
    knowledge = _knowledge(CORP)
    for year, report in ((2025, "11011"), (2025, "11012"), (2025, "11013"), (2025, "11014"),
                         (2026, "11012"), (2026, "11013")):  # fmt: skip
        knowledge.observe(Observation(_statement(year, report), COMPLETED, _at("2026-09-06T00:00")))
    knowledge.observe(Observation(_statement(2026, "11014"), NO_DATA, _at("2026-10-02T00:00")))
    knowledge.observe(
        Observation(_statement(2026, "11014", "OFS"), NO_DATA, _at("2026-10-02T00:00"))
    )
    assert _plan(knowledge, date(2026, 10, 8)) == {}
    assert _plan(knowledge, date(2026, 10, 9)) == {
        (2026, "11014", "CFS"): "season_retry",
        (2026, "11014", "OFS"): "season_retry",
    }
    # The season ends 45 + 30 days after the period: then every 90 days.
    late = Knowledge(stock_codes=knowledge.stock_codes, latest=dict(knowledge.latest))
    late.observe(Observation(_statement(2026, "11014"), NO_DATA, _at("2026-12-20T00:00")))
    late.observe(Observation(_statement(2026, "11014", "OFS"), NO_DATA, _at("2026-12-20T00:00")))
    late.observe(Observation(_statement(2026, "11011"), COMPLETED, _at("2027-03-02T00:00")))
    assert _plan(late, date(2027, 3, 19)) == {}
    assert _plan(late, date(2027, 3, 20)) == {
        (2026, "11014", "CFS"): "no_data_retry",
        (2026, "11014", "OFS"): "no_data_retry",
    }
    # Business years before the retry window wait for a filing instead.
    assert (2026, "11014", "CFS") not in _plan(late, date(2028, 6, 1))


def test_a_filing_on_or_after_the_last_ask_asks_again() -> None:
    knowledge = _knowledge(CORP)
    request = _statement(2026, "11012")
    knowledge.observe(Observation(request, COMPLETED, _at("2026-08-20T03:00")))
    knowledge.file([Filing(CORP, 2026, "11012", "20260820000001", date(2026, 8, 20), "Y")])
    # Asked and filed the same Seoul day: asked again the next day, once.
    assert _plan(knowledge, date(2026, 8, 20)).get((2026, "11012", "CFS")) is None
    assert _plan(knowledge, date(2026, 8, 21))[(2026, "11012", "CFS")] == "new_filing"
    knowledge.observe(Observation(request, COMPLETED, _at("2026-08-21T03:00")))
    assert (2026, "11012", "CFS") not in _plan(knowledge, date(2026, 8, 22))
    # An amendment filed later asks again; so does a filing after a NO_DATA answer.
    knowledge.file([Filing(CORP, 2026, "11012", "20261002000002", date(2026, 10, 2), "Y")])
    assert _plan(knowledge, date(2026, 10, 3))[(2026, "11012", "CFS")] == "new_filing"


def test_the_separate_statement_follows_a_consolidated_no_data() -> None:
    knowledge = _knowledge(CORP)
    knowledge.observe(Observation(_statement(2026, "11013"), NO_DATA, _at("2026-09-06T00:00")))
    assert _plan(knowledge, date(2026, 10, 3))[(2026, "11013", "OFS")] == "never_asked"
    knowledge.observe(Observation(_statement(2026, "11013"), COMPLETED, _at("2026-10-03T00:00")))
    assert (2026, "11013", "OFS") not in _plan(knowledge, date(2026, 10, 4))


def test_failed_and_unanswered_asks_wait_a_day() -> None:
    knowledge = _knowledge(CORP)
    request = _statement(2026, "11013")
    knowledge.observe(Observation(request, COMPLETED, _at("2026-09-06T00:00")))
    knowledge.unanswered[request.fingerprint] = _at("2026-10-03T00:00")
    assert knowledge.seen(request) == Observation(request, FAILED, _at("2026-10-03T00:00"))
    assert (2026, "11013", "CFS") not in _plan(knowledge, date(2026, 10, 3))
    assert _plan(knowledge, date(2026, 10, 4))[(2026, "11013", "CFS")] == "failed_retry"


def test_the_universe_is_narrowed_by_kind_and_widened_by_listed_filers() -> None:
    knowledge = _knowledge("00000101", "00000202")
    knowledge.kind_codes = frozenset({"000101"})
    filed = date(2026, 8, 14)
    knowledge.file(
        [
            Filing("00000303", 2026, "11012", "20260814000001", filed, "K"),
            Filing("00000404", 2026, "11012", "20260814000002", filed, "N"),
        ]
    )
    assert knowledge.corps == ("00000101", "00000303")


def test_list_days_are_covered_only_by_answers_after_the_day_ended() -> None:
    knowledge = Knowledge()
    today = date(2026, 10, 3)
    policy = CohortPolicy(list_lookback_days=2)
    days = [item.request.parameters["bgn_de"] for item in list_gaps(knowledge, today, policy)]
    assert days == ["20261001", "20261002"]
    first = DartRequest.list_page(date(2026, 10, 1), 1)
    # Answered during the day itself (Seoul): not yet covered.
    knowledge.observe(Observation(first, COMPLETED, _at("2026-10-01T05:00"), total_pages=2))
    assert next(item.request for item in list_gaps(knowledge, today, policy)) == first
    knowledge.observe(Observation(first, COMPLETED, _at("2026-10-01T16:00"), total_pages=2))
    gaps = [item.request for item in list_gaps(knowledge, today, policy)]
    assert gaps[0] == DartRequest.list_page(date(2026, 10, 1), 2)
    second = DartRequest.list_page(date(2026, 10, 2), 1)
    knowledge.observe(Observation(second, NO_DATA, _at("2026-10-02T15:00:01")))
    assert [item.request for item in list_gaps(knowledge, today, policy)] == gaps[:1]


def test_the_corp_code_list_is_refreshed_weekly() -> None:
    knowledge = _knowledge(CORP)
    assert plan_corp_codes(knowledge, date(2026, 10, 7), POLICY) == []
    assert len(plan_corp_codes(knowledge, date(2026, 10, 8), POLICY)) == 1
    assert len(plan_corp_codes(Knowledge(), date(2026, 10, 8), POLICY)) == 1


def _legacy_root(root: Path) -> None:
    """A legacy opendart root: the fixed cohort answered everything once, before Q3 ended."""
    (root / "receipts").mkdir(parents=True)
    (root / "raw").mkdir()
    lines = []
    entries = [
        ("corp_codes", "{}", "COMPLETED", corp_archive([(CORP, "000101"), ("00000202", "")])),
        ("financials", _statement(2026, "11013").parameters_json, "COMPLETED", b"{}"),
        ("financials", _statement(2026, "11012").parameters_json, "FAILED", b"{}"),
        ("financials", _statement(2025, "11014").parameters_json, "NO_DATA", b"{}"),
    ]
    for index, (endpoint, parameters, outcome, raw) in enumerate(entries):
        document = {
            "endpoint": endpoint,
            "observation_date": "2026-09-06",
            "parameters_json": parameters,
        }
        fingerprint = hashlib.sha256(canonical_json_bytes(document)).hexdigest()
        stamp = f"2026-09-{10 + index:02d}T03:00:00.000000Z"
        lines.append(canonical_json_bytes({"request": document, "timestamp": stamp}))
        receipt = {"request": document, "outcome": outcome, "retrieved_at_utc": stamp}
        (root / "receipts" / f"{fingerprint}.json").write_bytes(json.dumps(receipt).encode())
        (root / "raw" / f"{fingerprint}.raw").write_bytes(raw)
        if outcome == "FAILED":
            validation = {"validated_outcome": "COMPLETED"}
            name = f"{fingerprint}.validation-v1.json"
            (root / "receipts" / name).write_bytes(json.dumps(validation).encode())
    (root / "attempts.jsonl").write_bytes(b"\n".join(lines) + b"\n")


def test_the_legacy_ledger_replay_plans_the_quarter_its_fixed_cohort_never_asks(
    tmp_path: Path,
) -> None:
    root = tmp_path / "opendart"
    _legacy_root(root)
    replayed = replay(root)
    assert replayed.report() == {
        "ledger_lines": 4,
        "requests": 4,
        "receipts_missing": 0,
        "revalidated": 1,
        "last_attempt": "2026-09-13T03:00:00.000000Z",
        "outcomes": {"COMPLETED": 3, "NO_DATA": 1},
        "observation_dates": {"2026-09-06": 4},
        "listed_corps": 1,
    }
    plan = _plan(replayed.knowledge, date(2026, 10, 3))
    assert plan == {
        (2026, "11014", "CFS"): "never_asked",
        (2025, "11011", "CFS"): "never_asked",
        (2025, "11012", "CFS"): "never_asked",
        (2025, "11013", "CFS"): "never_asked",
        # The legacy NO_DATA answer is not terminal: its separate statement is next.
        (2025, "11014", "OFS"): "never_asked",
    }
