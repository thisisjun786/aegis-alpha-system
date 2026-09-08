from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from aegis_alpha.data import fmp_windows
from aegis_alpha.data.fmp_windows import (
    BACKFILL_CHUNK_DAYS,
    LIST_PAGE_LIMIT,
    MAX_RECORDS_PER_WINDOW,
    CollectorContractError,
    DateWindow,
    ListPageOutcome,
    ListWalk,
    UniverseEntry,
    WindowOutcome,
    WindowWalk,
    classify_window_response,
    parse_universe_manifest,
    plan_backfill_windows,
    plan_incremental_window,
    split_window,
    validate_window_dates,
)

FIXTURE_ROOT = (
    Path(__file__).resolve().parents[1] / "fixtures" / "provider_neutral" / "fmp_collector"
)
AS_OF = date(2026, 7, 29)
_THREE_DAY_WINDOW = 3
_TEN_DAY_WINDOW = 10
_SHA256_HEX_LENGTH = 64
_EXPECTED_IDENTITIES = 212


def _load_fixture(name: str) -> object:
    return json.loads((FIXTURE_ROOT / name).read_text(encoding="utf-8"))


def _records(count: int, *, key: str = "symbol", offset: int = 0) -> list[dict[str, object]]:
    return [{key: f"synth{index + offset:05d}"} for index in range(count)]


def test_date_window_rejects_reversed_and_aware_values() -> None:
    with pytest.raises(ValueError, match="cannot precede"):
        DateWindow(date(2026, 7, 2), date(2026, 7, 1))
    with pytest.raises(TypeError, match="naive calendar date"):
        DateWindow(datetime(2026, 7, 1, tzinfo=UTC), date(2026, 7, 2))  # type: ignore[arg-type]


def test_date_window_boundaries_are_inclusive() -> None:
    window = DateWindow(date(2026, 7, 1), date(2026, 7, 3))
    assert window.day_count == _THREE_DAY_WINDOW
    assert window.contains(date(2026, 7, 1))
    assert window.contains(date(2026, 7, 3))
    assert not window.contains(date(2026, 6, 30))
    assert window.as_parameters() == {"from": "2026-07-01", "to": "2026-07-03"}


def test_split_window_uses_inclusive_integer_midpoint() -> None:
    first, second = split_window(DateWindow(date(2026, 7, 1), date(2026, 7, 10)))
    assert (first.start, first.end) == (date(2026, 7, 1), date(2026, 7, 5))
    assert (second.start, second.end) == (date(2026, 7, 6), date(2026, 7, 10))
    assert first.day_count + second.day_count == _TEN_DAY_WINDOW


def test_split_window_refuses_a_single_day() -> None:
    with pytest.raises(ValueError, match="irreducible"):
        split_window(DateWindow(AS_OF, AS_OF))


def test_exactly_five_thousand_records_requires_a_split() -> None:
    window = DateWindow(date(2026, 1, 1), date(2026, 3, 1))
    assert classify_window_response(window, MAX_RECORDS_PER_WINDOW) is WindowOutcome.SPLIT_REQUIRED
    assert classify_window_response(window, MAX_RECORDS_PER_WINDOW - 1) is WindowOutcome.COMPLETE


def test_single_day_truncation_is_irreducible() -> None:
    window = DateWindow(AS_OF, AS_OF)
    outcome = classify_window_response(window, MAX_RECORDS_PER_WINDOW)
    assert outcome is WindowOutcome.IRREDUCIBLE_TRUNCATION


def test_empty_window_inside_bounds_is_a_gap_not_a_stop() -> None:
    window = DateWindow(date(2020, 1, 1), date(2020, 3, 1))
    assert classify_window_response(window, 0) is WindowOutcome.COVERAGE_GAP


def test_over_cap_response_blocks_the_run() -> None:
    with pytest.raises(CollectorContractError, match="record provider cap"):
        classify_window_response(DateWindow(AS_OF, AS_OF), MAX_RECORDS_PER_WINDOW + 1)


def test_returned_dates_outside_the_window_block_the_run() -> None:
    window = DateWindow(date(2026, 7, 1), date(2026, 7, 3))
    validate_window_dates(window, [date(2026, 7, 1), date(2026, 7, 3)])
    with pytest.raises(CollectorContractError, match="outside the requested window"):
        validate_window_dates(window, [date(2026, 7, 2), date(2026, 7, 4)])


def test_window_walk_records_each_split_and_recollects_both_halves() -> None:
    parent = DateWindow(date(2026, 7, 1), date(2026, 7, 10))
    walk = WindowWalk([parent])
    assert walk.next_window() == parent
    first, second = walk.record_split(parent)
    assert walk.pending == (second, first)
    assert len(walk.splits) == 1
    receipt_entry = walk.splits[0].as_receipt_entry()
    assert receipt_entry["parent_from"] == "2026-07-01"
    assert receipt_entry["second_to"] == "2026-07-10"


def test_incremental_window_reaches_five_collected_days_behind_the_watermark() -> None:
    collected = [date(2026, 7, 20) + timedelta(days=index) for index in range(8)]
    watermark = collected[-1]
    plan = plan_incremental_window(as_of=AS_OF, watermark=watermark, collected_dates=collected)
    assert plan.backfill_required is False
    assert plan.window is not None
    assert plan.window.start == collected[-6]
    assert plan.window.end == AS_OF


def test_incremental_window_falls_back_to_earliest_when_fewer_than_five_dates() -> None:
    collected = [date(2026, 7, 27), date(2026, 7, 28)]
    plan = plan_incremental_window(
        as_of=AS_OF,
        watermark=date(2026, 7, 28),
        collected_dates=collected,
    )
    assert plan.window is not None
    assert plan.window.start == date(2026, 7, 27)


def test_missing_watermark_switches_to_backfill() -> None:
    plan = plan_incremental_window(as_of=AS_OF, watermark=None, collected_dates=[])
    assert plan.backfill_required is True
    assert plan.window is None


def test_watermark_after_as_of_is_rejected() -> None:
    with pytest.raises(ValueError, match="cannot exceed"):
        plan_incremental_window(
            as_of=AS_OF,
            watermark=AS_OF + timedelta(days=1),
            collected_dates=[],
        )


def test_backfill_chunks_are_1500_days_laid_out_backward() -> None:
    entry = UniverseEntry(symbol="synth.a", ipo_date=None, delisted_date=None, active=True)
    plan = plan_backfill_windows(entry=entry, operator_from=date(2016, 1, 1), as_of=AS_OF)
    assert plan.skipped_reason is None
    assert plan.bounded_range == DateWindow(date(2016, 1, 1), AS_OF)
    assert plan.windows[0].end == AS_OF
    assert plan.windows[0].day_count == BACKFILL_CHUNK_DAYS
    assert plan.windows[-1].start == date(2016, 1, 1)
    for later, earlier in zip(plan.windows, plan.windows[1:], strict=False):
        assert earlier.end == later.start - timedelta(days=1)
    assert sum(window.day_count for window in plan.windows) == (AS_OF - date(2016, 1, 1)).days + 1


def test_backfill_range_is_clamped_by_manifest_ipo_and_delisted_dates() -> None:
    entry = UniverseEntry(
        symbol="synth.b",
        ipo_date=date(2019, 5, 6),
        delisted_date=date(2021, 8, 9),
        active=False,
    )
    plan = plan_backfill_windows(entry=entry, operator_from=date(2016, 1, 1), as_of=AS_OF)
    assert plan.bounded_range == DateWindow(date(2019, 5, 6), date(2021, 8, 9))
    assert plan.windows[0].end == date(2021, 8, 9)
    assert plan.windows[-1].start == date(2019, 5, 6)


def test_symbol_delisted_before_operator_from_is_skipped() -> None:
    entry = UniverseEntry(
        symbol="synth.c",
        ipo_date=date(1998, 1, 1),
        delisted_date=date(2001, 2, 3),
        active=False,
    )
    plan = plan_backfill_windows(entry=entry, operator_from=date(2016, 1, 1), as_of=AS_OF)
    assert plan.windows == ()
    assert plan.bounded_range is None
    assert plan.skipped_reason == "empty_range"


def test_manifest_dates_not_response_emptiness_bound_a_long_delisted_symbol() -> None:
    entry = UniverseEntry(
        symbol="synth.d",
        ipo_date=date(2004, 3, 1),
        delisted_date=date(2009, 6, 30),
        active=False,
    )
    plan = plan_backfill_windows(entry=entry, operator_from=date(2000, 1, 1), as_of=AS_OF)
    walk = WindowWalk(plan.windows)
    visited: list[DateWindow] = []
    while (window := walk.next_window()) is not None:
        visited.append(window)
        # Every window in this bounded range returns nothing; the walk must
        # still reach the symbol's active window rather than stopping early.
        assert classify_window_response(window, 0) is WindowOutcome.COVERAGE_GAP
    assert visited[-1].start == date(2004, 3, 1)


def test_list_walk_ends_normally_on_a_short_page() -> None:
    walk = ListWalk("symbol")
    assert walk.observe(page=0, records=_records(LIST_PAGE_LIMIT)) is ListPageOutcome.CONTINUE
    outcome = walk.observe(page=1, records=_records(12, offset=LIST_PAGE_LIMIT))
    assert outcome is ListPageOutcome.COMPLETE
    assert len(walk.identities) == LIST_PAGE_LIMIT + 12


def test_list_walk_learns_a_smaller_effective_page_size() -> None:
    walk = ListWalk("symbol")
    assert walk.observe(page=0, records=_records(100)) is ListPageOutcome.CONTINUE
    assert walk.observe(page=1, records=_records(100, offset=100)) is ListPageOutcome.CONTINUE
    outcome = walk.observe(page=2, records=_records(12, offset=200))
    assert outcome is ListPageOutcome.COMPLETE
    assert len(walk.identities) == _EXPECTED_IDENTITIES


def test_empty_page_after_an_effective_full_page_confirms_exhaustion() -> None:
    walk = ListWalk("symbol")
    walk.observe(page=0, records=_records(100))
    assert walk.observe(page=1, records=[]) is ListPageOutcome.RETRY_REQUIRED
    assert walk.observe(page=1, records=[], retried=True) is ListPageOutcome.COMPLETE


def test_empty_page_after_a_full_page_is_retried_then_confirms_exhaustion() -> None:
    walk = ListWalk("symbol")
    walk.observe(page=0, records=_records(LIST_PAGE_LIMIT))
    assert walk.observe(page=1, records=[]) is ListPageOutcome.RETRY_REQUIRED
    assert walk.observe(page=1, records=[], retried=True) is ListPageOutcome.COMPLETE
    assert [entry["outcome"] for entry in walk.as_receipt_pages()] == [
        "continue",
        "retry_required",
        "complete",
    ]


def test_empty_first_page_is_retried_then_blocks_the_endpoint() -> None:
    walk = ListWalk("cik")
    assert walk.observe(page=0, records=[]) is ListPageOutcome.RETRY_REQUIRED
    assert walk.observe(page=0, records=[], retried=True) is ListPageOutcome.ENDPOINT_BLOCKED
    assert walk.identities == ()


def test_retried_empty_page_after_a_short_page_blocks_rather_than_exhausts() -> None:
    walk = ListWalk("symbol")
    walk.observe(page=0, records=_records(5))
    walk.observe(page=1, records=[])
    assert walk.observe(page=1, records=[], retried=True) is ListPageOutcome.ENDPOINT_BLOCKED


def test_page_identity_overlap_is_recorded_without_blocking() -> None:
    walk = ListWalk("symbol")
    walk.observe(page=0, records=_records(LIST_PAGE_LIMIT))
    outcome = walk.observe(page=1, records=_records(3))
    assert outcome is ListPageOutcome.COMPLETE
    assert len(walk.identities) == LIST_PAGE_LIMIT + 3


def test_repeated_full_page_without_new_identities_is_contract_drift() -> None:
    walk = ListWalk("symbol")
    records = _records(LIST_PAGE_LIMIT)
    assert walk.observe(page=0, records=records) is ListPageOutcome.CONTINUE
    with pytest.raises(CollectorContractError, match="no identity progress"):
        walk.observe(page=1, records=records)


def test_unpaged_first_page_over_the_limit_completes_the_walk() -> None:
    walk = ListWalk("symbol")
    records = _records(LIST_PAGE_LIMIT + 1)
    assert walk.observe(page=0, records=records) is ListPageOutcome.COMPLETE
    assert len(walk.identities) == LIST_PAGE_LIMIT + 1


def test_later_page_over_the_limit_still_blocks() -> None:
    walk = ListWalk("symbol")
    walk.observe(page=0, records=_records(LIST_PAGE_LIMIT))
    with pytest.raises(CollectorContractError, match="page limit"):
        walk.observe(page=1, records=_records(LIST_PAGE_LIMIT + 1, offset=LIST_PAGE_LIMIT))


def test_later_page_over_the_learned_page_size_blocks() -> None:
    walk = ListWalk("symbol")
    walk.observe(page=0, records=_records(100))
    with pytest.raises(CollectorContractError, match="learned 100-record page size"):
        walk.observe(page=1, records=_records(101, offset=100))


def test_list_walk_blocks_at_the_runaway_page_ceiling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fmp_windows, "MAX_LIST_PAGES", 2)
    walk = ListWalk("symbol")
    assert walk.observe(page=0, records=_records(100)) is ListPageOutcome.CONTINUE
    with pytest.raises(CollectorContractError, match="runaway ceiling"):
        walk.observe(page=1, records=_records(100, offset=100))


def test_list_walk_requires_the_identity_key_and_sequential_pages() -> None:
    walk = ListWalk("cik")
    with pytest.raises(CollectorContractError, match="identity key"):
        walk.observe(page=0, records=[{"companyName": "SYNTH"}])
    with pytest.raises(ValueError, match="expected page"):
        ListWalk("symbol").observe(page=2, records=_records(1))


def test_universe_manifest_fixture_parses_with_provenance() -> None:
    manifest = parse_universe_manifest(_load_fixture("universe_manifest.json"))
    assert manifest.generated_at_utc.tzinfo is not None
    assert manifest.symbols() == ("synth.a", "synth.b", "synth.c")
    assert manifest.sources[0].endpoint.startswith("/stable/")
    assert len(manifest.sources[0].raw_content_sha256) == _SHA256_HEX_LENGTH


@pytest.mark.parametrize(
    ("fixture_name", "message"),
    [
        ("universe_manifest_duplicate_symbol.json", "duplicate symbol"),
        ("universe_manifest_malformed_date.json", "ISO calendar date"),
        ("universe_manifest_missing_provenance.json", "raw_content_sha256"),
    ],
)
def test_universe_manifest_rejections(fixture_name: str, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        parse_universe_manifest(_load_fixture(fixture_name))
