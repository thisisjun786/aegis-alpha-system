from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from typing import cast

import pytest

from aegis_alpha.data.fmp_universe import (
    ListWalkSource,
    RawListPage,
    UniverseCompositionError,
    build_universe_manifest,
    universe_manifest_document,
)
from aegis_alpha.data.fmp_universe_work import terminal_successful_list_pages
from aegis_alpha.data.fmp_windows import (
    LIST_PAGE_LIMIT,
    ListPageOutcome,
    ListWalk,
    UniverseManifest,
    parse_universe_manifest,
)
from aegis_alpha.data.serialization import canonical_json_bytes, content_sha256

ACTIVE_ENDPOINT = "/stable/actively-trading-list"
DELISTED_ENDPOINT = "/stable/delisted-companies"
GENERATED_AT = datetime(2026, 8, 18, 12, 0, tzinfo=UTC)
FIRST_PAGE_AT = datetime(2026, 8, 18, 10, 0, tzinfo=UTC)
SECOND_PAGE_AT = datetime(2026, 8, 18, 10, 1, tzinfo=UTC)
THIRD_PAGE_AT = datetime(2026, 8, 18, 10, 2, tzinfo=UTC)
_FULL_PAGE: tuple[dict[str, object], ...] = tuple(
    {"symbol": f"synth{index:05d}"} for index in range(LIST_PAGE_LIMIT)
)
_DELISTED_RECORDS: tuple[dict[str, object], ...] = (
    {"symbol": "synth.delisted", "ipoDate": "1990-06-30", "delistedDate": "2020-01-02"},
)


def _page(records: Sequence[Mapping[str, object]], retrieved_at: datetime) -> RawListPage:
    return RawListPage(body=json.dumps(records).encode("utf-8"), retrieved_at_utc=retrieved_at)


def _terminated_walk(
    events: Sequence[tuple[int, Sequence[Mapping[str, object]], bool]],
    identity_key: str = "symbol",
) -> ListWalk:
    walk = ListWalk(identity_key)
    outcome: ListPageOutcome | None = None
    for page_number, records, retried in events:
        outcome = walk.observe(page=page_number, records=records, retried=retried)
    assert outcome is ListPageOutcome.COMPLETE
    return walk


def _source(endpoint: str, records: Sequence[Mapping[str, object]]) -> ListWalkSource:
    return ListWalkSource(
        endpoint=endpoint,
        walk=_terminated_walk([(0, records, False)]),
        pages=(_page(records, FIRST_PAGE_AT),),
    )


def _delisted() -> ListWalkSource:
    return _source(DELISTED_ENDPOINT, _DELISTED_RECORDS)


def _build(
    active_records: Sequence[Mapping[str, object]],
    delisted_records: Sequence[Mapping[str, object]] = _DELISTED_RECORDS,
    *,
    generated_at: datetime = GENERATED_AT,
) -> UniverseManifest:
    return build_universe_manifest(
        generated_at_utc=generated_at,
        active=_source(ACTIVE_ENDPOINT, active_records),
        delisted=_source(DELISTED_ENDPOINT, delisted_records),
    )


def _happy_manifest() -> UniverseManifest:
    return _build(
        [
            {"symbol": "synth.a", "companyName": "Synth A"},
            {"symbol": "synth.b", "ipoDate": "2019-05-06"},
        ],
        [{"symbol": "synth.c", "ipoDate": "1998-01-01", "delistedDate": "2001-02-03"}],
    )


def _happy_document() -> dict[str, object]:
    return json.loads(canonical_json_bytes(universe_manifest_document(_happy_manifest())))


def _patch_first(document: dict[str, object], key: str, patch: dict[str, object]) -> None:
    items = document[key]
    assert isinstance(items, list)
    first = items[0]
    assert isinstance(first, dict)
    document[key] = [{**first, **patch}]


# -- composition -----------------------------------------------------------


def test_build_composes_walks_into_a_manifest_that_round_trips_with_equality() -> None:
    manifest = _happy_manifest()
    assert manifest.generated_at_utc == GENERATED_AT
    assert [source.endpoint for source in manifest.sources] == [
        ACTIVE_ENDPOINT,
        DELISTED_ENDPOINT,
    ]
    assert manifest.symbols() == ("synth.a", "synth.b", "synth.c")
    assert [(entry.ipo_date, entry.delisted_date, entry.active) for entry in manifest.entries] == [
        (None, None, True),
        (date(2019, 5, 6), None, True),
        (date(1998, 1, 1), date(2001, 2, 3), False),
    ]
    document = json.loads(canonical_json_bytes(universe_manifest_document(manifest)))
    assert parse_universe_manifest(document) == manifest


def test_source_hash_covers_every_consumed_page_and_changes_with_any_byte() -> None:
    tail_page = [{"symbol": "synth.tail"}]
    walk = _terminated_walk([(0, _FULL_PAGE, False), (1, tail_page, False)])
    pages = (_page(_FULL_PAGE, FIRST_PAGE_AT), _page(tail_page, SECOND_PAGE_AT))
    manifest = build_universe_manifest(
        generated_at_utc=GENERATED_AT,
        active=ListWalkSource(ACTIVE_ENDPOINT, walk, pages),
        delisted=_delisted(),
    )
    expected = content_sha256(tuple(hashlib.sha256(page.body).hexdigest() for page in pages))
    assert manifest.sources[0].raw_content_sha256 == expected
    assert manifest.sources[0].raw_content_sha256 != hashlib.sha256(pages[0].body).hexdigest()
    assert manifest.sources[0].retrieved_at_utc == SECOND_PAGE_AT
    mutated_tail = RawListPage(pages[1].body + b" ", pages[1].retrieved_at_utc)
    mutated = build_universe_manifest(
        generated_at_utc=GENERATED_AT,
        active=ListWalkSource(ACTIVE_ENDPOINT, walk, (pages[0], mutated_tail)),
        delisted=_delisted(),
    )
    assert mutated.sources[0].raw_content_sha256 != manifest.sources[0].raw_content_sha256


def test_exhaustion_confirmation_pages_are_part_of_the_consumed_source_bytes() -> None:
    walk = _terminated_walk([(0, _FULL_PAGE, False), (1, [], False), (1, [], True)])
    pages = (
        _page(_FULL_PAGE, FIRST_PAGE_AT),
        _page([], THIRD_PAGE_AT),
        _page([], SECOND_PAGE_AT),
    )
    manifest = build_universe_manifest(
        generated_at_utc=GENERATED_AT,
        active=ListWalkSource(ACTIVE_ENDPOINT, walk, pages),
        delisted=_delisted(),
    )
    expected = content_sha256(tuple(hashlib.sha256(page.body).hexdigest() for page in pages))
    assert manifest.sources[0].raw_content_sha256 == expected
    # The maximum, not the final page: the middle page carries the latest timestamp.
    assert manifest.sources[0].retrieved_at_utc == THIRD_PAGE_AT


def test_missing_record_dates_become_null_without_invented_facts() -> None:
    manifest = _build([{"symbol": "synth.a"}], [{"symbol": "synth.c"}])
    entry = manifest.entries[-1]
    assert (entry.ipo_date, entry.delisted_date, entry.active) == (None, None, False)


# -- composition failures ---------------------------------------------------


def test_a_symbol_in_both_lists_is_preserved_for_classification() -> None:
    manifest = _build(
        [{"symbol": "synth.a"}, {"symbol": "synth.b"}],
        [{"symbol": "synth.b", "delistedDate": "2020-01-02"}],
    )
    assert [entry.symbol for entry in manifest.entries] == ["synth.a", "synth.b", "synth.b"]
    assert [entry.active for entry in manifest.entries] == [True, True, False]


def test_in_page_duplicate_symbols_keep_the_first_occurrence() -> None:
    manifest = _build([{"symbol": "synth.a"}, {"symbol": "synth.a"}])
    assert [entry.symbol for entry in manifest.entries if entry.active] == ["synth.a"]


def test_provider_mixed_case_symbols_are_stored_verbatim() -> None:
    manifest = _build([{"symbol": "NB2.F"}])
    assert manifest.entries[0].symbol == "NB2.F"


def test_case_insensitive_duplicates_keep_the_first_occurrence() -> None:
    manifest = _build([{"symbol": "nb2.f"}, {"symbol": "NB2.F"}])
    assert [entry.symbol for entry in manifest.entries if entry.active] == ["nb2.f"]


def test_an_active_record_carrying_a_delisted_date_is_preserved() -> None:
    manifest = _build([{"symbol": "synth.a", "delistedDate": "2020-01-02"}])
    assert manifest.entries[0].active is True
    delisted_date = manifest.entries[0].delisted_date
    assert delisted_date is not None
    assert delisted_date.isoformat() == "2020-01-02"


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ("18/08/2026", "ISO calendar date"),
        ("2026-08-18T00:00:00", "ISO calendar date"),
        (20260818, "ISO date or null"),
    ],
)
def test_malformed_record_dates_are_rejected(value: str | int, message: str) -> None:
    with pytest.raises(UniverseCompositionError, match=message):
        _build([{"symbol": "synth.a"}], [{"symbol": "synth.c", "delistedDate": value}])


def test_pages_omitting_the_exhaustion_confirmation_are_rejected() -> None:
    walk = _terminated_walk([(0, _FULL_PAGE, False), (1, [], False), (1, [], True)])
    active = ListWalkSource(ACTIVE_ENDPOINT, walk, (_page(_FULL_PAGE, FIRST_PAGE_AT),))
    with pytest.raises(UniverseCompositionError, match="page count"):
        build_universe_manifest(generated_at_utc=GENERATED_AT, active=active, delisted=_delisted())


def test_rechunked_page_boundaries_are_rejected() -> None:
    tail = [{"symbol": "synth.tail"}]
    walk = _terminated_walk([(0, _FULL_PAGE, False), (1, tail, False)])
    half = LIST_PAGE_LIMIT // 2
    pages = (
        _page(_FULL_PAGE[:half], FIRST_PAGE_AT),
        _page((*_FULL_PAGE[half:], *tail), SECOND_PAGE_AT),
    )
    active = ListWalkSource(ACTIVE_ENDPOINT, walk, pages)
    with pytest.raises(UniverseCompositionError, match="record count"):
        build_universe_manifest(generated_at_utc=GENERATED_AT, active=active, delisted=_delisted())


def test_a_walk_that_did_not_terminate_complete_is_rejected() -> None:
    walk = ListWalk("symbol")
    assert walk.observe(page=0, records=_FULL_PAGE) is ListPageOutcome.CONTINUE
    active = ListWalkSource(ACTIVE_ENDPOINT, walk, (_page(_FULL_PAGE, FIRST_PAGE_AT),))
    with pytest.raises(UniverseCompositionError, match="did not terminate complete"):
        build_universe_manifest(generated_at_utc=GENERATED_AT, active=active, delisted=_delisted())


def test_a_blocked_walk_is_rejected() -> None:
    walk = ListWalk("symbol")
    assert walk.observe(page=0, records=[]) is ListPageOutcome.RETRY_REQUIRED
    assert walk.observe(page=0, records=[], retried=True) is ListPageOutcome.ENDPOINT_BLOCKED
    pages = (_page([], FIRST_PAGE_AT), _page([], SECOND_PAGE_AT))
    active = ListWalkSource(ACTIVE_ENDPOINT, walk, pages)
    with pytest.raises(UniverseCompositionError, match="did not terminate complete"):
        build_universe_manifest(generated_at_utc=GENERATED_AT, active=active, delisted=_delisted())


def test_pages_that_disagree_with_the_walk_identities_are_rejected() -> None:
    walk = _terminated_walk([(0, [{"symbol": "synth.a"}, {"symbol": "synth.b"}], False)])
    tampered = _page([{"symbol": "synth.a"}, {"symbol": "synth.x"}], FIRST_PAGE_AT)
    active = ListWalkSource(ACTIVE_ENDPOINT, walk, (tampered,))
    with pytest.raises(UniverseCompositionError, match="do not match"):
        build_universe_manifest(generated_at_utc=GENERATED_AT, active=active, delisted=_delisted())


def test_a_walk_with_a_non_symbol_identity_key_is_rejected() -> None:
    walk = _terminated_walk([(0, [{"cik": "0000000001"}], False)], identity_key="cik")
    pages = (_page([{"cik": "0000000001"}], FIRST_PAGE_AT),)
    active = ListWalkSource(ACTIVE_ENDPOINT, walk, pages)
    with pytest.raises(UniverseCompositionError, match="identity key"):
        build_universe_manifest(generated_at_utc=GENERATED_AT, active=active, delisted=_delisted())


def test_a_naive_generation_timestamp_is_rejected() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        _build(
            [{"symbol": "synth.a"}],
            generated_at=datetime(2026, 8, 18, 12, 0),  # noqa: DTZ001 - deliberate
        )


def test_a_naive_page_timestamp_is_rejected() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        RawListPage(body=b"[]", retrieved_at_utc=datetime(2026, 8, 18, 10, 0))  # noqa: DTZ001


def test_terminal_list_pages_select_success_and_ignore_retries_and_substring_paths() -> None:
    retry_body = b'{"Error Message":"limit"}'
    success_body = b'[{"symbol":"synth.active"}]'
    confusing_body = b'[{"symbol":"synth.confusing"}]'
    retry = canonical_json_bytes(
        {
            "content_sha256": hashlib.sha256(retry_body).hexdigest(),
            "request_fingerprint": "fp-active-0",
            "retrieved_at_utc": FIRST_PAGE_AT,
            "source_uri": f"https://financialmodelingprep.com{ACTIVE_ENDPOINT}?limit=1000&page=0",
            "status_code": 429,
        }
    )
    success = canonical_json_bytes(
        {
            "content_sha256": hashlib.sha256(success_body).hexdigest(),
            "request_fingerprint": "fp-active-0",
            "retrieved_at_utc": SECOND_PAGE_AT,
            "source_uri": f"https://financialmodelingprep.com{ACTIVE_ENDPOINT}?limit=1000&page=0",
            "status_code": 200,
        }
    )
    confusing = canonical_json_bytes(
        {
            "content_sha256": hashlib.sha256(confusing_body).hexdigest(),
            "request_fingerprint": "fp-extra-0",
            "retrieved_at_utc": THIRD_PAGE_AT,
            "source_uri": (
                f"https://financialmodelingprep.com{ACTIVE_ENDPOINT}-extra?limit=1000&page=0"
            ),
            "status_code": 200,
        }
    )
    bodies = {
        hashlib.sha256(retry_body).hexdigest(): retry_body,
        hashlib.sha256(success_body).hexdigest(): success_body,
        hashlib.sha256(confusing_body).hexdigest(): confusing_body,
    }

    pages = terminal_successful_list_pages((retry, success, confusing), bodies, ACTIVE_ENDPOINT)

    assert len(pages) == 1
    assert pages[0].body == success_body
    assert pages[0].retrieved_at_utc == SECOND_PAGE_AT


# -- schema failures pinned through the round-trip --------------------------


def test_the_round_tripped_document_rejects_empty_entries() -> None:
    document = _happy_document()
    document["entries"] = []
    with pytest.raises(ValueError, match="at least one symbol entry"):
        parse_universe_manifest(document)


def test_the_round_tripped_document_rejects_duplicate_symbols() -> None:
    document = _happy_document()
    entries = document["entries"]
    assert isinstance(entries, list)
    document["entries"] = [*entries, entries[0]]
    with pytest.raises(ValueError, match="duplicate symbol"):
        parse_universe_manifest(document)


def test_the_round_tripped_document_accepts_provider_mixed_case_symbols() -> None:
    document = _happy_document()
    _patch_first(document, "entries", {"symbol": "NB2.F"})
    manifest = parse_universe_manifest(document)
    assert manifest.entries[0].symbol == "NB2.F"


def test_the_round_tripped_document_rejects_duplicate_active_flags() -> None:
    document = _happy_document()
    entries = cast("list[dict[str, object]]", document["entries"])
    document["entries"] = [*entries, {**entries[0], "symbol": str(entries[0]["symbol"]).upper()}]
    with pytest.raises(ValueError, match="duplicate symbol"):
        parse_universe_manifest(document)


def test_the_round_tripped_document_allows_the_same_symbol_active_and_delisted() -> None:
    document = _happy_document()
    entries = cast("list[dict[str, object]]", document["entries"])
    document["entries"] = [
        *entries,
        {**entries[0], "active": False, "delistedDate": "2020-01-02"},
    ]
    manifest = parse_universe_manifest(document)
    assert [entry.active for entry in manifest.entries if entry.symbol == entries[0]["symbol"]] == [
        True,
        False,
    ]


def test_the_round_tripped_document_rejects_malformed_source_hashes() -> None:
    document = _happy_document()
    _patch_first(document, "sources", {"raw_content_sha256": "z" * 64})
    with pytest.raises(ValueError, match="SHA-256"):
        parse_universe_manifest(document)


def test_the_round_tripped_document_rejects_malformed_entry_dates() -> None:
    document = _happy_document()
    _patch_first(document, "entries", {"ipoDate": "18/08/2026"})
    with pytest.raises(ValueError, match="ISO calendar date"):
        parse_universe_manifest(document)
