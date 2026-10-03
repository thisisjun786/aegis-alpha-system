"""FRED/ALFRED requests, answers and windows, offline against ``FakeFred``."""

# ruff: noqa: PLR2004 -- synthetic counts are the expected values
from __future__ import annotations

import hashlib
import itertools
import json
from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta

import pytest

from aegis_alpha.data import fred_collect as fred
from aegis_alpha.data.opendart import HttpAnswer, TransportError
from aegis_alpha.data.provider_request import COMPLETED, FAILED, NO_DATA, Response
from tests.data.opendart_support import FakeClock
from tests.data.us_collect_support import FRED_KEY, FakeFred

TODAY = date(2026, 9, 10)


def _answer(fake: FakeFred, request: fred.Request) -> Response:
    return fred.FredClient(FRED_KEY, fake, FakeClock()).request(request)


def test_a_request_names_its_window_and_never_its_key() -> None:
    request = fred.observations("DGS10", date(2026, 9, 1), date(2026, 9, 9))
    assert request.parameters == {
        "file_type": "json",
        "limit": "100000",
        "offset": "0",
        "realtime_end": "2026-09-09",
        "realtime_start": "2026-09-01",
        "series_id": "DGS10",
    }
    assert FRED_KEY not in json.dumps(request.document)
    # The fingerprint names the question: the same window is the same request any day.
    assert (
        request.fingerprint
        == fred.observations("DGS10", date(2026, 9, 1), date(2026, 9, 9)).fingerprint
    )
    assert (
        request.fingerprint
        != fred.vintage_dates("DGS10", date(2026, 9, 1), date(2026, 9, 9)).fingerprint
    )
    # The documented formula, spelled out: sha256 of the canonical JSON
    # ["aas-fred-request-v1", endpoint, parameters_json].
    parameters = json.dumps(request.parameters, sort_keys=True, separators=(",", ":"))
    spelled = json.dumps(["aas-fred-request-v1", "observations", parameters],
                         separators=(",", ":")).encode()  # fmt: skip
    assert request.fingerprint == hashlib.sha256(spelled).hexdigest()
    assert request.fingerprint == (
        "f8775e34d73ec79f1df60ca9b768e7316b2aa4551ebf97ac3e6deb5a29cd777f"
    )
    with pytest.raises(ValueError, match="runs forward"):
        fred.observations("DGS10", date(2026, 9, 9), date(2026, 9, 1))
    with pytest.raises(ValueError, match="upper-case"):
        fred.series_csv("dgs10")
    fake = FakeFred(TODAY)
    fake.publish("DGS10", date(2026, 9, 1), "4.10", date(2026, 9, 2))
    _answer(fake, fred.series_csv("DGS10"))
    _answer(fake, request)
    # The key reaches the provider URL of keyed endpoints only.
    assert fake.calls[0] == ("fredgraph.csv", {"id": "DGS10"})
    assert "api_key" not in fake.calls[1][1]  # the fake pops and checks it


def test_an_answer_echoing_the_key_is_not_retained() -> None:
    def echo(method: str, url: str, body: bytes | None, headers: Mapping[str, str]) -> HttpAnswer:
        del method, url, body, headers
        return HttpAnswer(200, (), json.dumps({"key": FRED_KEY}).encode())

    client = fred.FredClient(FRED_KEY, echo, FakeClock())
    with pytest.raises(TransportError, match="echoed"):
        client.request(fred.vintage_dates("DGS10", date(2026, 9, 1), date(2026, 9, 9)))
    with pytest.raises(ValueError, match="32 lowercase"):
        fred.FredClient("short", echo)


def test_answers_are_classified_and_a_refused_key_stops_the_run() -> None:
    fake = FakeFred(TODAY)
    fake.publish("DGS10", date(2026, 9, 1), "4.10", date(2026, 9, 2))
    window = fred.observations("DGS10", fred.ORIGIN, date(2026, 9, 9))
    answered = _answer(fake, window)
    assert fred.classify(window, answered) == (COMPLETED, None)
    page = fred.parse_page(window, answered.body)
    assert page.count == 1
    # The open period is clipped to the window's end, the last ended FRED day.
    assert page.rows == ((date(2026, 9, 1), date(2026, 9, 2), date(2026, 9, 9), "4.10"),)
    missing = fred.observations("NOPE", fred.ORIGIN, date(2026, 9, 9))
    outcome, message = fred.classify(missing, _answer(fake, missing))
    assert outcome == NO_DATA
    assert message is not None
    assert "does not exist" in message
    # A page whose rows lie outside the asked window is unreadable, so FAILED.
    shifted = fred.observations("DGS10", date(2026, 9, 3), date(2026, 9, 9))
    outcome, message = fred.classify(shifted, answered)
    assert outcome == FAILED
    assert message is not None
    assert message.startswith("unreadable answer")
    moment = datetime(2026, 9, 10, tzinfo=UTC)
    refused = Response(
        400, (), b'{"error_code":400,"error_message":"Bad Request. The value for variable '
        b'api_key is not registered."}', moment, moment,
    )  # fmt: skip
    assert fred.stops_run(refused)
    assert fred.stops_run(Response(429, (), b"", moment, moment))
    assert not fred.stops_run(Response(500, (), b"", moment, moment))
    csv = fred.series_csv("DEXKOUS")
    assert (
        fred.classify(csv, Response(200, (), b"observation_date,DEXKOUS\n", moment, moment))[0]
        == COMPLETED
    )
    assert fred.classify(csv, Response(200, (), b"DATE,DEXKOUS\n", moment, moment))[0] == FAILED


def test_windows_span_at_most_1990_vintages_and_chain_on_their_last_vintage() -> None:
    days = [date(2000, 1, 1) + timedelta(days=index) for index in range(4500)]
    end = days[-1] + timedelta(days=3)
    windows = fred.observation_windows(fred.ORIGIN, days, end)
    assert windows == [
        (fred.ORIGIN, days[1989]),
        (days[1989], days[3978]),
        (days[3978], end),
    ]
    # Each window stays below FRED's limit of 2000, its start counted or not.
    for first, last in windows:
        assert sum(first <= day <= last for day in days) <= 1990
    # Every vintage after the first window's start is inside some window's (start, end].
    for day in days:
        assert any(first < day <= last for first, last in windows)
    # From a known day, the known day counts as a vintage of the first window.
    known = days[100]
    later = fred.observation_windows(known, days[101:2101], end)
    assert later == [(known, days[2089]), (days[2089], end)]
    assert fred.observation_windows(known, [], end) == []


def test_rows_starting_on_a_window_start_restate_what_is_held() -> None:
    fake = FakeFred(TODAY)
    for day in range(1, 6):
        fake.publish("DGS10", date(2026, 8, day), f"4.{day}", date(2026, 8, day + 1))
    fake.publish("DGS10", date(2026, 8, 1), "4.9", date(2026, 9, 3))  # a revision
    window = fred.observations("DGS10", date(2026, 8, 4), date(2026, 9, 9))
    page = fred.parse_page(window, _answer(fake, window).body)
    kept, restated = fred.split_rows(window, page.rows)
    # ALFRED clipped the periods that began before the window to its start; the period that
    # began on it is held from the earlier collection too.
    assert restated == 3
    assert {(row[0], row[1], row[3]) for row in kept} == {
        (date(2026, 8, 4), date(2026, 8, 5), "4.4"),
        (date(2026, 8, 5), date(2026, 8, 6), "4.5"),
        (date(2026, 8, 1), date(2026, 9, 3), "4.9"),
    }
    origin = fred.observations("DGS10", fred.ORIGIN, date(2026, 9, 9))
    whole = fred.parse_page(origin, _answer(fake, origin).body)
    assert fred.split_rows(origin, whole.rows) == (list(whole.rows), 0)


def test_the_plan_checks_vintages_after_the_known_day_and_csv_once_a_day() -> None:
    policy = fred.FredPolicy(alfred_series=("DGS10", "GDP", "UNRATE"), csv_series={"DEXKOUS": "fx"})
    knowledge = fred.FredKnowledge()
    knowledge.vintage("DGS10", date(2026, 9, 2))
    knowledge.vintage("GDP", TODAY - timedelta(days=1))
    planned = fred.plan_alfred(knowledge, TODAY, policy)
    assert [(item.request.parameters["series_id"], item.reason, item.request.parameters[
        "realtime_start"]) for item in planned] == [
        ("DGS10", "vintage_check", "2026-09-03"),
        ("UNRATE", "origin", "1776-07-04"),
    ]  # fmt: skip
    assert {item.request.parameters["realtime_end"] for item in planned} == {"2026-09-09"}
    assert [item.request.parameters["id"] for item in fred.plan_csv(knowledge, TODAY, policy)] == [
        "DEXKOUS"
    ]
    knowledge.csv("DEXKOUS", TODAY)
    assert fred.plan_csv(knowledge, TODAY, policy) == []
    # The policy hash names the series it keeps current.
    assert policy.sha256 != fred.FredPolicy().sha256
    assert len(fred.DEFAULT_ALFRED_SERIES) == 36
    assert "DEXKOUS" in fred.DEFAULT_ALFRED_SERIES


@pytest.mark.parametrize("count", [0, 1, 99_999, 100_000, 100_001, 250_000])
def test_pages_follow_the_count(count: int) -> None:
    first = fred.observations("NFCI", fred.ORIGIN, TODAY)
    offsets = []
    request: fred.Request | None = first
    while request is not None:
        offset = fred.window(request).offset
        offsets.append(offset)
        rows = max(0, min(fred.PAGE_LIMIT, count - offset))
        request = fred.next_page(request, fred.Page(count, tuple(itertools.repeat(
            (TODAY, TODAY, TODAY, "1"), rows))))  # fmt: skip
    assert offsets == list(range(0, max(count, 1), fred.PAGE_LIMIT))
