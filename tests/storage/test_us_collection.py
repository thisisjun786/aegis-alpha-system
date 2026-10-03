"""Native SEC and FRED/ALFRED collection into an installation, offline.

The providers are ``tests.data.us_collect_support.FakeFred`` and ``FakeSec``, which answer
in the providers' shapes (ALFRED clips real-time periods to the asked window and refuses a
window over the collector's margin of 1990 vintage dates). Every series, value, CIK and
accession is synthetic.
Expected rows come from the fakes' unclipped histories, not from the collector's code.
"""

# ruff: noqa: PLR2004 -- synthetic counts are the expected values
from __future__ import annotations

import hashlib
import itertools
import json
import os
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast

import pyarrow as pa
import pytest

from aegis_alpha.application.cli import main
from aegis_alpha.data import fred_collect as fred
from aegis_alpha.data import sec_collect as sec
from aegis_alpha.data.opendart import HttpAnswer
from aegis_alpha.data.provider_request import Response
from aegis_alpha.storage import fred_collection, provider_collection, sec_collection, source_library
from aegis_alpha.storage.collection_ledger import Attempt
from aegis_alpha.storage.identity import mint_issuer
from aegis_alpha.storage.legacy_import.engine import apply_import
from aegis_alpha.storage.legacy_import.manifest import parse_manifest
from aegis_alpha.storage.promotion.engine import promote
from aegis_alpha.storage.promotion.mappers.fred import vintage_partitions
from aegis_alpha.storage.raw import put_raw
from aegis_alpha.storage.source_identity import SourceContent, SourceFile
from aegis_alpha.storage.verification import verify_workspace
from aegis_alpha.storage.workspace import Workspace, initialize, open_workspace
from tests.data.opendart_support import FakeClock
from tests.data.us_collect_support import (
    FRED_KEY,
    OPEN,
    USER_AGENT,
    FakeFred,
    FakeSec,
    SecFact,
    SecFiling,
    submissions_bytes,
)
from tests.storage.test_macro_fx import CHICAGO, _private, _rule, _spec, market_target

# 03:00 UTC on 2026-09-10 is 22:00 on 2026-09-09 in St. Louis.
FRED_NOW = datetime(2026, 9, 10, 3, tzinfo=UTC)
POLICY = fred.FredPolicy(alfred_series=("DGS10", "GDP"), csv_series={"DEXKOUS": "fx.usdkrw.fred"})


@pytest.fixture
def ws(tmp_path: Path) -> Iterator[Workspace]:
    root = tmp_path / "aas"
    initialize(root)
    with open_workspace(root, writable=True, strategy_write=True) as workspace:
        yield workspace


def _history(fake: FakeFred) -> None:
    """DGS10 daily with one revision, GDP quarterly with two vintages, DEXKOUS daily."""
    for offset in range(20):
        day = date(2026, 8, 10) + timedelta(days=offset)
        fake.publish("DGS10", day, f"4.{offset:02d}", day + timedelta(days=1))
        fake.publish("DEXKOUS", day, "" if offset == 5 else f"13{offset:02d}.50", day)
    fake.publish("DGS10", date(2026, 8, 12), "4.99", date(2026, 9, 1))
    fake.publish("GDP", date(2026, 4, 1), "30100.1", date(2026, 7, 30))
    fake.publish("GDP", date(2026, 4, 1), "30111.9", date(2026, 8, 28))


def _fred_run(ws: Workspace, fake: FakeFred, clock: FakeClock, **bounds: int) -> dict[str, object]:
    client = fred.FredClient(FRED_KEY, fake, clock)
    return fred_collection.collect_fred(
        ws, client, policy=POLICY, clock=clock, sleep=lambda _: None, **bounds
    )


def _rows(ws: Workspace, source_id: str, table: str, columns: str) -> list[tuple[object, ...]]:
    target = market_target(ws, source_id, table)
    return ws.market.execute(f"SELECT {columns} FROM {target} ORDER BY ALL").fetchall()  # noqa: S608 -- test SQL


def _sources(result: dict[str, object], prefix: str) -> list[dict[str, object]]:
    return [
        item
        for item in cast("list[dict[str, object]]", result["sources"])
        if str(item.get("source_id", "")).startswith(prefix)
    ]


def _expected_periods(fake: FakeFred, series: str, through: date) -> set[tuple[object, ...]]:
    """The unclipped periods FRED dates as starting by ``through``, as source columns."""
    return {
        (series, period.observation, period.start, period.value)
        for period in fake.history[series]
        if period.start <= through
    }


def _pin(source: dict[str, object]) -> dict[str, str]:
    source_id = str(source["source_id"])
    return {
        "source_id": source_id,
        "source_sha256": source_id.rsplit("-", 1)[-1],
        "table": str(source["table"]),
        "digest": str(source["digest"]),
    }


def _promote_alfred(ws: Workspace, pin: dict[str, str], parent: str | None) -> str | None:
    """Promote each vintage partition of one collected table in order; the new head."""
    target = market_target(ws, pin["source_id"], pin["table"])
    for partition in vintage_partitions(ws.market, target):
        document = _spec(
            "macro_observations",
            "macro.us.alfred",
            [pin],
            "fred.alfred@1",
            {},
            _rule("local_day_end@1", "revision", "vintage_start", CHICAGO),
            decimal={"value": "decimal_text@1"},
            parent=parent,
            partition=partition,
        )
        result = promote(ws, *document, apply=True)
        assert result["refusals"] == []
        parent = str(result["generation_id"])
    return parent


def _watermarks(ws: Workspace, dataset: str) -> dict[str, int]:
    return {
        str(partition): int(through)
        for partition, through in ws.state.execute(
            "SELECT partition_id, through_us FROM watermarks WHERE dataset_id=?", (dataset,)
        )
    }


# --- FRED -------------------------------------------------------------------------------------


def test_an_origin_run_collects_every_vintage_once_in_windows_of_at_most_1990(
    ws: Workspace,
) -> None:
    fake = FakeFred(date(2026, 9, 9))
    # 2,500 vintage days: one window cannot hold them, and ALFRED clips each window's start.
    for offset in range(2500):
        day = date(2019, 1, 1) + timedelta(days=offset)
        fake.publish("DGS10", date(2019, 1, 1) + timedelta(days=offset % 40), str(offset), day)
    policy = fred.FredPolicy(alfred_series=("DGS10",), csv_series={})
    clock = FakeClock(FRED_NOW)
    client = fred.FredClient(FRED_KEY, fake, clock)
    result = fred_collection.collect_fred(
        ws, client, policy=policy, clock=clock, sleep=lambda _: None
    )
    windows = [(p["realtime_start"], p["realtime_end"]) for p in fake.asked("observations")]
    assert len(windows) == 2
    assert windows[0][0] == "1776-07-04"
    assert windows[1][0] == windows[0][1]  # the next window starts on the last vintage read
    assert result["outcomes"] == {"COMPLETED": 3}
    (observations,) = _sources(result, "fred-alfred-observations-")
    rows = _rows(ws, str(observations["source_id"]), "observations",
                 "series_id, observation_date, realtime_start, value")  # fmt: skip
    # Each period FRED dates exactly once: no clipped restatement became a vintage.
    assert len(rows) == len(set(rows))
    assert set(rows) == _expected_periods(fake, "DGS10", date(2026, 9, 9))
    assert observations["restated_rows"] == 40  # the 40 periods current at the second start
    last = date(2019, 1, 1) + timedelta(days=2499)
    assert result["vintages_through"] == {"DGS10": last.isoformat()}
    attempts = dict(ws.state.execute("SELECT status,count(*) FROM collection_attempts GROUP BY 1"))
    usage = dict(ws.state.execute("SELECT kind,count(*) FROM usage_events GROUP BY 1"))
    assert attempts == {"succeeded": 3}
    assert usage == {"reserved": 3, "charged": 3}
    assert verify_workspace(ws)["verified"] is True


def test_the_watermark_advances_monotonically_across_daily_collections(ws: Workspace) -> None:
    fake = FakeFred(date(2026, 9, 9))
    _history(fake)
    clock = FakeClock(FRED_NOW)
    first = _fred_run(ws, fake, clock)
    assert first["stopped"] is None
    (day1,) = _sources(first, "fred-alfred-observations-")
    assert set(_rows(ws, str(day1["source_id"]), "observations",
                     "series_id, observation_date, realtime_start, value")) == (
        _expected_periods(fake, "DGS10", date(2026, 9, 9))
        | _expected_periods(fake, "GDP", date(2026, 9, 9))
    )  # fmt: skip
    head = _promote_alfred(ws, _pin(day1), None)
    marks = [_watermarks(ws, "macro.us.alfred")]
    known = [fred_collection.load_known(ws).knowledge.vintages]
    assert known[0] == {"DGS10": date(2026, 9, 1), "GDP": date(2026, 8, 28)}

    # Day 2: one new DGS10 vintage (a new observation and a revision); GDP is unchanged.
    # FRED dated it 2026-09-09, the day the first run saw as not yet ended.
    fake.today = date(2026, 9, 10)
    fake.publish("DGS10", date(2026, 9, 8), "4.20", date(2026, 9, 9))
    fake.publish("DGS10", date(2026, 8, 20), "4.55", date(2026, 9, 9))
    clock.advance(days=1)
    calls = len(fake.calls)
    second = _fred_run(ws, fake, clock)
    asked = [name for name, _ in fake.calls[calls:]]
    # A vintage check per series, observations only for the series that has a new vintage.
    assert asked == ["fredgraph.csv", "vintagedates", "observations", "vintagedates"]
    windows = fake.asked("observations")[-1]
    assert (windows["realtime_start"], windows["realtime_end"]) == ("2026-09-01", "2026-09-09")
    (day2,) = _sources(second, "fred-alfred-observations-")
    assert set(_rows(ws, str(day2["source_id"]), "observations",
                     "series_id, observation_date, realtime_start, value")) == {
        ("DGS10", date(2026, 9, 8), date(2026, 9, 9), "4.20"),
        ("DGS10", date(2026, 8, 20), date(2026, 9, 9), "4.55"),
    }  # fmt: skip
    head = _promote_alfred(ws, _pin(day2), head)
    marks.append(_watermarks(ws, "macro.us.alfred"))
    known.append(fred_collection.load_known(ws).knowledge.vintages)

    # Day 3: nothing new. The checks find no vintage and the promotion no delta.
    fake.today = date(2026, 9, 11)
    clock.advance(days=1)
    third = _fred_run(ws, fake, clock)
    assert _sources(third, "fred-alfred-observations-") == []
    assert [name for name, _ in fake.calls[-3:]] == ["fredgraph.csv", "vintagedates",
                                                     "vintagedates"]  # fmt: skip
    again = promote(ws, *_spec(
        "macro_observations", "macro.us.alfred", [_pin(day2)], "fred.alfred@1", {},
        _rule("local_day_end@1", "revision", "vintage_start", CHICAGO),
        decimal={"value": "decimal_text@1"}, parent=head,
        partition=(date(2026, 9, 9), date(2026, 9, 10)),
    ), apply=True)  # fmt: skip
    assert again["delta_rows"] == 0
    marks.append(_watermarks(ws, "macro.us.alfred"))
    known.append(fred_collection.load_known(ws).knowledge.vintages)

    # The promoted watermark only moves forward: no partition's mark ever falls, and the
    # latest mark advances exactly when a collection brought a later vintage.
    for before, after in itertools.pairwise(marks):
        assert set(before) <= set(after)
        assert all(after[partition] >= before[partition] for partition in before)
    latest = [max(mark.values()) for mark in marks]
    assert latest[0] < latest[1] == latest[2]
    # So does the collector's own mark: each series' latest collected vintage day.
    assert known[1] == {"DGS10": date(2026, 9, 9), "GDP": date(2026, 8, 28)}
    assert known[2] == known[1]
    stored = ws.market.execute(
        "SELECT value, op FROM macro_observations WHERE series_id='DGS10' AND "
        "observation_period=DATE '2026-08-20' ORDER BY source_vintage_start"
    ).fetchall()
    assert stored == [(Decimal("4.10"), "ASSERT"), (Decimal("4.55"), "SUPERSEDE")]
    assert verify_workspace(ws)["verified"] is True


def test_an_incomplete_window_is_asked_again_from_the_same_known_day(ws: Workspace) -> None:
    fake = FakeFred(date(2026, 9, 9))
    _history(fake)
    clock = FakeClock(FRED_NOW)
    _fred_run(ws, fake, clock)
    fake.today = date(2026, 9, 10)
    fake.publish("DGS10", date(2026, 9, 8), "4.20", date(2026, 9, 9))
    fake.fail = {"observations"}
    clock.advance(days=1)
    failed = _fred_run(ws, fake, clock)
    assert failed["uncertain"] == 1
    assert failed["incomplete_queries"] == {"DGS10": 1}
    assert _sources(failed, "fred-alfred-observations-") == []
    assert fred_collection.load_known(ws).knowledge.vintages["DGS10"] == date(2026, 9, 1)
    fake.fail = set()
    clock.advance(hours=1)
    retried = _fred_run(ws, fake, clock)
    assert fake.asked("observations")[-1]["realtime_start"] == "2026-09-01"
    (rows,) = _sources(retried, "fred-alfred-observations-")
    assert rows["rows"] == 1
    attempts = dict(ws.state.execute("SELECT status,count(*) FROM collection_attempts GROUP BY 1"))
    assert attempts["uncertain"] == 1


def test_a_crashed_run_is_settled_and_its_receipts_committed_by_the_next(ws: Workspace) -> None:
    fake = FakeFred(date(2026, 9, 9))
    _history(fake)
    fake.crash_after = 3  # csv, DGS10 vintage dates and its window; then the process dies
    clock = FakeClock(FRED_NOW)
    with pytest.raises(KeyboardInterrupt):
        _fred_run(ws, fake, clock)
    statuses = dict(ws.state.execute("SELECT status,count(*) FROM collection_attempts GROUP BY 1"))
    assert statuses == {"succeeded": 3, "started": 1}
    fake.crash_after = None
    result = _fred_run(ws, fake, clock)
    assert result["recovered_attempts"] == {"released": 0, "uncertain": 1}
    assert result["recovered_receipts"] == 3
    # The recovered DGS10 window was complete, so DGS10 only needs a vintage check now; the
    # CSV was downloaded this FRED day; GDP is asked from the origin.
    assert [name for name, _ in fake.calls[3:]] == ["vintagedates", "vintagedates",
                                                    "observations"]  # fmt: skip
    assert fake.asked("vintagedates")[-2]["realtime_start"] == "2026-09-02"
    assert fake.asked("vintagedates")[-1]["series_id"] == "GDP"
    vintages = fred_collection.load_known(ws).knowledge.vintages
    assert vintages == {"DGS10": date(2026, 9, 1), "GDP": date(2026, 8, 28)}


def test_a_refused_key_stops_the_run_and_keeps_the_answer(ws: Workspace) -> None:
    fake = FakeFred(date(2026, 9, 9))
    _history(fake)
    refusal = b'{"error_code":400,"error_message":"Bad Request.  The value for variable api_key'
    fake.answers["vintagedates"] = HttpAnswer(400, (), refusal + b' is not registered."}')
    result = _fred_run(ws, fake, FakeClock(FRED_NOW))
    assert result["stopped"] == "provider_refused:400"
    assert [name for name, _ in fake.calls] == ["fredgraph.csv", "vintagedates"]
    receipts = _sources(result, "fred-collect-receipts-")
    assert receipts[0]["rows"] == 2


def test_a_legacy_alfred_table_sets_the_known_vintage_day(ws: Workspace) -> None:
    # A legacy normalization: extra columns, and a vintage of the day it was retrieved on.
    schema = pa.schema([
        ("series_id", pa.string()), ("observation_date", pa.date32()),
        ("realtime_start", pa.date32()), ("value", pa.string()), ("provider", pa.string()),
        ("retrieved_at_utc", pa.timestamp("us", tz="UTC")),
    ])  # fmt: skip
    retrieved = datetime(2026, 9, 6, 2, 38, tzinfo=UTC)  # 2026-09-05 in St. Louis
    rows = [
        ("DGS10", date(2026, 9, 3), date(2026, 9, 4), "4.1", "fred_alfred", retrieved),
        ("DGS10", date(2026, 9, 4), date(2026, 9, 5), "4.2", "fred_alfred", retrieved),
    ]
    _, digest, size = put_raw(ws.paths.raw, b"synthetic legacy alfred")
    table = pa.table({name: [row[i] for row in rows] for i, name in enumerate(schema.names)},
                     schema=schema)  # fmt: skip
    source_library.import_content_arrow(
        ws, SourceContent("market", "legacy-alfred", 1, (SourceFile(digest, size),)),
        "observations", table.to_reader(),
    )  # fmt: skip
    plan = fred_collection.plan_fred(ws, today=date(2026, 9, 10), policy=POLICY)
    assert plan["provider_calls"] == 0
    series = cast("dict[str, dict[str, object]]", plan["series"])
    # The vintage of the retrieval day may have been half published: the day before counts.
    assert series["DGS10"]["known_vintage"] == "2026-09-04"
    assert series["GDP"]["known_vintage"] is None
    assert plan["requests"] == {"series_csv:daily": 1, "vintage_dates:origin": 1,
                                "vintage_dates:vintage_check": 1}  # fmt: skip


def test_the_csv_download_is_the_source_the_legacy_import_makes(
    tmp_path: Path, ws: Workspace
) -> None:
    fake = FakeFred(date(2026, 9, 9))
    _history(fake)
    result = _fred_run(ws, fake, FakeClock(FRED_NOW))
    (csv,) = _sources(result, "fred-series-csv-")
    payload = fake._csv("DEXKOUS").body  # noqa: SLF001 -- the same bytes the run downloaded
    digest = hashlib.sha256(payload).hexdigest()
    identity = ["aas-source-id-v1", 1, [[f"{digest[:2]}/{digest}", len(payload), digest]]]
    canonical = json.dumps(identity, separators=(",", ":"), sort_keys=True).encode()
    assert csv["source_id"] == "fred-series-csv-" + hashlib.sha256(canonical).hexdigest()
    legacy = _private(tmp_path / "legacy" / "DEXKOUS.csv", payload)
    entries = [{"name": "fred", "loader": "fred.series_csv@1", "path": str(legacy), "args": {},
                "expect": {}}]  # fmt: skip
    raw = json.dumps({"schema_version": "aas-legacy-import-v1", "entries": entries}).encode()
    report = apply_import(ws, parse_manifest(raw, hashlib.sha256(raw).hexdigest()))
    imported = cast("list[dict[str, object]]", report["entries"])[0]
    (source,) = cast("list[dict[str, object]]", imported["sources"])
    assert source["source_id"] == csv["source_id"]
    assert source["digest"] == csv["digest"]
    document = _spec(
        "fx_rates", "fx.usdkrw.fred", [_pin(csv)], "fred.fx_series@1",
        {"series": "DEXKOUS", "base": "USD", "quote": "KRW", "timezone": "America/New_York"},
        _rule("unknown_null@1", "record", None, None), decimal={"rate": "decimal_text@1"},
    )  # fmt: skip
    promoted = promote(ws, *document, apply=True)
    assert promoted["operations"] == {"ASSERT": 20}
    states = dict(ws.market.execute("SELECT value_state, count(*) FROM fx_rates GROUP BY 1")
                  .fetchall())  # fmt: skip
    assert states == {"present": 19, "missing": 1}
    # The next run the same FRED day downloads nothing again.
    again = _fred_run(ws, fake, FakeClock(FRED_NOW + timedelta(hours=1)))
    assert "fredgraph.csv" not in [name for name, _ in fake.calls[-2:]]
    assert again["asked"] == {"vintage_dates:vintage_check": 2}


def _interrupt_receipts(monkeypatch: pytest.MonkeyPatch, provider: str) -> list[str]:
    """Make the first commit of a receipts source die after its batch's derived commits."""
    original = provider_collection.commit
    committed: list[str] = []

    def commit(*args: object, **kwargs: object) -> dict[str, object]:
        content = cast("SourceContent", args[1])
        if content.source_id.startswith(f"{provider}-collect-receipts-") and committed:
            monkeypatch.setattr(provider_collection, "commit", original)
            raise KeyboardInterrupt  # the process dies before the completion marker
        result = original(*args, **kwargs)  # ty: ignore[invalid-argument-type]
        committed.append(content.source_id)
        return result

    monkeypatch.setattr(provider_collection, "commit", commit)
    return committed


def test_a_batch_whose_receipts_did_not_commit_is_derived_again_whole(
    ws: Workspace, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = FakeFred(date(2026, 9, 9))
    _history(fake)
    clock = FakeClock(FRED_NOW)
    derived = _interrupt_receipts(monkeypatch, "fred")
    with pytest.raises(KeyboardInterrupt):
        _fred_run(ws, fake, clock)
    # The run's one batch (the CSV and both series' vintage dates and windows) committed
    # its observations and CSV sources, then died before its receipts.
    assert len(fake.calls) == 5
    assert sorted(item.rsplit("-", 1)[0] for item in derived) == [
        "fred-alfred-observations", "fred-series-csv",
    ]  # fmt: skip
    assert fred_collection.load_known(ws).knowledge.vintages == {}
    result = _fred_run(ws, fake, clock)
    assert result["recovered_receipts"] == 5
    recovered = cast("list[dict[str, object]]", result["sources"])[:3]
    assert {str(item["source_id"]) for item in recovered[:2]} == set(derived)
    assert all(item["reused"] for item in recovered[:2])
    assert str(recovered[2]["source_id"]).startswith("fred-collect-receipts-")
    # Nothing is lost: the CSV day and both series' vintages are known, so the run asks
    # only the vintage checks.
    assert [name for name, _ in fake.calls[5:]] == ["vintagedates", "vintagedates"]
    known = fred_collection.load_known(ws).knowledge
    assert known.vintages == {"DGS10": date(2026, 9, 1), "GDP": date(2026, 8, 28)}
    assert known.csv_days == {"DEXKOUS": date(2026, 9, 9)}
    assert verify_workspace(ws)["verified"] is True


def test_orphans_are_committed_in_batches_of_the_run_bounds(ws: Workspace) -> None:
    fake = FakeFred(date(2026, 9, 9))
    _history(fake)
    fake.crash_after = 4  # csv, DGS10's vintage dates and window, GDP's vintage dates
    clock = FakeClock(FRED_NOW)
    with pytest.raises(KeyboardInterrupt):
        _fred_run(ws, fake, clock)
    fake.crash_after = None
    result = _fred_run(ws, fake, clock, batch_size=3)
    assert result["recovered_receipts"] == 4
    receipts = _sources(result, "fred-collect-receipts-")
    assert [item["rows"] for item in receipts][:2] == [3, 1]
    vintages = fred_collection.load_known(ws).knowledge.vintages
    assert vintages == {"DGS10": date(2026, 9, 1), "GDP": date(2026, 8, 28)}


def test_a_recovered_batch_closes_only_between_queries() -> None:
    moment = datetime(2026, 9, 10, 3, tzinfo=UTC)

    def retained(request: fred.Request, size: int) -> provider_collection.Retained:
        response = Response(200, (), b"x" * size, moment, moment)
        receipt = provider_collection.receipt_bytes(
            request, response, outcome="COMPLETED", provider_status=None,
            attempt=Attempt("job", 1),
        )  # fmt: skip
        return provider_collection.Retained("fred", receipt, response.body)

    first, last = date(2026, 9, 1), date(2026, 9, 9)
    items = [
        retained(fred.vintage_dates("DGS10", first, last), 1),
        retained(fred.observations("DGS10", first, last), 1),
        retained(fred.observations("DGS10", first, last, fred.PAGE_LIMIT), 1),
        retained(fred.observations("DGS10", first, last, 2 * fred.PAGE_LIMIT), 1),
        retained(fred.vintage_dates("GDP", first, last), 50),
        retained(fred.series_csv("DEXKOUS"), 1),
    ]
    joins = fred_collection._continues  # noqa: SLF001 -- the rule recovery applies
    sizes = [len(chunk) for chunk in provider_collection.chunks(items, 1, 1_000, joins)]
    # The query's later pages stay with its first page past the count bound.
    assert sizes == [1, 3, 1, 1]
    assert [len(chunk) for chunk in provider_collection.chunks(items, 1, 1_000)] == [1] * 6
    # The byte bound closes a batch too.
    sizes = [len(chunk) for chunk in provider_collection.chunks(items, 100, 50, joins)]
    assert sizes == [5, 1]


def test_three_transport_failures_in_a_row_stop_the_run(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "aas"
    initialize(root)
    fake = FakeFred(date(2026, 9, 9))
    _history(fake)
    fake.fail = {"fredgraph.csv", "vintagedates"}
    monkeypatch.setattr("aegis_alpha.data.opendart.urllib_transport", lambda **_: fake)
    key = tmp_path / "fred-key"
    key.write_text(FRED_KEY + "\n")
    os.chmod(key, 0o600)  # noqa: PTH101 -- explicit owner-only secret mode
    code = main(["collect", "fred", "run", "--home", str(root), "--key-file", str(key)])
    run = json.loads(capsys.readouterr().out)
    assert (run["stopped"], run["exit_code"], run["uncertain"]) == ("transport_failures", 1, 3)
    assert code == 1
    # The CSV and the first two series failed; no later series was asked.
    assert [name for name, _ in fake.calls] == ["fredgraph.csv", "vintagedates", "vintagedates"]


def test_a_series_stops_at_its_first_incomplete_window_and_resumes_there(
    ws: Workspace,
) -> None:
    fake = FakeFred(date(2026, 9, 9))
    days = [date(2019, 1, 1) + timedelta(days=offset) for offset in range(2500)]
    for offset, day in enumerate(days):
        fake.publish("DGS10", date(2019, 1, 1) + timedelta(days=offset % 40), str(offset), day)
    policy = fred.FredPolicy(alfred_series=("DGS10",), csv_series={})
    clock = FakeClock(FRED_NOW)
    client = fred.FredClient(FRED_KEY, fake, clock)
    boundary = days[1989]  # the first window's last vintage date
    fake.fail_starts = {boundary.isoformat()}
    first = fred_collection.collect_fred(ws, client, policy=policy, clock=clock,
                                         sleep=lambda _: None)  # fmt: skip
    assert first["incomplete_queries"] == {"DGS10": 1}
    (window1,) = _sources(first, "fred-alfred-observations-")
    rows = set(_rows(ws, str(window1["source_id"]), "observations",
                     "series_id, observation_date, realtime_start, value"))  # fmt: skip
    assert rows == _expected_periods(fake, "DGS10", boundary)
    assert fred_collection.load_known(ws).knowledge.vintages == {"DGS10": boundary}
    fake.fail_starts = set()
    clock.advance(hours=1)
    second = fred_collection.collect_fred(ws, client, policy=policy, clock=clock,
                                          sleep=lambda _: None)  # fmt: skip
    assert (
        fake.asked("vintagedates")[-1]["realtime_start"]
        == (boundary + timedelta(days=1)).isoformat()
    )
    assert fake.asked("observations")[-1]["realtime_start"] == boundary.isoformat()
    (window2,) = _sources(second, "fred-alfred-observations-")
    later = set(_rows(ws, str(window2["source_id"]), "observations",
                      "series_id, observation_date, realtime_start, value"))  # fmt: skip
    assert rows.isdisjoint(later)
    assert rows | later == _expected_periods(fake, "DGS10", date(2026, 9, 9))
    assert fred_collection.load_known(ws).knowledge.vintages == {"DGS10": days[-1]}


def test_receipts_and_batches_have_fixed_canonical_bytes() -> None:
    request = fred.series_csv("DEXKOUS")
    moment = datetime(2026, 9, 10, 3, tzinfo=UTC)
    body = b"observation_date,DEXKOUS\n"
    response = Response(200, (("content-type", "text/csv"),), body, moment, moment)
    receipt = provider_collection.receipt_bytes(
        request, response, outcome="COMPLETED", provider_status=None,
        attempt=Attempt("job-1", 1),
    )  # fmt: skip
    raw = hashlib.sha256(body).hexdigest()
    spelled = {
        "attempt": 1,
        "fingerprint": request.fingerprint,
        "headers": [["content-type", "text/csv"]],
        "http_status": 200,
        "job_id": "job-1",
        "outcome": "COMPLETED",
        "provider_status": None,
        "raw": {"sha256": raw, "size": len(body)},
        "request": {"endpoint": "series_csv", "parameters_json": '{"id":"DEXKOUS"}'},
        "requested_at_utc": "2026-09-10T03:00:00.000000Z",
        "retrieved_at_utc": "2026-09-10T03:00:00.000000Z",
        "schema_version": "aas-fred-receipt-v1",
        "selection": None,
    }
    assert receipt == json.dumps(spelled, sort_keys=True, separators=(",", ":")).encode()
    digest = hashlib.sha256(receipt).hexdigest()
    assert digest == "40524eb1dfd01e17127fddbbe1dc1da09de67db02b77807f05764b471aba5600"
    batch = provider_collection.Batch.of(
        "fred", [provider_collection.Retained("fred", receipt, body)]
    )
    manifest = {"receipts": [{"sha256": digest, "size": len(receipt)}],
                "schema_version": "aas-fred-batch-v1"}  # fmt: skip
    assert batch.manifest == json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    # The batch's files, by their raw/ path: the manifest, each receipt, its response.
    named = sorted((hashlib.sha256(f).hexdigest(), len(f)) for f in (batch.manifest, receipt, body))
    identity = ["aas-source-id-v1", 1, [[f"{h[:2]}/{h}", size, h] for h, size in named]]
    spelled_id = hashlib.sha256(
        json.dumps(identity, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()
    source_id = batch.content("collect-receipts").source_id
    assert source_id == f"fred-collect-receipts-{spelled_id}"
    assert source_id == (
        "fred-collect-receipts-2cf577283b96aa0eb1224f3a9d8ff0762668ac3ea7b7549b14196030260c49ff"
    )


# --- SEC --------------------------------------------------------------------------------------

CIK, OTHER = "0000000101", "0000000202"
MON, TUE, WED = date(2026, 9, 14), date(2026, 9, 15), date(2026, 9, 16)
OLD = SecFiling(CIK, "0000000101-26-000001", "10-K", date(2026, 3, 2), "2026-03-02T21:00:01.000Z")
Q2 = SecFiling(CIK, "0000000101-26-000002", "10-Q", MON, "2026-09-14T20:15:30.000Z", "2026-06-30")
EVENT = SecFiling(CIK, "0000000101-26-000003", "8-K", TUE, "2026-09-15T12:00:00.000Z")
FOREIGN = SecFiling(OTHER, "0000000202-26-000001", "6-K", TUE, "2026-09-15T11:00:00.000Z")
INSIDER = SecFiling(OTHER, "0000000202-26-000002", "4", TUE, "2026-09-15T22:00:00.000Z")
# Thursday 2026-09-17, 06:00 in New York.
SEC_NOW = datetime(2026, 9, 17, 10, tzinfo=UTC)


def _sec_fake() -> FakeSec:
    return FakeSec(
        filings=[OLD, Q2, EVENT, FOREIGN, INSIDER],
        facts={
            CIK: [
                SecFact(OLD.accession, "Assets", "900", date(2025, 12, 31), OLD.filed, "10-K"),
                SecFact(Q2.accession, "Assets", "1000.50", date(2026, 6, 30), MON),
                SecFact(Q2.accession, "Revenues", "250", date(2026, 6, 30), MON,
                        start=date(2026, 4, 1)),
            ]
        },
        holidays={WED},
    )  # fmt: skip


def _sec_run(
    ws: Workspace, fake: FakeSec, clock: FakeClock, policy: sec.SecPolicy | None = None,
    **bounds: object,
) -> dict[str, object]:  # fmt: skip
    client = sec.SecClient(USER_AGENT, fake, clock)
    return sec_collection.collect_sec(
        ws, client, policy=policy or sec.SecPolicy(issuers="all", lookback_days=3),
        clock=clock, sleep=lambda _: None, **bounds,  # ty: ignore[invalid-argument-type]
    )  # fmt: skip


def _sec_spec(pin: dict[str, str], *, facts: dict[str, str] | None = None) -> tuple[bytes, str]:
    rule = {"rule": "source_column@1", "basis": "revision", "input": "accepted_at", "args": {}}
    document = {
        "schema_version": "aas-promotion-v1",
        "target": {"domain": "fundamentals" if facts else "filings",
                   "dataset_id": "fundamentals.us.sec" if facts else "filings.us.sec",
                   "parent": None},
        "sources": [pin],
        "mapper": {"name": "sec.companyfacts@1" if facts else "sec.submissions@1",
                   "args": {"filings": facts} if facts else {}},
        "partition": None,
        "time_rules": {"available_at_us": rule, "revision_known_at_us": rule},
        "decimal_rule": {"value": "decimal_text@1"} if facts else {},
        "quality_rules": [],
        "tombstone_policy": {"mode": "never"},
        "identity_snapshot": None,
    }  # fmt: skip
    raw = json.dumps(document, sort_keys=True).encode()
    return raw, hashlib.sha256(raw).hexdigest()


def test_a_run_reads_indexes_then_documents_and_the_sec_mappers_promote_them(
    ws: Workspace,
) -> None:
    fake = _sec_fake()
    result = _sec_run(ws, fake, FakeClock(SEC_NOW))
    # Indexes for the weekdays from the lookback day, then one document per filer.
    assert fake.calls == [
        ("daily_index", "2026-09-14"), ("daily_index", "2026-09-15"),
        ("daily_index", "2026-09-16"),
        ("submissions", CIK), ("submissions", OTHER), ("companyfacts", CIK),
    ]  # fmt: skip
    assert result["outcomes"] == {"COMPLETED": 5, "NO_DATA": 1}
    # The holiday's 404 was read the next morning, so that day is not covered yet.
    assert result["covered_through"] == "2026-09-15"
    (filings,) = _sources(result, "sec-submissions-filings-")
    rows = _rows(ws, str(filings["source_id"]), "filings",
                 '"accessionNumber", "acceptanceDateTime", form')  # fmt: skip
    # Only the wanted filings: the 10-K the document restates and the Form 4 are not rows.
    assert rows == [
        (Q2.accession, Q2.accepted, "10-Q"),
        (EVENT.accession, EVENT.accepted, "8-K"),
        (FOREIGN.accession, FOREIGN.accepted, "6-K"),
    ]
    (facts,) = _sources(result, "sec-companyfacts-facts-")
    assert _rows(ws, str(facts["source_id"]), "facts", "accession_number, tag, value") == [
        (Q2.accession, "Assets", "1000.50"),
        (Q2.accession, "Revenues", "250"),
    ]
    (entries,) = _sources(result, "sec-daily-index-entries-")
    assert entries["rows"] == 4
    filed = promote(ws, *_sec_spec(_pin(filings)), apply=True)
    assert filed["operations"] == {"ASSERT": 3}
    generation = ws.state.execute(
        "SELECT dataset_id, version, generation_id, chain_hash, manifest_hash "
        "FROM dataset_versions WHERE generation_id=?",
        (str(filed["generation_id"]),),
    ).fetchone()
    names = ("dataset_id", "version", "generation_id", "chain_hash", "manifest_hash")
    filings_pin = dict(zip(names, (str(value) for value in generation), strict=True))
    funded = promote(ws, *_sec_spec(_pin(facts), facts=filings_pin), apply=True)
    assert funded["operations"] == {"ASSERT": 2}
    accepted = int(datetime(2026, 9, 14, 20, 15, 30, tzinfo=UTC).timestamp() * 1_000_000)
    assert ws.market.execute(
        "SELECT DISTINCT issuer_id, accepted_at_us, available_at_us FROM fundamentals"
    ).fetchall() == [(mint_issuer("sec_cik", CIK), accepted, accepted)]
    assert set(_watermarks(ws, "filings.us.sec")) == {"all"}
    assert set(_watermarks(ws, "fundamentals.us.sec")) == {"all"}
    assert verify_workspace(ws)["verified"] is True


def test_the_next_run_asks_only_what_is_missing(ws: Workspace) -> None:
    fake = _sec_fake()
    fake.unlisted = {EVENT.accession}  # EDGAR lists the 8-K in its filer's document late
    clock = FakeClock(SEC_NOW)
    _sec_run(ws, fake, clock)
    plan = sec_collection.plan_sec(ws, now=clock.now, policy=sec.SecPolicy(issuers="all"))
    assert plan["provider_calls"] == 0
    assert plan["requests"] == {"daily_index:index_gap": 1}  # the holiday, read too early
    fake.unlisted = set()
    clock.advance(days=1)  # Friday
    calls = len(fake.calls)
    second = _sec_run(ws, fake, clock)
    # The holiday's 404 is now final, Thursday's index is new, and only the missing filing's
    # document is asked again; companyfacts of the 10-Q already has its facts.
    assert fake.calls[calls:] == [
        ("daily_index", "2026-09-16"), ("daily_index", "2026-09-17"), ("submissions", CIK),
    ]  # fmt: skip
    (filings,) = _sources(second, "sec-submissions-filings-")
    assert _rows(ws, str(filings["source_id"]), "filings", '"accessionNumber"') == [
        (EVENT.accession,)
    ]
    assert second["covered_through"] == "2026-09-17"
    assert (
        sec_collection.plan_sec(ws, now=clock.now, policy=sec.SecPolicy(issuers="all"))["requests"]
        == {}
    )


def test_an_sec_batch_whose_receipts_did_not_commit_loses_no_filing(
    ws: Workspace, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = _sec_fake()
    clock = FakeClock(SEC_NOW)
    derived = _interrupt_receipts(monkeypatch, "sec")
    with pytest.raises(KeyboardInterrupt):
        _sec_run(ws, fake, clock, batch_size=3)
    # The three indexes' entries committed; their receipts did not, so no day is covered.
    assert [item.rsplit("-", 1)[0] for item in derived] == ["sec-daily-index-entries"]
    assert sec_collection.load_known(ws).knowledge.days == {}
    calls = len(fake.calls)
    result = _sec_run(ws, fake, clock, batch_size=3)
    assert result["recovered_receipts"] == 3
    entries = cast("list[dict[str, object]]", result["sources"])[0]
    assert (entries["source_id"], entries["reused"]) == (derived[0], True)
    # The indexes are known again, so the run asks the holiday again (read too early)
    # and every document the indexes name.
    assert fake.calls[calls:] == [
        ("daily_index", "2026-09-16"),
        ("submissions", CIK), ("submissions", OTHER), ("companyfacts", CIK),
    ]  # fmt: skip
    known = sec_collection.load_known(ws).knowledge
    assert known.listed == {Q2.accession, EVENT.accession, FOREIGN.accession}
    assert known.reported == {Q2.accession}
    assert verify_workspace(ws)["verified"] is True


def test_an_uncertain_document_ask_is_asked_again_the_next_day(ws: Workspace) -> None:
    fake = _sec_fake()
    fake.fail = {"submissions"}
    clock = FakeClock(SEC_NOW)
    first = _sec_run(ws, fake, clock)
    assert first["uncertain"] == 2
    assert first["stopped"] is None  # the companyfacts answer between them reset the count
    fake.fail = set()
    clock.advance(hours=2)
    calls = len(fake.calls)
    _sec_run(ws, fake, clock)
    # An ask without an answer counts as asked: not again the same day.
    assert [call for call in fake.calls[calls:] if call[0] == "submissions"] == []
    clock.advance(days=1)
    calls = len(fake.calls)
    _sec_run(ws, fake, clock)
    assert [call for call in fake.calls[calls:] if call[0] == "submissions"] == [
        ("submissions", CIK), ("submissions", OTHER),
    ]  # fmt: skip
    assert sec_collection.load_known(ws).knowledge.listed == {
        Q2.accession, EVENT.accession, FOREIGN.accession,
    }  # fmt: skip


def test_registered_issuers_limit_the_documents(ws: Workspace) -> None:
    ws.state.execute(
        "INSERT INTO issuers(issuer_id, name) VALUES (?, 'synthetic')",
        (mint_issuer("sec_cik", OTHER),),
    )
    ws.state.commit()
    fake = _sec_fake()
    result = _sec_run(ws, fake, FakeClock(SEC_NOW), sec.SecPolicy(lookback_days=3))
    assert [call for call in fake.calls if call[0] != "daily_index"] == [("submissions", OTHER)]
    pending = cast("dict[str, dict[str, dict[str, int]]]", result["pending"])
    assert pending["filings"]["submissions"]["outside_universe"] == 2


def test_a_refused_rate_stops_the_run_and_the_contact_is_never_retained(ws: Workspace) -> None:
    fake = _sec_fake()
    fake.refuse = 403
    result = _sec_run(ws, fake, FakeClock(SEC_NOW))
    assert result["stopped"] == "provider_refused:403"
    assert result["provider_calls"] == 1
    (receipts,) = _sources(result, "sec-collect-receipts-")
    assert receipts["rows"] == 1
    contact = USER_AGENT.encode()
    for path in ws.paths.raw.rglob("*"):
        if path.is_file():
            assert contact not in path.read_bytes()


def test_a_submissions_answer_whose_rows_do_not_read_is_failed() -> None:
    request = sec.submissions(CIK)
    moment = datetime(2026, 9, 17, tzinfo=UTC)
    readable = submissions_bytes(CIK, [Q2])
    answer = sec.Response(200, (), readable, moment, moment)
    assert sec_collection.classify(request, answer) == ("COMPLETED", None)
    document = json.loads(readable)
    document["filings"]["recent"]["unknownArray"] = ["x"]
    broken = sec.Response(200, (), json.dumps(document).encode(), moment, moment)
    outcome, reason = sec_collection.classify(request, broken)
    assert outcome == "FAILED"
    assert reason is not None
    assert "unknown array" in reason


def test_the_commands_plan_without_calls_and_run_through_the_cli(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "aas"
    initialize(root)
    assert main(["collect", "fred", "plan", "--home", str(root), "--today", "2026-09-10"]) == 0
    plan = json.loads(capsys.readouterr().out)
    assert (plan["provider_calls"], plan["realtime_end"]) == (0, "2026-09-09")
    assert plan["requests"] == {"series_csv:daily": 1, "vintage_dates:origin": 36}
    assert main(["collect", "sec", "plan", "--home", str(root), "--today", "2026-09-17",
                 "--since", "2026-09-14"]) == 0  # fmt: skip
    plan = json.loads(capsys.readouterr().out)
    assert (plan["provider_calls"], plan["index_days"]) == (0, 3)
    fake = FakeFred(date(2026, 9, 9))
    _history(fake)
    monkeypatch.setattr("aegis_alpha.data.opendart.urllib_transport", lambda **_: fake)
    key = tmp_path / "fred-key"
    key.write_text(FRED_KEY + "\n")
    os.chmod(key, 0o600)  # noqa: PTH101 -- explicit owner-only secret mode
    code = main(["collect", "fred", "run", "--home", str(root), "--key-file", str(key),
                 "--max-calls", "2"])  # fmt: skip
    run = json.loads(capsys.readouterr().out)
    assert (code, run["provider_calls"], run["stopped"], run["exit_code"]) == (0, 2, "budget", 0)
    sec_fake = _sec_fake()
    monkeypatch.setattr("aegis_alpha.data.opendart.urllib_transport", lambda **_: sec_fake)
    agent = tmp_path / "sec-agent"
    agent.write_text(USER_AGENT + "\n")
    os.chmod(agent, 0o600)  # noqa: PTH101 -- explicit owner-only secret mode
    code = main(["collect", "sec", "run", "--home", str(root), "--user-agent-file", str(agent),
                 "--issuers", "all", "--since", "2026-09-14", "--max-calls", "3"])  # fmt: skip
    run = json.loads(capsys.readouterr().out)
    assert (code, run["provider_calls"], run["exit_code"]) == (0, 3, 0)
    assert OPEN.year == 9999
