"""Completed Qveris jobs commit as content-addressed sources, one unit per job."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import cast

import pytest

from aegis_alpha.application.cli import main
from aegis_alpha.data.qveris_acquisition import acquire_jobs
from aegis_alpha.data.qveris_native import read_completed_job
from aegis_alpha.storage import kr_prices
from aegis_alpha.storage.qveris_import import (
    Completion,
    build_unit,
    completions,
    import_completions,
    import_unit,
    parse_identity,
)
from aegis_alpha.storage.source_library import list_sources, list_tables
from aegis_alpha.storage.workspace import Workspace, initialize, open_workspace
from tests.data.qveris_support import (
    KR_IDENTITY,
    ScriptedQveris,
    account_key,
    bar,
    bulk_job,
    complete,
    fx_job,
    history_job,
    identity_bytes,
)
from tests.data.test_qveris_acquisition import fred_job

# Serial: these tests take the host-wide Qveris account lease (an abstract Unix socket named by
# the account), so every file that takes it runs in one xdist worker.
pytestmark = pytest.mark.xdist_group("qveris-account-lease")

BARS = [
    "instrument_id",
    "venue",
    "instrument_type",
    "currency",
    "provider_symbol",
    "date",
    "open",
    "high",
    "low",
    "close",
    "adjusted_close",
    "volume",
    "source_fingerprint",
    "raw_sha256",
    "retrieved_at",
    "source_row",
    "calendar_verified",
    "independent_identity_verified",
]


@pytest.fixture
def ws(tmp_path: Path) -> Iterator[Workspace]:
    root = tmp_path / "aas"
    initialize(root)
    with open_workspace(root, writable=True, strategy_write=True) as workspace:
        yield workspace


@pytest.fixture
def raw(tmp_path: Path) -> Path:
    root = tmp_path / "qveris"
    complete(root, history_job(), [bar("2026-08-03"), bar("2026-08-04", open=None)])
    return root


def _selected(raw: Path, **filters: frozenset[str]) -> list[Completion]:
    found, missing = completions(raw, **filters)
    assert missing == []
    return found


def _committed(ws: Workspace) -> frozenset[str]:
    return frozenset(str(row["source_id"]) for row in list_sources(ws))


def test_one_job_commits_its_rows_and_held_rows_under_one_content_hex(
    ws: Workspace, raw: Path
) -> None:
    identity = parse_identity(identity_bytes())
    result = import_completions(raw, _selected(raw), identity, workspace=ws)
    assert (result["built"], result["failed"], result["rows"], result["held_rows"]) == (1, 0, 1, 1)
    assert result["provider_calls"] == 0
    (unit,) = cast("list[dict[str, object]]", result["units"])
    rows_id, held_id = str(unit["source_id"]), str(unit["held_source_id"])
    assert rows_id.startswith("qveris-kr-history-bars-")
    assert held_id == rows_id.replace("-bars-", "-quarantine-")
    (bars,) = list_tables(ws, rows_id)
    assert (bars["name"], bars["rows"], bars["columns"]) == ("bars", 1, BARS)
    (held,) = list_tables(ws, held_id)
    assert held["name"] == "quarantine"
    assert held["columns"] == ["source_fingerprint", "ordinal", "reason", "source_row_json"]
    # The KR price backfill finds both tables by its lineage prefix and table name.
    pins = kr_prices._tables(ws, "qveris-kr-history", "bars")  # noqa: SLF001 -- mapper discovery
    assert [pin["source_id"] for pin, _target, _rows in pins] == [rows_id]
    held_pins = kr_prices._tables(ws, "qveris-kr-history", "quarantine")  # noqa: SLF001
    assert [pin["source_id"] for pin, _target, _rows in held_pins] == [held_id]


def test_reimport_reuses_and_a_code_change_keeps_the_id(ws: Workspace, raw: Path) -> None:
    identity = parse_identity(identity_bytes())
    first = import_completions(raw, _selected(raw), identity, workspace=ws)
    committed = _committed(ws)
    again = import_completions(raw, _selected(raw), identity, workspace=ws, committed=committed)
    assert (again["built"], again["reused"]) == (0, 1)
    (completion,) = _selected(raw)
    history = read_completed_job(raw, completion.fingerprint, completion.completion_sha256)
    unit = build_unit(completion, history, identity, code={"normalizer": "another version"})
    report = import_unit(ws, unit)
    assert report["reused"] is True
    assert report["source_id"] == cast("list[dict[str, object]]", first["units"])[0]["source_id"]
    assert _committed(ws) == committed


def test_another_identity_document_is_another_source(ws: Workspace, raw: Path) -> None:
    one = import_completions(raw, _selected(raw), parse_identity(identity_bytes()), workspace=ws)
    renamed = {**KR_IDENTITY, "123456.KO": {**KR_IDENTITY["123456.KO"], "name": "Renamed"}}
    two = import_completions(
        raw, _selected(raw), parse_identity(identity_bytes(renamed)), workspace=ws
    )
    ids = [cast("list[dict[str, object]]", r["units"])[0]["source_id"] for r in (one, two)]
    assert ids[0] != ids[1]
    assert len(_committed(ws)) == 4  # noqa: PLR2004 -- bars and quarantine of each


def test_a_warned_download_commits_an_empty_rows_table_and_its_held_rows(
    ws: Workspace, tmp_path: Path
) -> None:
    root = tmp_path / "bulk"
    row = {"code": "123456", "exchange_short_name": "KO", **bar("2026-08-31")}
    complete(root, bulk_job("KO", "prices"), [row], warning=True)
    result = import_completions(
        root, _selected(root), parse_identity(identity_bytes()), workspace=ws
    )
    assert (result["rows"], result["held_rows"], result["provider_warnings"]) == (0, 1, 1)
    (unit,) = cast("list[dict[str, object]]", result["units"])
    assert str(unit["source_id"]).startswith("qveris-bulk-bars-")
    assert list_tables(ws, str(unit["source_id"]))[0]["rows"] == 0
    (held,) = list_tables(ws, str(unit["held_source_id"]))
    assert held["columns"] == ["ordinal", "reason", "source_row_json"]
    target = held["target"]
    reasons = ws.market.execute(f'SELECT reason FROM "{target}"').fetchall()  # noqa: S608 -- quoted
    assert reasons == [("provider_reported_partial",)]


def test_actions_and_forex_commit_their_own_shapes(ws: Workspace, tmp_path: Path) -> None:
    root = tmp_path / "mixed"
    complete(
        root,
        bulk_job("US", "splits"),
        [{"code": "AAA", "exchange": "US", "date": "2026-08-31", "split": "2/1"}],
    )
    complete(root, bulk_job("US", "dividends"), [])
    complete(root, fx_job(), [bar("2026-08-03")])
    result = import_completions(
        root, _selected(root), parse_identity(identity_bytes()), workspace=ws
    )
    shapes = sorted(
        str(unit["source_id"]).rsplit("-", 1)[0]
        for unit in cast("list[dict[str, object]]", result["units"])
    )
    assert shapes == ["qveris-dividends", "qveris-fx-history-bars", "qveris-splits"]
    assert "held_source_id" not in str(result["units"])
    fx_only = import_completions(
        root,
        _selected(root, markets=frozenset({"FX"})),
        None,
        workspace=ws,
        committed=_committed(ws),
    )
    assert (fx_only["requested"], fx_only["reused"], fx_only["built"]) == (1, 1, 0)


def test_identity_is_required_only_for_rows_that_name_instruments(raw: Path) -> None:
    with pytest.raises(ValueError, match="identity document"):
        import_completions(raw, _selected(raw), None, workspace=None)
    with pytest.raises(ValueError, match="lacks a text identity field"):
        parse_identity(identity_bytes({"123456.KO": {"instrument_id": "x"}}))


def test_unreadable_jobs_are_recorded_and_the_run_continues(ws: Workspace, tmp_path: Path) -> None:
    root = tmp_path / "two"
    broken = complete(root, history_job(), [bar("2026-08-03")])
    complete(root, history_job("AAA.US", "US"), [bar("2026-08-03")])
    page = root / "jobs" / broken / "0000.raw"
    page.write_bytes(page.read_bytes() + b" ")
    acquire_jobs((fred_job(),), root / "macro", ScriptedQveris({}, account_key(root / "macro")))
    found, missing = completions(root, fingerprints=[broken, "0" * 64])
    assert [m["status"] for m in missing] == ["no_completion"]
    result = import_completions(
        root, _selected(root), parse_identity(identity_bytes()), workspace=ws
    )
    assert (result["built"], result["failed"]) == (1, 1)
    (failure,) = cast("list[dict[str, object]]", result["failures"])
    assert failure["fingerprint"] == broken
    unsupported = import_completions(
        root / "macro", _selected(root / "macro"), None, workspace=None
    )
    assert (unsupported["unsupported"], unsupported["built"]) == (1, 0)
    assert len(found) == 1


def test_limit_bounds_new_units_and_plan_writes_nothing(ws: Workspace, tmp_path: Path) -> None:
    root = tmp_path / "many"
    complete(root, history_job("123456.KO", "KR"), [bar("2026-08-03")])
    complete(root, history_job("AAA.US", "US"), [bar("2026-08-03")])
    identity = parse_identity(identity_bytes())
    planned = import_completions(root, _selected(root), identity, workspace=None)
    assert (planned["mode"], planned["built"]) == ("plan", 2)
    assert _committed(ws) == frozenset()
    limited = import_completions(root, _selected(root), identity, workspace=ws, limit=1)
    assert (limited["built"], limited["pending"]) == (1, 1)
    rest = import_completions(
        root, _selected(root), identity, workspace=ws, committed=_committed(ws)
    )
    assert (rest["built"], rest["reused"], rest["pending"]) == (1, 1, 0)


def test_cli_imports_and_verify_passes(
    tmp_path: Path, raw: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    home = tmp_path / "home"
    initialize(home)
    identity = tmp_path / "identity.json"
    identity.write_bytes(identity_bytes())
    base = ["--home", str(home), "collect", "qveris", "import", "--raw-root", str(raw)]
    assert main([*base, "--identity", str(identity), "--plan"]) == 0
    planned = json.loads(capsys.readouterr().out)
    assert (planned["mode"], planned["built"], planned["exit_code"]) == ("plan", 1, 0)
    assert main([*base, "--identity", str(identity), "--market", "KR"]) == 0
    applied = json.loads(capsys.readouterr().out)
    assert (applied["mode"], applied["built"], applied["rows"]) == ("apply", 1, 1)
    assert main([*base, "--identity", str(identity)]) == 0
    assert json.loads(capsys.readouterr().out)["reused"] == 1
    assert main(["--home", str(home), "db", "verify"]) == 0
    verified = json.loads(capsys.readouterr().out)
    assert verified["source_library"]["linked"] == 2  # noqa: PLR2004 -- bars and quarantine
    assert main([*base, "--fingerprint", "f" * 64, "--identity", str(identity)]) == 1
    assert json.loads(capsys.readouterr().out)["missing"][0]["status"] == "no_completion"
