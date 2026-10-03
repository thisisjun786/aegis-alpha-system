"""Synthetic coverage for registering retained source strategy records as definitions."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from typing import TYPE_CHECKING

import pytest

from aegis_alpha.storage import source_library
from aegis_alpha.storage import strategy_registry as registry
from aegis_alpha.storage.publication import quarantine, recover_operations
from aegis_alpha.storage.state import get_operation
from aegis_alpha.storage.verification import verify_workspace
from aegis_alpha.storage.workspace import initialize, open_workspace
from tests.application.test_storage_cli import run_cli

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path

_STRATEGY = registry._STRATEGY_COLUMNS  # noqa: SLF001 -- the source format under test
_ASSET_PATHS = (
    ("/offensive", ("offensive",)),
    ("/defensive_rule/defensive", ("defensive_rule", "defensive")),
    ("/defensive_rule/unallocated", ("defensive_rule", "unallocated")),
    ("/canary/etf_list", ("canary", "etf_list")),
    ("/defensive_rule/abs_compare", ("defensive_rule", "abs_compare")),
    ("/asset_selection_rule/abs_compare", ("asset_selection_rule", "abs_compare")),
)
_KR = "prices.kr.eodhd"
_US = "prices.us.norgate"
_MACRO = "macro.us.alfred"


def _request(**changes: object) -> dict[str, object]:
    request: dict[str, object] = {
        "asset_selection_rule": None,
        "benchmark": "SPY",
        "canary": {"etf_list": ["AAA", "BBB"], "months": 6, "type": "ABS"},
        "cost": 0.001,
        "crash_protection": {
            "crash_protector": [
                {"func": "T10Y3M", "option1": 1.0},
                {"func": "SYNTH", "option1": 2.0},
            ],
            "type": "DEFENSIVE",
        },
        "data_basis": "M",
        "defensive_rule": {"abs_compare": None, "defensive": ["CCC", "DDD"], "unallocated": "CASH"},
        "exchange": "USD",
        "offensive": ["EEE", "FFF"],
        "weight_calculation_rule": {"constant": None, "func": "EQUAL"},
    }
    request.update(changes)
    return request


def _row(strategy_id: str, title: str, request: Mapping[str, object]) -> dict[str, object]:
    text = json.dumps(request, sort_keys=True, separators=(",", ":"))
    return {
        "id": strategy_id,
        "title": title,
        "source_type": "dynamic",
        "country": "US",
        "is_personal": 0,
        "report_path": f"reports/{strategy_id}.json",
        "request_json": text,
        "normalized_json": '{"derived":true}',
        "exact_hash": hashlib.sha256(text.encode()).hexdigest(),
        "rule_hash": "r" * 8,
        "family_hash": "f" * 8,
        "start_date": "2010-01-01",
        "finish_date": "2020-12-31",
        "source_data_basis": "M",
        "quality_status": "structurally_loaded",
    }


def _strategies() -> list[dict[str, object]]:
    kr = _request(
        benchmark="KOSPI",
        canary=None,
        crash_protection=None,
        defensive_rule={"abs_compare": None, "defensive": ["000002"], "unallocated": "CASH"},
        exchange="KRW",
        offensive=["000001", "AAA"],
        weight_calculation_rule={"constant": {"000001": 0.5, "AAA": 0.5}, "func": "CONSTANT"},
    )
    rows = [
        _row("synthetic-us", "Synthetic US ", _request()),
        _row("synthetic-kr", "Synthetic KR", kr),
    ]
    rows[1]["country"] = "KR"
    return rows


def _dependencies(
    rows: list[dict[str, object]],
) -> tuple[list[tuple[object, ...]], list[tuple[object, ...]]]:
    assets: list[tuple[object, ...]] = []
    macros: list[tuple[object, ...]] = []
    for row in rows:
        request = json.loads(str(row["request_json"]))
        for role, keys in _ASSET_PATHS:
            value: object = request
            for key in keys:
                value = value.get(key) if isinstance(value, dict) else None
            values = value if isinstance(value, list) else [] if value is None else [value]
            assets.extend(
                (row["id"], role, ordinal, json.dumps(token), "string")
                for ordinal, token in enumerate(values)
            )
        protection = request.get("crash_protection") or {}
        macros.extend(
            (row["id"], ordinal, json.dumps(signal))
            for ordinal, signal in enumerate(protection.get("crash_protector", []))
        )
    return assets, macros


def _write_source(path: Path, rows: list[dict[str, object]], *, drop_asset: bool = False) -> str:
    assets, macros = _dependencies(rows)
    if drop_asset:
        assets.pop()
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA journal_mode=DELETE")
        connection.execute("CREATE TABLE strategy (" + ",".join(_STRATEGY) + ")")
        connection.execute(
            "CREATE TABLE asset_dependency (strategy_id, role, ordinal, raw_token_json, token_kind)"
        )
        connection.execute("CREATE TABLE macro_dependency (strategy_id, ordinal, raw_json)")
        connection.executemany(
            "INSERT INTO strategy VALUES (" + ",".join("?" for _ in _STRATEGY) + ")",  # noqa: S608
            [tuple(row[name] for name in _STRATEGY) for row in rows],
        )
        connection.executemany("INSERT INTO asset_dependency VALUES (?,?,?,?,?)", assets)
        connection.executemany("INSERT INTO macro_dependency VALUES (?,?,?)", macros)
    connection.close()
    path.chmod(0o600)
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _import(home: Path, tmp_path: Path, source_id: str, rows: list[dict[str, object]]) -> str:
    path = tmp_path / f"{source_id}.sqlite3"
    digest = _write_source(path, rows)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        source_library.import_sqlite(workspace, path, source_id, digest)
    return digest


@pytest.fixture
def home(tmp_path: Path) -> Path:
    root = tmp_path / "aas"
    initialize(root)
    return root


def _register(home: Path, source_id: str, digest: str) -> dict[str, object]:
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        return registry.register_strategies(workspace, source_id, digest)


def test_requirement_map_derives_every_named_input() -> None:
    rows = _strategies()
    us = registry.definition_requirements(registry.definition_document(rows[0]))
    assert [(r.role, r.ordinal, r.token, r.dataset_id, r.mapping, r.reason) for r in us] == [
        ("/offensive", 0, "EEE", _US, "mapped", None),
        ("/offensive", 1, "FFF", _US, "mapped", None),
        ("/defensive_rule/defensive", 0, "CCC", _US, "mapped", None),
        ("/defensive_rule/defensive", 1, "DDD", _US, "mapped", None),
        ("/defensive_rule/unallocated", 0, "CASH", None, "not_applicable", "cash"),
        ("/canary/etf_list", 0, "AAA", _US, "mapped", None),
        ("/canary/etf_list", 1, "BBB", _US, "mapped", None),
        ("/benchmark", 0, "SPY", _US, "mapped", None),
        ("/crash_protection/crash_protector", 0, "T10Y3M", _MACRO, "mapped", None),
        ("/crash_protection/crash_protector", 1, "SYNTH", None, "unmapped", "derived_series"),
    ]
    assert us[8].series_id == "T10Y3M"
    kr = registry.definition_requirements(registry.definition_document(rows[1]))
    assert [(r.token, r.market, r.dataset_id, r.reason) for r in kr] == [
        ("000001", "kr", _KR, None),
        ("AAA", "us", _US, None),
        ("000002", "kr", _KR, None),
        ("CASH", None, None, "cash"),
        ("KOSPI", None, None, "composite_benchmark"),
    ]
    unlisted = _row(
        "x", "x", _request(weight_calculation_rule={"constant": {"ZZZ": 1.0}, "func": "CONSTANT"})
    )
    with pytest.raises(ValueError, match="unlisted asset"):
        registry.definition_requirements(registry.definition_document(unlisted))
    odd = registry.definition_document(_row("x", "x", _request(offensive=["not a ticker"])))
    assert registry.definition_requirements(odd)[0].reason == "unrecognized_token"


def test_plan_reports_every_record_and_reconciles_requirements_with_the_catalog(
    home: Path, tmp_path: Path
) -> None:
    digest = _import(home, tmp_path, "synthetic-records", _strategies())
    with open_workspace(home, writable=True) as workspace:
        workspace.state.execute(
            "INSERT INTO datasets VALUES (?,?,?,?)", (_US, "prices", "aas-market-rowset-v1", "t")
        )
        workspace.state.commit()
    with open_workspace(home) as workspace:
        plan = registry.plan_registration(workspace, "synthetic-records", digest)
        assert workspace.strategies is not None
        assert not registry.admit_registry(workspace.strategies, create=False)
    assert plan["plan"] is True
    assert plan["writes"] == 0
    assert (plan["strategies"], plan["new_strategies"], plan["new_versions"]) == (2, 2, 2)
    assert plan["requirements"] == {
        "total": 15,
        "by_mapping": {"mapped": 11, "not_applicable": 2, "unmapped": 2},
        "by_domain": {"cash": 2, "macro": 2, "prices": 11},
    }
    assert plan["datasets"] == [
        {
            "dataset_id": _MACRO,
            "requirements": 1,
            "strategies": 1,
            "in_catalog": False,
            "committed_versions": 0,
        },
        {
            "dataset_id": _KR,
            "requirements": 2,
            "strategies": 1,
            "in_catalog": False,
            "committed_versions": 0,
        },
        {
            "dataset_id": _US,
            "requirements": 8,
            "strategies": 2,
            "in_catalog": True,
            "committed_versions": 0,
        },
    ]
    assert plan["unmapped"] == [
        {"domain": "macro", "reason": "derived_series", "token": "SYNTH", "requirements": 1},
        {"domain": "prices", "reason": "composite_benchmark", "token": "KOSPI", "requirements": 1},
    ]
    assert plan["dependency_mismatches"] == []
    assert plan["execution_eligible"] is False


def test_apply_registers_the_planned_set_and_a_repeat_is_reused(home: Path, tmp_path: Path) -> None:
    digest = _import(home, tmp_path, "synthetic-records", _strategies())
    first = _register(home, "synthetic-records", digest)
    assert (first["reused"], first["new_versions"]) == (False, 2)
    second = _register(home, "synthetic-records", digest)
    assert (second["reused"], second["new_versions"], second["reused_versions"]) == (True, 0, 2)
    with open_workspace(home) as workspace:
        assert workspace.strategies is not None
        listed = registry.list_definitions(workspace.strategies)
        names = workspace.strategies.execute(
            "SELECT strategy_id,name,lifecycle FROM strategies ORDER BY strategy_id"
        ).fetchall()
        operation = get_operation(workspace.state, str(first["operation_id"]))
        report = verify_workspace(workspace)
        assert registry.registry_source_references(workspace.strategies) == {"synthetic-records"}
    assert [(row["strategy_id"], row["requirements"]) for row in listed] == [
        ("synthetic-kr", 5),
        ("synthetic-us", 10),
    ]
    assert all(row["execution_eligible"] is False for row in listed)
    assert [tuple(row) for row in names] == [
        ("synthetic-kr", "Synthetic KR", "active"),
        ("synthetic-us", "Synthetic US", "active"),
    ]
    assert operation is not None
    assert operation["phase"] == "COMPLETED"
    assert report["strategy_registry"] == {"registrations": 1, "strategies": 2, "definitions": 2}
    assert report["strategy_versions"] == 0


def test_definition_versions_are_immutable(home: Path, tmp_path: Path) -> None:
    rows = _strategies()
    first = _register(home, "records-a", _import(home, tmp_path, "records-a", rows))
    changed = [dict(rows[0]), rows[1]]
    changed[0].update(_row("synthetic-us", "Synthetic US ", _request(cost=0.002)))
    later = _register(home, "records-b", _import(home, tmp_path, "records-b", changed))
    same = _register(home, "records-c", _import(home, tmp_path, "records-c", rows))
    assert (later["new_versions"], later["reused_versions"]) == (1, 1)
    assert later["strategies_with_other_versions"] == 1
    assert (same["new_versions"], same["reused_versions"]) == (0, 2)
    with open_workspace(home, strategy_write=True) as workspace:
        connection = workspace.strategies
        assert connection is not None
        versions = registry.list_definitions(connection, "synthetic-us")
        sources = connection.execute(
            "SELECT version,count(*) FROM strategy_definition_sources "
            "WHERE strategy_id='synthetic-us' GROUP BY version ORDER BY count(*)"
        ).fetchall()
        for statement in (
            "UPDATE strategy_definitions SET title='edited'",
            "DELETE FROM strategy_definition_requirements",
            "DELETE FROM strategy_registrations",
        ):
            with pytest.raises(sqlite3.IntegrityError, match="immutable"):
                connection.execute(statement)
        report = verify_workspace(workspace)
    assert len(versions) == 2  # noqa: PLR2004 -- the original and the changed request
    assert [tuple(row)[1] for row in sources] == [1, 2]
    assert first["operation_id"] != later["operation_id"]
    assert report["strategy_registry"] == {"registrations": 3, "strategies": 2, "definitions": 3}


def test_a_dependency_table_that_disagrees_is_reported_and_refused(
    home: Path, tmp_path: Path
) -> None:
    path = tmp_path / "records.sqlite3"
    digest = _write_source(path, _strategies(), drop_asset=True)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        source_library.import_sqlite(workspace, path, "records", digest)
        plan = registry.plan_registration(workspace, "records", digest)
        with pytest.raises(ValueError, match="disagree"):
            registry.register_strategies(workspace, "records", digest)
        assert workspace.strategies is not None
        assert not registry.admit_registry(workspace.strategies, create=False)
    assert plan["dependency_mismatches"] == ["synthetic-kr"]


def test_a_source_of_another_shape_or_hash_is_refused(home: Path, tmp_path: Path) -> None:
    path = tmp_path / "other.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA journal_mode=DELETE")
        connection.execute("CREATE TABLE strategy (id, title, payload_json)")
    connection.close()
    path.chmod(0o600)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        source_library.import_sqlite(workspace, path, "other", digest)
        with pytest.raises(ValueError, match="is not snowball-request@1"):
            registry.plan_registration(workspace, "other", digest)
        with pytest.raises(ValueError, match="SHA-256 differs"):
            registry.plan_registration(workspace, "other", "0" * 64)


def test_an_interrupted_registration_is_recovered_from_its_marker(
    home: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    digest = _import(home, tmp_path, "records", _strategies())

    def interrupted(*_args: object) -> None:
        raise RuntimeError("interrupted")

    monkeypatch.setattr(registry, "complete_operation", interrupted)
    with pytest.raises(RuntimeError, match="interrupted"):
        _register(home, "records", digest)
    monkeypatch.undo()
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        pending = workspace.state.execute(
            "SELECT operation_id FROM storage_operations WHERE kind='strategy_registry'"
        ).fetchone()[0]
        with pytest.raises(ValueError, match="must be recovered"):
            quarantine(workspace, pending, "synthetic")
        # Recovery re-derives from the private marker and never reads the source again.
        monkeypatch.setattr(registry, "_read_source", interrupted)
        assert recover_operations(workspace)["recovered"] == [pending]
        report = verify_workspace(workspace)["strategy_registry"]
        assert isinstance(report, dict)
        assert report["definitions"] == 2  # noqa: PLR2004


def test_a_changed_definition_fails_verification(home: Path, tmp_path: Path) -> None:
    _register(home, "records", _import(home, tmp_path, "records", _strategies()))
    with open_workspace(home, strategy_write=True) as workspace:
        connection = workspace.strategies
        assert connection is not None
        connection.execute("DROP TRIGGER immutable_strategy_definition_requirements_update")
        connection.execute(
            "UPDATE strategy_definition_requirements SET dataset_id='prices.us.other' "
            "WHERE dataset_id=?",
            (_US,),
        )
        connection.commit()
    with (
        open_workspace(home) as workspace,
        pytest.raises(ValueError, match="requirements do not match"),
    ):
        verify_workspace(workspace)


def test_strategy_promote_cli_plans_applies_and_lists(home: Path, tmp_path: Path) -> None:
    digest = _import(home, tmp_path, "records", _strategies())
    arguments = ("strategy", "promote", "--source", "records", "--sha256", digest)
    plan = run_cli(*arguments, "--plan", home=home)
    assert plan.returncode == 0, plan.stderr
    assert json.loads(plan.stdout)["strategies"] == 2  # noqa: PLR2004
    applied = run_cli(*arguments, "--apply", home=home)
    assert applied.returncode == 0, applied.stderr
    listed = run_cli("strategy", "definitions", "--id", "synthetic-us", home=home)
    assert [row["strategy_id"] for row in json.loads(listed.stdout)["definitions"]] == [
        "synthetic-us"
    ]
    verified = run_cli("db", "verify", home=home)
    assert json.loads(verified.stdout)["strategy_registry"]["strategies"] == 2  # noqa: PLR2004
