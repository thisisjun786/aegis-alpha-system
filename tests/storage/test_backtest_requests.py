from __future__ import annotations

import hashlib
import json
from fractions import Fraction
from pathlib import Path

import pytest

from aegis_alpha.compute_resources import ComputeBudget
from aegis_alpha.storage.workspace import initialize, open_workspace

BUDGET = ComputeBudget(Fraction(1), 512 * 1024 * 1024)
J = "aas-canonical-json-sha256-v1"


def canonical(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()


def request(bindings: list[object]) -> bytes:
    # Complete storage root inventory, deliberately NOT financial admission.
    return canonical(
        {
            "schema": "aas-backtest-request-v1",
            "hash_format": J,
            "strategy": {},
            "bindings": bindings,
            "refs": [],
            "price_inputs": [],
            "macro_inputs": [],
            "derived_inputs": [],
            "proxy_rules": [],
            "period": {"start": "not-a-date", "end": "not-a-date"},
            "history": {},
            "cutoff": {},
            "decision_latency_us": 0,
            "explicit_decision_dates": None,
            "account": {"initial_cash": -1},
            "comparison": {},
            "envelope": {},
            "conventions": [],
            "engine": {},
            "environment": {},
        }
    )


def test_request_content_store_not_execution_admission(tmp_path: Path) -> None:
    from aegis_alpha.storage.backtest_requests import (  # noqa: PLC0415 -- RED missing owner
        read_backtest_request,
        register_backtest_request,
    )
    from aegis_alpha.storage.input_pins import register_input_bundle  # noqa: PLC0415
    from aegis_alpha.storage.run_schema import RunSchemaError, install_run_schema  # noqa: PLC0415

    home = tmp_path / "home"
    initialize(home)
    raw = canonical(
        {"schema": "aas-input-bundle-v1", "hash_format": J, "bundle_id": "b-empty", "bindings": []}
    )
    body = request([])
    digest = hashlib.sha256(body).hexdigest()
    with open_workspace(home, writable=True) as workspace:
        pin = register_input_bundle(
            workspace, raw, expected_file_sha256=hashlib.sha256(raw).hexdigest(), budget=BUDGET
        )
        assert (
            pin.content_hash == "7564757378179c7a3065e1d3bb6be8111543a161fe8bab92a493020e9cf8b869"
        )
        with pytest.raises(RunSchemaError, match="run_schema_required"):
            register_backtest_request(
                workspace, pin, body, expected_request_hash=digest, budget=BUDGET
            )
    install_run_schema(home)
    with open_workspace(home, writable=True) as workspace:
        assert (
            register_backtest_request(
                workspace, pin, body, expected_request_hash=digest, budget=BUDGET
            )
            == digest
        )
        assert (
            register_backtest_request(
                workspace, pin, body, expected_request_hash=digest, budget=BUDGET
            )
            == digest
        )
        altered = body + b"\n"
        with pytest.raises(ValueError, match="canonical"):
            register_backtest_request(
                workspace,
                pin,
                altered,
                expected_request_hash=hashlib.sha256(altered).hexdigest(),
                budget=BUDGET,
            )
    with open_workspace(home) as workspace:
        assert (
            read_backtest_request(workspace, pin, expected_request_hash=digest, budget=BUDGET)
            == body
        )
        assert workspace.state.total_changes == 0


@pytest.mark.parametrize(
    "fault",
    [
        "blob",
        "hash",
        "bundle_pin",
        "binding_insert",
        "binding_subset",
        "unknown_root",
        "duplicate_key",
        "bool_ordinal",
    ],
)
def test_request_integrity_rejects_without_repair(tmp_path: Path, fault: str) -> None:
    from dataclasses import replace  # noqa: PLC0415

    from aegis_alpha.storage.backtest_requests import (  # noqa: PLC0415
        read_backtest_request,
        register_backtest_request,
    )
    from aegis_alpha.storage.input_pins import (  # noqa: PLC0415
        InputBinding,
        binding_document,
        register_convention,
        register_input_bundle,
    )
    from aegis_alpha.storage.run_schema import install_run_schema  # noqa: PLC0415
    from tests.storage.test_input_pins import PIN_A, A  # noqa: PLC0415

    home = tmp_path / "home"
    initialize(home)
    install_run_schema(home)
    with open_workspace(home, writable=True) as workspace:
        register_convention(workspace.state, A, expected_file_sha256=PIN_A.hash)
        binding = binding_document(
            InputBinding("basis", 0, "convention:basis", PIN_A.id, PIN_A.version, PIN_A.hash)
        )
        raw = canonical(
            {
                "schema": "aas-input-bundle-v1",
                "hash_format": J,
                "bundle_id": "b",
                "bindings": [binding],
            }
        )
        pin = register_input_bundle(
            workspace, raw, expected_file_sha256=hashlib.sha256(raw).hexdigest(), budget=BUDGET
        )
        body = request([binding])
        digest = hashlib.sha256(body).hexdigest()
        register_backtest_request(workspace, pin, body, expected_request_hash=digest, budget=BUDGET)
        if fault in {"blob", "hash"}:
            trigger = workspace.state.execute(
                "SELECT sql FROM sqlite_schema WHERE name='immutable_backtest_requests_update'"
            ).fetchone()[0]
            workspace.state.execute("DROP TRIGGER immutable_backtest_requests_update")
            if fault == "blob":
                workspace.state.execute(
                    "UPDATE backtest_requests SET request_bytes=?", (body + b"\n",)
                )
            else:
                workspace.state.execute("UPDATE backtest_requests SET request_hash=?", ("0" * 64,))
            workspace.state.execute(trigger)
            workspace.state.commit()
        elif fault == "bundle_pin":
            pin = replace(pin, content_hash="0" * 64)
        elif fault == "binding_insert":
            workspace.state.execute(
                "INSERT INTO input_bindings VALUES ('b','basis',1,'convention:basis',?,'1',?)",
                (PIN_A.id, PIN_A.hash),
            )
            workspace.state.commit()
        else:
            parsed = json.loads(body)
            if fault == "binding_subset":
                parsed["bindings"] = []
            elif fault == "unknown_root":
                parsed["metadata"] = {"created_at_us": 1}
            elif fault == "bool_ordinal":
                parsed["bindings"][0]["ordinal"] = True
            body = canonical(parsed)
            if fault == "duplicate_key":
                body = body.replace(b'"account":', b'"account":{},"account":')
            digest = hashlib.sha256(body).hexdigest()
        before = "\n".join(workspace.state.iterdump())
        if fault in {"blob", "hash", "bundle_pin", "binding_insert"}:
            with pytest.raises(
                ValueError,
                match=r"request|bundle|binding|duplicate|integer|canonical|pin version|invalid pin",
            ):
                read_backtest_request(workspace, pin, expected_request_hash=digest, budget=BUDGET)
        with pytest.raises(
            ValueError,
            match=r"request|bundle|binding|duplicate|integer|canonical|pin version|invalid pin",
        ):
            register_backtest_request(
                workspace, pin, body, expected_request_hash=digest, budget=BUDGET
            )
        assert "\n".join(workspace.state.iterdump()) == before


def test_complete_addon_backup_validates_definitions_bundles_and_request(tmp_path: Path) -> None:
    from aegis_alpha.storage.backtest_requests import (  # noqa: PLC0415
        read_backtest_request,
        register_backtest_request,
    )
    from aegis_alpha.storage.backup import backup, restore  # noqa: PLC0415
    from aegis_alpha.storage.input_pins import (  # noqa: PLC0415
        InputBinding,
        binding_document,
        read_definition,
        register_definition,
        register_input_bundle,
    )
    from aegis_alpha.storage.run_schema import install_run_schema  # noqa: PLC0415
    from tests.storage.test_input_pins import derived_document  # noqa: PLC0415

    home = tmp_path / "home"
    initialize(home)
    install_run_schema(home)
    with open_workspace(home, writable=True) as workspace:
        raw = derived_document(workspace)
        definition = register_definition(
            workspace, raw, expected_file_sha256=hashlib.sha256(raw).hexdigest(), budget=BUDGET
        )
        exact = read_definition(workspace, definition, budget=BUDGET)
        binding = binding_document(
            InputBinding(
                "derived", 0, "derived", definition.id, definition.version, definition.hash
            )
        )
        raw_bundle = canonical(
            {
                "schema": "aas-input-bundle-v1",
                "hash_format": J,
                "bundle_id": "b",
                "bindings": [binding],
            }
        )
        bundle = register_input_bundle(
            workspace,
            raw_bundle,
            expected_file_sha256=hashlib.sha256(raw_bundle).hexdigest(),
            budget=BUDGET,
        )
        body = request([binding])
        digest = hashlib.sha256(body).hexdigest()
        register_backtest_request(
            workspace, bundle, body, expected_request_hash=digest, budget=BUDGET
        )
    saved = Path(str(backup(home, tmp_path / "backup")["backup_root"]))
    fresh = tmp_path / "restored"
    restore(saved, fresh)
    with open_workspace(fresh) as workspace:
        assert read_definition(workspace, definition, budget=BUDGET) == exact
        assert (
            read_backtest_request(workspace, bundle, expected_request_hash=digest, budget=BUDGET)
            == body
        )
        assert workspace.state.total_changes == 0


@pytest.mark.parametrize(
    "fault",
    [
        "unknown_role",
        "wrong_kind",
        "wrong_schema",
        "gap",
        "duplicate_position",
        "duplicate_reference",
        "latest",
    ],
)
def test_binding_wire_rejects_real_malformed_values(fault: str) -> None:
    from aegis_alpha.storage.input_pins import parse_bindings  # noqa: PLC0415

    binding = {
        "role": "macro",
        "ordinal": 0,
        "ref_kind": "generation",
        "ref_id": "g",
        "ref_version": "1",
        "hash": "a" * 64,
        "ref_schema": "aas-generation-pin-v1",
        "hash_format": "aas-market-generation-chain-v1",
    }
    rows = [binding]
    if fault == "unknown_role":
        binding["role"] = "raw_source"
    elif fault == "wrong_kind":
        binding["ref_kind"] = "identity"
    elif fault == "wrong_schema":
        binding["ref_schema"] = "aas-universe-version-v1"
    elif fault == "gap":
        binding["ordinal"] = 1
    elif fault == "latest":
        binding["ref_version"] = "latest"
    else:
        rows.append({**binding, "ordinal": 0 if fault == "duplicate_position" else 1})
    with pytest.raises(
        ValueError, match=r"request|bundle|binding|duplicate|integer|canonical|pin version"
    ):
        parse_bindings(rows)


def test_refreshed_backup_file_hash_cannot_bless_corrupt_request(tmp_path: Path) -> None:
    import sqlite3  # noqa: PLC0415
    from contextlib import closing  # noqa: PLC0415

    from aegis_alpha.storage.backtest_requests import register_backtest_request  # noqa: PLC0415
    from aegis_alpha.storage.backup import backup, restore  # noqa: PLC0415
    from aegis_alpha.storage.input_pins import register_input_bundle  # noqa: PLC0415
    from aegis_alpha.storage.run_schema import install_run_schema  # noqa: PLC0415
    from aegis_alpha.storage.workspace import write_json  # noqa: PLC0415

    home = tmp_path / "home"
    initialize(home)
    install_run_schema(home)
    with open_workspace(home, writable=True) as workspace:
        raw = canonical(
            {"schema": "aas-input-bundle-v1", "hash_format": J, "bundle_id": "b", "bindings": []}
        )
        pin = register_input_bundle(
            workspace, raw, expected_file_sha256=hashlib.sha256(raw).hexdigest(), budget=BUDGET
        )
        body = request([])
        register_backtest_request(
            workspace,
            pin,
            body,
            expected_request_hash=hashlib.sha256(body).hexdigest(),
            budget=BUDGET,
        )
    saved = Path(str(backup(home, tmp_path / "backup")["backup_root"]))
    with closing(sqlite3.connect(saved / "state.sqlite3")) as connection:
        trigger = connection.execute(
            "SELECT sql FROM sqlite_schema WHERE name='immutable_backtest_requests_update'"
        ).fetchone()[0]
        connection.execute("DROP TRIGGER immutable_backtest_requests_update")
        connection.execute("UPDATE backtest_requests SET request_bytes=?", (body + b"\n",))
        connection.execute(trigger)
        connection.commit()
        assert connection.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
    raw = (saved / "state.sqlite3").read_bytes()
    manifest = json.loads((saved / "backup.json").read_bytes())
    manifest["files"]["state.sqlite3"] = {
        "size_bytes": len(raw),
        "sha256": hashlib.sha256(raw).hexdigest(),
    }
    write_json(saved / "backup.json", manifest)
    with pytest.raises(ValueError, match="canonical"):
        restore(saved, tmp_path / "rejected")
    assert (
        json.loads((tmp_path / "rejected" / "installation.json").read_bytes())["phase"]
        == "restore-incomplete"
    )
