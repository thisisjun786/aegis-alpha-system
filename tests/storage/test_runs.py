from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from dataclasses import dataclass, replace
from decimal import Decimal
from fractions import Fraction
from pathlib import Path
from typing import cast

import pytest

from aegis_alpha.compute_resources import ComputeBudget, ComputeResourceError
from aegis_alpha.data.serialization import content_sha256
from aegis_alpha.storage.backtest_requests import register_backtest_request
from aegis_alpha.storage.backup import backup
from aegis_alpha.storage.input_pins import register_input_bundle
from aegis_alpha.storage.publication import quarantine, recover_operations
from aegis_alpha.storage.run_schema import install_run_schema
from aegis_alpha.storage.runs import (
    RunIntent,
    RunResult,
    RunStorageError,
    RunStrategyPin,
    _ordered,
    _require_admitted_pins,
    _seal,
    _stored_rows,
    commit_run,
    fail_run,
    list_runs,
    open_run,
    read_run,
    recover_run,
)
from aegis_alpha.storage.verification import verify_workspace
from aegis_alpha.storage.workspace import Workspace, initialize, open_workspace

BUDGET = ComputeBudget(Fraction(1), 512 * 1024 * 1024)
TIGHT = replace(
    BUDGET, reserved_bytes=BUDGET.memory_limit_bytes - BUDGET.duckdb_memory_limit_bytes - 1
)
HASH_FORMAT = "aas-canonical-json-sha256-v1"
ENGINE: dict[str, object] = {
    "schema": "aas-engine-identity-v1",
    "hash_format": HASH_FORMAT,
    "package_version": "0.0.0",
    "calculation_source_hash": "c" * 64,
}
ENVIRONMENT: dict[str, object] = {
    "schema": "aas-environment-identity-v1",
    "hash_format": HASH_FORMAT,
    "versions": [{"name": "python_version", "version": "3.13.0"}],
}
READER = """
import json, sys
from fractions import Fraction
from pathlib import Path
from aegis_alpha.compute_resources import ComputeBudget
from aegis_alpha.storage.runs import read_run
from aegis_alpha.storage.workspace import open_workspace

with open_workspace(Path(sys.argv[1])) as workspace:
    budget = ComputeBudget(Fraction(1), 512 * 1024 * 1024)
    print(json.dumps(read_run(workspace, sys.argv[2], budget=budget), sort_keys=True))
"""


def canonical(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()


BUNDLE = canonical(
    {
        "schema": "aas-input-bundle-v1",
        "hash_format": HASH_FORMAT,
        "bundle_id": "b-empty",
        "bindings": [],
    }
)
ENVELOPE = canonical(
    {"module": "aegis", "targets": {"2024-01-02": {"AAA": 1.0}, "2024-01-03": {"AAA": 0.5}}}
)
ENVELOPE_SHA256 = hashlib.sha256(ENVELOPE).hexdigest()
NAV: list[dict[str, object]] = [
    {"date": "2024-01-02", "equity": 1000.0, "cash": 0.0},
    {"date": "2024-01-03", "equity": 1100.0, "cash": 10.0},
]
FILLS: list[dict[str, object]] = [
    {
        "decision_date": "2024-01-01",
        "execution_date": "2024-01-02",
        "symbol": "AAA",
        "shares": 10.0,
        "price": 100.0,
        "fee": 1.0,
    },
    {
        "decision_date": "2024-01-02",
        "execution_date": "2024-01-03",
        "symbol": "BBB",
        "shares": 2.0,
        "price": 50.0,
        "fee": 0.5,
    },
]
COUNTS = {
    "equity_points": 2,
    "positions": 0,
    "signals": 0,
    "simulated_trades": 2,
    "target_weights": 2,
}
METRICS = {
    "final_equity": {"value": "1100.000000000000", "value_state": "present"},
    "sharpe": {"value": None, "value_state": "not_collected"},
    "sortino": {"value": None, "value_state": "not_collected"},
    "total_return": {"value": "0.100000000000", "value_state": "present"},
}


def backtest(nav: list[dict[str, object]], fills: list[dict[str, object]]) -> bytes:
    return canonical(
        {
            "module": "aegis",
            "input_sha256": ENVELOPE_SHA256,
            "result": {"nav": nav, "fills": fills},
        }
    )


RESULT = backtest(NAV, FILLS)


def request_document(pin: RunStrategyPin) -> bytes:
    """A request shaped like the real one, sealing the identities a run must match."""
    return canonical(
        {
            "schema": "aas-backtest-request-v1",
            "hash_format": HASH_FORMAT,
            "strategy": {
                "strategy_store_id": pin.store_id,
                "strategy_id": pin.strategy_id,
                "version": pin.version,
                "raw_sha256": pin.raw_hash,
                "contract_sha256": pin.contract_hash,
            },
            "bindings": [],
            "refs": [],
            "price_inputs": [],
            "macro_inputs": [],
            "derived_inputs": [],
            "proxy_rules": [],
            "period": {"start": "2024-01-02", "end": "2024-01-03"},
            "history": {},
            "cutoff": {},
            "decision_latency_us": 0,
            "explicit_decision_dates": None,
            "account": {"initial_cash": 1000},
            "comparison": {},
            "envelope": {},
            "conventions": [],
            "engine": ENGINE,
            "environment": ENVIRONMENT,
        }
    )


def preparation_document(request_hash: str, envelope_bytes: bytes = ENVELOPE) -> bytes:
    return canonical(
        {
            "schema": "aas-prepared-backtest-v1",
            "hash_format": HASH_FORMAT,
            "request_hash": request_hash,
            "envelope_sha256": hashlib.sha256(envelope_bytes).hexdigest(),
        }
    )


@dataclass(frozen=True, slots=True)
class Fixture:
    """One installation whose request, strategy and preparation already agree."""

    home: Path
    pin: RunStrategyPin
    request_hash: str


def admit_strategy(home: Path, root: Path) -> RunStrategyPin:
    """Admit one strategy version through the real registration path."""
    from aegis_alpha.storage.strategy_import import register_strategy  # noqa: PLC0415
    from tests.engine.engine_support import contract, raw_bundle  # noqa: PLC0415

    raw = raw_bundle(contract())
    digest = hashlib.sha256(raw).hexdigest()
    bundle = root / "bundle.json"
    bundle.write_bytes(raw)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        assert workspace.strategies is not None
        register_strategy(workspace, bundle, digest, "synthetic-probe", "1")
        store_id, contract_hash = workspace.strategies.execute(
            "SELECT (SELECT store_id FROM store_info), contract_sha256 FROM strategy_versions "
            "WHERE strategy_id=? AND version=?",
            ("synthetic-probe", "1"),
        ).fetchone()
    return RunStrategyPin(
        module="aegis",
        ordinal=0,
        store_id=store_id,
        strategy_id="synthetic-probe",
        version="1",
        raw_hash=digest,
        contract_hash=contract_hash,
    )


def prepared(root: Path) -> Fixture:
    """An installation whose bundle, strategy and request already admit one run."""
    home = root / "home"
    initialize(home)
    install_run_schema(home)
    pin = admit_strategy(home, root)
    body = request_document(pin)
    digest = hashlib.sha256(body).hexdigest()
    with open_workspace(home, writable=True) as workspace:
        bundle_pin = register_input_bundle(
            workspace,
            BUNDLE,
            expected_file_sha256=hashlib.sha256(BUNDLE).hexdigest(),
            budget=BUDGET,
        )
        register_backtest_request(
            workspace, bundle_pin, body, expected_request_hash=digest, budget=BUDGET
        )
    return Fixture(home=home, pin=pin, request_hash=digest)


def intent(
    fx: Fixture,
    run_id: str,
    *,
    prior_run_id: str | None = None,
    pins: tuple[RunStrategyPin, ...] | None = None,
) -> RunIntent:
    return RunIntent(
        request_hash=fx.request_hash,
        bundle_id="b-empty",
        engine_hash=content_sha256(ENGINE),
        environment_hash=content_sha256(ENVIRONMENT),
        reason="synthetic run",
        envelope_bytes=ENVELOPE,
        preparation_bytes=preparation_document(fx.request_hash),
        strategy_pins=(fx.pin,) if pins is None else pins,
        prior_run_id=prior_run_id,
        run_id=run_id,
    )


def states(payload: dict[str, object]) -> dict[str, dict[str, object]]:
    """Reduce a read payload to the metric values and their states."""
    metrics = cast("dict[str, dict[str, object]]", payload["metrics"])
    return {
        name: {"value": entry["value"], "value_state": entry["value_state"]}
        for name, entry in metrics.items()
    }


def observed(home: Path, run_id: str) -> tuple[str | None, str | None]:
    with open_workspace(home) as workspace:
        run = workspace.state.execute(
            "SELECT status FROM runs WHERE run_id=?", (run_id,)
        ).fetchone()
        operation = workspace.state.execute(
            "SELECT phase FROM storage_operations WHERE target_id=?", (run_id,)
        ).fetchone()
        return (
            None if run is None else run[0],
            None if operation is None else operation[0],
        )


def test_committed_run_reads_back_identically_in_a_separate_process(tmp_path: Path) -> None:
    fx = prepared(tmp_path)
    home = fx.home
    admitted = fx.pin
    with open_workspace(home, writable=True) as workspace:
        handle = open_run(workspace, intent(fx, "run-1", pins=(admitted,)))
        committed = commit_run(workspace, handle, RunResult(RESULT), budget=BUDGET)
    assert committed["status"] == "SUCCESS"
    assert committed["table_counts"] == COUNTS
    assert committed["metrics"] == METRICS
    assert committed["research_only"] is True
    separate = subprocess.run(  # noqa: S603 -- a second process is the point of the check
        [sys.executable, "-c", READER, str(home), "run-1"],
        capture_output=True,
        text=True,
        check=True,
    )
    payload = json.loads(separate.stdout)
    assert payload["result_hash"] == committed["result_hash"]
    assert payload["artifacts"] == committed["artifacts"]
    assert payload["table_hashes"] == committed["table_hashes"]
    assert payload["table_counts"] == committed["table_counts"]
    # The reader keeps the comparison references a metric would need, so an absent
    # number is never confused with an absent reason.
    assert cast("dict[str, dict[str, object]]", payload["metrics"])["sharpe"] == {
        "value": None,
        "value_state": "not_collected",
        "benchmark_ref": None,
        "risk_free_ref": None,
        "cost_ref": None,
        "comparison_condition_hash": None,
    }
    assert states(payload) == METRICS
    assert payload["research_only"] is True
    assert payload["strategy_pins"] == [
        {
            "module": "aegis",
            "ordinal": 0,
            "strategy_store_id": admitted.store_id,
            "strategy_id": "synthetic-probe",
            "version": "1",
            "raw_hash": admitted.raw_hash,
            "contract_hash": admitted.contract_hash,
        }
    ]
    with open_workspace(home) as workspace:
        assert verify_workspace(workspace, budget=BUDGET)["verified"] is True
        assert [row["status"] for row in list_runs(workspace)] == ["SUCCESS"]


def test_row_order_in_the_result_does_not_move_a_receipt_or_an_ordinal(tmp_path: Path) -> None:
    fx = prepared(tmp_path)
    home = fx.home
    shuffled = backtest(list(reversed(NAV)), list(reversed(FILLS)))
    with open_workspace(home, writable=True) as workspace:
        first = commit_run(
            workspace, open_run(workspace, intent(fx, "run-1")), RunResult(RESULT), budget=BUDGET
        )
        second = commit_run(
            workspace,
            open_run(workspace, intent(fx, "run-2", prior_run_id="run-1")),
            RunResult(shuffled),
            budget=BUDGET,
        )
        stored = {
            run_id: workspace.market.execute(
                "SELECT ordinal,instrument_id FROM simulated_trades WHERE run_id=? "
                "ORDER BY ordinal",
                [run_id],
            ).fetchall()
            for run_id in ("run-1", "run-2")
        }
    assert first["table_hashes"] == second["table_hashes"]
    assert stored["run-1"] == stored["run-2"] == [(0, "AAA"), (1, "BBB")]
    # The artifact bytes are the reproducibility anchor, so a different document is a
    # different result even when every stored row matches.
    assert first["result_hash"] != second["result_hash"]


def test_declared_keys_alone_do_not_decide_an_ordinal() -> None:
    rows: list[dict[str, object]] = [
        {
            "module": "aegis",
            "at_us": 0,
            "instrument_id": "AAA",
            "signal_id": "s",
            "value": 2.0,
            "value_state": "present",
        },
        {
            "module": "aegis",
            "at_us": 0,
            "instrument_id": "AAA",
            "signal_id": "s",
            "value": 1.0,
            "value_state": "present",
        },
    ]
    assert [row["value"] for _ordinal, row in _ordered("signals", rows)] == [1.0, 2.0]
    assert [row["value"] for _ordinal, row in _ordered("signals", list(reversed(rows)))] == [
        1.0,
        2.0,
    ]


def test_an_interrupted_intent_never_leaves_a_run_that_recovery_cannot_find(tmp_path: Path) -> None:
    fx = prepared(tmp_path)
    home = fx.home
    with open_workspace(home, writable=True) as workspace:
        open_run(workspace, intent(fx, "run-1"))
        assert verify_workspace(workspace, budget=BUDGET)["pending_operations"] == 1
    with open_workspace(home, writable=True) as workspace:
        assert recover_operations(workspace)["recovered"] == ["run:run-1"]
    assert observed(home, "run-1") == ("INTERRUPTED", "QUARANTINED")
    with open_workspace(home) as workspace:
        assert verify_workspace(workspace, budget=BUDGET)["pending_operations"] == 0
        with pytest.raises(RunStorageError, match="INTERRUPTED"):
            read_run(workspace, "run-1", budget=BUDGET)


def test_recovery_finishes_a_committed_result_without_calculating_again(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fx = prepared(tmp_path)
    home = fx.home
    with open_workspace(home, writable=True) as workspace:
        handle = open_run(workspace, intent(fx, "run-1"))
        monkeypatch.setattr(
            "aegis_alpha.storage.runs._finish",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("interrupted")),
        )
        with pytest.raises(RuntimeError, match="interrupted"):
            commit_run(workspace, handle, RunResult(RESULT), budget=BUDGET)
    monkeypatch.undo()
    assert observed(home, "run-1") == ("RUNNING", "PREPARED")
    with open_workspace(home, writable=True) as workspace:
        assert recover_operations(workspace)["recovered"] == ["run:run-1"]
    assert observed(home, "run-1") == ("SUCCESS", "COMPLETED")
    with open_workspace(home) as workspace:
        payload = read_run(workspace, "run-1", budget=BUDGET)
        assert payload["table_counts"] == COUNTS
        assert states(payload) == METRICS
        assert verify_workspace(workspace, budget=BUDGET)["verified"] is True


def test_a_marker_that_disagrees_with_its_evidence_is_kept_not_overwritten(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fx = prepared(tmp_path)
    home = fx.home
    with open_workspace(home, writable=True) as workspace:
        handle = open_run(workspace, intent(fx, "run-1"))
        monkeypatch.setattr(
            "aegis_alpha.storage.runs._finish",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("interrupted")),
        )
        with pytest.raises(RuntimeError, match="interrupted"):
            commit_run(workspace, handle, RunResult(RESULT), budget=BUDGET)
    monkeypatch.undo()
    with open_workspace(home, writable=True) as workspace:
        workspace.market.execute("BEGIN TRANSACTION")
        workspace.market.execute("UPDATE result_commits SET manifest_hash=?", ["0" * 64])
        workspace.market.execute("COMMIT")
    with open_workspace(home, writable=True) as workspace:
        assert recover_operations(workspace)["recovered"] == ["run:run-1"]
    assert observed(home, "run-1") == ("QUARANTINED", "QUARANTINED")
    with open_workspace(home) as workspace:
        kept = workspace.market.execute("SELECT manifest_hash FROM result_commits").fetchone()
        assert kept is not None
        assert kept[0] == "0" * 64
        assert workspace.state.execute("SELECT count(*) FROM artifacts").fetchone()[0] == 0
        report = verify_workspace(workspace, budget=BUDGET)
        assert report["pending_operations"] == 0
        assert report["unfinished_runs"] == ["run-1"]
        with pytest.raises(RunStorageError, match="QUARANTINED"):
            read_run(workspace, "run-1", budget=BUDGET)


def test_a_handled_calculation_failure_blocks_neither_recovery_nor_backup(tmp_path: Path) -> None:
    fx = prepared(tmp_path)
    home = fx.home
    with open_workspace(home, writable=True) as workspace:
        handle = open_run(workspace, intent(fx, "run-1"))
        assert fail_run(workspace, handle, "calculation raised")["status"] == "FAILED"
    with open_workspace(home, writable=True) as workspace:
        assert recover_operations(workspace) == {
            "recovered": [],
            "pending": [],
            "provider_calls": 0,
        }
    assert observed(home, "run-1") == ("FAILED", "QUARANTINED")
    assert backup(home, tmp_path / "backup")["backup_root"] == str(tmp_path / "backup")


def test_a_committed_run_cannot_be_failed_or_quarantined_by_hand(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fx = prepared(tmp_path)
    home = fx.home
    with open_workspace(home, writable=True) as workspace:
        handle = open_run(workspace, intent(fx, "run-1"))
        with pytest.raises(ValueError, match="ended by recovery"):
            quarantine(workspace, handle.operation_id, "manual")
        monkeypatch.setattr(
            "aegis_alpha.storage.runs._finish",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("interrupted")),
        )
        with pytest.raises(RuntimeError, match="interrupted"):
            commit_run(workspace, handle, RunResult(RESULT), budget=BUDGET)
        monkeypatch.undo()
        with pytest.raises(RunStorageError, match="must be recovered, not failed"):
            fail_run(workspace, handle, "too late")


def test_a_run_requires_a_registered_request_and_its_own_transaction(tmp_path: Path) -> None:
    fx = prepared(tmp_path)
    home = fx.home
    with (
        open_workspace(home, writable=True) as workspace,
        pytest.raises(RunStorageError, match="registered request"),
    ):
        open_run(workspace, replace(intent(fx, "run-1"), request_hash="c" * 64))
    with open_workspace(home, writable=True) as workspace:
        workspace.state.execute("BEGIN IMMEDIATE")
        with pytest.raises(RunStorageError, match="another state transaction"):
            open_run(workspace, intent(fx, "run-1"))
        workspace.state.rollback()


def test_a_tampered_artifact_refuses_to_read_back(tmp_path: Path) -> None:
    fx = prepared(tmp_path)
    home = fx.home
    with open_workspace(home, writable=True) as workspace:
        handle = open_run(workspace, intent(fx, "run-1"))
        commit_run(workspace, handle, RunResult(RESULT), budget=BUDGET)
        sealed = workspace.paths.runs / "run-1" / "backtest.json"
    sealed.write_bytes(backtest(NAV, FILLS[:1]))
    with open_workspace(home) as workspace:
        with pytest.raises(RunStorageError, match="disagrees with the sealed evidence"):
            read_run(workspace, "run-1", budget=BUDGET)
        with pytest.raises(ValueError, match="run artifact hash/size mismatch"):
            verify_workspace(workspace, budget=BUDGET)


TIGHT = replace(
    BUDGET, reserved_bytes=BUDGET.memory_limit_bytes - BUDGET.duckdb_memory_limit_bytes - 1
)
UNIT_NAV: list[dict[str, object]] = [
    {"date": "2024-01-02", "unit_value": 1.0, "units": 1000.0, "external_flow": 0.0},
    {"date": "2024-01-03", "unit_value": 1.02, "units": 1000.0, "external_flow": 500.0},
]


def cashflow(nav: list[dict[str, object]], unit_nav: list[dict[str, object]]) -> bytes:
    return canonical(
        {
            "module": "aegis",
            "input_sha256": ENVELOPE_SHA256,
            "result": {
                "account": {"nav": nav, "fills": FILLS},
                "unit_nav": unit_nav,
                "cashflows": [{"date": "2024-01-03", "amount": 500.0}],
            },
        }
    )


def interrupt(monkeypatch: pytest.MonkeyPatch, target: str) -> None:
    monkeypatch.setattr(
        "aegis_alpha.storage.runs." + target,
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("interrupted")),
    )


def committed_run(fx: Fixture, monkeypatch: pytest.MonkeyPatch) -> None:
    """Leave a run whose market marker is committed but whose state record is not."""
    with open_workspace(fx.home, writable=True) as workspace:
        handle = open_run(workspace, intent(fx, "run-1"))
        interrupt(monkeypatch, "_finish")
        with pytest.raises(RuntimeError, match="interrupted"):
            commit_run(workspace, handle, RunResult(RESULT), budget=BUDGET)
    monkeypatch.undo()


def alter_market(home: Path, statement: str, parameters: list[object]) -> None:
    with open_workspace(home, writable=True) as workspace:
        workspace.market.execute("BEGIN TRANSACTION")
        workspace.market.execute(statement, parameters)
        workspace.market.execute("COMMIT")


@pytest.mark.parametrize(
    ("statement", "parameters"),
    [
        ("UPDATE result_commits SET table_hashes=?", ["not json"]),
        ("UPDATE equity_points SET equity=? WHERE ordinal=0", [Decimal("1.5")]),
    ],
)
def test_corrupted_results_end_the_run_instead_of_blocking_recovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, statement: str, parameters: list[object]
) -> None:
    fx = prepared(tmp_path)
    home = fx.home
    committed_run(fx, monkeypatch)
    alter_market(home, statement, parameters)
    with open_workspace(home, writable=True) as workspace:
        assert recover_operations(workspace)["recovered"] == ["run:run-1"]
    assert observed(home, "run-1") == ("QUARANTINED", "QUARANTINED")
    # A corrupted result must not hold backup hostage forever.
    assert backup(home, tmp_path / "backup")["backup_root"] == str(tmp_path / "backup")


def test_a_refused_materialization_leaves_the_run_open_instead_of_quarantining_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fx = prepared(tmp_path)
    home = fx.home
    committed_run(fx, monkeypatch)
    with open_workspace(home, writable=True) as workspace:
        operation = workspace.state.execute(
            "SELECT * FROM storage_operations WHERE target_id='run-1'"
        ).fetchone()
        with pytest.raises(ComputeResourceError):
            recover_run(workspace, operation, budget=TIGHT)
    # Having no room to work says nothing about the stored result.
    assert observed(home, "run-1") == ("RUNNING", "PREPARED")
    with open_workspace(home, writable=True) as workspace:
        assert recover_operations(workspace)["recovered"] == ["run:run-1"]
    assert observed(home, "run-1") == ("SUCCESS", "COMPLETED")


def test_reading_and_verifying_refuse_to_materialize_beyond_the_allowance(
    tmp_path: Path,
) -> None:
    fx = prepared(tmp_path)
    home = fx.home
    with open_workspace(home, writable=True) as workspace:
        commit_run(
            workspace, open_run(workspace, intent(fx, "run-1")), RunResult(RESULT), budget=BUDGET
        )
    with open_workspace(home) as workspace:
        with pytest.raises(ComputeResourceError):
            read_run(workspace, "run-1", budget=TIGHT)
        with pytest.raises(ComputeResourceError):
            verify_workspace(workspace, budget=TIGHT)


def test_an_intent_from_another_run_cannot_end_this_one(tmp_path: Path) -> None:
    fx = prepared(tmp_path)
    home = fx.home
    with open_workspace(home, writable=True) as workspace:
        first = open_run(workspace, intent(fx, "run-1"))
        second = open_run(workspace, intent(fx, "run-2"))
        borrowed = replace(second, operation_id=first.operation_id)
        with pytest.raises(RunStorageError, match="does not belong to this run"):
            fail_run(workspace, borrowed, "wrong run")
    assert observed(home, "run-1") == ("RUNNING", "PREPARED")
    assert observed(home, "run-2") == ("RUNNING", "PREPARED")


def test_an_identical_retry_resumes_instead_of_failing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fx = prepared(tmp_path)
    home = fx.home
    with open_workspace(home, writable=True) as workspace:
        first = open_run(workspace, intent(fx, "run-1"))
        assert open_run(workspace, intent(fx, "run-1")) == first
        with pytest.raises(RunStorageError, match="different or finished run"):
            open_run(workspace, replace(intent(fx, "run-1"), reason="a different question"))
        interrupt(monkeypatch, "_finish")
        with pytest.raises(RuntimeError, match="interrupted"):
            commit_run(workspace, first, RunResult(RESULT), budget=BUDGET)
        monkeypatch.undo()
        committed = commit_run(workspace, first, RunResult(RESULT), budget=BUDGET)
    assert committed["status"] == "SUCCESS"
    with open_workspace(home) as workspace:
        assert (
            read_run(workspace, "run-1", budget=BUDGET)["result_hash"] == committed["result_hash"]
        )


def test_a_sealed_artifact_is_never_replaced_by_different_bytes(tmp_path: Path) -> None:
    fx = prepared(tmp_path)
    home = fx.home
    with open_workspace(home, writable=True) as workspace:
        handle = open_run(workspace, intent(fx, "run-1"))
        commit_run(workspace, handle, RunResult(RESULT), budget=BUDGET)
        with pytest.raises(RunStorageError, match="already holds different bytes"):
            _seal(workspace, "run-1", "backtest.json", backtest(NAV, FILLS[:1]))


def test_an_interrupted_seal_still_leaves_a_discoverable_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fx = prepared(tmp_path)
    home = fx.home
    sealed: list[str] = []
    original = _seal

    def once(workspace: Workspace, run_id: str, name: str, raw: bytes) -> str:
        if sealed:
            raise RuntimeError("interrupted between seals")
        sealed.append(name)
        return original(workspace, run_id, name, raw)

    monkeypatch.setattr("aegis_alpha.storage.runs._seal", once)
    with (
        open_workspace(home, writable=True) as workspace,
        pytest.raises(RuntimeError, match="between seals"),
    ):
        open_run(workspace, intent(fx, "run-1"))
    monkeypatch.undo()
    assert sealed == ["envelope.json"]
    assert observed(home, "run-1") == ("RUNNING", "PREPARED")
    with open_workspace(home, writable=True) as workspace:
        assert recover_operations(workspace)["recovered"] == ["run:run-1"]
    assert observed(home, "run-1") == ("INTERRUPTED", "QUARANTINED")


def test_metrics_do_not_depend_on_the_supplied_array_order(tmp_path: Path) -> None:
    fx = prepared(tmp_path)
    home = fx.home
    with open_workspace(home, writable=True) as workspace:
        forward = commit_run(
            workspace, open_run(workspace, intent(fx, "run-1")), RunResult(RESULT), budget=BUDGET
        )
        reversed_document = backtest(list(reversed(NAV)), list(reversed(FILLS)))
        backward = commit_run(
            workspace,
            open_run(workspace, intent(fx, "run-2")),
            RunResult(reversed_document),
            budget=BUDGET,
        )
    assert forward["metrics"] == backward["metrics"] == METRICS


def test_an_unrepresentable_return_is_recorded_as_unsupported_not_as_a_number(
    tmp_path: Path,
) -> None:
    fx = prepared(tmp_path)
    home = fx.home
    extreme: list[dict[str, object]] = [
        {"date": "2024-01-02", "equity": 1e-12, "cash": 0.0},
        {"date": "2024-01-03", "equity": 1e25, "cash": 0.0},
    ]
    with open_workspace(home, writable=True) as workspace:
        committed = commit_run(
            workspace,
            open_run(workspace, intent(fx, "run-1")),
            RunResult(backtest(extreme, [])),
            budget=BUDGET,
        )
    metrics = cast("dict[str, dict[str, object]]", committed["metrics"])
    assert metrics["total_return"] == {"value": None, "value_state": "unsupported"}
    assert metrics["final_equity"]["value_state"] == "present"


def test_a_cashflow_result_measures_return_without_the_contributions(tmp_path: Path) -> None:
    fx = prepared(tmp_path)
    home = fx.home
    with open_workspace(home, writable=True) as workspace:
        committed = commit_run(
            workspace,
            open_run(workspace, intent(fx, "run-1")),
            RunResult(cashflow(NAV, UNIT_NAV)),
            budget=BUDGET,
        )
    metrics = cast("dict[str, dict[str, object]]", committed["metrics"])
    # The account grew 10 percent, but 500 of that arrived as a contribution. The unit
    # values are what the strategy actually earned.
    assert metrics["final_equity"]["value"] == "1100.000000000000"
    assert metrics["total_return"] == {"value": "0.020000000000", "value_state": "present"}
    with open_workspace(home) as workspace:
        assert read_run(workspace, "run-1", budget=BUDGET)["table_counts"] == COUNTS


def test_an_unsupported_result_contract_is_refused_before_it_is_sealed(tmp_path: Path) -> None:
    fx = prepared(tmp_path)
    home = fx.home
    with open_workspace(home, writable=True) as workspace:
        handle = open_run(workspace, intent(fx, "run-1"))
        unsupported = canonical(
            {
                "module": "aegis",
                "input_sha256": ENVELOPE_SHA256,
                "result": {"positions": []},
            }
        )
        with pytest.raises(RunStorageError, match="unsupported backtest result contract"):
            commit_run(workspace, handle, RunResult(unsupported), budget=BUDGET)
        assert not (workspace.paths.runs / "run-1" / "backtest.json").exists()
    assert observed(home, "run-1") == ("RUNNING", "PREPARED")


def test_verification_rejects_a_successful_run_whose_stored_rows_changed(tmp_path: Path) -> None:
    fx = prepared(tmp_path)
    home = fx.home
    with open_workspace(home, writable=True) as workspace:
        commit_run(
            workspace, open_run(workspace, intent(fx, "run-1")), RunResult(RESULT), budget=BUDGET
        )
    alter_market(home, "UPDATE equity_points SET cash=? WHERE ordinal=0", [Decimal("7.5")])
    with open_workspace(home) as workspace:
        with pytest.raises(ValueError, match="disagree with the sealed evidence"):
            verify_workspace(workspace, budget=BUDGET)
        with pytest.raises(ValueError, match="disagree with the sealed evidence"):
            read_run(workspace, "run-1", budget=BUDGET)


def linked_preparation(request_hash: str, envelope_bytes: bytes) -> bytes:
    """A preparation document shaped like the real aas-prepared-backtest-v1 sidecar."""
    return canonical(
        {
            "schema": "aas-prepared-backtest-v1",
            "hash_format": HASH_FORMAT,
            "request_hash": request_hash,
            "envelope_sha256": hashlib.sha256(envelope_bytes).hexdigest(),
        }
    )


def test_a_preparation_from_another_request_is_never_sealed(tmp_path: Path) -> None:
    fx = prepared(tmp_path)
    home = fx.home
    foreign = canonical(
        {
            "schema": "aas-prepared-backtest-v1",
            "hash_format": HASH_FORMAT,
            "request_hash": "c" * 64,
            "envelope_sha256": hashlib.sha256(ENVELOPE).hexdigest(),
        }
    )
    with open_workspace(home, writable=True) as workspace:
        with pytest.raises(RunStorageError, match="preparation request names different evidence"):
            open_run(workspace, replace(intent(fx, "run-1"), preparation_bytes=foreign))
        assert not (workspace.paths.runs / "run-1").exists()
        other = canonical({"module": "aegis", "targets": {"2024-01-02": {"ZZZ": 1.0}}})
        with pytest.raises(RunStorageError, match="preparation envelope names different evidence"):
            open_run(
                workspace,
                replace(
                    intent(fx, "run-1"),
                    preparation_bytes=linked_preparation(fx.request_hash, other),
                ),
            )
    assert observed(home, "run-1") == (None, None)


def test_a_result_computed_from_another_envelope_is_refused(tmp_path: Path) -> None:
    fx = prepared(tmp_path)
    home = fx.home
    other = canonical({"module": "aegis", "targets": {"2024-01-02": {"ZZZ": 1.0}}})
    mismatched = canonical(
        {
            "module": "aegis",
            "input_sha256": hashlib.sha256(other).hexdigest(),
            "result": {"nav": NAV, "fills": FILLS},
        }
    )
    with open_workspace(home, writable=True) as workspace:
        handle = open_run(
            workspace,
            replace(
                intent(fx, "run-1"), preparation_bytes=linked_preparation(fx.request_hash, ENVELOPE)
            ),
        )
        with pytest.raises(RunStorageError, match="backtest envelope names different evidence"):
            commit_run(workspace, handle, RunResult(mismatched), budget=BUDGET)
        assert not (workspace.paths.runs / "run-1" / "backtest.json").exists()
    assert observed(home, "run-1") == ("RUNNING", "PREPARED")


def test_a_result_from_another_module_is_refused(tmp_path: Path) -> None:
    fx = prepared(tmp_path)
    home = fx.home
    envelope = canonical({"module": "aegis", "targets": {"2024-01-02": {"AAA": 1.0}}})
    foreign = canonical(
        {
            "module": "hedge",
            "input_sha256": hashlib.sha256(envelope).hexdigest(),
            "result": {"nav": NAV, "fills": FILLS},
        }
    )
    with open_workspace(home, writable=True) as workspace:
        handle = open_run(
            workspace,
            replace(
                intent(fx, "run-1"),
                envelope_bytes=envelope,
                preparation_bytes=preparation_document(fx.request_hash, envelope),
            ),
        )
        with pytest.raises(RunStorageError, match="module conflicts with the envelope"):
            commit_run(workspace, handle, RunResult(foreign), budget=BUDGET)
        assert not (workspace.paths.runs / "run-1" / "backtest.json").exists()


def test_a_rejected_result_leaves_the_run_open_for_a_corrected_retry(tmp_path: Path) -> None:
    fx = prepared(tmp_path)
    home = fx.home
    malformed = canonical(
        {
            "module": "aegis",
            "input_sha256": ENVELOPE_SHA256,
            "result": {"nav": [], "fills": "bad"},
        }
    )
    with open_workspace(home, writable=True) as workspace:
        handle = open_run(workspace, intent(fx, "run-1"))
        with pytest.raises(RunStorageError, match="fills must be an array"):
            commit_run(workspace, handle, RunResult(malformed), budget=BUDGET)
        assert not (workspace.paths.runs / "run-1" / "backtest.json").exists()
        # The corrected result is not blocked by an artifact the rejected one left behind.
        committed = commit_run(workspace, handle, RunResult(RESULT), budget=BUDGET)
    assert committed["table_counts"] == COUNTS


def test_a_retry_cannot_substitute_a_different_preparation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fx = prepared(tmp_path)
    home = fx.home
    sealed: list[str] = []
    original = _seal

    def once(workspace: Workspace, run_id: str, name: str, raw: bytes) -> str:
        if sealed:
            raise RuntimeError("interrupted between seals")
        sealed.append(name)
        return original(workspace, run_id, name, raw)

    monkeypatch.setattr("aegis_alpha.storage.runs._seal", once)
    with (
        open_workspace(home, writable=True) as workspace,
        pytest.raises(RuntimeError, match="between seals"),
    ):
        open_run(workspace, intent(fx, "run-1"))
    monkeypatch.undo()
    substitute = canonical(
        {
            "schema": "aas-prepared-backtest-v1",
            "hash_format": HASH_FORMAT,
            "request_hash": fx.request_hash,
            "envelope_sha256": ENVELOPE_SHA256,
            "note": "substituted",
        }
    )
    with open_workspace(home, writable=True) as workspace:
        # The intent commits both input digests, so the accepted preparation cannot be
        # swapped by a retry that reuses the envelope.
        with pytest.raises(ValueError, match="already identifies a different request"):
            open_run(workspace, replace(intent(fx, "run-1"), preparation_bytes=substitute))
        assert open_run(workspace, intent(fx, "run-1")).run_id == "run-1"


def test_provenance_identities_must_match_the_registered_request(tmp_path: Path) -> None:
    fx = prepared(tmp_path)
    with open_workspace(fx.home, writable=True) as workspace:
        for wrong in (
            replace(intent(fx, "run-1"), engine_hash="e" * 64),
            replace(intent(fx, "run-1"), environment_hash="f" * 64),
        ):
            with pytest.raises(RunStorageError, match="does not match the registered request"):
                open_run(workspace, wrong)
    assert observed(fx.home, "run-1") == (None, None)


def test_a_run_id_cannot_escape_the_runs_directory(tmp_path: Path) -> None:
    fx = prepared(tmp_path)
    with open_workspace(fx.home, writable=True) as workspace:
        for unsafe in ("../escape", "with/slash", " leading", ".hidden"):
            with pytest.raises(RunStorageError, match="run_id"):
                open_run(workspace, replace(intent(fx, "run-1"), run_id=unsafe))


def test_strategy_pins_must_match_the_registered_request(tmp_path: Path) -> None:
    fx = prepared(tmp_path)
    admitted = fx.pin
    with open_workspace(fx.home, writable=True) as workspace:
        for wrong in (
            replace(admitted, raw_hash="a" * 64),
            replace(admitted, store_id="elsewhere"),
            replace(admitted, version="2"),
        ):
            with pytest.raises(RunStorageError, match="do not match the registered request"):
                open_run(workspace, intent(fx, "run-1", pins=(wrong,)))
        with pytest.raises(RunStorageError, match="do not match the registered request"):
            open_run(workspace, intent(fx, "run-1", pins=()))
    assert observed(fx.home, "run-1") == (None, None)


def test_a_pin_the_private_store_never_admitted_is_refused(tmp_path: Path) -> None:
    fx = prepared(tmp_path)
    with open_workspace(fx.home, writable=True) as workspace:
        # The second line of defence: even a pin the request agrees with must name a
        # version the private store actually holds.
        with pytest.raises(RunStorageError, match="does not match an admitted strategy version"):
            _require_admitted_pins(workspace, (replace(fx.pin, raw_hash="a" * 64),))
        with pytest.raises(RunStorageError, match="names a different strategy store"):
            _require_admitted_pins(workspace, (replace(fx.pin, store_id="elsewhere"),))


def test_an_exact_integer_is_not_rounded_through_binary64(tmp_path: Path) -> None:
    fx = prepared(tmp_path)
    home = fx.home
    exact = 9007199254740993
    nav: list[dict[str, object]] = [
        {"date": "2024-01-02", "equity": 1000, "cash": 0},
        {"date": "2024-01-03", "equity": exact, "cash": 0},
    ]
    with open_workspace(home, writable=True) as workspace:
        committed = commit_run(
            workspace,
            open_run(workspace, intent(fx, "run-1")),
            RunResult(backtest(nav, [])),
            budget=BUDGET,
        )
    metrics = cast("dict[str, dict[str, object]]", committed["metrics"])
    # binary64 cannot hold this integer; routing it through float would store ...992.
    assert metrics["final_equity"]["value"] == "9007199254740993.000000000000"
    with open_workspace(home) as workspace:
        stored = workspace.market.execute(
            "SELECT equity FROM equity_points WHERE run_id='run-1' ORDER BY ordinal"
        ).fetchall()
        assert stored[-1][0] == Decimal("9007199254740993.000000000000")


def test_a_tampered_metric_reference_is_refused(tmp_path: Path) -> None:
    fx = prepared(tmp_path)
    home = fx.home
    with open_workspace(home, writable=True) as workspace:
        commit_run(
            workspace, open_run(workspace, intent(fx, "run-1")), RunResult(RESULT), budget=BUDGET
        )
    with open_workspace(home, writable=True) as workspace:
        # The immutable trigger blocks UPDATE, so the row is replaced through the
        # same path an attacker with database access would have to use.
        workspace.state.execute("PRAGMA writable_schema=ON")
        workspace.state.execute("DROP TRIGGER immutable_run_metrics_update")
        workspace.state.execute("PRAGMA writable_schema=OFF")
        workspace.state.execute(
            "UPDATE run_metrics SET risk_free_ref='invented' WHERE metric='final_equity'"
        )
        workspace.state.commit()
    with (
        open_workspace(home) as workspace,
        pytest.raises(RunStorageError, match="recorded metrics disagree"),
    ):
        read_run(workspace, "run-1", budget=BUDGET)


def test_a_tampered_module_manifest_is_refused(tmp_path: Path) -> None:
    fx = prepared(tmp_path)
    home = fx.home
    with open_workspace(home, writable=True) as workspace:
        commit_run(
            workspace, open_run(workspace, intent(fx, "run-1")), RunResult(RESULT), budget=BUDGET
        )
    with open_workspace(home, writable=True) as workspace:
        workspace.state.execute("PRAGMA writable_schema=ON")
        workspace.state.execute("DROP TRIGGER immutable_module_manifests_update")
        workspace.state.execute("PRAGMA writable_schema=OFF")
        workspace.state.execute("UPDATE module_manifests SET row_count=row_count+1")
        workspace.state.commit()
    with open_workspace(home) as workspace:
        with pytest.raises(RunStorageError, match="module manifest disagrees"):
            read_run(workspace, "run-1", budget=BUDGET)
        with pytest.raises(ValueError, match="module manifest disagrees"):
            verify_workspace(workspace, budget=BUDGET)


def test_recovery_charges_its_reads_without_a_caller_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fx = prepared(tmp_path)
    home = fx.home
    committed_run(fx, monkeypatch)
    seen: list[object] = []
    original = _stored_rows

    def record(
        workspace: Workspace, run_id: str, budget: ComputeBudget | None = None
    ) -> dict[str, list[dict[str, object]]]:
        seen.append(budget)
        return original(workspace, run_id, budget)

    monkeypatch.setattr("aegis_alpha.storage.runs._stored_rows", record)
    with open_workspace(home, writable=True) as workspace:
        assert recover_operations(workspace)["recovered"] == ["run:run-1"]
    monkeypatch.undo()
    assert seen != []
    # The CLI hands down no budget, so recovery supplies one rather than read unchecked.
    assert None not in seen


def test_a_pin_filed_under_another_module_is_refused(tmp_path: Path) -> None:
    fx = prepared(tmp_path)
    with open_workspace(fx.home, writable=True) as workspace:
        for misplaced in (replace(fx.pin, module="hedge"), replace(fx.pin, ordinal=99)):
            with pytest.raises(RunStorageError, match="not placed on the run module"):
                open_run(workspace, intent(fx, "run-1", pins=(misplaced,)))
    assert observed(fx.home, "run-1") == (None, None)


def test_a_date_the_columns_cannot_hold_is_refused_before_sealing(tmp_path: Path) -> None:
    fx = prepared(tmp_path)
    ancient: list[dict[str, object]] = [
        {
            "decision_date": "1969-12-30",
            "execution_date": "1969-12-31",
            "symbol": "AAA",
            "shares": 1.0,
            "price": 1.0,
            "fee": 0.0,
        }
    ]
    with open_workspace(fx.home, writable=True) as workspace:
        handle = open_run(workspace, intent(fx, "run-1"))
        with pytest.raises(RunStorageError, match="before 1970 are not storable"):
            commit_run(workspace, handle, RunResult(backtest(NAV, ancient)), budget=BUDGET)
        # The constraint is met before sealing, so a corrected result can still commit.
        assert not (workspace.paths.runs / "run-1" / "backtest.json").exists()
        assert (
            commit_run(workspace, handle, RunResult(RESULT), budget=BUDGET)["status"] == "SUCCESS"
        )


def test_an_orphan_trade_decision_row_is_refused(tmp_path: Path) -> None:
    fx = prepared(tmp_path)
    with open_workspace(fx.home, writable=True) as workspace:
        commit_run(
            workspace, open_run(workspace, intent(fx, "run-1")), RunResult(RESULT), budget=BUDGET
        )
    alter_market(
        fx.home,
        "INSERT INTO result_trade_decisions(run_id,module,ordinal,decision_at_us) VALUES (?,?,?,?)",
        ["run-1", "aegis", 99, 0],
    )
    with open_workspace(fx.home) as workspace:
        with pytest.raises(RunStorageError, match="trade decision rows disagree"):
            read_run(workspace, "run-1", budget=BUDGET)
        with pytest.raises(ValueError, match="trade decision rows disagree"):
            verify_workspace(workspace, budget=BUDGET)


def test_provenance_rewritten_while_running_is_refused(tmp_path: Path) -> None:
    fx = prepared(tmp_path)
    with open_workspace(fx.home, writable=True) as workspace:
        handle = open_run(workspace, intent(fx, "run-1"))
        # completed_run only freezes a run once it stops being RUNNING, so the identity
        # columns really are writable at this point.
        workspace.state.execute("UPDATE runs SET engine_hash=? WHERE run_id=?", ("e" * 64, "run-1"))
        workspace.state.commit()
        commit_run(workspace, handle, RunResult(RESULT), budget=BUDGET)
    with open_workspace(fx.home) as workspace:
        with pytest.raises(RunStorageError, match="engine identity disagrees"):
            read_run(workspace, "run-1", budget=BUDGET)
        with pytest.raises(ValueError, match="engine identity disagrees"):
            verify_workspace(workspace, budget=BUDGET)


def test_a_corrupted_result_hash_fails_verification_not_only_reads(tmp_path: Path) -> None:
    fx = prepared(tmp_path)
    with open_workspace(fx.home, writable=True) as workspace:
        commit_run(
            workspace, open_run(workspace, intent(fx, "run-1")), RunResult(RESULT), budget=BUDGET
        )
    with open_workspace(fx.home, writable=True) as workspace:
        workspace.state.execute("PRAGMA writable_schema=ON")
        workspace.state.execute("DROP TRIGGER completed_run")
        workspace.state.execute("PRAGMA writable_schema=OFF")
        workspace.state.execute("UPDATE runs SET result_hash=? WHERE run_id=?", ("0" * 64, "run-1"))
        workspace.state.commit()
    with open_workspace(fx.home) as workspace:
        # Verification must refuse it too, or backup certifies what read_run rejects.
        with pytest.raises(ValueError, match="result hash disagrees"):
            verify_workspace(workspace, budget=BUDGET)
        with pytest.raises(RunStorageError, match="result hash disagrees"):
            read_run(workspace, "run-1", budget=BUDGET)


def test_oversized_run_inputs_are_refused_before_decoding(tmp_path: Path) -> None:
    fx = prepared(tmp_path)
    with (
        open_workspace(fx.home, writable=True) as workspace,
        pytest.raises(ComputeResourceError, match="run inputs exceed"),
    ):
        open_run(workspace, intent(fx, "run-1"), budget=TIGHT)
    assert observed(fx.home, "run-1") == (None, None)


def test_a_run_cannot_be_its_own_predecessor(tmp_path: Path) -> None:
    fx = prepared(tmp_path)
    with (
        open_workspace(fx.home, writable=True) as workspace,
        pytest.raises(RunStorageError, match="its own predecessor"),
    ):
        open_run(workspace, intent(fx, "run-1", prior_run_id="run-1"))
    assert observed(fx.home, "run-1") == (None, None)


@pytest.mark.parametrize(
    ("column", "value"),
    [("reason", "invented"), ("created_at_us", 1), ("prior_run_id", None)],
)
def test_metadata_rewritten_while_running_is_refused(
    tmp_path: Path, column: str, value: object
) -> None:
    fx = prepared(tmp_path)
    with open_workspace(fx.home, writable=True) as workspace:
        first = open_run(workspace, intent(fx, "run-1"))
        commit_run(workspace, first, RunResult(RESULT), budget=BUDGET)
        handle = open_run(workspace, intent(fx, "run-2", prior_run_id="run-1"))
        # completed_run leaves these columns writable while the run is RUNNING.
        workspace.state.execute(
            "UPDATE runs SET " + column + "=? WHERE run_id=?",  # noqa: S608
            (value, "run-2"),
        )
        workspace.state.commit()
        commit_run(workspace, handle, RunResult(RESULT), budget=BUDGET)
    with open_workspace(fx.home) as workspace:
        with pytest.raises(RunStorageError, match="metadata disagrees with its opening evidence"):
            read_run(workspace, "run-2", budget=BUDGET)
        with pytest.raises(ValueError, match="metadata disagrees with its opening evidence"):
            verify_workspace(workspace, budget=BUDGET)


def test_an_unbounded_reason_is_refused(tmp_path: Path) -> None:
    fx = prepared(tmp_path)
    with (
        open_workspace(fx.home, writable=True) as workspace,
        pytest.raises(RunStorageError, match="reason is too large"),
    ):
        open_run(workspace, replace(intent(fx, "run-1"), reason="x" * 5000))
    assert observed(fx.home, "run-1") == (None, None)


def test_a_rewritten_bundle_identity_is_refused(tmp_path: Path) -> None:
    fx = prepared(tmp_path)
    with open_workspace(fx.home, writable=True) as workspace:
        handle = open_run(workspace, intent(fx, "run-1"))
        # bundle_id stays writable while the run is RUNNING and has no copy in runs,
        # so the durable intent is the only immutable record of it.
        columns = [
            row[1]
            for row in workspace.state.execute("PRAGMA table_info(input_bundles)")
            if row[1] != "bundle_id"
        ]
        listed = ",".join(columns)
        workspace.state.execute(
            "INSERT INTO input_bundles(bundle_id," + listed + ") "  # noqa: S608
            "SELECT 'b-other'," + listed + " FROM input_bundles WHERE bundle_id='b-empty'"
        )
        workspace.state.execute("UPDATE runs SET bundle_id='b-other' WHERE run_id='run-1'")
        workspace.state.commit()
        commit_run(workspace, handle, RunResult(RESULT), budget=BUDGET)
    with (
        open_workspace(fx.home) as workspace,
        pytest.raises(RunStorageError, match="disagree with the durable intent"),
    ):
        read_run(workspace, "run-1", budget=BUDGET)
