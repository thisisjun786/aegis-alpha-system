from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from dataclasses import replace
from fractions import Fraction
from pathlib import Path
from typing import cast

import pytest

from aegis_alpha.compute_resources import ComputeBudget
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
    commit_run,
    fail_run,
    list_runs,
    open_run,
    read_run,
)
from aegis_alpha.storage.verification import verify_workspace
from aegis_alpha.storage.workspace import initialize, open_workspace

BUDGET = ComputeBudget(Fraction(1), 512 * 1024 * 1024)
HASH_FORMAT = "aas-canonical-json-sha256-v1"
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
REQUEST = canonical(
    {
        "schema": "aas-backtest-request-v1",
        "hash_format": HASH_FORMAT,
        "strategy": {},
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
        "engine": {},
        "environment": {},
    }
)
REQUEST_HASH = hashlib.sha256(REQUEST).hexdigest()
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


def states(payload: dict[str, object]) -> dict[str, dict[str, object]]:
    """Reduce a read payload to the metric values and their states."""
    metrics = cast("dict[str, dict[str, object]]", payload["metrics"])
    return {
        name: {"value": entry["value"], "value_state": entry["value_state"]}
        for name, entry in metrics.items()
    }


ENVELOPE = canonical({"targets": {"2024-01-02": {"AAA": 1.0}, "2024-01-03": {"AAA": 0.5}}})
PREPARATION = canonical({"schema": "aas-backtest-preparation-v1"})
NAV: list[dict[str, object]] = [
    {"date": "2024-01-02", "equity": 1000.0, "cash": 0.0},
    {"date": "2024-01-03", "equity": 1100.0, "cash": 10.0},
]
FILLS: list[dict[str, object]] = [
    {"execution_date": "2024-01-02", "symbol": "AAA", "shares": 10.0, "price": 100.0, "fee": 1.0},
    {"execution_date": "2024-01-03", "symbol": "BBB", "shares": 2.0, "price": 50.0, "fee": 0.5},
]


def backtest(nav: list[dict[str, object]], fills: list[dict[str, object]]) -> bytes:
    return canonical({"module": "aegis", "result": {"nav": nav, "fills": fills}})


RESULT = backtest(NAV, FILLS)


def prepared(home: Path) -> Path:
    """An installation whose bundle and request already admit one run."""
    initialize(home)
    install_run_schema(home)
    with open_workspace(home, writable=True) as workspace:
        pin = register_input_bundle(
            workspace,
            BUNDLE,
            expected_file_sha256=hashlib.sha256(BUNDLE).hexdigest(),
            budget=BUDGET,
        )
        register_backtest_request(
            workspace, pin, REQUEST, expected_request_hash=REQUEST_HASH, budget=BUDGET
        )
    return home


def intent(run_id: str, *, prior_run_id: str | None = None) -> RunIntent:
    return RunIntent(
        request_hash=REQUEST_HASH,
        bundle_id="b-empty",
        engine_hash="e" * 64,
        environment_hash="f" * 64,
        reason="synthetic run",
        envelope_bytes=ENVELOPE,
        preparation_bytes=PREPARATION,
        strategy_pins=(RunStrategyPin("aegis", 0, "store", "strategy", "1", "a" * 64, "b" * 64),),
        prior_run_id=prior_run_id,
        run_id=run_id,
    )


def observed(home: Path, run_id: str) -> tuple[str, str | None]:
    with open_workspace(home) as workspace:
        run = workspace.state.execute(
            "SELECT status FROM runs WHERE run_id=?", (run_id,)
        ).fetchone()
        operation = workspace.state.execute(
            "SELECT phase FROM storage_operations WHERE target_id=?", (run_id,)
        ).fetchone()
        return run[0], None if operation is None else operation[0]


def test_committed_run_reads_back_identically_in_a_separate_process(tmp_path: Path) -> None:
    home = prepared(tmp_path / "home")
    with open_workspace(home, writable=True) as workspace:
        handle = open_run(workspace, intent("run-1"))
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
            "strategy_store_id": "store",
            "strategy_id": "strategy",
            "version": "1",
            "raw_hash": "a" * 64,
            "contract_hash": "b" * 64,
        }
    ]
    with open_workspace(home) as workspace:
        assert verify_workspace(workspace, budget=BUDGET)["verified"] is True
        assert [row["status"] for row in list_runs(workspace)] == ["SUCCESS"]


def test_row_order_in_the_result_does_not_move_a_receipt_or_an_ordinal(tmp_path: Path) -> None:
    home = prepared(tmp_path / "home")
    shuffled = backtest(list(reversed(NAV)), list(reversed(FILLS)))
    with open_workspace(home, writable=True) as workspace:
        first = commit_run(
            workspace, open_run(workspace, intent("run-1")), RunResult(RESULT), budget=BUDGET
        )
        second = commit_run(
            workspace,
            open_run(workspace, intent("run-2", prior_run_id="run-1")),
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
    home = prepared(tmp_path / "home")
    with open_workspace(home, writable=True) as workspace:
        open_run(workspace, intent("run-1"))
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
    home = prepared(tmp_path / "home")
    with open_workspace(home, writable=True) as workspace:
        handle = open_run(workspace, intent("run-1"))
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
    home = prepared(tmp_path / "home")
    with open_workspace(home, writable=True) as workspace:
        handle = open_run(workspace, intent("run-1"))
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
    home = prepared(tmp_path / "home")
    with open_workspace(home, writable=True) as workspace:
        handle = open_run(workspace, intent("run-1"))
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
    home = prepared(tmp_path / "home")
    with open_workspace(home, writable=True) as workspace:
        handle = open_run(workspace, intent("run-1"))
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
    home = prepared(tmp_path / "home")
    with (
        open_workspace(home, writable=True) as workspace,
        pytest.raises(RunStorageError, match="registered request"),
    ):
        open_run(workspace, replace(intent("run-1"), request_hash="c" * 64))
    with open_workspace(home, writable=True) as workspace:
        workspace.state.execute("BEGIN IMMEDIATE")
        with pytest.raises(RunStorageError, match="another state transaction"):
            open_run(workspace, intent("run-1"))
        workspace.state.rollback()


def test_a_tampered_artifact_refuses_to_read_back(tmp_path: Path) -> None:
    home = prepared(tmp_path / "home")
    with open_workspace(home, writable=True) as workspace:
        handle = open_run(workspace, intent("run-1"))
        commit_run(workspace, handle, RunResult(RESULT), budget=BUDGET)
        sealed = workspace.paths.runs / "run-1" / "backtest.json"
    sealed.write_bytes(backtest(NAV, FILLS[:1]))
    with open_workspace(home) as workspace:
        with pytest.raises(RunStorageError, match="disagrees with the sealed evidence"):
            read_run(workspace, "run-1", budget=BUDGET)
        with pytest.raises(ValueError, match="run artifact hash/size mismatch"):
            verify_workspace(workspace, budget=BUDGET)
