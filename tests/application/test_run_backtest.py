"""Integrated run regressions: one execution path for the CLI and the Python API.

Every installation here is registered through the real CLI on a synthetic home, and
every envelope comes from the real export path, so the sealed-session rules are
exercised by construction rather than by a hand-built minimal document.
"""

from __future__ import annotations

import json
import shutil
import sqlite3
from pathlib import Path
from typing import Any, cast

import pytest

from aegis_alpha.application import run_backtest as run_module
from aegis_alpha.application.cli import main
from aegis_alpha.application.run_backtest import RunBacktestRequest, run_backtest
from aegis_alpha.compute_resources import ComputeBudget
from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.storage.paths import load_paths
from aegis_alpha.storage.run_schema import inspect_run_schema
from aegis_alpha.storage.runs import RunStorageError, list_runs, read_run
from aegis_alpha.storage.workspace import open_workspace
from tests.application.test_prepare_cli import assert_accounting, register_fixture, sha
from tests.application.test_storage_cli import run_cli

type Document = dict[str, Any]

_SESSIONS = 5
_FILLS = 3
_TARGET_ROWS = 2
_IDENTITY_READS = 2
_EXPECTED_COUNTS = {
    "equity_points": _SESSIONS,
    "positions": 0,
    "signals": 0,
    "simulated_trades": _FILLS,
    "target_weights": _TARGET_ROWS,
}


@pytest.fixture(autouse=True)
def compute_environment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AAS_HOST_CPU_LIMIT", "1")
    monkeypatch.setenv("AAS_HOST_MEMORY_LIMIT_BYTES", str(1024 * 1024 * 1024))
    monkeypatch.setenv("AAS_CPU_LIMIT", "1")
    monkeypatch.setenv("AAS_MEMORY_LIMIT_BYTES", str(512 * 1024 * 1024))
    monkeypatch.setenv("AAS_COMPUTE_LOCK_FILE", str(tmp_path / "compute.lock"))


@pytest.fixture(scope="module")
def template(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, Path]:
    """One installation registered entirely through the shipped CLI, copied per test."""
    root = tmp_path_factory.mktemp("run-integration")
    seeded = root / "home"

    def cli(*args: str) -> Document:
        result = run_cli(*args, home=seeded)
        assert result.returncode == 0, result.stdout + result.stderr
        return cast("Document", json.loads(result.stdout))

    with pytest.MonkeyPatch.context() as patch:
        patch.setenv("AAS_HOST_CPU_LIMIT", "1")
        patch.setenv("AAS_HOST_MEMORY_LIMIT_BYTES", str(1024 * 1024 * 1024))
        patch.setenv("AAS_CPU_LIMIT", "1")
        patch.setenv("AAS_MEMORY_LIMIT_BYTES", str(512 * 1024 * 1024))
        patch.setenv("AAS_COMPUTE_LOCK_FILE", str(root / "compute.lock"))
        home, request = register_fixture(root, cli)
    return home, request


@pytest.fixture
def case(template: tuple[Path, Path], tmp_path: Path) -> tuple[Path, Path]:
    """A private copy, so one test never observes another test's recorded runs."""
    source_home, source_request = template
    home = tmp_path / "home"
    shutil.copytree(source_home, home)
    request = tmp_path / "request.json"
    request.write_bytes(source_request.read_bytes())
    return home, request


def _cli(home: Path, *args: str) -> Document:
    result = run_cli(*args, home=home)
    assert result.returncode == 0, result.stdout + result.stderr
    return cast("Document", json.loads(result.stdout))


def _arguments(request: Path, *extra: str) -> tuple[str, ...]:
    return (
        "run",
        "execute",
        "--request",
        str(request),
        "--sha256",
        sha(request.read_bytes()),
        *extra,
    )


def _api(home: Path, request: Path, **extra: object) -> Document:
    return cast(
        "Document",
        run_backtest(
            RunBacktestRequest(
                request=request,
                request_sha256=sha(request.read_bytes()),
                home=home,
                **cast("Any", extra),
            )
        ),
    )


def _statuses(home: Path) -> list[str]:
    with open_workspace(home) as workspace:
        return [str(row["status"]) for row in list_runs(workspace)]


def _schema(home: Path) -> str:
    with open_workspace(home) as workspace:
        return inspect_run_schema(workspace).state


def _comparable(receipt: Document) -> Document:
    """Everything two runs of the same request must agree on: run identity is excluded."""
    body = json.loads(json.dumps(receipt, sort_keys=True))
    del body["run"]["run_id"]
    body.pop("exported", None)
    return cast("Document", body)


def test_clean_installation_runs_and_reads_the_result_back(
    case: tuple[Path, Path], tmp_path: Path
) -> None:
    home, request = case
    # The add-on is absent, so the command must name its install and compute nothing.
    assert _schema(home) == "absent"
    refused = run_cli(*_arguments(request), home=home)
    assert refused.returncode == 1
    assert not refused.stdout
    assert "run-install" in json.loads(refused.stderr)["error"]
    assert _schema(home) == "absent"

    assert _cli(home, "db", "run-install")["state"] == "complete"
    output = tmp_path / "envelope.json"
    receipt = _cli(home, *_arguments(request, "--envelope-output", str(output)))

    assert receipt["executed"] is True
    assert receipt["certified"] is False
    assert receipt["backtest"]["live_orders"] is False
    assert receipt["backtest"]["point_in_time_verified"] is False
    assert_accounting(receipt["backtest"])
    assert receipt["target_weights"] == {
        "2026-01-29": {"ASSET_A": 1.0},
        "2026-02-26": {"ASSET_B": 1.0},
    }
    assert receipt["run"]["status"] == "SUCCESS"
    assert receipt["run"]["module"] == "aegis"
    assert receipt["run"]["research_only"] is True
    assert receipt["run"]["table_counts"] == _EXPECTED_COUNTS
    assert receipt["run"]["metrics"]["final_equity"]["value"] == "80.000000000000"
    assert receipt["run"]["metrics"]["total_return"]["value"] == "-0.200000000000"
    assert receipt["run"]["metrics"]["sharpe"]["value_state"] == "not_collected"
    # The reported accounting is the sealed artifact itself, not a separate copy.
    assert (
        sha(canonical_json_bytes(receipt["backtest"]))
        == receipt["run"]["artifacts"]["backtest.json"]
    )

    for key in ("envelope", "preparation"):
        exported = Path(receipt["exported"][key]["path"])
        assert sha(exported.read_bytes()) == receipt["exported"][key]["sha256"]
        assert receipt["exported"][key]["sha256"] == receipt[key]["sha256"]
    assert output.exists()

    # Re-read in a fresh process: the record, its marker and its files must still agree.
    shown = _cli(home, "run", "show", "--run-id", receipt["run"]["run_id"])["run"]
    assert shown["result_hash"] == receipt["run"]["result_hash"]
    assert shown["table_hashes"] == receipt["run"]["table_hashes"]
    assert shown["artifacts"] == receipt["run"]["artifacts"]
    assert shown["request_hash"] == receipt["request_hash"]
    assert shown["bundle_id"] == receipt["bundle_id"]
    assert shown["reason"] == "integrated prepared backtest"
    assert [pin["strategy_id"] for pin in shown["strategy_pins"]] == ["synthetic-probe"]

    listed = _cli(home, "run", "list")["runs"]
    assert [row["run_id"] for row in listed] == [receipt["run"]["run_id"]]
    # Nothing is still held: verification and a second run both succeed afterwards.
    assert _cli(home, "db", "verify")["verified"] is True
    again = _cli(home, *_arguments(request))
    assert again["run"]["run_id"] != receipt["run"]["run_id"]
    assert again["run"]["result_hash"] == receipt["run"]["result_hash"]


def _broken(body: Document, fault: str) -> Document:
    if fault == "unsupported_strategy":
        body["strategy"]["version"] = "999"
    elif fault == "wrong_pin":
        binding = body["bindings"][0]
        binding["hash"] = "0" * len(binding["hash"])
    else:
        body["strategy"]["raw_sha256"] = "0" * len(body["strategy"]["raw_sha256"])
    return body


@pytest.mark.parametrize("fault", ["unsupported_strategy", "wrong_pin", "wrong_strategy_hash"])
def test_refused_request_records_no_run(
    case: tuple[Path, Path], tmp_path: Path, fault: str
) -> None:
    home, request = case
    _cli(home, "db", "run-install")
    broken = tmp_path / "broken-request.json"
    broken.write_bytes(canonical_json_bytes(_broken(json.loads(request.read_bytes()), fault)))
    result = run_cli(*_arguments(broken), home=home)
    assert result.returncode == 1
    assert not result.stdout
    assert json.loads(result.stderr).keys() == {"error"}
    assert _statuses(home) == []


def test_declared_hash_must_match_the_request_bytes(case: tuple[Path, Path]) -> None:
    home, request = case
    _cli(home, "db", "run-install")
    arguments = list(_arguments(request))
    arguments[arguments.index("--sha256") + 1] = sha(b"")
    result = run_cli(*arguments, home=home)
    assert result.returncode == 1
    assert not result.stdout
    assert "SHA-256" in json.loads(result.stderr)["error"]
    assert _statuses(home) == []


def test_identity_change_after_preparation_fails_the_run(
    case: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    home, request = case
    _cli(home, "db", "run-install")
    genuine = run_module._installation  # noqa: SLF001 -- staged identity injection point
    seen: list[object] = []

    def moved(workspace: object) -> tuple[str, str, str, str | None]:
        seen.append(workspace)
        recorded = genuine(cast("Any", workspace))
        if len(seen) == 1:
            return recorded
        return (recorded[0], "a-different-state-store", recorded[2], recorded[3])

    monkeypatch.setattr(run_module, "_installation", moved)
    with pytest.raises(ValueError, match="installation identity changed"):
        _api(home, request)
    assert len(seen) == _IDENTITY_READS
    assert _statuses(home) == ["FAILED"]
    with open_workspace(home) as workspace:
        recorded = list_runs(workspace)
        with pytest.raises(RunStorageError, match="no readable result"):
            read_run(workspace, str(recorded[0]["run_id"]))


def test_calculation_failure_ends_the_run_without_a_marker(
    case: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    home, request = case
    _cli(home, "db", "run-install")

    def refuse(*_args: object, **_kwargs: object) -> dict[str, object]:
        raise ValueError("injected accounting failure")

    monkeypatch.setattr(run_module, "run_document", refuse)
    with pytest.raises(ValueError, match="injected accounting failure"):
        _api(home, request)
    assert _statuses(home) == ["FAILED"]
    with open_workspace(home) as workspace:
        reasons = [
            str(row[0])
            for row in workspace.state.execute("SELECT reason FROM run_events WHERE kind='failed'")
        ]
    assert len(reasons) == 1
    assert "calculation produced no result" in reasons[0]


def test_interrupted_run_is_recovered_as_interrupted(
    case: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    home, request = case
    _cli(home, "db", "run-install")

    def lost(*_args: object, **_kwargs: object) -> dict[str, object]:
        raise RuntimeError("process lost before the commit")

    # A crash ends no run: the intent and the RUNNING row are all that survive.
    monkeypatch.setattr(run_module, "_abandon", lambda *_args: "left RUNNING by the test")
    monkeypatch.setattr(run_module, "_record", lost)
    with pytest.raises(RuntimeError, match="process lost"):
        _api(home, request)
    assert _statuses(home) == ["RUNNING"]
    recovery = _cli(home, "db", "recover")
    assert recovery["pending"] == []
    assert len(recovery["recovered"]) == 1
    assert _statuses(home) == ["INTERRUPTED"]


def test_recomputation_links_a_new_run_to_its_predecessor(case: tuple[Path, Path]) -> None:
    home, request = case
    _cli(home, "db", "run-install")
    first = _api(home, request)
    second = _api(home, request, prior_run_id=first["run"]["run_id"])
    assert second["run"]["run_id"] != first["run"]["run_id"]
    assert second["run"]["result_hash"] == first["run"]["result_hash"]
    shown = _cli(home, "run", "show", "--run-id", second["run"]["run_id"])["run"]
    assert shown["prior_run_id"] == first["run"]["run_id"]


def test_two_requests_over_the_same_pinned_inputs_both_run(
    case: tuple[Path, Path], tmp_path: Path
) -> None:
    """A changed request keeps the same bindings, so the default bundle name must differ.

    Storage holds exactly one immutable request per bundle. Naming the bundle after the
    bindings alone would make the second request fail on a stored-request mismatch.
    """
    home, request = case
    _cli(home, "db", "run-install")
    first = _cli(home, *_arguments(request))
    body = json.loads(request.read_bytes())
    body["account"]["initial_cash"] = 250.0
    changed = tmp_path / "changed-request.json"
    changed.write_bytes(canonical_json_bytes(body))

    second = _cli(home, *_arguments(changed))
    assert second["request_hash"] != first["request_hash"]
    assert second["bundle_id"] != first["bundle_id"]
    assert second["run"]["result_hash"] != first["run"]["result_hash"]
    # Same decisions over the same pinned inputs, scaled by the larger account.
    assert second["target_weights"] == first["target_weights"]
    assert second["run"]["metrics"]["total_return"]["value"] == "-0.200000000000"
    assert sorted(_statuses(home)) == ["SUCCESS", "SUCCESS"]


class _UnprintableError(Exception):
    """A failure that cannot describe itself; naming it must not raise in a cleanup."""

    def __str__(self) -> str:
        raise RuntimeError("this failure cannot describe itself")


@pytest.mark.parametrize(
    ("cleanup_error", "expected"),
    [
        (RunStorageError("a run with a committed marker must be recovered"), "was left for"),
        (OSError("injected cleanup failure"), "could not be ended"),
        (TypeError("injected cleanup bug"), "could not be ended"),
        (KeyboardInterrupt(), "could not be ended"),
        (_UnprintableError(), "<unprintable>"),
    ],
    ids=["refusal", "storage_fault", "cleanup_bug", "interrupt", "unprintable"],
)
def test_a_failed_cleanup_never_replaces_the_original_failure(
    case: tuple[Path, Path],
    monkeypatch: pytest.MonkeyPatch,
    cleanup_error: BaseException,
    expected: str,
) -> None:
    """The caller keeps its own error, and still learns what became of the run."""
    home, request = case
    _cli(home, "db", "run-install")

    def refuse(*_args: object, **_kwargs: object) -> dict[str, object]:
        raise ValueError("injected accounting failure")

    def unavailable(*_args: object, **_kwargs: object) -> None:
        raise cleanup_error

    monkeypatch.setattr(run_module, "run_document", refuse)
    monkeypatch.setattr(run_module, "fail_run", unavailable)
    with pytest.raises(ValueError, match="injected accounting failure") as failure:
        _api(home, request)
    notes = getattr(failure.value, "__notes__", [])
    assert len(notes) == 1
    assert expected in notes[0]
    assert "aas db recover" in notes[0]
    # Nothing was force-failed, so the run stays discoverable to recovery.
    assert _statuses(home) == ["RUNNING"]


def test_cli_rejects_an_unknown_run_id(case: tuple[Path, Path]) -> None:
    home, _request = case
    _cli(home, "db", "run-install")
    result = run_cli("run", "show", "--run-id", "run-absent", home=home)
    assert result.returncode == 1
    assert not result.stdout
    assert "no such run" in json.loads(result.stderr)["error"]


def test_later_stages_reserve_what_preparation_keeps_live(
    case: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A stage that runs beside a resident preparation must not get the full allowance."""
    home, request = case
    _cli(home, "db", "run-install")
    genuine_open = run_module._open  # noqa: SLF001 -- stage budget observation point
    genuine_record = run_module._record  # noqa: SLF001 -- stage budget observation point
    seen: dict[str, int] = {}

    def watched_open(root: Path, opening: object, wanted: object, budget: ComputeBudget) -> object:
        opening_any = cast("Any", opening)
        seen["held"] = (
            len(opening_any.projection_bytes)
            + len(opening_any.envelope_bytes)
            + len(opening_any.provenance_bytes)
        )
        seen["envelope"] = len(opening_any.envelope_bytes)
        seen["open"] = budget.reserved_bytes
        return genuine_open(root, cast("Any", opening), cast("Any", wanted), budget)

    def watched_record(
        root: Path, opened: object, result: object, request_hash: str, budget: ComputeBudget
    ) -> object:
        seen["result"] = len(cast("Any", result).backtest_bytes)
        seen["record"] = budget.reserved_bytes
        return genuine_record(root, cast("Any", opened), cast("Any", result), request_hash, budget)

    monkeypatch.setattr(run_module, "_open", watched_open)
    monkeypatch.setattr(run_module, "_record", watched_record)
    receipt = _api(home, request)

    assert receipt["run"]["status"] == "SUCCESS"
    assert seen["held"] > 0
    assert seen["result"] > 0
    # The preparation is released before stage A, so stage A carries only the documents
    # it reads, and the commit narrows further to the envelope and the result.
    assert seen["open"] == seen["held"]
    assert seen["record"] == seen["envelope"] + seen["result"]


def test_an_export_cannot_target_the_managed_runs_directory(case: tuple[Path, Path]) -> None:
    """An export inside the runs root could become the run's own immutable artifact."""
    home, request = case
    _cli(home, "db", "run-install")
    managed = load_paths(home).runs / "run-collide"
    managed.mkdir(parents=True, exist_ok=True)
    result = run_cli(
        *_arguments(
            request,
            "--run-id",
            "run-collide",
            "--envelope-output",
            str(managed / "envelope.json"),
        ),
        home=home,
    )
    assert result.returncode == 1
    assert not result.stdout
    assert "managed runs directory" in json.loads(result.stderr)["error"]
    assert not (managed / "envelope.json").exists()
    assert _statuses(home) == []


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("run_id", "not a plain identifier", "plain identifier"),
        ("reason", "x" * 5000, "too large"),
        ("prior_run_id", "run-never-recorded", "no recorded run"),
    ],
)
def test_a_bad_run_field_registers_nothing(
    case: tuple[Path, Path], field: str, value: str, reason: str
) -> None:
    """A run-only field must be refused before stage A binds a bundle name for good."""
    home, request = case
    _cli(home, "db", "run-install")
    option = "--" + field.replace("_", "-")
    result = run_cli(*_arguments(request, option, value), home=home)
    assert result.returncode == 1
    assert not result.stdout
    assert reason in json.loads(result.stderr)["error"]
    assert _statuses(home) == []
    with open_workspace(home) as workspace:
        registered = workspace.state.execute("SELECT count(*) FROM backtest_requests").fetchone()
        bundles = workspace.state.execute("SELECT count(*) FROM input_bundles").fetchone()
    assert registered[0] == 0
    assert bundles[0] == 0


@pytest.mark.parametrize("cleanup_error", [TypeError, OSError])
def test_cli_failure_reports_what_happened_to_the_run(
    case: tuple[Path, Path],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    cleanup_error: type[Exception],
) -> None:
    """A cleanup that cannot finish must never replace the failure the operator needs."""
    home, request = case
    _cli(home, "db", "run-install")

    def refuse(*_args: object, **_kwargs: object) -> dict[str, object]:
        raise ValueError("injected accounting failure")

    def unavailable(*_args: object, **_kwargs: object) -> None:
        raise cleanup_error("injected cleanup failure")

    monkeypatch.setattr(run_module, "run_document", refuse)
    monkeypatch.setattr(run_module, "fail_run", unavailable)
    assert main(["--home", str(home), *_arguments(request)]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    diagnostic = json.loads(captured.err)
    assert diagnostic["error"] == "injected accounting failure"
    assert len(diagnostic["notes"]) == 1
    assert "could not be ended" in diagnostic["notes"][0]
    assert "aas db recover" in diagnostic["notes"][0]
    assert _statuses(home) == ["RUNNING"]


@pytest.mark.parametrize("stage", ["calculation", "commit"])
def test_a_driver_fault_keeps_its_run_diagnostic(
    case: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch, stage: str
) -> None:
    """Translating a driver fault into its operator message must not drop the run note."""
    home, request = case
    _cli(home, "db", "run-install")

    def refuse(*_args: object, **_kwargs: object) -> dict[str, object]:
        raise sqlite3.OperationalError("injected driver fault")

    monkeypatch.setattr(
        run_module, "run_document" if stage == "calculation" else "commit_run", refuse
    )
    with pytest.raises(ValueError, match="local database operation failed") as failure:
        _api(home, request)
    notes = getattr(failure.value, "__notes__", [])
    assert len(notes) == 1
    assert "ended FAILED" in notes[0]
    assert _statuses(home) == ["FAILED"]


def test_main_returns_the_same_failure_code_in_process(
    case: tuple[Path, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    home, request = case
    assert main(["--home", str(home), *_arguments(request)]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "run-install" in json.loads(captured.err)["error"]


def test_cli_and_api_are_one_execution_path(case: tuple[Path, Path], tmp_path: Path) -> None:
    home, request = case
    _cli(home, "db", "run-install")
    api_output = tmp_path / "api-envelope.json"
    cli_output = tmp_path / "cli-envelope.json"
    through_api = _api(home, request, envelope_output=api_output)
    through_cli = _cli(home, *_arguments(request, "--envelope-output", str(cli_output)))

    assert through_api["run"]["run_id"] != through_cli["run"]["run_id"]
    assert _comparable(through_api) == _comparable(through_cli)
    # The two exports are separate files holding byte-identical evidence.
    for key in ("envelope", "preparation"):
        assert through_api["exported"][key]["path"] != through_cli["exported"][key]["path"]
        assert through_api["exported"][key]["sha256"] == through_cli["exported"][key]["sha256"]
    assert api_output.read_bytes() == cli_output.read_bytes()
    assert sorted(_statuses(home)) == ["SUCCESS", "SUCCESS"]


@pytest.mark.parametrize("leaf", ["envelope.json", "envelope.json.preparation.json"])
def test_an_existing_export_path_is_never_overwritten(
    case: tuple[Path, Path], tmp_path: Path, leaf: str
) -> None:
    """The export follows the same no-overwrite rule as `aas prepare`, and runs nothing."""
    home, request = case
    _cli(home, "db", "run-install")
    occupied = tmp_path / leaf
    occupied.write_bytes(b"foreign content")
    result = run_cli(
        *_arguments(request, "--envelope-output", str(tmp_path / "envelope.json")), home=home
    )
    assert result.returncode == 1
    assert not result.stdout
    assert json.loads(result.stderr).keys() == {"error"}
    assert occupied.read_bytes() == b"foreign content"
    exports = {"envelope.json", "envelope.json.preparation.json"}
    assert {path.name for path in tmp_path.iterdir()} & exports == {leaf}
    assert _statuses(home) == []


def test_no_implicit_compute_budget(
    case: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """An unconfigured host gets no silent default budget, and records no run."""
    home, request = case
    _cli(home, "db", "run-install")
    for name in (
        "AAS_HOST_CPU_LIMIT",
        "AAS_HOST_MEMORY_LIMIT_BYTES",
        "AAS_CPU_LIMIT",
        "AAS_MEMORY_LIMIT_BYTES",
        "AAS_COMPUTE_LOCK_FILE",
    ):
        monkeypatch.delenv(name)
    result = run_cli(*_arguments(request), home=home)
    assert result.returncode == 1
    assert not result.stdout
    assert "compute budget" in json.loads(result.stderr)["error"]
    assert _statuses(home) == []


def test_empty_schedule_with_cashflows_records_a_no_fill_result(
    case: tuple[Path, Path], tmp_path: Path
) -> None:
    home, request = case
    _cli(home, "db", "run-install")
    body = json.loads(request.read_bytes())
    body["explicit_decision_dates"] = []
    body["envelope"]["schema_version"] = "aas-etf-backtest-v2"
    body["account"]["cashflows"] = [{"date": "2026-02-02", "amount": 20}]
    supported = tmp_path / "cashflow-request.json"
    supported.write_bytes(canonical_json_bytes(body))

    receipt = _cli(home, *_arguments(supported))
    account = receipt["backtest"]["result"]["account"]
    assert account["fills"] == []
    assert account["nav"][-1] == {
        "date": "2026-03-30",
        "equity": 120.0,
        "cash": 120.0,
        "fee": 0.0,
    }
    assert "withdrawals use existing cash only" in receipt["backtest"]["cashflow_convention"]
    assert receipt["target_weights"] == {}
    assert receipt["run"]["table_counts"]["simulated_trades"] == 0
    assert receipt["run"]["table_counts"]["target_weights"] == 0
    assert receipt["run"]["metrics"]["final_equity"]["value"] == "120.000000000000"
    # The account grew only by its contribution, so the unit value did not move.
    assert receipt["run"]["metrics"]["total_return"]["value_state"] == "present"
    shown = _cli(home, "run", "show", "--run-id", receipt["run"]["run_id"])["run"]
    assert shown["result_hash"] == receipt["run"]["result_hash"]


def test_a_run_cannot_be_its_own_predecessor(case: tuple[Path, Path]) -> None:
    """The store refuses a self-reference only after stage A registers, so refuse it first."""
    home, request = case
    _cli(home, "db", "run-install")
    arguments = _arguments(request, "--run-id", "run-self", "--prior-run-id", "run-self")
    result = run_cli(*arguments, home=home)
    assert result.returncode == 1
    assert not result.stdout
    assert "own predecessor" in json.loads(result.stderr)["error"]
    assert _statuses(home) == []
    with open_workspace(home) as workspace:
        bundles = workspace.state.execute("SELECT count(*) FROM input_bundles").fetchone()
    assert bundles[0] == 0


def test_the_parsed_request_is_handed_over_not_retained(
    case: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Stage 0 takes the decoded request; no outer frame keeps it for the rest of the run."""
    home, request = case
    _cli(home, "db", "run-install")
    genuine = run_module._staged  # noqa: SLF001 -- carrier handover observation point
    seen: dict[str, int] = {}

    def watched(
        root: Path, carrier: object, wanted: object, budget: object, output: object
    ) -> dict[str, object]:
        slot = cast("list[object]", carrier)
        seen["before"] = len(slot)
        result = genuine(
            root,
            cast("Any", carrier),
            cast("Any", wanted),
            cast("Any", budget),
            cast("Any", output),
        )
        seen["after"] = len(slot)
        return result

    monkeypatch.setattr(run_module, "_staged", watched)
    receipt = _api(home, request)

    assert receipt["run"]["status"] == "SUCCESS"
    assert seen["before"] == 1
    assert seen["after"] == 0
