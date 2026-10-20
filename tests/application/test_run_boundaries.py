"""Direct CLI/API evidence for the integration boundaries tracked in #15 and #124."""

from __future__ import annotations

import json
import multiprocessing
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from multiprocessing.connection import Connection
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from aegis_alpha.application import run_backtest as strict_module
from aegis_alpha.application import run_research as declared_module
from aegis_alpha.application.cli import main
from aegis_alpha.application.run_backtest import RunBacktestRequest, run_backtest
from aegis_alpha.application.run_research import RunResearchRequest, run_research
from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.engine import serialize_bundle
from aegis_alpha.storage.paths import load_paths
from aegis_alpha.storage.strategies import load_strategy
from aegis_alpha.storage.strategy_import import register_strategy
from aegis_alpha.storage.workspace import open_workspace
from tests.application.test_prepare_cli import register_fixture, sha
from tests.application.test_run_backtest import case, template  # noqa: F401 -- shared fixture
from tests.application.test_run_research import (  # noqa: F401 -- shared fixtures
    _as_sleeve_run,
    _composition,
    _installed,
    _write,
    composed,
    compute_environment,
    sample,
)
from tests.application.test_storage_cli import run_cli
from tests.engine.test_requirements import rich_contract

type Document = dict[str, Any]


@dataclass
class RunCase:
    home: Path
    path: Path
    route: str

    @property
    def module(self) -> ModuleType:
        return strict_module if self.route == "strict" else declared_module

    def arguments(self) -> list[str]:
        verb, option = (
            ("execute", "--request") if self.route == "strict" else ("research", "--declaration")
        )
        return [
            "--home",
            str(self.home),
            "run",
            verb,
            option,
            str(self.path),
            "--sha256",
            sha(self.path.read_bytes()),
        ]

    def execute(self) -> Document:
        if self.route == "strict":
            return run_backtest(
                RunBacktestRequest(self.path, sha(self.path.read_bytes()), home=self.home)
            )
        return run_research(
            RunResearchRequest(self.path, sha(self.path.read_bytes()), home=self.home)
        )


@pytest.fixture(params=["strict", "sleeve", "composition"])
def boundary(request: pytest.FixtureRequest, tmp_path: Path) -> RunCase:
    route = str(request.param)
    if route == "strict":
        home, path = request.getfixturevalue("case")
    else:
        home, base, offense, defense = request.getfixturevalue("sample")
        body = (
            _as_sleeve_run(base, offense)
            if route == "sleeve"
            else _composition(base, offense, defense)
        )
        path = _write(tmp_path, "declaration.json", body)
    _installed(home)
    return RunCase(home, path, route)


def _hold(home: Path, connection: Connection) -> None:
    with open_workspace(home, writable=True):
        connection.send("locked")
        connection.recv()
    connection.close()


@contextmanager
def held_installation(home: Path) -> Iterator[None]:
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe()
    process = context.Process(target=_hold, args=(home, child))
    process.start()
    child.close()
    try:
        assert parent.poll(20), "lock holder did not become ready"
        assert parent.recv() == "locked"
        yield
    finally:
        parent.send("release")
        process.join(10)
        if process.is_alive():
            process.terminate()
            process.join(10)
        parent.close()
        assert not process.is_alive()
        assert process.exitcode == 0
        process.close()


def _empty(home: Path) -> None:
    with open_workspace(home) as workspace:
        assert workspace.state.execute("SELECT count(*) FROM runs").fetchone()[0] == 0
        assert workspace.state.execute("SELECT count(*) FROM input_bundles").fetchone()[0] == 0
        assert workspace.market.execute("SELECT count(*) FROM result_commits").fetchall()[0][0] == 0


def test_other_process_holds_installation_busy_for_cli_and_api(boundary: RunCase) -> None:
    with held_installation(boundary.home):
        result = run_cli(*boundary.arguments(), home=boundary.home)
        assert result.returncode == 1
        assert result.stdout == ""
        assert json.loads(result.stderr).keys() == {"error"}
        assert "installation_busy" in json.loads(result.stderr)["error"]
        with pytest.raises(RuntimeError, match="installation_busy"):
            boundary.execute()
    _empty(boundary.home)
    # Releasing the real process restores the normal path and its locks are reusable.
    assert boundary.execute()["run"]["status"] == "SUCCESS"
    assert run_cli("db", "verify", home=boundary.home).returncode == 0


def _alter(boundary: RunCase, fault: str) -> None:
    body = json.loads(boundary.path.read_bytes())
    dataset = (
        next(pin["ref_id"] for pin in body["bindings"] if pin["role"] == "signal_prices")
        if boundary.route == "strict"
        else body["observations"][0]["dataset_id"]
    )
    with open_workspace(boundary.home, writable=True) as workspace:
        if fault == "generation_pin":
            workspace.market.execute(
                "UPDATE market_generations SET chain_hash=? WHERE dataset_id=?",
                ["0" * 64, dataset],
            )
            return
        digest = workspace.state.execute(
            "SELECT transform_hash FROM dataset_versions WHERE dataset_id=?", (dataset,)
        ).fetchone()[0]
    # Replace the exact retained transform of one used input, not the request or
    # an unrelated file. This is between calculation and the commit admission.
    path = load_paths(boundary.home).raw / digest[:2] / digest
    assert path.is_file()
    path.chmod(0o600)
    path.write_bytes(b"{}")


@pytest.mark.parametrize("surface", ["api", "cli"])
@pytest.mark.parametrize("fault", ["generation_pin", "retained_file"])
def test_changed_inputs_between_preparation_and_commit_never_succeed(
    boundary: RunCase,
    surface: str,
    fault: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    genuine = boundary.module.run_document
    calls = []

    def changed(raw: bytes, digest: str) -> Document:
        response = genuine(raw, digest)
        # This synchronous hook runs after preparation/open_run and before record.
        # Opening the writable workspace also proves calculation holds no DB lock.
        _alter(boundary, fault)
        calls.append(True)
        return response

    monkeypatch.setattr(boundary.module, "run_document", changed)
    if surface == "api":
        with pytest.raises(ValueError, match=r"pin|hash|generation|publication|content"):
            boundary.execute()
    else:
        assert main(boundary.arguments()) == 1
        captured = capsys.readouterr()
        assert captured.out == ""
        diagnostic = json.loads(captured.err)
        assert any(
            word in diagnostic["error"]
            for word in ("pin", "hash", "generation", "publication", "content")
        )
        assert "ended FAILED" in diagnostic["notes"][0]
    assert calls == [True]
    with open_workspace(boundary.home) as workspace:
        assert [row[0] for row in workspace.state.execute("SELECT status FROM runs")] == ["FAILED"]
        assert workspace.market.execute("SELECT count(*) FROM result_commits").fetchall()[0][0] == 0


def _equity_case(boundary: RunCase) -> None:
    root = boundary.path.parent / "equity"
    root.mkdir()
    home = root / "home"

    def cli(*arguments: str) -> Document:
        args = list(arguments)
        if args[:2] in (["data", "register-prices"], ["data", "binding-import"]):
            path = Path(args[args.index("--spec") + 1])
            document = json.loads(path.read_bytes())
            for instrument in document.get("instruments", []):
                if instrument["instrument_id"] == "ASSET_A":
                    instrument["asset_type"] = "equity"
            path.write_bytes(canonical_json_bytes(document))
            args[args.index("--sha256") + 1] = sha(path.read_bytes())
        result = run_cli(*args, home=home)
        assert result.returncode == 0, result.stderr
        return json.loads(result.stdout)

    boundary.home, boundary.path = register_fixture(root, cli)
    _installed(boundary.home)


def _unsupported(boundary: RunCase) -> None:
    if boundary.route == "strict":
        # Register a real synthetic equity input, preserving all immutable evidence.
        # The generic strategy store admits it; strict run accounting requires ETFs.
        _equity_case(boundary)
    body = json.loads(boundary.path.read_bytes())
    pin = (
        body["composition"]["sleeves"]["offense"]
        if boundary.route == "composition"
        else body["strategy"]
    )
    with open_workspace(boundary.home, writable=True, strategy_write=True) as workspace:
        assert workspace.strategies is not None
        original = load_strategy(
            workspace.strategies, pin["strategy_id"], pin["version"], pin["raw_sha256"]
        )
        contract = (
            original.contract
            if boundary.route == "strict"
            else replace(
                original.contract,
                derived_series=rich_contract().derived_series,
                macro_signals=rich_contract().macro_signals,
            )
        )
        raw = serialize_bundle(replace(original, bundle_version="2", contract=contract))
        path = boundary.path.parent / "unsupported.json"
        path.write_bytes(raw)
        registered = register_strategy(workspace, path, sha(raw), original.bundle_id, "2")
        # Prove registration and lookup succeeded; this is not a missing version.
        loaded = load_strategy(
            workspace.strategies, original.bundle_id, "2", str(registered["raw_sha256"])
        )
        assert loaded.bundle_version == "2"
        pin.update({key: registered[key] for key in ("version", "raw_sha256", "contract_sha256")})
    boundary.path.write_bytes(canonical_json_bytes(body))


def test_registered_but_unsupported_strategy_is_distinct_from_a_missing_version(
    boundary: RunCase,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _unsupported(boundary)
    with pytest.raises(ValueError, match=r"classified ETF|requires derived inputs") as refused:
        boundary.execute()
    assert "not registered" not in str(refused.value)
    assert main(boundary.arguments()) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert json.loads(captured.err)["error"] == str(refused.value)
    _empty(boundary.home)
