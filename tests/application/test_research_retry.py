"""Failed declared attempts retain evidence and can complete through an explicit retry."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import replace
from typing import TYPE_CHECKING, Any, cast

import pytest

from aegis_alpha.application import run_research as research_module
from aegis_alpha.application.cli import main
from aegis_alpha.application.run_research import RunResearchRequest, run_research
from aegis_alpha.storage import runs
from aegis_alpha.storage.paths import load_paths
from aegis_alpha.storage.runs import read_run
from aegis_alpha.storage.workspace import open_workspace
from tests.application.test_prepare_cli import sha
from tests.application.test_run_research import (  # noqa: F401 -- shared synthetic fixture
    _as_sleeve_run,
    _cli,
    _command,
    _composition,
    _installed,
    _write,
    composed,
    compute_environment,
)

if TYPE_CHECKING:
    from pathlib import Path

type Document = dict[str, Any]

_CRASH_EXIT = 71


def test_failed_calculation_retries_without_replacing_its_evidence(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    home, base, offense, defense = request.getfixturevalue("composed")
    _installed(home)
    path = _write(tmp_path, "declaration.json", _composition(base, offense, defense))
    wanted = RunResearchRequest(path, sha(path.read_bytes()), home=home)

    def fail(*_args: object) -> Document:
        raise RuntimeError("one transient calculation failure")

    with monkeypatch.context() as patch:
        patch.setattr(research_module, "run_document", fail)
        with pytest.raises(RuntimeError, match="one transient"):
            run_research(wanted)
    with open_workspace(home) as workspace:
        first = dict(workspace.state.execute("SELECT * FROM runs").fetchone())
        events = [tuple(row) for row in workspace.state.execute("SELECT * FROM run_events")]
    assert first["status"] == "FAILED"
    run_id = first["run_id"]
    sealed = {file.name: file.read_bytes() for file in (load_paths(home).runs / run_id).iterdir()}
    with pytest.raises(ValueError, match="different calculation or bundle"):
        run_research(replace(wanted, prior_run_id=run_id, bundle_id="different-input-bundle"))
    receipt = cast("Document", run_research(replace(wanted, prior_run_id=run_id)))
    assert receipt["run"]["status"] == "SUCCESS"
    assert receipt["run"]["run_id"] != run_id
    with open_workspace(home) as workspace:
        assert read_run(workspace, receipt["run"]["run_id"])["prior_run_id"] == run_id
        assert (
            dict(workspace.state.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone())
            == first
        )
        assert [
            tuple(row)
            for row in workspace.state.execute("SELECT * FROM run_events WHERE run_id=?", (run_id,))
        ] == events
    assert {
        file.name: file.read_bytes() for file in (load_paths(home).runs / run_id).iterdir()
    } == sealed
    assert json.loads(sealed["preparation.json"])


def test_a_different_calculation_keeps_existing_predecessor_link_semantics(
    request: pytest.FixtureRequest,
    tmp_path: Path,
) -> None:
    home, base, offense, defense = request.getfixturevalue("composed")
    _installed(home)
    sleeve = _write(tmp_path, "sleeve.json", _as_sleeve_run(base, offense))
    composition = _write(tmp_path, "composition.json", _composition(base, offense, defense))
    first = cast(
        "Document", run_research(RunResearchRequest(sleeve, sha(sleeve.read_bytes()), home=home))
    )
    prior = first["run"]["run_id"]
    second = cast(
        "Document",
        run_research(
            RunResearchRequest(
                composition,
                sha(composition.read_bytes()),
                home=home,
                prior_run_id=prior,
            )
        ),
    )
    assert first["calculation_id"] != second["calculation_id"]
    assert second["run"]["run_id"] == second["calculation_id"]
    with open_workspace(home) as workspace:
        assert read_run(workspace, second["run"]["run_id"])["prior_run_id"] == prior


@pytest.mark.parametrize("surface", ["api", "cli"])
@pytest.mark.parametrize("phase", ["calculation", "before_marker"])
@pytest.mark.parametrize("shape", ["sleeve", "composition"])
def test_retry_chain_preserves_failed_attempts_and_refuses_duplicate_success(  # noqa: PLR0913, PLR0917 -- surfaces, phases and contracts
    request: pytest.FixtureRequest,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    surface: str,
    phase: str,
    shape: str,
) -> None:

    home, base, offense, defense = request.getfixturevalue("composed")
    _installed(home)
    body = (
        _as_sleeve_run(base, offense) if shape == "sleeve" else _composition(base, offense, defense)
    )
    path = _write(tmp_path, "retry.json", body)
    wanted = RunResearchRequest(path, sha(path.read_bytes()), home=home)

    def execute(prior: str | None = None) -> Document:
        if surface == "api":
            return run_research(replace(wanted, prior_run_id=prior))
        args = () if prior is None else ("--prior-run-id", prior)
        code = main(_command(home, path, *args))
        captured = capsys.readouterr()
        if code:
            assert code == 1
            assert captured.out == ""
            raise ValueError(json.loads(captured.err)["error"])
        assert captured.err == ""
        return json.loads(captured.out)

    def fail(*_args: object, **_kwargs: object) -> Document:
        raise ValueError("transient before marker")

    prior = None
    frozen = {}
    # Retry a failed retry too: one deterministic successor per predecessor.
    for _ in range(2):
        with monkeypatch.context() as patch:
            patch.setattr(
                research_module if phase == "calculation" else runs,
                "run_document" if phase == "calculation" else "_write_marker",
                fail,
            )
            with pytest.raises(ValueError, match="transient before marker"):
                execute(prior)
        with open_workspace(home) as workspace:
            row = workspace.state.execute(
                "SELECT run_id FROM runs WHERE prior_run_id IS ?", (prior,)
            ).fetchone()
            prior = row[0]
            frozen[prior] = (
                dict(
                    workspace.state.execute(
                        "SELECT * FROM runs WHERE run_id=?", (prior,)
                    ).fetchone()
                ),
                [
                    tuple(item)
                    for item in workspace.state.execute(
                        "SELECT * FROM run_events WHERE run_id=?", (prior,)
                    )
                ],
                {
                    file.name: file.read_bytes()
                    for file in (load_paths(home).runs / prior).iterdir()
                },
            )
            assert (
                workspace.market.execute("SELECT count(*) FROM result_commits").fetchall()[0][0]
                == 0
            )
    receipt = execute(prior)
    attempt = receipt["run"]["run_id"]
    assert receipt["run"]["status"] == "SUCCESS"
    assert receipt["calculation_id"] == next(iter(frozen))
    reproduction = research_module.rerun_research_run(
        attempt, home=home, declaration=path, declaration_sha256=sha(path.read_bytes())
    )
    assert reproduction["result_reproduced"] is True
    assert reproduction["identity_reproduced"] is True
    with open_workspace(home) as workspace:
        assert read_run(workspace, attempt)["prior_run_id"] == prior
        for run_id, (record, events, files) in frozen.items():
            assert (
                dict(
                    workspace.state.execute(
                        "SELECT * FROM runs WHERE run_id=?", (run_id,)
                    ).fetchone()
                )
                == record
            )
            assert [
                tuple(item)
                for item in workspace.state.execute(
                    "SELECT * FROM run_events WHERE run_id=?", (run_id,)
                )
            ] == events
            assert {
                file.name: file.read_bytes() for file in (load_paths(home).runs / run_id).iterdir()
            } == files

    def no_calculation(*_args: object) -> Document:
        pytest.fail("duplicate attempt reached calculation")

    monkeypatch.setattr(research_module, "run_document", no_calculation)
    for duplicate in (None, *frozen, attempt):
        with pytest.raises(
            ValueError, match=r"finished run|already has an attempt|FAILED or INTERRUPTED"
        ):
            execute(duplicate)
    with open_workspace(home) as workspace:
        assert workspace.state.execute("SELECT count(*) FROM runs").fetchone()[0] == len(frozen) + 1
        assert workspace.market.execute("SELECT count(*) FROM result_commits").fetchall()[0][0] == 1


@pytest.mark.parametrize("surface", ["api", "cli"])
@pytest.mark.parametrize("phase", ["calculation", "after_marker"])
def test_process_exit_recovers_before_retry_and_never_recalculates_a_marker(  # noqa: PLR0913, PLR0917 -- crash phases and surfaces
    request: pytest.FixtureRequest,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    surface: str,
    phase: str,
) -> None:

    home, base, offense, defense = request.getfixturevalue("composed")
    _installed(home)
    path = _write(tmp_path, "crash.json", _composition(base, offense, defense))
    script = """
import os, sys
from pathlib import Path
from aegis_alpha.application import run_research as module
from aegis_alpha.application.cli import main
from aegis_alpha.storage import runs

def crash(*args, **kwargs):
    os._exit(71)

if sys.argv[1] == "calculation":
    module.run_document = crash
else:
    runs._finish = crash
raise SystemExit(main(sys.argv[2:]))
"""
    child = subprocess.run(  # noqa: S603 -- fixed interpreter, synthetic fixture only
        [sys.executable, "-c", script, phase, *_command(home, path)],
        env={**os.environ},
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert child.returncode == _CRASH_EXIT, child.stderr
    assert child.stdout == ""
    with open_workspace(home) as workspace:
        run_id, status = workspace.state.execute("SELECT run_id,status FROM runs").fetchone()
        assert status == "RUNNING"
        marker_count = workspace.market.execute("SELECT count(*) FROM result_commits").fetchall()[
            0
        ][0]
        assert marker_count == (phase == "after_marker")
    wanted = RunResearchRequest(path, sha(path.read_bytes()), home=home, prior_run_id=run_id)

    def forbidden(*_args: object) -> Document:
        pytest.fail("recovery or a duplicate attempt recalculated")

    def refused(message: str) -> None:
        if surface == "api":
            with pytest.raises(ValueError, match=message):
                run_research(wanted)
        else:
            assert main(_command(home, path, "--prior-run-id", run_id)) == 1
            captured = capsys.readouterr()
            assert captured.out == ""
            assert message in json.loads(captured.err)["error"]

    with monkeypatch.context() as patch:
        patch.setattr(research_module, "run_document", forbidden)
        refused("recover RUNNING")
        # Exercise recovery in this process so the calculation tripwire applies.
        assert main(["--home", str(home), "db", "recover"]) == 0
        assert json.loads(capsys.readouterr().out)["pending"] == []
        if phase == "after_marker":
            refused("FAILED or INTERRUPTED")
    if phase == "calculation":
        with open_workspace(home) as workspace:
            assert workspace.state.execute("SELECT status FROM runs").fetchone()[0] == "INTERRUPTED"
        if surface == "api":
            receipt = cast("Document", run_research(wanted))
        else:
            receipt = _cli(home, *_command(home, path, "--prior-run-id", run_id))
        assert receipt["run"]["status"] == "SUCCESS"
        assert receipt["run"]["run_id"] != run_id
    else:
        assert _cli(home, "run", "show", "--run-id", run_id)["run"]["status"] == "SUCCESS"
    assert _cli(home, "db", "verify")["verified"] is True
