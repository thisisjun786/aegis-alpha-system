"""The declared research lifecycle as an installed command, not as callable internals.

AAS-9 built the declared contract, its execution and its durable record, and AAS-10
widened the run store to hold it, but nothing a user installs could drive that sequence:
every step was reachable only from a test calling it directly. These regressions run the
whole lifecycle through the shipped surface instead, both contracts, and hold the
recorded run to the same refusals the strict path keeps on the same data.

Every installation is synthetic and thrown away. Nothing here certifies a calculation,
and the claims the receipt makes about itself are all negative by design.
"""

from __future__ import annotations

import json
import shutil
from typing import TYPE_CHECKING, Any, cast

import pytest

from aegis_alpha.application import run_research as research_module
from aegis_alpha.application.cli import main
from aegis_alpha.application.research_run import (
    EXECUTION_MODE,
    RESEARCH_COMPOSITION_SCHEMA,
    RESEARCH_RUN_SCHEMA,
    SWITCH_RULE,
)
from aegis_alpha.application.run_research import (
    RunResearchRequest,
    rerun_research_run,
    run_research,
)
from aegis_alpha.data.serialization import canonical_json_bytes, content_sha256
from aegis_alpha.storage.backtest_requests import read_backtest_request
from aegis_alpha.storage.input_pins import InputBundleRef
from aegis_alpha.storage.market_inputs import GenerationPin, admit_native_input
from aegis_alpha.storage.workspace import open_workspace
from tests.application.test_backtest_prepare import BUDGET
from tests.application.test_prepare_cli import sha
from tests.application.test_research_composition import (  # noqa: F401 -- shared fixtures
    _as_sleeve_run,
    _composition,
    composed,
    compute_environment,
)
from tests.application.test_storage_cli import run_cli
from tests.storage.test_run_migration import install_v1

if TYPE_CHECKING:
    from pathlib import Path

type Document = dict[str, Any]


@pytest.fixture
def sample(request: pytest.FixtureRequest) -> tuple[Path, Document, Document, Document]:
    """The composition installation AAS-11 already builds, requested indirectly.

    Asked for by name so the imported fixture is reused rather than rebuilt, and so no
    test parameter shadows the name this module imported it under.
    """
    return cast("tuple[Path, Document, Document, Document]", request.getfixturevalue("composed"))


def _cli(home: Path, *args: str) -> Document:
    """A shipped command in its own process, which is what an operator actually runs."""
    result = run_cli(*args, home=home)
    assert result.returncode == 0, result.stdout + result.stderr
    return cast("Document", json.loads(result.stdout))


def _installed(home: Path) -> None:
    _cli(home, "db", "run-install")


def _write(root: Path, name: str, declaration: Document) -> Path:
    """Write the declaration exactly as its author would hand it over."""
    path = root / name
    _ = path.write_bytes(canonical_json_bytes(declaration))
    return path


def _command(home: Path, path: Path, *extra: str) -> list[str]:
    return [
        "--home",
        str(home),
        "run",
        "research",
        "--declaration",
        str(path),
        "--sha256",
        sha(path.read_bytes()),
        *extra,
    ]


def _output(capsys: pytest.CaptureFixture[str]) -> Document:
    return cast("Document", json.loads(capsys.readouterr().out))


def _execute(home: Path, path: Path, capsys: pytest.CaptureFixture[str], *extra: str) -> Document:
    assert main(_command(home, path, *extra)) == 0
    return _output(capsys)


def _show(home: Path, run_id: str, capsys: pytest.CaptureFixture[str]) -> Document:
    assert main(["--home", str(home), "run", "show", "--run-id", run_id]) == 0
    return _output(capsys)


def _rerun(home: Path, run_id: str, capsys: pytest.CaptureFixture[str], *extra: str) -> Document:
    assert main(["--home", str(home), "run", "rerun", "--run-id", run_id, *extra]) == 0
    return _output(capsys)


def _run_id(receipt: Document) -> str:
    return str(cast("Document", receipt["run"])["run_id"])


def _assert_claims_nothing(document: Document) -> None:
    """Every statement a declared record makes about its own standing is negative."""
    assert document["certified"] is False
    assert document["non_executable"] is True
    assert document["executable_prices"] is False
    assert document["point_in_time_certified"] is False
    assert document["observed_prices_verified"] is False
    assert document["research_only"] is True
    assert document["source_parity"] == "unknown"
    assert document["execution_mode"] == EXECUTION_MODE


def test_a_declared_sleeve_run_is_recorded_and_requeried_through_the_installed_commands(
    sample: tuple[Path, Document, Document, Document],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The acceptance this slice exists for: one command drives the whole lifecycle."""
    home, base, offense, _defense = sample
    _installed(home)
    path = _write(tmp_path, "sleeve.json", _as_sleeve_run(base, offense))
    receipt = _execute(home, path, capsys)

    assert receipt["executed"] is True
    assert receipt["request_schema"] == RESEARCH_RUN_SCHEMA
    assert receipt["scope"] == "sleeve"
    assert receipt["declaration_sha256"] == sha(path.read_bytes())
    assert receipt["composition"] is None
    _assert_claims_nothing(receipt)
    recorded = cast("Document", receipt["run"])
    assert recorded["status"] == "SUCCESS"
    # A sleeve run binds its one membership, and that binding is the whole bundle.
    assert cast("Document", receipt["bindings"])["membership_bound"] is True
    assert cast("Document", receipt["bindings"])["bound_roles"] == ["membership"]

    # Requeried by the stable identity, from the same installed command any run uses.
    payload = cast("Document", _show(home, _run_id(receipt), capsys)["run"])
    assert payload["run_id"] == recorded["run_id"]
    assert payload["status"] == "SUCCESS"
    assert payload["request_schema"] == RESEARCH_RUN_SCHEMA
    assert payload["research_only"] is True
    assert payload["request_hash"] == receipt["request_hash"]
    assert [pin["ordinal"] for pin in cast("list[Document]", payload["strategy_pins"])] == [0]


def test_a_declared_composition_records_both_sleeves_and_binds_no_membership(
    sample: tuple[Path, Document, Document, Document],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The weaker tie a composition carries is reported as one rather than papered over.

    A composition pins one membership per sleeve while a bundle binding holds a single
    membership. Binding either sleeve's would leave the bundle describing half the run
    while looking complete, so the store binds nothing and the declaration's own content
    hash is what covers both. The receipt says exactly that.
    """
    home, base, offense, defense = sample
    _installed(home)
    path = _write(tmp_path, "sample.json", _composition(base, offense, defense))
    receipt = _execute(home, path, capsys)

    assert receipt["request_schema"] == RESEARCH_COMPOSITION_SCHEMA
    assert receipt["scope"] == "sample-composition"
    _assert_claims_nothing(receipt)
    bindings = cast("Document", receipt["bindings"])
    assert bindings["bound_roles"] == []
    assert bindings["membership_bound"] is False
    assert cast("list[str]", bindings["covered_by_declaration_hash_only"]) == [
        "calendar",
        "observations",
        "composition.sleeves.defense.membership",
        "composition.sleeves.offense.membership",
    ]

    block = cast("Document", receipt["composition"])
    assert block["switch"] == SWITCH_RULE
    assert block["sample_id"] == "synthetic-sample"
    assert block["defensive_decision_count"] == len(cast("list[str]", block["defensive_decisions"]))
    # Both sleeves ran, so both are recorded; only the ordinal tells them apart.
    payload = cast("Document", _show(home, _run_id(receipt), capsys)["run"])
    pins = cast("list[Document]", payload["strategy_pins"])
    assert [pin["ordinal"] for pin in pins] == [0, 1]
    assert pins[0]["strategy_id"] == offense["strategy_id"]
    assert pins[1]["strategy_id"] == defense["strategy_id"]


def test_the_registered_request_is_the_declaration_in_its_canonical_form(
    sample: tuple[Path, Document, Document, Document],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A file may be formatted however its author left it; identity is the canonical form."""
    home, base, offense, _defense = sample
    _installed(home)
    declaration = _as_sleeve_run(base, offense)
    path = tmp_path / "pretty.json"
    _ = path.write_text(json.dumps(declaration, indent=4, sort_keys=False), encoding="utf-8")
    receipt = _execute(home, path, capsys)

    canonical = canonical_json_bytes(declaration)
    assert receipt["declaration_sha256"] == sha(path.read_bytes())
    assert receipt["declaration_sha256"] != sha(canonical)
    assert receipt["request_hash"] == content_sha256(declaration)
    with open_workspace(home) as workspace:
        row = workspace.state.execute(
            "SELECT bundle_id,content_hash FROM input_bundles WHERE bundle_id=?",
            (receipt["bundle_id"],),
        ).fetchone()
        stored = read_backtest_request(
            workspace,
            InputBundleRef(*row),
            expected_request_hash=str(receipt["request_hash"]),
            budget=BUDGET,
        )
    assert stored == canonical


def test_one_declaration_records_one_run_and_a_second_attempt_is_refused(
    sample: tuple[Path, Document, Document, Document],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The stable identity is what stops one calculation from appearing as two results."""
    home, base, offense, _defense = sample
    _installed(home)
    path = _write(tmp_path, "sleeve.json", _as_sleeve_run(base, offense))
    first = _execute(home, path, capsys)
    assert main(_command(home, path)) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "different or finished run" in json.loads(captured.err)["error"]
    assert main(["--home", str(home), "run", "list"]) == 0
    assert [run["run_id"] for run in cast("list[Document]", _output(capsys)["runs"])] == [
        _run_id(first)
    ]


def test_a_stored_declared_run_reproduces_from_its_own_sealed_evidence(
    sample: tuple[Path, Document, Document, Document],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Determinism is two claims, and the command reports each one separately."""
    home, base, offense, defense = sample
    _installed(home)
    path = _write(tmp_path, "sample.json", _composition(base, offense, defense))
    receipt = _execute(home, path, capsys)
    run_id = _run_id(receipt)

    result_only = _rerun(home, run_id, capsys)
    assert result_only["checked"] == ["result"]
    assert result_only["reproduced"] is True
    checks = cast("Document", result_only["checks"])
    assert cast("Document", checks["result"])["reproduced"] is True

    both = _rerun(
        home, run_id, capsys, "--declaration", str(path), "--sha256", sha(path.read_bytes())
    )
    assert both["checked"] == ["preparation", "result"]
    assert both["reproduced"] is True
    preparation = cast("Document", cast("Document", both["checks"])["preparation"])
    assert preparation["matches"] == {"run_id": True, "envelope": True, "preparation": True}
    assert preparation["request_hash"] == receipt["request_hash"]


def test_a_result_that_no_longer_reproduces_is_reported_as_such(
    sample: tuple[Path, Document, Document, Document],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The comparison is real: an accounting that answers differently is not reproduced."""
    home, base, offense, _defense = sample
    _installed(home)
    path = _write(tmp_path, "sleeve.json", _as_sleeve_run(base, offense))
    run_id = _run_id(_execute(home, path, capsys))
    genuine = research_module.run_document

    def altered(raw: bytes, digest: str) -> Document:
        return {**genuine(raw, digest), "execution_convention": "a convention nobody ran"}

    monkeypatch.setattr(research_module, "run_document", altered)
    report = rerun_research_run(run_id, home=home)
    assert report["reproduced"] is False
    result = cast("Document", cast("Document", report["checks"])["result"])
    assert result["reproduced"] is False
    assert result["recorded_sha256"] != result["recomputed_sha256"]


def test_a_rerun_takes_a_declaration_and_its_digest_together_or_not_at_all(
    sample: tuple[Path, Document, Document, Document], tmp_path: Path
) -> None:
    home, base, offense, _defense = sample
    path = _write(tmp_path, "sleeve.json", _as_sleeve_run(base, offense))
    with pytest.raises(ValueError, match="supplied together or not at all"):
        _ = rerun_research_run("whatever", home=home, declaration=path)


def test_only_a_declared_run_is_re_prepared_from_a_declaration(
    sample: tuple[Path, Document, Document, Document],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Re-preparing is something a declaration supports and a certified request does not.

    Asked against a run opened under another contract, this refuses rather than answering
    with a comparison that would report "not reproduced" for a reason that has nothing to
    do with determinism.
    """
    home, base, offense, _defense = sample
    _installed(home)
    sleeve = _write(tmp_path, "sleeve.json", _as_sleeve_run(base, offense))
    composition = _write(tmp_path, "sample.json", _composition(base, offense, _defense))
    run_id = _run_id(_execute(home, sleeve, capsys))
    # The stored run is a sleeve run, so its own declaration re-prepares and matches.
    assert (
        _rerun(
            home, run_id, capsys, "--declaration", str(sleeve), "--sha256", sha(sleeve.read_bytes())
        )["reproduced"]
        is True
    )
    # Another declaration prepares to another identity, which is a real disagreement.
    other = rerun_research_run(
        run_id,
        home=home,
        declaration=composition,
        declaration_sha256=sha(composition.read_bytes()),
    )
    assert other["reproduced"] is False
    preparation = cast("Document", cast("Document", other["checks"])["preparation"])
    assert cast("Document", preparation["matches"])["run_id"] is False


def test_a_run_cannot_be_recorded_as_its_own_predecessor(
    sample: tuple[Path, Document, Document, Document],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Refused before stage A registers anything, so a bad run field strands nothing.

    A declaration always prepares to the same identity, so naming that identity as the
    predecessor is how a caller reaches this. The run store refuses it too, but only
    once the declaration is already filed under a bundle name it keeps for good.
    """
    home, base, offense, _defense = sample
    _installed(home)
    path = _write(tmp_path, "sleeve.json", _as_sleeve_run(base, offense))
    run_id = _run_id(_execute(home, path, capsys))
    with open_workspace(home) as workspace:
        before = workspace.state.execute("SELECT count(*) FROM input_bundles").fetchone()[0]
    with pytest.raises(ValueError, match="its own predecessor"):
        _ = run_research(
            RunResearchRequest(
                declaration=path,
                declaration_sha256=sha(path.read_bytes()),
                home=home,
                bundle_id="a-name-of-its-own",
                prior_run_id=run_id,
            )
        )
    with open_workspace(home) as workspace:
        assert workspace.state.execute("SELECT count(*) FROM input_bundles").fetchone()[0] == before


def test_backup_and_restore_carry_the_declared_run_into_a_fresh_root(
    sample: tuple[Path, Document, Document, Document],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A record nobody can move is not durable. Both contracts survive the round trip."""
    home, base, offense, defense = sample
    _installed(home)
    sleeve = _write(tmp_path, "sleeve.json", _as_sleeve_run(base, offense))
    composition = _write(tmp_path, "sample.json", _composition(base, offense, defense))
    identities = [_run_id(_execute(home, document, capsys)) for document in (sleeve, composition)]
    before = [cast("Document", _show(home, run_id, capsys)["run"]) for run_id in identities]

    archive = tmp_path / "backup"
    assert _cli(home, "db", "backup", "--output", str(archive))["backed_up"] is True
    restored = tmp_path / "restored"
    assert (
        _cli(home, "db", "restore", "--backup", str(archive), "--home", str(restored))["restored"]
        is True
    )
    assert [
        cast("Document", _show(restored, run_id, capsys)["run"]) for run_id in identities
    ] == before
    assert _cli(restored, "db", "verify")["verified"] is True


def test_an_add_on_that_predates_the_contract_names_its_migration_before_calculating(
    sample: tuple[Path, Document, Document, Document],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """An installation that cannot record the run says so first, not after a full replay."""
    home, base, offense, _defense = sample
    install_v1(home)
    path = _write(tmp_path, "sleeve.json", _as_sleeve_run(base, offense))
    assert main(_command(home, path)) == 1
    assert "aas db run-migrate" in json.loads(capsys.readouterr().err)["error"]
    with open_workspace(home) as workspace:
        assert workspace.state.execute("SELECT count(*) FROM runs").fetchone()[0] == 0


def test_the_recorded_run_does_not_make_its_observations_executable(
    sample: tuple[Path, Document, Document, Document],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A durable record is legibility, never eligibility. The strict route still refuses."""
    home, base, offense, _defense = sample
    _installed(home)
    declaration = _as_sleeve_run(base, offense)
    path = _write(tmp_path, "sleeve.json", declaration)
    receipt = _execute(home, path, capsys)
    assert cast("Document", receipt["backtest"])["non_executable"] is True

    pin = cast("list[Document]", declaration["observations"])[0]
    with (
        open_workspace(home) as workspace,
        pytest.raises(ValueError, match="pin has incompatible dataset/domain"),
    ):
        _ = admit_native_input(
            workspace,
            GenerationPin(
                *(
                    str(pin[key])
                    for key in (
                        "dataset_id",
                        "version",
                        "generation_id",
                        "chain_hash",
                        "manifest_hash",
                    )
                )
            ),
            expected_schema="aas-price-transform-v1",
            budget=BUDGET,
        )
    # The executable command refuses the same document it just recorded as a declaration.
    assert (
        main(
            [
                "--home",
                str(home),
                "run",
                "execute",
                "--request",
                str(path),
                "--sha256",
                sha(path.read_bytes()),
            ]
        )
        == 1
    )
    assert capsys.readouterr().err


def test_the_declared_command_refuses_a_document_that_is_neither_declared_contract(
    sample: tuple[Path, Document, Document, Document],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Refused by name, not by whichever parser happened to fail on it first."""
    home, base, offense, _defense = sample
    _installed(home)
    path = _write(
        tmp_path, "wrong.json", {**_as_sleeve_run(base, offense), "schema_version": "aas-etf-v9"}
    )
    assert main(_command(home, path)) == 1
    error = str(json.loads(capsys.readouterr().err)["error"])
    assert RESEARCH_RUN_SCHEMA in error
    assert RESEARCH_COMPOSITION_SCHEMA in error


def test_a_python_run_and_a_command_run_of_one_declaration_are_the_same_execution(
    sample: tuple[Path, Document, Document, Document],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The adapter holds no decisions, so the two surfaces cannot drift apart."""
    home, base, offense, defense = sample
    _installed(home)
    path = _write(tmp_path, "sample.json", _composition(base, offense, defense))
    twin = tmp_path / "twin"
    shutil.copytree(home, twin)

    through_command = _execute(home, path, capsys)
    through_python = cast(
        "Document",
        run_research(
            RunResearchRequest(
                declaration=path, declaration_sha256=sha(path.read_bytes()), home=twin
            )
        ),
    )
    for field in (
        "request_hash",
        "bundle_id",
        "scope",
        "request_schema",
        "envelope",
        "composition",
    ):
        assert through_python[field] == through_command[field]
    assert _run_id(through_python) == _run_id(through_command)
    assert (
        cast("Document", through_python["run"])["result_hash"]
        == cast("Document", through_command["run"])["result_hash"]
    )


def test_the_capability_report_names_the_declared_path_without_claiming_anything(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert main(["status"]) == 0
    declared = cast("Document", _output(capsys)["declared_research_run"])
    assert declared["command"] == "aas run research"
    assert declared["request_schemas"] == [RESEARCH_RUN_SCHEMA, RESEARCH_COMPOSITION_SCHEMA]
    assert declared["execution_mode"] == EXECUTION_MODE
    assert all(
        declared[field] is False
        for field in (
            "certified",
            "executable_prices",
            "point_in_time_certified",
            "observed_prices_verified",
        )
    )
    assert declared["non_executable"] is True
    assert declared["source_parity"] == "unknown"
