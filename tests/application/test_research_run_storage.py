"""Recording a declared uncertified research run, then reading it back from storage.

This is the wiring the declared path could not reach while the run add-on carried one
request contract: the declaration itself is the run's registered request, the run is
opened against the envelope and the sealed declaration, the result is committed, and the
same run is read back by identity, verified, backed up and restored.

Everything runs on synthetic panels over a synthetic calendar, on an installation this
module creates, migrates and throws away. Nothing here certifies the calculation. The
refusals the strict path puts on this data are exercised beside the stored run rather
than removed, and the stored run says in its own record what contract describes it.
"""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING, Any, cast

import pytest

from aegis_alpha.application.backtest_cli import DECLARED_RESEARCH_MODE, run_document
from aegis_alpha.application.backtest_prepare import PreparedResearchRun, prepare_research_run
from aegis_alpha.application.research_run import (
    PREPARED_SCHEMA,
    RESEARCH_RUN_SCHEMA,
    parse_research_run_request,
)
from aegis_alpha.data.serialization import canonical_json_bytes, content_sha256
from aegis_alpha.storage.backtest_requests import (
    read_backtest_request,
    register_backtest_request,
    research_bindings,
)
from aegis_alpha.storage.backup import backup, restore
from aegis_alpha.storage.input_pins import BUNDLE_SCHEMA, HASH_FORMAT, register_input_bundle
from aegis_alpha.storage.market_inputs import GenerationPin, admit_native_input
from aegis_alpha.storage.run_schema import migrate_run_schema
from aegis_alpha.storage.runs import (
    RunIntent,
    RunResult,
    RunStorageError,
    RunStrategyPin,
    _read_sealed,
    commit_run,
    manifest_hash,
    open_run,
    read_run,
)
from aegis_alpha.storage.verification import verify_workspace
from aegis_alpha.storage.workspace import open_workspace
from tests.application.test_backtest_prepare import BUDGET
from tests.application.test_research_execution import (  # noqa: F401 -- shared fixtures
    SERIES,
    compute_environment,
    installation,
)
from tests.storage.test_run_migration import install_v1

if TYPE_CHECKING:
    from pathlib import Path

type Document = dict[str, Any]


@pytest.fixture
def research(request: pytest.FixtureRequest) -> tuple[Path, Document, Document]:
    """The declared-run installation AAS-9 already builds, under a name of our own.

    Requested indirectly so the imported fixture is reused rather than rebuilt here, and
    so no test parameter shadows the name this module imported it under.
    """
    return cast("tuple[Path, Document, Document]", request.getfixturevalue("installation"))


def migrated(home: Path) -> None:
    """Install the old add-on and migrate it, which is the sequence an operator has."""
    install_v1(home)
    assert migrate_run_schema(home)["migrated"] is True


def prepare(home: Path, declaration: Document) -> PreparedResearchRun:
    with open_workspace(home) as workspace:
        return prepare_research_run(
            workspace,
            parse_research_run_request(canonical_json_bytes(declaration)),
            budget=BUDGET,
        )


def bundle_bytes(declaration: Document) -> bytes:
    """The bundle a declared run is registered against: its calendar and its membership."""
    return canonical_json_bytes(
        {
            "schema": BUNDLE_SCHEMA,
            "hash_format": HASH_FORMAT,
            "bundle_id": "research-inputs-1",
            "bindings": research_bindings(declaration),
        }
    )


def strategy_pin(prepared: PreparedResearchRun) -> RunStrategyPin:
    declared = prepared.declaration
    return RunStrategyPin(
        module="aegis",
        ordinal=0,
        store_id=declared.strategy_store_id,
        strategy_id=declared.strategy_id,
        version=declared.strategy_version,
        raw_hash=declared.strategy_raw_sha256,
        contract_hash=declared.strategy_contract_sha256,
    )


def record(home: Path, prepared: PreparedResearchRun, declaration: Document) -> dict[str, object]:
    """Register the declaration, open the run against its sealed inputs, commit the result."""
    raw = canonical_json_bytes(declaration)
    sealed = cast("Document", json.loads(prepared.provenance))
    bundle_raw = bundle_bytes(declaration)
    result = run_document(prepared.envelope.canonical_bytes, prepared.envelope.envelope_sha256)
    with open_workspace(home, writable=True) as workspace:
        bundle = register_input_bundle(
            workspace,
            bundle_raw,
            expected_file_sha256=hashlib.sha256(bundle_raw).hexdigest(),
            budget=BUDGET,
        )
        register_backtest_request(
            workspace,
            bundle,
            raw,
            expected_request_hash=prepared.declaration.request_sha256,
            budget=BUDGET,
        )
        handle = open_run(
            workspace,
            RunIntent(
                request_hash=prepared.declaration.request_sha256,
                bundle_id=bundle.bundle_id,
                # The declaration names neither, because no certified request stands
                # behind it. Both come from the preparation it sealed.
                engine_hash=content_sha256(cast("Document", sealed["engine"])),
                environment_hash=content_sha256(cast("Document", sealed["environment"])),
                reason="declared uncertified research run",
                envelope_bytes=prepared.envelope.canonical_bytes,
                preparation_bytes=prepared.provenance,
                strategy_pins=(strategy_pin(prepared),),
                run_id=prepared.run_id,
            ),
            budget=BUDGET,
        )
        return commit_run(workspace, handle, RunResult(canonical_json_bytes(result)), budget=BUDGET)


def read(home: Path, run_id: str) -> dict[str, object]:
    with open_workspace(home) as workspace:
        return read_run(workspace, run_id, budget=BUDGET)


def test_a_declared_research_run_is_recorded_and_read_back_by_its_own_identity(
    research: tuple[Path, Document, Document],
) -> None:
    """The acceptance the declared path could not reach: a durable, requeryable record."""
    home, _body, declaration = research
    migrated(home)
    prepared = prepare(home, declaration)
    committed = record(home, prepared, declaration)
    payload = read(home, prepared.run_id)
    assert payload["run_id"] == prepared.run_id
    assert payload["status"] == "SUCCESS"
    # The run says which contract describes it, from the immutable row the add-on's
    # widened CHECK admitted, not from anything the caller passed at read time.
    assert payload["request_schema"] == RESEARCH_RUN_SCHEMA
    assert payload["research_only"] is True
    assert payload["request_hash"] == prepared.declaration.request_sha256
    assert payload["result_hash"] == committed["result_hash"]
    assert payload["table_hashes"] == committed["table_hashes"]
    assert payload["strategy_pins"] == [
        {
            "module": "aegis",
            "ordinal": 0,
            "strategy_store_id": prepared.declaration.strategy_store_id,
            "strategy_id": prepared.declaration.strategy_id,
            "version": prepared.declaration.strategy_version,
            "raw_hash": prepared.declaration.strategy_raw_sha256,
            "contract_hash": prepared.declaration.strategy_contract_sha256,
        }
    ]


def test_the_requery_reads_sealed_evidence_and_never_recalculates(
    research: tuple[Path, Document, Document],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reading a run back must not reach the accounting, let alone a provider."""
    home, _body, declaration = research
    migrated(home)
    prepared = prepare(home, declaration)
    record(home, prepared, declaration)

    def refuse(*_args: object, **_kwargs: object) -> None:
        pytest.fail("reading a stored run recalculated it")

    from aegis_alpha.engine import execution  # noqa: PLC0415 -- the calculation entry point

    monkeypatch.setattr(execution, "replay_next_open", refuse)
    monkeypatch.setattr(execution, "replay_next_open_cashflows", refuse)
    assert read(home, prepared.run_id)["request_schema"] == RESEARCH_RUN_SCHEMA


def test_the_stored_run_reruns_deterministically_from_its_own_sealed_envelope(
    research: tuple[Path, Document, Document],
) -> None:
    """The record carries enough to reproduce the result it certifies, byte for byte."""
    home, _body, declaration = research
    migrated(home)
    prepared = prepare(home, declaration)
    committed = record(home, prepared, declaration)
    with open_workspace(home) as workspace:
        envelope = _read_sealed(workspace, prepared.run_id, "envelope.json")
        stored = _read_sealed(workspace, prepared.run_id, "backtest.json")
    artifacts = cast("dict[str, str]", read(home, prepared.run_id)["artifacts"])
    rerun = canonical_json_bytes(run_document(envelope, hashlib.sha256(envelope).hexdigest()))
    assert rerun == stored
    assert (
        manifest_hash(
            prepared.declaration.request_sha256,
            artifacts,
            (committed["table_hashes"], committed["table_counts"]),
        )
        == committed["result_hash"]
    )
    # Preparing the declaration again lands on the same run identity, and a finished run
    # is never reopened under it.
    again = prepare(home, declaration)
    assert again.run_id == prepared.run_id
    assert again.envelope.canonical_bytes == prepared.envelope.canonical_bytes
    assert again.provenance == prepared.provenance


def test_backup_and_restore_carry_the_declared_run_into_a_new_root(
    research: tuple[Path, Document, Document], tmp_path: Path
) -> None:
    home, _body, declaration = research
    migrated(home)
    prepared = prepare(home, declaration)
    record(home, prepared, declaration)
    before = read(home, prepared.run_id)
    receipt = backup(home, tmp_path / "backup", budget=BUDGET)
    assert receipt["backed_up"] is True
    restored = tmp_path / "restored"
    assert restore(tmp_path / "backup", restored, budget=BUDGET)["restored"] is True
    assert read(restored, prepared.run_id) == before
    with open_workspace(restored) as workspace:
        assert verify_workspace(workspace, budget=BUDGET)["verified"] is True


def test_the_registered_request_is_the_declaration_itself(
    research: tuple[Path, Document, Document],
) -> None:
    """Content identity: the stored request bytes are exactly the declaration's."""
    home, _body, declaration = research
    migrated(home)
    prepared = prepare(home, declaration)
    record(home, prepared, declaration)
    raw = canonical_json_bytes(declaration)
    with open_workspace(home) as workspace:
        bundle_row = workspace.state.execute(
            "SELECT bundle_id,content_hash FROM input_bundles WHERE bundle_id=?",
            ("research-inputs-1",),
        ).fetchone()
        from aegis_alpha.storage.input_pins import InputBundleRef  # noqa: PLC0415

        assert (
            read_backtest_request(
                workspace,
                InputBundleRef(*bundle_row),
                expected_request_hash=prepared.declaration.request_sha256,
                budget=BUDGET,
            )
            == raw
        )


def test_a_declaration_filed_under_a_bundle_it_does_not_name_is_refused(
    research: tuple[Path, Document, Document],
) -> None:
    """A declared run's bundle has to be the inputs the declaration actually pins."""
    home, _body, declaration = research
    migrated(home)
    prepared = prepare(home, declaration)
    empty = canonical_json_bytes(
        {
            "schema": BUNDLE_SCHEMA,
            "hash_format": HASH_FORMAT,
            "bundle_id": "unrelated-1",
            "bindings": [],
        }
    )
    with open_workspace(home, writable=True) as workspace:
        bundle = register_input_bundle(
            workspace, empty, expected_file_sha256=hashlib.sha256(empty).hexdigest(), budget=BUDGET
        )
        with pytest.raises(ValueError, match="binding order/complete bundle content mismatch"):
            register_backtest_request(
                workspace,
                bundle,
                canonical_json_bytes(declaration),
                expected_request_hash=prepared.declaration.request_sha256,
                budget=BUDGET,
            )


def test_a_finished_declared_run_is_not_reopened_under_its_own_identity(
    research: tuple[Path, Document, Document],
) -> None:
    home, _body, declaration = research
    migrated(home)
    prepared = prepare(home, declaration)
    record(home, prepared, declaration)
    with (
        open_workspace(home, writable=True) as workspace,
        pytest.raises(RunStorageError, match="different or finished run"),
    ):
        open_run(
            workspace,
            RunIntent(
                request_hash=prepared.declaration.request_sha256,
                bundle_id="research-inputs-1",
                engine_hash=content_sha256(
                    cast("Document", cast("Document", json.loads(prepared.provenance))["engine"])
                ),
                environment_hash=content_sha256(
                    cast(
                        "Document",
                        cast("Document", json.loads(prepared.provenance))["environment"],
                    )
                ),
                reason="declared uncertified research run",
                envelope_bytes=prepared.envelope.canonical_bytes,
                preparation_bytes=prepared.provenance,
                strategy_pins=(strategy_pin(prepared),),
                run_id=prepared.run_id,
            ),
            budget=BUDGET,
        )


def test_the_stored_run_does_not_make_its_observations_executable(
    research: tuple[Path, Document, Document],
) -> None:
    """A durable record is legibility, never eligibility. The strict route still refuses."""
    home, _body, declaration = research
    migrated(home)
    prepared = prepare(home, declaration)
    record(home, prepared, declaration)
    sealed = cast("Document", json.loads(prepared.provenance))
    assert sealed["schema"] == PREPARED_SCHEMA
    assert sealed["certified"] is False
    assert sealed["executable_prices"] is False
    assert sealed["point_in_time_certified"] is False
    result = cast("Document", json.loads(_stored_result(home, prepared.run_id)))
    assert result["observed_prices_verified"] is False
    assert result["point_in_time_verified"] is False
    assert result["live_orders"] is False
    pin = cast("list[Document]", declaration["observations"])[0]
    with (
        open_workspace(home) as workspace,
        pytest.raises(ValueError, match="pin has incompatible dataset/domain"),
    ):
        admit_native_input(
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
    assert set(SERIES.values())


def _stored_result(home: Path, run_id: str) -> bytes:
    with open_workspace(home) as workspace:
        return _read_sealed(workspace, run_id, "backtest.json")


# What the runner stamps on a declared result. A later reader has to be able to take
# these at face value, which is what makes them worth checking through storage rather
# than only where they are produced.
DECLARED_STATUS = {"certified": False, "non_executable": True, "executable_prices": False}


def test_the_declared_status_survives_recording_requery_and_restore(
    research: tuple[Path, Document, Document], tmp_path: Path
) -> None:
    """The runner says this run is uncertified; storage has to keep saying it.

    Checked on the produced response, on the sealed artifact read back through the run
    record, and on the same artifact after a restore into a new root. The caller's own
    copy is never the evidence: each read comes from what storage actually holds.
    """
    home, _body, declaration = research
    migrated(home)
    prepared = prepare(home, declaration)
    produced = run_document(prepared.envelope.canonical_bytes, prepared.envelope.envelope_sha256)
    assert produced["research_mode"] == DECLARED_RESEARCH_MODE
    assert {key: produced[key] for key in DECLARED_STATUS} == DECLARED_STATUS
    record(home, prepared, declaration)
    sealed = cast("Document", json.loads(_stored_result(home, prepared.run_id)))
    assert {key: sealed[key] for key in DECLARED_STATUS} == DECLARED_STATUS
    assert read(home, prepared.run_id)["research_only"] is True
    _ = backup(home, tmp_path / "status-backup", budget=BUDGET)
    restored = tmp_path / "status-restored"
    _ = restore(tmp_path / "status-backup", restored, budget=BUDGET)
    assert cast("Document", json.loads(_stored_result(restored, prepared.run_id))) == sealed
    assert read(restored, prepared.run_id)["request_schema"] == RESEARCH_RUN_SCHEMA


def test_a_flipped_uncertified_claim_stops_the_run_reading_back(
    research: tuple[Path, Document, Document],
) -> None:
    """The claim is covered by the recorded result identity, not just written beside it.

    Carrying the words is not enough: an edited artifact that still reads as valid JSON
    and still names the right envelope must stop verifying, or a stored run could report
    itself executable and every later check would agree.
    """
    home, _body, declaration = research
    migrated(home)
    prepared = prepare(home, declaration)
    record(home, prepared, declaration)
    with open_workspace(home) as workspace:
        artifact = workspace.paths.runs / prepared.run_id / "backtest.json"
    claimed = cast("Document", json.loads(artifact.read_bytes())) | {"non_executable": False}
    # Rewritten canonically, so what fails is the changed claim rather than its spelling.
    _ = artifact.write_bytes(canonical_json_bytes(claimed))
    with (
        open_workspace(home) as workspace,
        pytest.raises(RunStorageError, match=r"disagrees with the sealed evidence"),
    ):
        _ = read_run(workspace, prepared.run_id, budget=BUDGET)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("certified", True),
        ("point_in_time_certified", True),
        ("executable_prices", True),
        ("execution_mode", "certified-backtest"),
        ("declaration_schema", "aas-backtest-request-v1"),
    ],
)
def test_a_preparation_that_contradicts_its_own_kind_seals_no_run(
    research: tuple[Path, Document, Document], field: str, value: object
) -> None:
    """The links still match; only the claim changed. Storage refuses it anyway.

    A declared run's whole provenance is this document, so a copy that keeps the same
    declaration and envelope while claiming certification would record an uncertified
    calculation as a certified one, and every later verification would agree with it.
    """
    home, _body, declaration = research
    migrated(home)
    prepared = prepare(home, declaration)
    raw = canonical_json_bytes(declaration)
    bundle_raw = bundle_bytes(declaration)
    tampered = canonical_json_bytes(
        cast("Document", json.loads(prepared.provenance)) | {field: value}
    )
    with open_workspace(home, writable=True) as workspace:
        bundle = register_input_bundle(
            workspace,
            bundle_raw,
            expected_file_sha256=hashlib.sha256(bundle_raw).hexdigest(),
            budget=BUDGET,
        )
        register_backtest_request(
            workspace,
            bundle,
            raw,
            expected_request_hash=prepared.declaration.request_sha256,
            budget=BUDGET,
        )
        sealed = cast("Document", json.loads(prepared.provenance))
        with pytest.raises(RunStorageError, match=r"research preparation|uncertified status"):
            open_run(
                workspace,
                RunIntent(
                    request_hash=prepared.declaration.request_sha256,
                    bundle_id=bundle.bundle_id,
                    engine_hash=content_sha256(cast("Document", sealed["engine"])),
                    environment_hash=content_sha256(cast("Document", sealed["environment"])),
                    reason="declared uncertified research run",
                    envelope_bytes=prepared.envelope.canonical_bytes,
                    preparation_bytes=tampered,
                    strategy_pins=(strategy_pin(prepared),),
                    run_id=prepared.run_id,
                ),
                budget=BUDGET,
            )
        assert (
            workspace.state.execute(
                "SELECT count(*) FROM runs WHERE run_id=?", (prepared.run_id,)
            ).fetchone()[0]
            == 0
        )
