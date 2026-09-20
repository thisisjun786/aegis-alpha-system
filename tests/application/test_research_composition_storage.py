"""Recording a declared sample composition, and refusing a document of the wrong kind.

The run store admits two declared contracts. A sample composition is not a sleeve run and
its sealed preparation is not a sleeve preparation, so the store pairs the two exactly
rather than by family. This module records a real composed run end to end, then relabels
one half of a pair at a time and shows each layer of that pairing refusing it.

A composed run records both sleeves. The switch chooses between them decision by
decision, so a record naming only the offense would describe a calculation the defense
also took part in.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from typing import TYPE_CHECKING, Any, cast

import pytest

from aegis_alpha.application.backtest_cli import run_document
from aegis_alpha.application.backtest_prepare import PreparedResearchRun
from aegis_alpha.application.research_run import (
    PREPARED_COMPOSITION_SCHEMA,
    PREPARED_SCHEMA,
    RESEARCH_COMPOSITION_SCHEMA,
)
from aegis_alpha.data.serialization import canonical_json_bytes, content_sha256
from aegis_alpha.storage.backtest_requests import register_backtest_request, research_bindings
from aegis_alpha.storage.input_pins import BUNDLE_SCHEMA, HASH_FORMAT, register_input_bundle
from aegis_alpha.storage.runs import (
    RunIntent,
    RunResult,
    RunStorageError,
    RunStrategyPin,
    commit_run,
    open_run,
    read_run,
)
from aegis_alpha.storage.verification import verify_workspace
from aegis_alpha.storage.workspace import Workspace, open_workspace
from tests.application.test_backtest_prepare import BUDGET
from tests.application.test_research_composition import (  # noqa: F401 -- shared fixtures
    _as_sleeve_run,
    _composition,
    _prepared_composition,
    composed,
    compute_environment,
)
from tests.application.test_research_execution import _prepared
from tests.application.test_research_run_storage import migrated

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


def sleeve_pin(sleeve: Document, ordinal: int) -> RunStrategyPin:
    """One composed sleeve as the run store's own pin shape."""
    return RunStrategyPin(
        module="aegis",
        ordinal=ordinal,
        store_id=str(sleeve["strategy_store_id"]),
        strategy_id=str(sleeve["strategy_id"]),
        version=str(sleeve["version"]),
        raw_hash=str(sleeve["raw_sha256"]),
        contract_hash=str(sleeve["contract_sha256"]),
    )


def bundle_bytes(declaration: Document, bundle_id: str) -> bytes:
    """Built through the store's own rule, so the test cannot disagree with it."""
    return canonical_json_bytes(
        {
            "schema": BUNDLE_SCHEMA,
            "hash_format": HASH_FORMAT,
            "bundle_id": bundle_id,
            "bindings": research_bindings(declaration),
        }
    )


def register_declared(
    workspace: Workspace, prepared: PreparedResearchRun, declaration: Document, bundle_id: str
) -> str:
    raw = bundle_bytes(declaration, bundle_id)
    bundle = register_input_bundle(
        workspace, raw, expected_file_sha256=hashlib.sha256(raw).hexdigest(), budget=BUDGET
    )
    _ = register_backtest_request(
        workspace,
        bundle,
        canonical_json_bytes(declaration),
        expected_request_hash=prepared.declaration.request_sha256,
        budget=BUDGET,
    )
    return bundle.bundle_id


def declared_intent(
    prepared: PreparedResearchRun, bundle_id: str, pins: tuple[RunStrategyPin, ...]
) -> RunIntent:
    sealed = cast("Document", json.loads(prepared.provenance))
    return RunIntent(
        request_hash=prepared.declaration.request_sha256,
        bundle_id=bundle_id,
        engine_hash=content_sha256(cast("Document", sealed["engine"])),
        environment_hash=content_sha256(cast("Document", sealed["environment"])),
        reason="declared uncertified sample composition",
        envelope_bytes=prepared.envelope.canonical_bytes,
        preparation_bytes=prepared.provenance,
        strategy_pins=pins,
        run_id=prepared.run_id,
    )


def record(
    home: Path, prepared: PreparedResearchRun, declaration: Document, sleeves: tuple[Document, ...]
) -> dict[str, object]:
    result = run_document(prepared.envelope.canonical_bytes, prepared.envelope.envelope_sha256)
    pins = tuple(sleeve_pin(sleeve, ordinal) for ordinal, sleeve in enumerate(sleeves))
    with open_workspace(home, writable=True) as workspace:
        bundle_id = register_declared(workspace, prepared, declaration, "composition-inputs-1")
        handle = open_run(workspace, declared_intent(prepared, bundle_id, pins), budget=BUDGET)
        return commit_run(workspace, handle, RunResult(canonical_json_bytes(result)), budget=BUDGET)


def test_a_declared_composition_is_recorded_and_read_back_under_its_own_schema(
    sample: tuple[Path, Document, Document, Document],
) -> None:
    """The allow-list extension, proved by a composed run that actually reaches storage."""
    home, base, offense, defense = sample
    migrated(home)
    declaration = _composition(base, offense, defense)
    prepared = _prepared_composition(home, declaration)
    committed = record(home, prepared, declaration, (offense, defense))
    with open_workspace(home) as workspace:
        payload = read_run(workspace, prepared.run_id, budget=BUDGET)
        assert verify_workspace(workspace, budget=BUDGET)["verified"] is True
    assert payload["request_schema"] == RESEARCH_COMPOSITION_SCHEMA
    assert payload["research_only"] is True
    assert payload["result_hash"] == committed["result_hash"]
    # Both sleeves, in the order the switch may choose between them.
    assert [
        (pin["ordinal"], pin["strategy_id"], pin["version"])
        for pin in cast("list[Document]", payload["strategy_pins"])
    ] == [
        (0, offense["strategy_id"], offense["version"]),
        (1, defense["strategy_id"], defense["version"]),
    ]
    assert cast("Document", json.loads(prepared.provenance))["schema"] == (
        PREPARED_COMPOSITION_SCHEMA
    )


def test_a_composition_that_records_only_its_offense_is_refused(
    sample: tuple[Path, Document, Document, Document],
) -> None:
    """Half the provenance is not the provenance; the defense sleeve ran too."""
    home, base, offense, defense = sample
    migrated(home)
    declaration = _composition(base, offense, defense)
    prepared = _prepared_composition(home, declaration)
    with open_workspace(home, writable=True) as workspace:
        bundle_id = register_declared(workspace, prepared, declaration, "composition-inputs-1")
        with pytest.raises(RunStorageError, match="strategy pins do not match"):
            _ = open_run(
                workspace,
                declared_intent(prepared, bundle_id, (sleeve_pin(offense, 0),)),
                budget=BUDGET,
            )


@pytest.mark.parametrize(
    ("edits", "refusal"),
    [
        ({"schema": PREPARED_COMPOSITION_SCHEMA}, r"does not name the declaration contract"),
        (
            {
                "schema": PREPARED_COMPOSITION_SCHEMA,
                "declaration_schema": RESEARCH_COMPOSITION_SCHEMA,
            },
            r"scope of its own kind",
        ),
        (
            {
                "schema": PREPARED_COMPOSITION_SCHEMA,
                "declaration_schema": RESEARCH_COMPOSITION_SCHEMA,
                "scope": "sample-composition",
            },
            r"does not match the request contract",
        ),
    ],
)
def test_a_composition_preparation_under_a_sleeve_declaration_is_refused(
    sample: tuple[Path, Document, Document, Document], edits: Document, refusal: str
) -> None:
    """Relabelled one field at a time, so each layer of the pairing is shown refusing.

    The links stay valid throughout: the preparation still names the declaration it was
    prepared for and the envelope it produced. Only the kind moves, which is exactly the
    case a family-level check would let through.
    """
    home, base, offense, _defense = sample
    migrated(home)
    declaration = _as_sleeve_run(base, offense)
    prepared = _prepared(home, declaration)
    relabelled = canonical_json_bytes(cast("Document", json.loads(prepared.provenance)) | edits)
    with open_workspace(home, writable=True) as workspace:
        bundle_id = register_declared(workspace, prepared, declaration, "sleeve-inputs-1")
        with pytest.raises(RunStorageError, match=refusal):
            _ = open_run(
                workspace,
                replace(
                    declared_intent(prepared, bundle_id, (sleeve_pin(offense, 0),)),
                    preparation_bytes=relabelled,
                ),
                budget=BUDGET,
            )


def test_a_sleeve_preparation_under_a_composition_declaration_is_refused(
    sample: tuple[Path, Document, Document, Document],
) -> None:
    """The other direction of the same pair, relabelled far enough to reach the pairing."""
    home, base, offense, defense = sample
    migrated(home)
    declaration = _composition(base, offense, defense)
    prepared = _prepared_composition(home, declaration)
    relabelled = canonical_json_bytes(
        cast("Document", json.loads(prepared.provenance))
        | {
            "schema": PREPARED_SCHEMA,
            "declaration_schema": "aas-research-run-v2",
            "scope": "sleeve",
        }
    )
    with open_workspace(home, writable=True) as workspace:
        bundle_id = register_declared(workspace, prepared, declaration, "composition-inputs-1")
        with pytest.raises(RunStorageError, match="does not match the request contract"):
            _ = open_run(
                workspace,
                replace(
                    declared_intent(
                        prepared, bundle_id, (sleeve_pin(offense, 0), sleeve_pin(defense, 1))
                    ),
                    preparation_bytes=relabelled,
                ),
                budget=BUDGET,
            )
