"""Observed research prices: binary64 admission, uncertified pins and route separation."""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import struct
from contextlib import closing
from dataclasses import replace
from decimal import Decimal, localcontext
from fractions import Fraction
from typing import TYPE_CHECKING

import pytest

from aegis_alpha.application.data_cli import execute_native_data
from aegis_alpha.application.storage_cli import add_commands
from aegis_alpha.compute_resources import ComputeBudget, ComputeResourceError
from aegis_alpha.data.descriptor_tree import DescriptorTreeError
from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.storage import (
    import_document,
    market,
    market_inputs,
    publication,
    research_inputs,
)
from aegis_alpha.storage.backup import backup, restore
from aegis_alpha.storage.raw import put_raw
from aegis_alpha.storage.verification import verify_workspace
from aegis_alpha.storage.workspace import initialize, open_workspace
from tests.storage.test_research_inputs import (
    _change,
    _hash_json,
    _proxy_spec,
    _register_domain,
    _source_row,
    _spec,
)

if TYPE_CHECKING:
    from pathlib import Path

    from aegis_alpha.storage.market_inputs import History, Row
    from aegis_alpha.storage.source_reader import SourcePin
    from aegis_alpha.storage.workspace import Workspace

BUDGET = ComputeBudget(Fraction(1), 64 * 1024 * 1024)
NUMBERS = ("open", "high", "low", "close", "volume")
# Exactly a promoted binary32, like every value in the retained research panel.
PANEL_OPEN = 49.29364776611328
DECIMAL_SCALE = Decimal("0.000000000001")
# An additively adjusted series legitimately falls below zero.
NEGATIVE_OBSERVATION = -1.5
CHUNKS = 2
# Enough published points that the sealed document is worth about as much as the
# retained chain once decoded, which is what separates a charged lease from an
# uncharged one.
POINTS = 150
COMMON_FIELDS = frozenset(
    {
        "generation_id",
        "record_id",
        "revision_id",
        "supersedes_revision_id",
        "op",
        "available_at_us",
        "revision_known_at_us",
        "ingested_at_us",
        "source_snapshot_id",
        "source_row_hash",
    }
)


def _definition(source: dict[str, object], role: str) -> dict[str, object]:
    return {
        "series_id": "OBSERVED",
        "version": "v1",
        "observation_role": role,
        "basis": "adjusted",
        "currency": "USD",
        "price_role": "reference",
        "adjustment": "total_return",
        "value_domain": "positive",
        "certified": False,
        "normalization": {"input_number": "ieee_float", "output": "ieee754_binary64"},
        "observed_source": source,
        "calendar_ref": {"id": "CAL", "version": "1", "sha256": "e" * 64},
    }


def _observation_spec(
    workspace: Workspace,
    path: Path,
    *,
    value: float | None = PANEL_OPEN,
    changes: dict[str, object] | None = None,
    options: dict[str, object] | None = None,
) -> Path:
    # The observed panel is pinned as provenance, independently of the mapped point table.
    settings: dict[str, object] = {
        "asset_type": "etf",
        "instrument": "ASSET_A",
        "role": "close",
        "feature_at_us": 20,
        "panel_rows": 1,
        "extra_instruments": 0,
        "knowledge": None,
        "dataset": None,
        "points": 1,
        **(options or {}),
    }
    instrument = str(settings["instrument"])
    # One shared observed panel, so every generation pins the same provenance and contract.
    name = path.parent.name + "-observed-panel"
    panel = path.parent / (name + ".json")
    if not panel.exists():
        # The panel is only ever pinned and hashed, so extra rows just make it bigger.
        wide = [
            {**_source_row(), "record_id": "panel-" + str(n), "revision_id": "pr-" + str(n)}
            for n in range(int(str(settings["panel_rows"])))
        ]
        panel = _spec(workspace, path.parent / (name + ".sqlite3"), wide)
    definition = _definition(json.loads(panel.read_bytes())["source"], str(settings["role"]))
    definition.update(changes or {})
    row = {key: item for key, item in _source_row().items() if key in COMMON_FIELDS}
    row.update(
        contract_id=str(definition["series_id"]) + "/" + str(definition["observation_role"]),
        contract_version=definition["version"],
        contract_hash=_hash_json(definition),
        input_bundle_hash=_hash_json([definition["observed_source"], definition["calendar_ref"]]),
        instrument_id=instrument,
        feature_at_us=settings["feature_at_us"],
        value=value,
        value_state="missing" if value is None else "present",
        available_at_us=settings["knowledge"],
        revision_known_at_us=settings["knowledge"],
    )
    natural = [
        "contract_id",
        "contract_version",
        "input_bundle_hash",
        "instrument_id",
        "feature_at_us",
    ]
    row["record_id"] = _hash_json(
        ["aas-record-v1", "feature_values", [[key, row[key]] for key in natural]]
    )
    # Extra points only make the published document larger, which is what the charge
    # for the sealed document a verifier holds live is measured against. One point
    # reproduces the single-row spec exactly.
    points = [row]
    for offset in range(1, int(str(settings["points"]))):
        extra = {**row, "feature_at_us": int(str(settings["feature_at_us"])) + offset}
        extra["revision_id"] = str(row["revision_id"]) + "-" + str(offset)
        extra["record_id"] = _hash_json(
            ["aas-record-v1", "feature_values", [[key, extra[key]] for key in natural]]
        )
        points.append(extra)
    spec = _spec(workspace, path, points)
    document = json.loads(spec.read_bytes())
    for key in ("price", "calendar", "decimal_conversion"):
        del document[key]
    document.update(
        schema_version="aas-observation-transform-v1",
        observation=definition,
        instruments=[
            {
                "instrument_id": instrument,
                "asset_type": settings["asset_type"],
                "venue": "SYNTHETIC",
            },
            # Declared-but-unused identities only make the transform document larger,
            # which is what the live-transform charge is measured against.
            *(
                {
                    "instrument_id": "PAD_" + str(n),
                    "asset_type": settings["asset_type"],
                    "venue": "SYNTHETIC",
                }
                for n in range(int(str(settings["extra_instruments"])))
            ),
        ],
    )
    document["dataset"] = settings["dataset"] or {
        "dataset_id": path.stem,
        "version": "1",
        "generation_id": path.stem,
        "operation_id": "op-" + path.stem,
        "parent_id": None,
    }
    spec.write_text(json.dumps(document, indent=2))
    return spec


def _pin(workspace: Workspace, dataset: str) -> market_inputs.GenerationPin:
    row = publication.read_dataset(workspace, dataset, "1")
    return market_inputs.GenerationPin(
        str(row["dataset_id"]),
        str(row["version"]),
        str(row["generation_id"]),
        str(row["chain_hash"]),
        str(row["manifest_hash"]),
    )


def test_panel_refused_as_price_is_admitted_as_uncertified_observation(tmp_path: Path) -> None:
    # Given the retained panel's own number, which is exactly a promoted binary32.
    assert struct.unpack("<f", struct.pack("<f", PANEL_OPEN))[0] == PANEL_OPEN
    with localcontext() as context:
        context.prec = 50
        assert Decimal(PANEL_OPEN) != Decimal(PANEL_OPEN).quantize(DECIMAL_SCALE)
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        # The executable price route still refuses it, for its documented reason.
        priced = _spec(
            workspace,
            tmp_path / "priced.sqlite3",
            [{**_source_row(), **dict.fromkeys(NUMBERS, PANEL_OPEN)}],
        )
        _change(priced, "decimal_conversion", dict.fromkeys(NUMBERS, "ieee_float"))
        with pytest.raises(ValueError, match="exact DECIMAL"):
            _ = _register_domain(workspace, priced, "price")
        # When the same number goes through the observation route,
        spec = _observation_spec(workspace, tmp_path / "observed.sqlite3")
        result = _register_domain(workspace, spec, "observation")
        # Then it is stored as binary64 without rounding, and stays uncertified.
        assert result["certified"] is False
        assert result["non_executable"] is True
        stored = market.read_generation(workspace.market, "observed")[0]
        assert struct.pack(">d", stored["value"]) == struct.pack(">d", PANEL_OPEN)
        assert stored["available_at_us"] is stored["revision_known_at_us"] is None
        assert _register_domain(workspace, spec, "observation") == result


def test_uncertified_reader_reports_unknown_evidence_and_survives_a_later_generation(
    tmp_path: Path,
) -> None:
    # Given one registered observation series and a pinned read of it.
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        spec = _observation_spec(workspace, tmp_path / "first.sqlite3")
        _ = _register_domain(workspace, spec, "observation")
        origin = _pin(workspace, "first")
        series = market_inputs.load_pinned_observations(workspace, origin, budget=BUDGET)
        assert series.certified is False
        assert series.non_executable is True
        projected = series.project_as_of(30)
        assert "observation_uncertified" in projected.coverage.reasons
        assert "observation_non_executable" in projected.coverage.reasons
        assert projected.coverage.certified is False
        research = series.project_as_of(30, mode="observed_snapshot_research")
        assert "observed_snapshot_research" in research.coverage.reasons
        # The panel carries no knowledge time, so nothing is promoted to PIT evidence.
        assert "unknown_observation_evidence" in research.coverage.reasons
        # When an independent later generation is added,
        later = _observation_spec(
            workspace, tmp_path / "second.sqlite3", value=50.5, options={"feature_at_us": 40}
        )
        _ = _register_domain(workspace, later, "observation")
        # Then the existing pin reads exactly what it read before.
        assert (
            market_inputs.load_pinned_observations(workspace, origin, budget=BUDGET).history
            == series.history
        )


def test_workspace_verification_admits_the_observation_definition_schema(tmp_path: Path) -> None:
    # Given a registered observation contract, whose record_schema is new to verification.
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        spec = _observation_spec(workspace, tmp_path / "observed.sqlite3")
        _ = _register_domain(workspace, spec, "observation")
        stored = workspace.state.execute(
            "SELECT record_schema FROM feature_contracts WHERE name='OBSERVED/close'"
        ).fetchone()
        assert stored[0] == "aas-observation-definition-v1"
        # When the whole workspace is verified, Then the schema is dispatched, not refused.
        assert verify_workspace(workspace, budget=BUDGET)


@pytest.mark.parametrize(
    ("changes", "options", "message"),
    [
        ({"price_role": "canonical"}, {}, "reference"),
        ({"certified": True}, {}, "never certified"),
        ({"observation_role": "high"}, {}, "observed open or close"),
        ({"value_domain": "unbounded"}, {}, "value domain"),
        ({}, {"asset_type": "proxy"}, "proxies"),
    ],
)
def test_invalid_observation_contracts_fail_before_publication(
    tmp_path: Path,
    changes: dict[str, object],
    options: dict[str, object],
    message: str,
) -> None:
    # Given a retained source that is otherwise registrable.
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        spec = _observation_spec(
            workspace, tmp_path / "bad.sqlite3", changes=changes, options=options
        )
        # When the contract breaks an invariant, Then nothing is published.
        with pytest.raises((ValueError, TypeError), match=message):
            _ = _register_domain(workspace, spec, "observation")
        assert (
            workspace.state.execute(
                "SELECT count(*) FROM dataset_versions WHERE dataset_id='bad'"
            ).fetchone()[0]
            == 0
        )


def test_strict_pit_never_selects_reference_observations(tmp_path: Path) -> None:
    # Given an observation whose knowledge times are fully known, not null.
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        spec = _observation_spec(workspace, tmp_path / "known.sqlite3", options={"knowledge": 10})
        _ = _register_domain(workspace, spec, "observation")
        series = market_inputs.load_pinned_observations(
            workspace, _pin(workspace, "known"), budget=BUDGET
        )
        # When strict PIT projects well after every stored timestamp,
        strict = series.project_as_of(30)
        # Then the always-reference contract selects none of it, and says so.
        assert strict.rows == ()
        assert "reference_observation" in strict.coverage.reasons
        assert strict.coverage.present_count == 0
        assert strict.coverage.expected_count == 1
        # Only the explicit research mode projects it.
        research = series.project_as_of(30, mode="observed_snapshot_research")
        assert len(research.rows) == 1
        assert "observed_snapshot_research" in research.coverage.reasons


def test_real_value_domain_admits_a_negative_observation(tmp_path: Path) -> None:
    # Given an additively adjusted series, which legitimately falls below zero.
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        spec = _observation_spec(
            workspace,
            tmp_path / "real.sqlite3",
            value=NEGATIVE_OBSERVATION,
            changes={"value_domain": "real"},
        )
        result = _register_domain(workspace, spec, "observation")
        assert result["certified"] is False
        assert market.read_generation(workspace.market, "real")[0]["value"] == NEGATIVE_OBSERVATION
        # The positive domain still refuses the same value.
        positive = _observation_spec(
            workspace, tmp_path / "pos.sqlite3", value=NEGATIVE_OBSERVATION
        )
        with pytest.raises(ValueError, match="value domain"):
            _ = _register_domain(workspace, positive, "observation")


def test_observation_extension_cannot_switch_contract(tmp_path: Path) -> None:
    # Given a registered observation generation.
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        first = _observation_spec(workspace, tmp_path / "chain.sqlite3")
        _ = _register_domain(workspace, first, "observation")
        # When a later generation names it as parent but changes the observed role,
        later = _observation_spec(
            workspace,
            tmp_path / "chain2.sqlite3",
            options={
                "role": "open",
                "feature_at_us": 40,
                "dataset": {
                    "dataset_id": "chain",
                    "version": "2",
                    "generation_id": "chain2",
                    "operation_id": "op-chain2",
                    "parent_id": "chain",
                },
            },
        )
        # Then the mixed chain is refused before it is published.
        with pytest.raises(ValueError, match="same contract"):
            _ = _register_domain(workspace, later, "observation")
        assert (
            workspace.state.execute(
                "SELECT count(*) FROM dataset_versions WHERE dataset_id='chain' AND version='2'"
            ).fetchone()[0]
            == 0
        )


def test_cli_registers_an_observation_transform(tmp_path: Path) -> None:
    # Given the same spec the Python route accepts.
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        spec = _observation_spec(workspace, tmp_path / "viacli.sqlite3")
        digest = hashlib.sha256(spec.read_bytes()).hexdigest()
        # When the data command dispatches it, Then the same publication happens.
        result = execute_native_data(
            workspace,
            argparse.Namespace(data_command="register-observations", spec=spec, sha256=digest),
        )
        assert result["published"] is True
        assert result["certified"] is False
        assert result["non_executable"] is True
        assert result["transform_sha256"] == digest
    # The parser accepts the subcommand alongside its sibling registration routes.
    parser = argparse.ArgumentParser()
    add_commands(parser.add_subparsers(dest="command", required=True))
    parsed = parser.parse_args(
        ["data", "register-observations", "--spec", str(spec), "--sha256", digest]
    )
    assert parsed.data_command == "register-observations"


def test_observation_extension_requires_an_ancestor_from_this_route(tmp_path: Path) -> None:
    # Given a registered generation, and an extension that keeps its contract identity
    # but declares a different observation definition.
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        first = _observation_spec(workspace, tmp_path / "root.sqlite3")
        _ = _register_domain(workspace, first, "observation")
        later = _observation_spec(
            workspace,
            tmp_path / "root2.sqlite3",
            changes={"adjustment": "a-different-adjustment"},
            options={
                "feature_at_us": 40,
                "dataset": {
                    "dataset_id": "root",
                    "version": "2",
                    "generation_id": "root2",
                    "operation_id": "op-root2",
                    "parent_id": "root",
                },
            },
        )
        # Then the ancestor's own transform is checked, not just its row identities.
        with pytest.raises(ValueError, match="ancestor"):
            _ = _register_domain(workspace, later, "observation")
        assert (
            workspace.state.execute(
                "SELECT count(*) FROM dataset_versions WHERE dataset_id='root' AND version='2'"
            ).fetchone()[0]
            == 0
        )


def test_extending_a_chain_leaves_the_earlier_pin_byte_identical(tmp_path: Path) -> None:
    # Given a registered generation and a pinned read of it.
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        first = _observation_spec(workspace, tmp_path / "ext.sqlite3")
        _ = _register_domain(workspace, first, "observation")
        origin = _pin(workspace, "ext")
        before = market_inputs.load_pinned_observations(workspace, origin, budget=BUDGET)
        # When the same contract is extended in place, as a bounded panel chunk is,
        later = _observation_spec(
            workspace,
            tmp_path / "ext2.sqlite3",
            value=51.25,
            options={
                "feature_at_us": 40,
                "dataset": {
                    "dataset_id": "ext",
                    "version": "2",
                    "generation_id": "ext2",
                    "operation_id": "op-ext2",
                    "parent_id": "ext",
                },
            },
        )
        _ = _register_domain(workspace, later, "observation")
        # Then the earlier pin still reads exactly what it read before,
        assert (
            market_inputs.load_pinned_observations(workspace, origin, budget=BUDGET).history
            == before.history
        )
        # and the new head carries both generations.
        head = publication.read_dataset(workspace, "ext", "2")
        extended = market_inputs.load_pinned_observations(
            workspace,
            market_inputs.GenerationPin(
                str(head["dataset_id"]),
                str(head["version"]),
                str(head["generation_id"]),
                str(head["chain_hash"]),
                str(head["manifest_hash"]),
            ),
            budget=BUDGET,
        )
        assert len(extended.history) == len(before.history) + 1


def test_observation_delta_must_be_derived_from_its_pinned_source(tmp_path: Path) -> None:
    # Given a registered observation whose exact transform is retained in raw/.
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        spec = _observation_spec(workspace, tmp_path / "sub.sqlite3")
        _ = _register_domain(workspace, spec, "observation")
        origin = _pin(workspace, "sub")
        # A failed attempt can leave an observation transform in raw/. Craft the one
        # naming the generation about to be published, so schema, contract identity
        # and publication identity all line up and only the values are wrong.
        forged_transform = json.loads(spec.read_bytes())
        forged_transform["dataset"] = {
            "dataset_id": "sub",
            "version": "2",
            "generation_id": "sub-forged",
            "operation_id": "op-sub-forged",
            "parent_id": "sub",
        }
        forged_raw = canonical_json_bytes(forged_transform)
        put_raw(workspace.paths.raw, forged_raw)
        # When a generic publication points at it while carrying a value that
        # transform never produced,
        sealed = workspace.paths.raw / origin.manifest_hash[:2] / origin.manifest_hash
        document = json.loads(sealed.read_bytes())
        prior = document["rows"][0]
        document.update(
            version="2",
            generation_id="sub-forged",
            operation_id="op-sub-forged",
            parent_id="sub",
            transform_sha256=hashlib.sha256(forged_raw).hexdigest(),
        )
        document["rows"] = [
            {
                **prior,
                "value": PANEL_OPEN + 1.0,
                "op": "SUPERSEDE",
                "supersedes_revision_id": prior["revision_id"],
                "revision_id": "forged-r1",
            }
        ]
        _ = publication.publish_document(
            workspace, import_document.parse_import(canonical_json_bytes(document))
        )
        head = publication.read_dataset(workspace, "sub", "2")
        forged = market_inputs.GenerationPin(
            str(head["dataset_id"]),
            str(head["version"]),
            str(head["generation_id"]),
            str(head["chain_hash"]),
            str(head["manifest_hash"]),
        )
        # Then the reader refuses values it never derived from the pinned source.
        with pytest.raises(ValueError, match=r"pinned source"):
            _ = market_inputs.load_pinned_observations(workspace, forged, budget=BUDGET)
        # The untouched earlier pin still reads.
        assert market_inputs.load_pinned_observations(workspace, origin, budget=BUDGET).history


def test_observation_verification_is_charged_against_the_retained_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given a registered observation the reader will hold live while it verifies.
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        spec = _observation_spec(workspace, tmp_path / "lease.sqlite3")
        _ = _register_domain(workspace, spec, "observation")
        origin = _pin(workspace, "lease")
        seen: list[ComputeBudget] = []
        original = market_inputs.verify_observation_content

        def record(target: Workspace, history: History, *, budget: ComputeBudget) -> str:
            seen.append(budget)
            return original(target, history, budget=budget)

        monkeypatch.setattr(market_inputs, "verify_observation_content", record)
        # When the series is read under an explicit lease,
        series = market_inputs.load_pinned_observations(workspace, origin, budget=BUDGET)
        # Then the verifier is charged for what the history holds live, so charge plus
        # live stays inside the caller's allowance rather than double-spending it.
        retained = market_inputs._retained_bytes(series.history)  # noqa: SLF001 -- accounting under test
        assert retained > 0
        assert seen[0].reserved_bytes == BUDGET.reserved_bytes + retained
        assert seen[0].available_bytes == BUDGET.available_bytes - retained


def test_chunked_panel_resolves_its_upstream_source_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given a panel published as two generations of one contract, as a bounded
    # chunk sequence is, and every generation sharing the same upstream pin.
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        first = _observation_spec(workspace, tmp_path / "chunk.sqlite3")
        _ = _register_domain(workspace, first, "observation")
        later = _observation_spec(
            workspace,
            tmp_path / "chunk2.sqlite3",
            value=52.5,
            options={
                "feature_at_us": 40,
                "dataset": {
                    "dataset_id": "chunk",
                    "version": "2",
                    "generation_id": "chunk2",
                    "operation_id": "op-chunk2",
                    "parent_id": "chunk",
                },
            },
        )
        _ = _register_domain(workspace, later, "observation")
        head = publication.read_dataset(workspace, "chunk", "2")
        pin = market_inputs.GenerationPin(
            str(head["dataset_id"]),
            str(head["version"]),
            str(head["generation_id"]),
            str(head["chain_hash"]),
            str(head["manifest_hash"]),
        )
        calls: list[SourcePin] = []
        original = research_inputs.resolve_source

        def counted(target: Workspace, source: SourcePin) -> dict[str, object]:
            calls.append(source)
            return original(target, source)

        monkeypatch.setattr(research_inputs, "resolve_source", counted)
        # When the whole chain is read,
        series = market_inputs.load_pinned_observations(workspace, pin, budget=BUDGET)
        # Then the upstream panel digest is recomputed once, not once per generation.
        assert len(series.history) == CHUNKS
        assert len(calls) == 1


def test_upstream_panel_is_admitted_before_it_is_hashed(tmp_path: Path) -> None:
    # Given an upstream panel much larger than the transform's mapped point table.
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        large = tmp_path / "wide"
        large.mkdir()
        spec = _observation_spec(workspace, large / "big.sqlite3", options={"panel_rows": 4000})
        _ = _register_domain(workspace, spec, "observation")
        # When re-derivation would hash that panel under a caller-owned lease,
        # Then it is refused before the hash instead of allocating outside the lease.
        with pytest.raises(ComputeResourceError, match="source table"):
            _ = research_inputs.native_input_document(
                workspace,
                spec.read_bytes(),
                expected_schema="aas-observation-transform-v1",
                budget=BUDGET,
            )
        # The bound is proportional, not a blanket refusal: a panel the lease covers
        # still re-derives.
        small = tmp_path / "narrow"
        small.mkdir()
        modest = _observation_spec(
            workspace,
            small / "ok.sqlite3",
            changes={"series_id": "OBSERVED-NARROW"},
            options={"panel_rows": 4},
        )
        _ = _register_domain(workspace, modest, "observation")
        document, _source = research_inputs.native_input_document(
            workspace,
            modest.read_bytes(),
            expected_schema="aas-observation-transform-v1",
            budget=BUDGET,
        )
        assert document.rows


def _lose_observation_contract(state_path: Path, name: str) -> None:
    """Lose one contract and its child inputs on the closed file, schema intact.

    Losing rows is an external event, so it is simulated on the closed file. The
    immutability triggers are restored so the schema still matches its checksum and only
    the rows are missing, which is the situation being tested.
    """
    with closing(sqlite3.connect(state_path)) as raw_state:
        triggers = [
            row[0]
            for row in raw_state.execute(
                "SELECT sql FROM sqlite_master WHERE type='trigger' AND tbl_name IN "
                "('feature_contracts','feature_inputs') AND sql LIKE '%DELETE%'"
            )
        ]
        for table in ("feature_contracts", "feature_inputs"):
            _ = raw_state.execute("DROP TRIGGER IF EXISTS immutable_" + table + "_delete")
        for table in ("feature_inputs", "feature_contracts"):
            _ = raw_state.execute(
                "DELETE FROM " + table + " WHERE name=?",  # noqa: S608 -- fixed table names
                (name,),
            )
        for statement in triggers:
            _ = raw_state.execute(statement)
        raw_state.commit()


def _generic_feature_import(workspace: Workspace) -> None:
    """Commit one generic feature_values import: an opaque commitment, no preimage kept.

    This is what the offline import route leaves behind. dataset_versions.transform_hash
    is NOT NULL, so this generation carries a 64-hex pointer exactly like a native one,
    while raw/ holds nothing under it.
    """
    raw = canonical_json_bytes(
        {
            "schema_version": "aas-market-import-v1",
            "dataset_id": "generic-features",
            "version": "1",
            "generation_id": "generic-features",
            "operation_id": "op-generic-features",
            "parent_id": None,
            "domain": "feature_values",
            "provider": "synthetic",
            "publication_at_us": None,
            "normalizer_version": "synthetic-v1",
            "transform_sha256": hashlib.sha256(b"unretained generic transform").hexdigest(),
            "instruments": [
                {"instrument_id": "ASSET_G", "asset_type": "equity", "venue": "SYNTHETIC"}
            ],
            "rows": [
                {
                    "contract_id": "GENERIC",
                    "contract_version": "1",
                    "contract_hash": "b" * 64,
                    "input_bundle_hash": "c" * 64,
                    "instrument_id": "ASSET_G",
                    "feature_at_us": 20,
                    "value": 1.5,
                    "value_state": "present",
                    "revision_id": "generic-r1",
                    "supersedes_revision_id": None,
                    "op": "ASSERT",
                    "available_at_us": 20,
                    "revision_known_at_us": 20,
                    "ingested_at_us": 30,
                }
            ],
        }
    )
    _ = publication.publish_document(workspace, import_document.parse_import(raw))


def test_db_verify_fails_when_an_observation_contract_is_lost(tmp_path: Path) -> None:
    # Given a committed observation publication whose SQLite contract rows are later
    # lost while the DuckDB generation and its catalog entry remain.
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        spec = _observation_spec(workspace, tmp_path / "lost.sqlite3")
        _ = _register_domain(workspace, spec, "observation")
        assert verify_workspace(workspace, budget=BUDGET)
        state_path = workspace.paths.state
    _lose_observation_contract(state_path, "OBSERVED/close")
    # When the workspace is verified, Then the orphaned publication fails instead of
    # an empty contract scan reporting success.
    with (
        open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace,
        pytest.raises(ValueError, match="no registered contract"),
    ):
        _ = verify_workspace(workspace, budget=BUDGET)


def test_live_transform_is_charged_before_the_upstream_is_admitted(tmp_path: Path) -> None:
    # Given a transform whose own decoded bytes are significant against the lease,
    # and an upstream panel that fits only while that charge is ignored.
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        spec = _observation_spec(
            workspace,
            tmp_path / "wide.sqlite3",
            options={"panel_rows": 700, "extra_instruments": 3000},
        )
        _ = _register_domain(workspace, spec, "observation")
        raw = spec.read_bytes()
        digest = hashlib.sha256(raw).hexdigest()
        # Ignoring what the transform holds live, the upstream admission passes.
        document, *_rest = research_inputs._observation_document(  # noqa: SLF001 -- the charge is under test
            workspace, raw, digest, None, BUDGET
        )
        assert document.rows
        # Charging it, as the entrypoint now does, refuses before the panel is hashed.
        charged = replace(BUDGET, reserved_bytes=BUDGET.reserved_bytes + len(raw) * 32)
        with pytest.raises(ComputeResourceError, match="source table"):
            _ = research_inputs._observation_document(  # noqa: SLF001 -- the charge is under test
                workspace, raw, digest, None, charged
            )
        # And native_input_document applies that charge itself rather than relying
        # on the caller to have subtracted it.
        with pytest.raises(ComputeResourceError, match="source table"):
            _ = research_inputs.native_input_document(
                workspace,
                raw,
                expected_schema="aas-observation-transform-v1",
                budget=BUDGET,
            )


def _sealed_bytes(workspace: Workspace, pin: market_inputs.GenerationPin) -> bytes:
    marker = market.marker_for(workspace.market, pin.generation_id)
    digest = str(marker["request_hash"])
    return (workspace.paths.raw / digest[:2] / digest).read_bytes()


def _lease(materialization_bytes: int) -> ComputeBudget:
    """One lease whose Python materialization allowance is exactly the measured size.

    DuckDB keeps three quarters of a budget's memory limit, so the allowance a caller
    can materialize under is the remaining quarter.
    """
    budget = ComputeBudget(Fraction(1), 4 * materialization_bytes)
    assert budget.available_bytes == materialization_bytes
    return budget


def test_a_generic_feature_import_coexists_with_an_observation_publication(
    tmp_path: Path,
) -> None:
    # Given one observation publication and one generic feature_values import, whose
    # transform_sha256 is an opaque commitment no preimage was ever retained for.
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        spec = _observation_spec(workspace, tmp_path / "mixed.sqlite3")
        _ = _register_domain(workspace, spec, "observation")
        _generic_feature_import(workspace)
        # When the workspace is verified, Then the opaque commitment is left closed
        # rather than opened as a native transform, and the observation publication
        # beside it is still authenticated.
        assert verify_workspace(workspace, budget=BUDGET)["verified"]
        state_path = workspace.paths.state
    # Backup and restore run the same verifier, so they carry the same coexistence.
    archive, target = tmp_path / "backup", tmp_path / "restored"
    backup(tmp_path / "home", archive)
    assert restore(archive, target)["restored"]
    _lose_observation_contract(state_path, "OBSERVED/close")
    # And when the contract rows are lost, the scan still fails: a generic import in
    # the same workspace does not hide the orphaned publication.
    with (
        open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace,
        pytest.raises(ValueError, match="no registered contract"),
    ):
        _ = verify_workspace(workspace, budget=BUDGET)


def test_the_sealed_document_is_charged_while_its_publication_is_verified(
    tmp_path: Path,
) -> None:
    # Given a committed observation publication whose sealed import document is
    # significant against the lease that classifies and verifies it.
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        spec = _observation_spec(workspace, tmp_path / "sealed.sqlite3", options={"points": POINTS})
        _ = _register_domain(workspace, spec, "observation")
        pin = _pin(workspace, "sealed")
        sealed = len(_sealed_bytes(workspace, pin))
        history = market_inputs.load_pinned_observations(workspace, pin, budget=BUDGET).history
        retained = market_inputs._retained_bytes(history)  # noqa: SLF001 -- accounting under test
        identity = ("OBSERVED/close", "v1")
        # A lease that admits the retained chain, and cannot also hold the sealed
        # document live while that chain is authenticated.
        lease = _lease(32 * sealed + 2 * retained)
        # Ignoring what the classifier holds live, the chain is admitted and the run
        # only stops further in, at the bounded read of a sealed delta.
        with pytest.raises(DescriptorTreeError, match="size cap"):
            _ = market_inputs._verify_observation_publication(  # noqa: SLF001 -- the charge is under test
                workspace, pin, {identity}, {}, lease
            )
        # Charging it, as the scan now does before it hands the lease down, moves the
        # refusal onto the admission itself instead of spending the allowance twice.
        with pytest.raises(ComputeResourceError, match="chain memory estimate"):
            market_inputs.verify_feature_publications(workspace, budget=lease)
        # A lease that can carry both still verifies the whole workspace.
        assert verify_workspace(workspace, budget=BUDGET)["verified"]


def test_contract_identities_are_admitted_before_they_are_fetched(tmp_path: Path) -> None:
    # Given a registered observation contract, whose name and version are TEXT with no
    # length bound in the schema.
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        spec = _observation_spec(workspace, tmp_path / "admit.sqlite3")
        _ = _register_domain(workspace, spec, "observation")
        identities, live = market_inputs._feature_contracts(  # noqa: SLF001 -- admission under test
            workspace, "aas-observation-definition-v1", BUDGET
        )
        assert identities == {("OBSERVED/close", "v1")}
        assert live > 0
        # When the lease cannot hold that set, Then it is refused before the fetch rather
        # than materialized first and accounted for afterwards.
        starved = replace(BUDGET, reserved_bytes=BUDGET.available_bytes - live + 1)
        with pytest.raises(ComputeResourceError, match="contract identities"):
            _ = market_inputs._feature_contracts(  # noqa: SLF001 -- admission under test
                workspace, "aas-observation-definition-v1", starved
            )


def test_the_loaded_chain_is_charged_before_its_transforms_are_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given a committed observation chain the verifier loads and then walks.
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        spec = _observation_spec(workspace, tmp_path / "walk.sqlite3", options={"points": POINTS})
        _ = _register_domain(workspace, spec, "observation")
        pin = _pin(workspace, "walk")
        history = market_inputs.load_pinned_observations(workspace, pin, budget=BUDGET).history
        retained = market_inputs._retained_bytes(history)  # noqa: SLF001 -- accounting under test
        seen: list[ComputeBudget] = []
        original = market_inputs._transform  # noqa: SLF001 -- accounting under test

        def record(
            target: Workspace, generation: str, budget: ComputeBudget
        ) -> tuple[str, dict[str, object], ComputeBudget]:
            seen.append(budget)
            return original(target, generation, budget)

        monkeypatch.setattr(market_inputs, "_transform", record)
        # When that publication is verified,
        _ = market_inputs._verify_observation_publication(  # noqa: SLF001 -- accounting under test
            workspace, pin, {("OBSERVED/close", "v1")}, {}, BUDGET
        )
        # Then the walk's transform reads run on a lease charged for the history that is
        # still live, while the classifying read before the load is not.
        assert seen[0].reserved_bytes == BUDGET.reserved_bytes
        assert seen[1].reserved_bytes >= BUDGET.reserved_bytes + retained


def test_a_cached_chain_is_charged_while_the_next_publication_is_classified(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given two observation datasets, so the scan classifies the second one while the
    # first dataset's verified chain is still cached.
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        first = _observation_spec(workspace, tmp_path / "one.sqlite3", options={"points": POINTS})
        _ = _register_domain(workspace, first, "observation")
        second = _observation_spec(
            workspace,
            tmp_path / "two.sqlite3",
            changes={"series_id": "OBSERVED-TWO"},
            options={"points": POINTS},
        )
        _ = _register_domain(workspace, second, "observation")
        pins = [_pin(workspace, "one"), _pin(workspace, "two")]
        cached = market_inputs._retained_bytes(  # noqa: SLF001 -- accounting under test
            market_inputs.load_pinned_observations(workspace, pins[0], budget=BUDGET).history
        )
        reads: dict[str, int] = {}
        original = market_inputs._raw_payload  # noqa: SLF001 -- accounting under test

        def record(target: Workspace, digest: str, budget: ComputeBudget) -> bytes:
            reads.setdefault(digest, budget.reserved_bytes)
            return original(target, digest, budget)

        monkeypatch.setattr(market_inputs, "_raw_payload", record)
        # When the whole scan runs,
        market_inputs.verify_feature_publications(workspace, budget=BUDGET)
        # Then the second dataset's sealed document is read on a lease that already
        # charges the chain the first dataset left live.
        assert reads[pins[1].manifest_hash] >= reads[pins[0].manifest_hash] + cached


def test_the_proxy_delta_and_transform_are_charged_where_they_stay_live(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given a registered proxy publication, verified through the same merged pass.
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        path = _proxy_spec(workspace, tmp_path / "proxy.sqlite3", ("PROXY", "v1", "0.1"))
        _ = _register_domain(workspace, path, "proxy")
        pin = _pin(workspace, "proxy")
        sealed = len(_sealed_bytes(workspace, pin))
        reads: dict[str, int] = {}
        deltas: list[History] = []
        outer: list[ComputeBudget] = []
        inner: list[ComputeBudget] = []
        original_payload = market_inputs._raw_payload  # noqa: SLF001 -- accounting under test
        original_delta = market_inputs._proxy_publication_delta  # noqa: SLF001 -- accounting under test
        original_content = market_inputs.verify_proxy_content
        original_publication = market_inputs._proxy_publication  # noqa: SLF001 -- accounting under test

        def record_payload(target: Workspace, digest: str, budget: ComputeBudget) -> bytes:
            _ = reads.setdefault(digest, budget.reserved_bytes)
            return original_payload(target, digest, budget)

        def record_delta(
            target: Workspace,
            generation: market_inputs.GenerationPin,
            document: import_document.ImportDocument,
            budget: ComputeBudget,
        ) -> History:
            selected = original_delta(target, generation, document, budget)
            deltas.append(selected)
            return selected

        def record_content(target: Workspace, history: History, *, budget: ComputeBudget) -> str:
            outer.append(budget)
            return original_content(target, history, budget=budget)

        def record_publication(
            target: Workspace,
            transform_hash: str,
            transform: dict[str, object],
            budget: ComputeBudget,
        ) -> History:
            inner.append(budget)
            return original_publication(target, transform_hash, transform, budget)

        monkeypatch.setattr(market_inputs, "_raw_payload", record_payload)
        monkeypatch.setattr(market_inputs, "_proxy_publication_delta", record_delta)
        monkeypatch.setattr(market_inputs, "verify_proxy_content", record_content)
        monkeypatch.setattr(market_inputs, "_proxy_publication", record_publication)
        # When the scan verifies it,
        market_inputs.verify_feature_publications(workspace, budget=BUDGET)
        # Then between reading the sealed document and verifying the definition the lease
        # grew by exactly that document plus the whole delta, both of which stay live,
        # and the rebuild below it charges the decoded transform on top.
        held = market_inputs._retained_bytes(deltas[0])  # noqa: SLF001 -- accounting under test
        assert held > 0
        assert outer[0].reserved_bytes - reads[pin.manifest_hash] == 32 * sealed + held
        assert inner[0].reserved_bytes > outer[0].reserved_bytes


def test_the_second_identity_set_is_admitted_against_the_first(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given both a proxy and an observation contract, so neither identity set is empty.
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        path = _proxy_spec(workspace, tmp_path / "proxy.sqlite3", ("PROXY", "v1", "0.1"))
        _ = _register_domain(workspace, path, "proxy")
        spec = _observation_spec(workspace, tmp_path / "pair.sqlite3")
        _ = _register_domain(workspace, spec, "observation")
        seen: list[tuple[int, int]] = []
        original = market_inputs._feature_contracts  # noqa: SLF001 -- admission under test

        def record(
            target: Workspace, record_schema: str, budget: ComputeBudget
        ) -> tuple[set[tuple[str, str]], int]:
            identities, live = original(target, record_schema, budget)
            seen.append((budget.reserved_bytes, live))
            return identities, live

        monkeypatch.setattr(market_inputs, "_feature_contracts", record)
        # When the scan admits them,
        market_inputs.verify_feature_publications(workspace, budget=BUDGET)
        # Then the first set is admitted on the caller's lease and the second on what is
        # left once the first one is already live.
        assert seen[0][0] == BUDGET.reserved_bytes
        assert seen[0][1] > 0
        assert seen[1][0] == BUDGET.reserved_bytes + seen[0][1]


def test_a_price_chain_is_charged_with_its_own_schema(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given a committed prices generation, whose rows are wider than a feature row.
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        spec = _spec(workspace, tmp_path / "prices.sqlite3", [_source_row()])
        _ = _register_domain(workspace, spec, "price")
        generation = str(
            publication.read_dataset(workspace, "synthetic-prices", "1")["generation_id"]
        )
        seen: list[ComputeBudget] = []
        original = market_inputs._sealed_publication  # noqa: SLF001 -- accounting under test

        def record(
            target: Workspace, marker: Row, delta: History, budget: ComputeBudget
        ) -> import_document.ImportDocument:
            seen.append(budget)
            return original(target, marker, delta, budget)

        monkeypatch.setattr(market_inputs, "_sealed_publication", record)
        # When its sealed deltas are verified,
        history = market_inputs.verify_sealed_publication(workspace, generation, budget=BUDGET)
        # Then the retained history is charged with the prices schema, which is wider
        # than the feature schema this module defaults to.
        charged = market_inputs._retained_bytes(history, "prices")  # noqa: SLF001 -- accounting under test
        assert charged > market_inputs._retained_bytes(history)  # noqa: SLF001 -- accounting under test
        assert seen[0].reserved_bytes == BUDGET.reserved_bytes + charged
