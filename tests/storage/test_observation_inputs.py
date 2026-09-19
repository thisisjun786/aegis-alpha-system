"""Observed research prices: binary64 admission, uncertified pins and route separation."""

from __future__ import annotations

import argparse
import hashlib
import json
import struct
from decimal import Decimal, localcontext
from fractions import Fraction
from typing import TYPE_CHECKING

import pytest

from aegis_alpha.application.data_cli import execute_native_data
from aegis_alpha.application.storage_cli import add_commands
from aegis_alpha.compute_resources import ComputeBudget
from aegis_alpha.storage import market, market_inputs, publication
from aegis_alpha.storage.verification import verify_workspace
from aegis_alpha.storage.workspace import initialize, open_workspace
from tests.storage.test_research_inputs import (
    _change,
    _hash_json,
    _register_domain,
    _source_row,
    _spec,
)

if TYPE_CHECKING:
    from pathlib import Path

    from aegis_alpha.storage.workspace import Workspace

BUDGET = ComputeBudget(Fraction(1), 64 * 1024 * 1024)
NUMBERS = ("open", "high", "low", "close", "volume")
# Exactly a promoted binary32, like every value in the retained research panel.
PANEL_OPEN = 49.29364776611328
DECIMAL_SCALE = Decimal("0.000000000001")
# An additively adjusted series legitimately falls below zero.
NEGATIVE_OBSERVATION = -1.5
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
        "knowledge": None,
        "dataset": None,
        **(options or {}),
    }
    instrument = str(settings["instrument"])
    # One shared observed panel, so every generation pins the same provenance and contract.
    panel = path.parent / "observed-panel.json"
    if not panel.exists():
        panel = _spec(workspace, path.parent / "observed-panel.sqlite3", [_source_row()])
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
    spec = _spec(workspace, path, [row])
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
            }
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
