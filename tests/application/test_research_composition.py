"""A declared sample composition: two pinned sleeves and the engine's own switch.

A Snowball sample is not one strategy. It is an offensive sleeve, a defensive sleeve and
a canary that chooses between them, and the public engine has no single contract for
that shape. This module holds the composition to the one thing that makes it safe: the
condition is the switch the installed engine already computes from the offense sleeve's
declared canary, named by a literal, never an expression a declaration carries.
"""

from __future__ import annotations

import hashlib
import json
import time
from datetime import date
from decimal import Decimal
from typing import TYPE_CHECKING, Any, cast

import pytest

from aegis_alpha.application.backtest_prepare import PreparedResearchRun, prepare_research_run
from aegis_alpha.application.research_run import (
    PREPARED_COMPOSITION_SCHEMA,
    RESEARCH_COMPOSITION_SCHEMA,
    SWITCH_RULE,
    ResearchRunError,
    parse_research_composition_request,
    parse_research_run_request,
)
from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.engine import (
    ENGINE_BUNDLE_SCHEMA_V1,
    ENGINE_CONTRACT_VERSION_V1,
    CalendarConventions,
    EngineContract,
    FeatureMatrixSpec,
    MembershipRow,
    StaleGateSpec,
    StrategyRecord,
    membership_hash,
)
from aegis_alpha.storage.input_pins import register_definition
from aegis_alpha.storage.strategy_import import register_strategy
from aegis_alpha.storage.workspace import initialize, open_workspace
from tests.application.test_backtest_prepare import BUDGET, DAYS, micros, stored_request
from tests.application.test_research_execution import (
    _declaration,
    _panel,
    _prepared,
)

if TYPE_CHECKING:
    from pathlib import Path

type Document = dict[str, Any]

J = "aas-canonical-json-sha256-v1"
# is_bad_score fires on a strictly negative return, and ASSET_A rises to January and
# falls after it. Over the full fixture window a canary on ASSET_A therefore fires on the
# March decision and not on the two before it, so both branches of the switch are
# exercised by what the panel actually contains rather than by forcing a flag.
CANARY = "ASSET_A"


def _record(name: str, assets: list[str], canary: list[str]) -> StrategyRecord:
    return StrategyRecord(
        name=name,
        description="synthetic sleeve",
        cash_asset="CASH",
        role="research",
        similarity_group=None,
        offensive_config={
            "strategy_type": "relative",
            "assets": assets,
            "top_n": 1,
            "scoring": {"method": "return_rate", "horizon": 2},
            "reference_asset": assets[0],
        },
        defensive_config={"assets": []},
        canary_config=(
            {
                "canary_mode": "OR",
                "assets": canary,
                "enabled": [True] * len(canary),
                "scoring": {"method": "return_rate", "horizon": 2},
            }
            if canary
            else {"canary_mode": "OR", "assets": [], "enabled": []}
        ),
        signals_config={},
        variant_of=None,
        variant_spec=None,
    )


def _bundle(name: str, assets: list[str], canary: list[str]) -> tuple[bytes, Document]:
    """One sleeve's exact bundle bytes and the membership its contract names."""
    rows = (MembershipRow(name, Decimal(1)),)
    digest = membership_hash(rows)
    contract = EngineContract(
        contract_version=ENGINE_CONTRACT_VERSION_V1,
        pack=(_record(name, assets, canary),),
        feature_matrix=FeatureMatrixSpec(
            momentum_scores=(),
            moving_average_months=(),
            ma_window_includes_current_month=True,
            return_months=(2,),
            includes_latest_price=True,
        ),
        macro_signals=(),
        calendar=CalendarConventions(
            "calendar_month_end",
            "prior_calendar_month_end",
            1,
            3,
            "synthetic",
            "synthetic",
            "synthetic",
            "synthetic",
        ),
        stale_gates=StaleGateSpec(50, 50),
        ensemble_membership_reference="ensemble:" + digest,
        derived_series=(),
    )
    raw = canonical_json_bytes(
        {
            "schema_version": ENGINE_BUNDLE_SCHEMA_V1,
            "bundle_id": name,
            "bundle_version": "1",
            "contract": contract,
        }
    )
    member = {
        "schema": "aas-ensemble-membership-v1",
        "hash_format": J,
        "id": name + "-membership",
        "version": "1",
        "membership_sha256": digest,
        "rows": [{"name": name, "weight": "1"}],
    }
    return raw, member


@pytest.fixture(autouse=True)
def compute_environment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AAS_HOST_CPU_LIMIT", "1")
    monkeypatch.setenv("AAS_HOST_MEMORY_LIMIT_BYTES", str(1024 * 1024 * 1024))
    monkeypatch.setenv("AAS_CPU_LIMIT", "1")
    monkeypatch.setenv("AAS_MEMORY_LIMIT_BYTES", str(512 * 1024 * 1024))
    monkeypatch.setenv("AAS_COMPUTE_LOCK_FILE", str(tmp_path / "compute.lock"))


def _register(workspace: object, root: Path, name: str, raw: bytes, member: Document) -> Document:
    path = root / (name + ".json")
    _ = path.write_bytes(raw)
    receipt = register_strategy(
        cast("Any", workspace), path, hashlib.sha256(raw).hexdigest(), name, "1"
    )
    body = canonical_json_bytes(member)
    pin = register_definition(
        cast("Any", workspace),
        body,
        expected_file_sha256=hashlib.sha256(body).hexdigest(),
        budget=BUDGET,
    )
    store = (
        cast("Any", workspace).strategies.execute("SELECT store_id FROM store_info").fetchone()[0]
    )
    return {
        "strategy_store_id": store,
        "strategy_id": receipt["strategy_id"],
        "version": receipt["version"],
        "raw_sha256": receipt["raw_sha256"],
        "contract_sha256": receipt["contract_sha256"],
        "membership": {
            "kind": pin.kind,
            "id": pin.id,
            "version": pin.version,
            "hash": pin.hash,
        },
    }


@pytest.fixture
def composed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[Path, Document, Document, Document]:
    """One installation carrying an observation panel and two registered sleeves."""
    monkeypatch.setattr(time, "time_ns", lambda: micros(date(2026, 6, 1)) * 1000)
    home = tmp_path / "home"
    _ = initialize(home)
    offense_raw, offense_member = _bundle("syn-offense", ["ASSET_B", "REF_X"], [CANARY])
    # The defensive sleeve holds a different asset, so a switched decision is visible in
    # the targets rather than only in the record of which sleeve ran.
    defense_raw, defense_member = _bundle("syn-defense", ["REF_X"], [])
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        body = stored_request(workspace, tmp_path)
        close = _panel(workspace, tmp_path, "obs-close", "close")
        opening = _panel(workspace, tmp_path, "obs-open", "open")
        offense = _register(workspace, tmp_path, "syn-offense", offense_raw, offense_member)
        defense = _register(workspace, tmp_path, "syn-defense", defense_raw, defense_member)
        workspace.state.commit()
        assert workspace.strategies is not None
        workspace.strategies.commit()
        _ = workspace.market.execute("CHECKPOINT")
    return home, _wide(_declaration(body, close, opening)), offense, defense


def _wide(base: Document) -> Document:
    """Open the window to the whole fixture, so the canary has somewhere to turn down."""
    return base | {
        "history": {"start": DAYS[0].isoformat(), "end": DAYS[-1].isoformat()},
        "period": {"start": DAYS[2].isoformat(), "end": DAYS[-1].isoformat()},
    }


def _composition(base: Document, offense: Document, defense: Document) -> Document:
    """The same declared run, with a sample composition in place of one strategy."""
    body = cast("Document", json.loads(json.dumps(base)))
    del body["strategy"]
    del body["membership"]
    body["schema_version"] = RESEARCH_COMPOSITION_SCHEMA
    body["composition"] = {
        "sample_id": "synthetic-sample",
        "switch": SWITCH_RULE,
        "sleeves": {"offense": offense, "defense": defense},
    }
    return body


def _as_sleeve_run(base: Document, sleeve: Document) -> Document:
    """The same declaration as a v2 sleeve run over one of the composed sleeves."""
    strategy = {key: value for key, value in sleeve.items() if key != "membership"}
    return base | {"strategy": strategy, "membership": sleeve["membership"]}


def _prepared_composition(home: Path, body: Document) -> PreparedResearchRun:
    with open_workspace(home) as workspace:
        return prepare_research_run(
            workspace,
            parse_research_composition_request(canonical_json_bytes(body)),
            budget=BUDGET,
        )


def _refused(home: Path, body: Document, message: str) -> None:
    with pytest.raises((ValueError, ResearchRunError), match=message):
        _ = _prepared_composition(home, body)


def test_the_switch_moves_decisions_between_the_two_sleeves(
    composed: tuple[Path, Document, Document, Document],
) -> None:
    """Both branches run, chosen by the engine's own canary rather than a declared flag.

    ASSET_A rises to January and falls after it, so a canary on it fires on the March
    decision and not on the two before. If either branch stopped being exercised this
    test would stop proving that a composition is a switch rather than a relabelled run.
    """
    home, base, offense, defense = composed
    prepared = _prepared_composition(home, _composition(base, offense, defense))
    roles = [decision.sleeve.role for decision in prepared.decisions]
    assert set(roles) == {"offense", "defense"}
    block = cast("Document", json.loads(prepared.provenance)["composition"])
    assert block["switch"] == SWITCH_RULE
    assert block["defensive_decision_count"] == roles.count("defense")
    # The sealed record names the dates the defensive sleeve supplied, so it can be held
    # against the envelope's own targets rather than merely counted.
    defensive = [
        decision.receipt.as_of.isoformat()
        for decision in prepared.decisions
        if decision.sleeve.role == "defense"
    ]
    assert cast("list[str]", block["defensive_decisions"]) == defensive
    targets = cast("Document", json.loads(prepared.envelope.canonical_bytes)["targets"])
    # The defensive sleeve holds a different asset, so the switch is visible in what the
    # run actually targeted, not only in what it recorded about itself.
    assert all(set(cast("Document", targets[day])) == {"REF_X"} for day in defensive)
    assert all(
        set(cast("Document", weights)) == {"ASSET_B"}
        for day, weights in targets.items()
        if day not in defensive
    )


def test_a_composition_that_never_switches_still_records_that_honestly(
    composed: tuple[Path, Document, Document, Document],
) -> None:
    """A window with no downturn leaves every decision with the offensive sleeve."""
    home, base, offense, defense = composed
    narrow = base | {
        "history": {"start": DAYS[0].isoformat(), "end": DAYS[4].isoformat()},
        "period": {"start": DAYS[2].isoformat(), "end": DAYS[4].isoformat()},
    }
    prepared = _prepared_composition(home, _composition(narrow, offense, defense))
    assert {decision.sleeve.role for decision in prepared.decisions} == {"offense"}
    block = cast("Document", json.loads(prepared.provenance)["composition"])
    assert block["defensive_decisions"] == []
    assert block["defensive_decision_count"] == 0


def test_a_composition_is_not_a_sleeve_run(
    composed: tuple[Path, Document, Document, Document],
) -> None:
    """Sample and sleeve are different records even over identical inputs."""
    home, base, offense, defense = composed
    sleeve = _prepared(home, _as_sleeve_run(base, offense))
    sample = _prepared_composition(home, _composition(base, offense, defense))
    assert sleeve.run_id != sample.run_id
    assert json.loads(sleeve.provenance)["scope"] == "sleeve"
    assert json.loads(sample.provenance)["scope"] == "sample-composition"
    assert json.loads(sample.provenance)["schema"] == PREPARED_COMPOSITION_SCHEMA
    assert json.loads(sleeve.provenance)["composition"] is None
    # The sleeve run is the offensive sleeve alone, so the switched decision is absent
    # from it. A sample is not its own offensive sleeve under another name.
    assert sleeve.envelope.canonical_bytes != sample.envelope.canonical_bytes


def test_a_composition_repeats_exactly(
    composed: tuple[Path, Document, Document, Document],
) -> None:
    home, base, offense, defense = composed
    body = _composition(base, offense, defense)
    first, second = _prepared_composition(home, body), _prepared_composition(home, body)
    assert first.run_id == second.run_id
    assert first.envelope.canonical_bytes == second.envelope.canonical_bytes
    assert first.provenance == second.provenance


def test_a_defensive_sleeve_with_its_own_canary_is_refused(
    composed: tuple[Path, Document, Document, Document], tmp_path: Path
) -> None:
    """One switch is what this contract names; a second would make it recursive."""
    home, base, offense, _defense = composed
    raw, member = _bundle("syn-defense-canary", ["REF_X"], [CANARY])
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        recursive = _register(workspace, tmp_path, "syn-defense-canary", raw, member)
        workspace.state.commit()
        assert workspace.strategies is not None
        workspace.strategies.commit()
    _refused(home, _composition(base, offense, recursive), "declares its own canary")


def test_one_sleeve_named_twice_is_refused(
    composed: tuple[Path, Document, Document, Document],
) -> None:
    """A switch that cannot change the outcome is not a composition."""
    _home, base, offense, _defense = composed
    body = _composition(base, offense, offense)
    with pytest.raises(ResearchRunError, match="two distinct sleeves"):
        parse_research_composition_request(canonical_json_bytes(body))


def test_a_condition_the_contract_does_not_name_is_refused(
    composed: tuple[Path, Document, Document, Document],
) -> None:
    """The switch is a literal. A declaration cannot bring a condition of its own."""
    _home, base, offense, defense = composed
    body = _composition(base, offense, defense)
    cast("Document", body["composition"])["switch"] = "weights > 0.5"
    with pytest.raises(ResearchRunError, match="composition switch must be"):
        parse_research_composition_request(canonical_json_bytes(body))


def test_a_sleeve_the_store_never_admitted_is_refused(
    composed: tuple[Path, Document, Document, Document],
) -> None:
    """How S1 stays refused: it compiles to no bundle, so no store can pin one.

    The refusal is the ordinary strategy-pin check rather than a special case, which is
    what keeps it from being worked around by declaring a sample id.
    """
    home, base, offense, defense = composed
    absent = cast("Document", json.loads(json.dumps(defense)))
    absent["strategy_id"] = "5c4d8a49-never-compiled-offense"
    _refused(home, _composition(base, offense, absent), "strategy|admitted|pin")


def test_the_composition_schema_and_the_run_schema_do_not_admit_each_other(
    composed: tuple[Path, Document, Document, Document],
) -> None:
    """A consumer admitting one schema does not silently admit the other."""
    _home, base, offense, defense = composed
    body = _composition(base, offense, defense)
    # Each parser refuses the other's root outright, before any version is read.
    with pytest.raises(ResearchRunError, match="unknown keys: composition"):
        parse_research_run_request(canonical_json_bytes(body))
    with pytest.raises(ResearchRunError, match="unknown keys: membership, strategy"):
        parse_research_composition_request(canonical_json_bytes(_as_sleeve_run(base, offense)))
    # And the version check itself, on a root that would otherwise pass.
    versioned = body | {"schema_version": "aas-research-run-v2"}
    with pytest.raises(ResearchRunError, match="is not " + RESEARCH_COMPOSITION_SCHEMA):
        parse_research_composition_request(canonical_json_bytes(versioned))
