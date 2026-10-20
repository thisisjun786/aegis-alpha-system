"""Re-authenticate stored run inputs after calculation, without evaluating a strategy."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

from aegis_alpha.application.research_run import parse_declared_request
from aegis_alpha.application.run_staging import admit, json_object, json_text
from aegis_alpha.engine.codec import decode_json
from aegis_alpha.storage.input_pins import DefinitionPin, read_definition
from aegis_alpha.storage.market_inputs import (
    GenerationPin,
    admit_native_input,
    load_pinned_observations,
    verify_head_binding,
    verify_sealed_publication,
)
from aegis_alpha.storage.strategies import load_strategy

if TYPE_CHECKING:
    from aegis_alpha.compute_resources import ComputeBudget
    from aegis_alpha.storage.workspace import Workspace


def _strategy(workspace: Workspace, pin: dict[str, object]) -> None:
    connection = workspace.strategies
    if connection is None:
        raise ValueError("private strategy database is unavailable")
    if (
        connection.execute("SELECT store_id FROM store_info").fetchone()[0]
        != pin["strategy_store_id"]
    ):
        raise ValueError("strategy store pin mismatch")
    bundle = load_strategy(
        connection,
        json_text(pin["strategy_id"], "strategy_id"),
        json_text(pin["version"], "version"),
        json_text(pin["raw_sha256"], "raw_sha256"),
    )
    if bundle.contract_sha256 != pin["contract_sha256"]:
        raise ValueError("strategy contract hash mismatch")


def verify_run_inputs(workspace: Workspace, raw: bytes, *, budget: ComputeBudget) -> None:
    """Verify the stored request's references in the same admission as commit_run.

    Bundle verification already checks every bound pin. Strict generation bindings
    additionally need their retained publication/transform bytes. Declared requests
    leave several references unbound; check both sleeves and every declared data grant
    through their existing storage owners. Each materialization is released before the
    next, and neither replay nor accounting is called here.
    """
    # The JSON mapping and parsed declaration can coexist. Reserve both graphs
    # before expanding either, at the run store's document expansion allowance.
    admit(budget, 2 * len(raw))
    budget = replace(budget, reserved_bytes=budget.reserved_bytes + 256 * len(raw))
    body = json_object(decode_json(raw), "stored request")
    if body.get("schema") == "aas-backtest-request-v1":
        _strategy(workspace, json_object(body["strategy"], "strategy"))
        _strict_publications(workspace, body, budget)
        return
    declaration = parse_declared_request(raw)
    if declaration.composition is None:
        sleeves = [json_object(body["strategy"], "strategy") | {"membership": body["membership"]}]
    else:
        composition = json_object(body["composition"], "composition")
        sleeves = [
            json_object(pin, "sleeve")
            for pin in json_object(composition["sleeves"], "sleeves").values()
        ]
    for sleeve in sleeves:
        _strategy(workspace, sleeve)
        membership = json_object(sleeve["membership"], "membership")
        read_definition(
            workspace,
            DefinitionPin(
                *(json_text(membership[key], key) for key in ("kind", "id", "version", "hash"))
            ),
            budget=budget,
        )
    for observation in declaration.observations:
        load_pinned_observations(
            workspace,
            GenerationPin(
                observation.dataset_id,
                observation.version,
                observation.generation_id,
                observation.chain_hash,
                observation.manifest_hash,
            ),
            budget=budget,
        )
    bindings = [grant.binding for grant in declaration.macro]
    if declaration.prices is not None:
        bindings.append(declaration.prices)
    for grant in declaration.fx_conversions:
        bindings.append(grant.binding)
        if grant.prices is not None:
            bindings.append(grant.prices)
    for binding in bindings:
        verify_head_binding(workspace, binding, budget=budget)


def _strict_publications(
    workspace: Workspace, body: dict[str, object], budget: ComputeBudget
) -> None:
    bindings = body["bindings"]
    if not isinstance(bindings, list):
        raise TypeError("bindings must be an array")
    for item in bindings:
        binding = json_object(item, "binding")
        if binding["ref_kind"] != "generation":
            continue
        row = workspace.state.execute(
            "SELECT dataset_id,version,generation_id,chain_hash,manifest_hash "
            "FROM dataset_versions "
            "WHERE dataset_id=? AND version=? AND status='committed'",
            (binding["ref_id"], binding["ref_version"]),
        ).fetchone()
        if row is None:
            raise ValueError("generation reference is not registered")
        pin = GenerationPin(*row)
        if binding["role"] in {"signal_prices", "execution_prices", "sessions"}:
            admit_native_input(
                workspace,
                pin,
                expected_schema="aas-sessions-transform-v1"
                if binding["role"] == "sessions"
                else "aas-price-transform-v1",
                budget=budget,
            )
        else:
            verify_sealed_publication(workspace, pin.generation_id, budget=budget)
