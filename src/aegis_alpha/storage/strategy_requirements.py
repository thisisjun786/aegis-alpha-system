"""SELECT-only execution definitions from explicit admitted workspace connections."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import replace

from aegis_alpha.engine.requirements import (
    ExecutionDefinition,
    derive_execution_definition,
)
from aegis_alpha.storage.input_pins import ConventionPin, read_convention
from aegis_alpha.storage.strategies import load_strategy


def read_execution_definition(  # noqa: PLR0913 -- exact strategy pin and explicit stores
    connection: sqlite3.Connection,
    id: str,  # noqa: A002 -- approved public keyword
    version: str,
    sha256: str,
    convention_bindings: tuple[ConventionPin, ...] | None = None,
    *,
    state_connection: sqlite3.Connection | None = None,
) -> ExecutionDefinition:
    """Verify stored evidence before resolving supported supplied conventions.

    The caller owns admission, lifetime and same-workspace provenance of both
    connections. No state is discovered. Opaque non-basis documents are verified
    for integrity only; their roles remain unresolved and never grant execution.
    """
    definition = derive_execution_definition(load_strategy(connection, id, version, sha256))
    if convention_bindings is None:
        return definition
    if not isinstance(convention_bindings, tuple):
        raise TypeError("convention bindings must be a tuple of ConventionPin values")
    kinds: set[str] = set()
    for pin in convention_bindings:
        if not isinstance(pin, ConventionPin):
            raise TypeError("convention bindings must contain ConventionPin values")
        if pin.kind in kinds or pin.kind not in definition.required_convention_roles:
            raise ValueError("duplicate or unrequired convention kind: " + pin.kind)
        kinds.add(pin.kind)
    if not convention_bindings:
        return definition
    if state_connection is None:
        raise ValueError("nonempty convention bindings require an explicit state connection")
    return _bind_conventions(definition, convention_bindings, state_connection)


def _bind_conventions(
    definition: ExecutionDefinition,
    bindings: tuple[ConventionPin, ...],
    state: sqlite3.Connection,
) -> ExecutionDefinition:
    for pin in bindings:
        canonical = read_convention(state, pin)
        if pin.kind != "basis":
            continue
        # T37 already validated these canonical whole-document bytes and basis schema.
        basis = json.loads(canonical)["payload"]["price_basis"]
        for requirement in definition.input_requirements:
            if requirement.basis == "capital" and basis == "total_return":
                raise ValueError(
                    "total_return basis is incompatible with capital requirement: "
                    + ", ".join(requirement.identifiers)
                )
        definition = replace(
            definition,
            input_requirements=tuple(
                replace(requirement, basis=basis) if requirement.role == "prices" else requirement
                for requirement in definition.input_requirements
            ),
            unresolved_convention_roles=tuple(
                role for role in definition.unresolved_convention_roles if role != "basis"
            ),
        )
    return definition
