"""Synthetic identity registry documents and an installation with one linked source."""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

from aegis_alpha.storage.identity import REGISTRY_SCHEMA
from aegis_alpha.storage.raw import put_raw
from aegis_alpha.storage.state import atomic

if TYPE_CHECKING:
    from aegis_alpha.storage.workspace import Workspace

SOURCE = "sl:synthetic-master"
UNBOUNDED = -(2**63)


def link_source(workspace: Workspace, snapshot_id: str = SOURCE, content: bytes = b"m") -> None:
    relative, digest, size = put_raw(workspace.paths.raw, content)
    with atomic(workspace.state):
        workspace.state.execute(
            "INSERT INTO source_snapshots VALUES (?,'source-library',1,2,NULL,'raw_verified')",
            (snapshot_id,),
        )
        workspace.state.execute(
            "INSERT INTO source_files VALUES (?,?,?,?)", (snapshot_id, relative, digest, size)
        )


def anchor(token: str, namespace: str = "norgate_assetid") -> dict[str, object]:
    return {"anchor_namespace": namespace, "anchor_token": token}


def instrument(
    token: str, *, issuer: dict[str, object] | None = None, venue: str = "NYSE"
) -> dict[str, object]:
    return {**anchor(token), "issuer": issuer, "asset_type": "equity", "venue": venue}


def assertion(  # noqa: PLR0913 -- one synthetic row spells every retained column
    token: str,
    *,
    namespace: str = "ticker",
    value: str | None = None,
    valid: tuple[int, int | None] = (UNBOUNDED, None),
    known: int = 2,
    supersedes: str | None = None,
    source: str = SOURCE,
) -> dict[str, object]:
    key = value if value is not None else token
    return {
        "instrument": anchor(token),
        "provider": "norgate",
        "namespace": namespace,
        "token": key,
        "valid_from_us": valid[0],
        "valid_to_us": valid[1],
        "known_from_us": known,
        "supersedes_assertion_id": supersedes,
        "source_snapshot_id": source,
        "source_hash": hashlib.sha256(f"{token}/{key}/{known}".encode()).hexdigest(),
    }


def document(
    *,
    issuers: list[dict[str, object]] | None = None,
    instruments: list[dict[str, object]] | None = None,
    assertions: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    return {
        "schema": REGISTRY_SCHEMA,
        "issuers": issuers or [],
        "instruments": instruments or [],
        "assertions": assertions or [],
    }


def master(count: int) -> dict[str, object]:
    """A Norgate-master-shaped registry: one anchor assertion per minted instrument."""
    tokens = [str(100000 + index) for index in range(count)]
    return document(
        instruments=[instrument(token) for token in tokens],
        assertions=[assertion(token, namespace="norgate_assetid") for token in tokens],
    )
