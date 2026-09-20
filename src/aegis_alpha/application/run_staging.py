"""Staging shared by the two integrated run entry points.

`run_backtest` records a run of a certified request and `run_research` records a declared
uncertified one. The documents they prepare and the requests they register are different
contracts, and nothing here converts one into the other. What surrounds the calculation
is one procedure in both: resolve the installation, charge a materialization before it
happens rather than after it is in memory, hold nothing live across a stage that
materializes, and end a durable run rather than strand it when something fails.

Nothing here calculates, registers or commits, and nothing here reads a request
contract. The one document it builds is the input-bundle name, which is derived from
content that either contract supplies.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, replace
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING, cast

from aegis_alpha.compute_resources import ComputeResourceError
from aegis_alpha.data.serialization import content_sha256
from aegis_alpha.engine.codec import decode_json
from aegis_alpha.storage.backtest_requests import read_backtest_request
from aegis_alpha.storage.input_pins import HASH_FORMAT
from aegis_alpha.storage.locks import private_directory, storage_lock_targets
from aegis_alpha.storage.paths import load_paths, resolve_home
from aegis_alpha.storage.runs import RunStorageError, commit_run, fail_run
from aegis_alpha.storage.workspace import open_workspace

if TYPE_CHECKING:
    from aegis_alpha.compute_resources import ComputeBudget
    from aegis_alpha.storage.input_pins import InputBundleRef
    from aegis_alpha.storage.runs import RunHandle, RunResult
    from aegis_alpha.storage.workspace import Workspace

__all__ = [
    "MAX_REASON_BYTES",
    "MAX_REASON_CHARS",
    "RUN_ID",
    "Opened",
    "abandon",
    "admit",
    "bundle_name",
    "describe",
    "installation_identity",
    "json_object",
    "json_text",
    "record",
    "require_exportable",
    "resolve_installation",
    "retaining",
    "sealed_targets",
    "translated",
]

_BUNDLE_NAME_SCHEMA = "aas-run-input-bundle-name-v1"
# A failure reason is stored and read back on every verification, so what an arbitrary
# exception carries is bounded here rather than written through at whatever length.
MAX_REASON_CHARS = 512
# The run store bounds a recorded reason and accepts only a plain identifier as a run
# name, because both become durable. Checked by a caller too so a mistake is refused
# before stage A registers anything, not after.
MAX_REASON_BYTES = 4096
RUN_ID = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}")
# A sealed document is decoded whole before it is used, so it is charged at the same
# expansion the run store already charges for decoding one of its own artifacts.
_DOCUMENT_EXPANSION = 128


@dataclass(frozen=True, slots=True)
class Opened:
    """What stage A produced: a durable run, its bundle and the installation it ran on."""

    handle: RunHandle
    bundle: InputBundleRef
    installation: tuple[str, str, str, str | None]


def json_object(value: object, field: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(field + " must be a JSON object")
    return cast("dict[str, object]", value)


def json_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(field + " must be nonempty text")
    return value


def installation_identity(workspace: Workspace) -> tuple[str, str, str, str | None]:
    """The store identities a reopened workspace must still present before a commit."""
    return (
        workspace.installation_id,
        workspace.state.execute("SELECT store_id FROM store_info").fetchone()[0],
        workspace.market.execute("SELECT store_id FROM store_info").fetchall()[0][0],
        None
        if workspace.strategies is None
        else workspace.strategies.execute("SELECT store_id FROM store_info").fetchone()[0],
    )


def bundle_name(explicit: str | None, bindings: object, request_hash: str, *, prefix: str) -> str:
    """Name the input bundle after its content unless the caller names it.

    The request hash is part of the name, not only the bindings. Storage keeps exactly
    one immutable request per bundle, so two requests that pin the same inputs while
    differing in period, account or schedule need two bundle names; sharing one would
    make the second run fail on a stored-request mismatch instead of running. A declared
    research composition binds nothing at all, so for it the hash is the whole name.
    """
    if explicit is not None:
        return explicit
    return prefix + content_sha256(
        {
            "schema": _BUNDLE_NAME_SCHEMA,
            "hash_format": HASH_FORMAT,
            "bindings": bindings,
            "request_hash": request_hash,
        }
    )


def sealed_targets(envelope_bytes: bytes) -> dict[str, dict[str, object]]:
    """Read the decision weights back from the sealed envelope.

    The preparation holds the same mapping, but keeping either a copy or a reference to
    it would carry the decoded graph through every later stage for one receipt field.
    The envelope is retained and reserved anyway, and it is the authority the result was
    projected against, so the reported weights are by construction the ones that ran.
    """
    envelope = json_object(decode_json(envelope_bytes), "envelope")
    targets = json_object(envelope.get("targets"), "envelope targets")
    return {
        json_text(day, "target date"): dict(sorted(json_object(weights, "target weights").items()))
        for day, weights in sorted(targets.items())
    }


def require_exportable(home: Path, output: Path) -> None:
    """Refuse an export that could collide with the run's own immutable artifacts.

    The run store accepts identical bytes under a sealed name as a resumed seal, so an
    export written into the managed runs directory can become the artifact itself.
    Removing the export afterwards would then make the run unreadable.
    """
    runs = Path(os.path.realpath(load_paths(home).runs))
    parent = Path(os.path.realpath(output.parent))
    if parent == runs or runs in parent.parents:
        raise ValueError("an envelope export cannot be written inside the managed runs directory")


def retaining(budget: ComputeBudget, *held: bytes) -> ComputeBudget:
    """Charge what a command keeps live against what a later stage may materialize.

    The prepared envelope and provenance stay referenced until the receipt is built, and
    the accounting response stays referenced until the result is sealed. Handing a later
    stage the unchanged allowance would let two materializations that each fit exceed the
    allocation together, which is the same reservation storage makes when one projection
    stays live beside another. `ComputeBudget` refuses a reservation that leaves no
    allowance at all, so a run too large for this host stops before that stage runs.
    """
    return replace(budget, reserved_bytes=budget.reserved_bytes + sum(len(item) for item in held))


def admit(budget: ComputeBudget, size: int) -> None:
    """Charge a materialization before it happens, never after it is in memory."""
    if size * _DOCUMENT_EXPANSION > budget.available_bytes:
        raise ComputeResourceError("accounting exceeds the remaining materialization budget")


def record(
    home: Path, opened: Opened, result: RunResult, request_hash: str, budget: ComputeBudget
) -> dict[str, object]:
    """Stage C: reopen briefly, re-authenticate installation and pins, then commit."""
    with open_workspace(home, writable=True) as workspace:
        if installation_identity(workspace) != opened.installation:
            raise ValueError("installation identity changed after the inputs were prepared")
        # Re-reading the registered request re-verifies every binding, so a generation
        # or pinned document that moved during the calculation is refused before a
        # result is recorded rather than after.
        read_backtest_request(
            workspace, opened.bundle, expected_request_hash=request_hash, budget=budget
        )
        return commit_run(workspace, opened.handle, result, budget=budget)


def abandon(home: Path, handle: RunHandle, reason: str) -> str:
    """End a run that recorded no result and report what happened to it.

    The caller must still see its own failure, so nothing here is raised over it. What
    this attempt did is returned instead of discarded: a refusal and an unexpected
    storage fault look identical to a suppressing caller, and only one of them is
    normal. A run whose market marker is already committed belongs to `recover_run`,
    and `fail_run` refuses it by design.
    """
    try:
        with open_workspace(home, writable=True) as workspace:
            fail_run(workspace, handle, reason[:MAX_REASON_CHARS])
    except RunStorageError as refused:
        return handle.run_id + " was left for 'aas db recover' (" + describe(refused) + ")"
    except BaseException as failure:  # noqa: BLE001 -- the caller's own failure must survive
        # Deliberately widest. Lock contention, a driver fault, an interrupt arriving
        # during cleanup and an outright bug in this path all land here, and any of them
        # raised onward would replace the error the caller actually needs. The command is
        # already failing, so the run's fate is reported and the original error is what
        # propagates. Nothing is hidden: every outcome is named in the returned text.
        return (
            handle.run_id + " could not be ended (" + describe(failure) + "); run 'aas db recover'"
        )
    return handle.run_id + " ended FAILED"


def describe(error: BaseException) -> str:
    """Name a failure without trusting its own formatting.

    This runs while another exception is already in flight, so an error whose `__str__`
    raises must not become the failure the caller sees instead of its own.
    """
    try:
        return type(error).__name__ + ": " + str(error)
    except BaseException:  # noqa: BLE001 -- an unprintable failure is still a failure
        return type(error).__name__ + ": <unprintable>"


def resolve_installation(home: Path | None) -> tuple[Path, tuple[Path, ...], type[Exception]]:
    """Resolve the installation and load the optional driver, as every native command does."""
    resolved = resolve_home(home)
    private_directory(resolved)
    targets = storage_lock_targets(resolved, load_paths(resolved).stores())
    return resolved, targets, cast("type[Exception]", import_module("duckdb").Error)


def translated(error: Exception) -> ValueError:
    """Replace a driver fault with its operator message, keeping what happened to the run."""
    result = ValueError("local database operation failed; run aas db verify")
    for note in getattr(error, "__notes__", ()):
        result.add_note(note)
    return result
