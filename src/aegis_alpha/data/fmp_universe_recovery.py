"""Strict universe post-success recovery-token consistency validation."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from collections.abc import Mapping, Sequence
from datetime import UTC
from pathlib import Path
from typing import Final, cast

from aegis_alpha.collection.records import CollectionRunPlan, CollectionRunState, RunEventType
from aegis_alpha.data.fmp_collector import FmpCollector, publish_bundle
from aegis_alpha.data.fmp_collector_state import marker_path
from aegis_alpha.data.serialization import canonical_json_bytes

REQUIRED_NAME: Final = "recovery-required.json"
COMPLETED_NAME: Final = "recovery-completed.json"
_SCHEMA_VERSION: Final = 1
_MAX_TOKEN_BYTES: Final = 64 * 1024
_MAX_MANIFEST_BYTES: Final = 32 * 1024 * 1024
_ENVELOPE_DOMAIN: Final = "aegis-alpha/fmp-universe-recovery-envelope/v1"
_REQUIRED_DOMAIN: Final = "aegis-alpha/fmp-universe-recovery-required/v1"
_COMPLETED_DOMAIN: Final = "aegis-alpha/fmp-universe-recovery-completed/v1"
_COMMON_FIELDS = {
    "artifact_path",
    "artifact_sha256",
    "created_at_utc",
    "domain",
    "expected_lifecycle",
    "manifest_phase",
    "plan_id",
    "plan_sha256",
    "publication_marker_sha256",
    "run_id",
    "schema_version",
}


def _strict_object(pairs: Sequence[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"recovery token contains duplicate key {key!r}")
        result[key] = value
    return result


def _entry_present(path: Path) -> bool:
    try:
        path.lstat()
    except FileNotFoundError:
        return False
    except OSError:
        raise ValueError("recovery token directory entry cannot be inspected") from None
    return True


def _bounded_read(path: Path, *, limit: int = _MAX_TOKEN_BYTES) -> bytes:
    try:
        entry = path.lstat()
    except OSError:
        raise ValueError("recovery token cannot be inspected safely") from None
    if not stat.S_ISREG(entry.st_mode):
        raise ValueError("recovery token is not a regular file")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        raise ValueError("recovery token cannot be opened safely") from None
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or (entry.st_dev, entry.st_ino) != (before.st_dev, before.st_ino)
            or before.st_size > limit
        ):
            raise ValueError("recovery token is not a bounded regular file")
        payload = os.read(descriptor, limit + 1)
        after = os.fstat(descriptor)
        if (
            len(payload) > limit
            or len(payload) != before.st_size
            or (before.st_ino, before.st_size, before.st_mtime_ns)
            != (after.st_ino, after.st_size, after.st_mtime_ns)
        ):
            raise ValueError("recovery token changed during its bounded read")
        return payload
    finally:
        os.close(descriptor)


def _decode(path: Path, expected_fields: set[str]) -> tuple[bytes, dict[str, object]]:
    raw = _bounded_read(path)
    try:
        envelope = json.loads(raw, object_pairs_hook=_strict_object)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
        raise ValueError("recovery token is malformed or contains duplicate keys") from None
    if not isinstance(envelope, dict) or set(envelope) != {
        "domain",
        "payload",
        "payload_sha256",
    }:
        raise ValueError("recovery token envelope has unknown or missing fields")
    payload = envelope["payload"]
    if (
        envelope["domain"] != _ENVELOPE_DOMAIN
        or not isinstance(payload, dict)
        or set(payload) != expected_fields
        or envelope["payload_sha256"] != hashlib.sha256(canonical_json_bytes(payload)).hexdigest()
        or raw != canonical_json_bytes(envelope)
    ):
        raise ValueError("recovery token envelope or content digest is invalid")
    return raw, cast("dict[str, object]", payload)


def _marker_artifact(
    collector: FmpCollector, run_id: str, plan_id: str, destination: Path
) -> tuple[bytes, Mapping[str, object]]:
    marker_bytes = _bounded_read(marker_path(collector, run_id))
    marker = json.loads(marker_bytes, object_pairs_hook=_strict_object)
    if not isinstance(marker, dict) or marker_bytes != canonical_json_bytes(marker):
        raise ValueError("universe publication marker is not strict canonical JSON")
    if marker.get("run_id") != run_id or marker.get("plan_id") != plan_id:
        raise ValueError("universe publication marker identifies another run or plan")
    artifacts = marker.get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) != 1:
        raise ValueError("universe publication marker has invalid artifact cardinality")
    artifact = artifacts[0]
    if not isinstance(artifact, dict) or set(artifact) != {"path", "sha256"}:
        raise ValueError("universe publication marker artifact is invalid")
    if Path(str(artifact["path"])).resolve() != destination.resolve():
        raise ValueError("universe publication marker identifies another artifact")
    artifact_bytes = _bounded_read(destination, limit=_MAX_MANIFEST_BYTES)
    if hashlib.sha256(artifact_bytes).hexdigest() != artifact["sha256"]:
        raise ValueError("universe manifest digest does not match its marker")
    return marker_bytes, cast("Mapping[str, object]", artifact)


def _base_payload(
    collector: FmpCollector,
    plan: CollectionRunPlan,
    run_id: str,
    destination: Path,
    *,
    domain: str,
) -> dict[str, object]:
    marker_bytes, artifact = _marker_artifact(collector, run_id, plan.plan_id, destination)
    return {
        "artifact_path": str(destination.resolve()),
        "artifact_sha256": artifact["sha256"],
        "created_at_utc": plan.created_at_utc.astimezone(UTC).isoformat(),
        "domain": domain,
        "expected_lifecycle": RunEventType.RUN_SUCCEEDED.value,
        "manifest_phase": "post_marker_success",
        "plan_id": plan.plan_id,
        "plan_sha256": plan.plan_sha256,
        "publication_marker_sha256": hashlib.sha256(marker_bytes).hexdigest(),
        "run_id": run_id,
        "schema_version": _SCHEMA_VERSION,
    }


def _encode(payload: Mapping[str, object]) -> bytes:
    return canonical_json_bytes(
        {
            "domain": _ENVELOPE_DOMAIN,
            "payload": payload,
            "payload_sha256": hashlib.sha256(canonical_json_bytes(payload)).hexdigest(),
        }
    )


def publish_required(
    collector: FmpCollector, plan: CollectionRunPlan, run_id: str, destination: Path
) -> None:
    payload = _base_payload(collector, plan, run_id, destination, domain=_REQUIRED_DOMAIN)
    collector.require_bound_approval()
    publish_bundle([(marker_path(collector, run_id).parent / REQUIRED_NAME, _encode(payload))])


def validate_pending(
    collector: FmpCollector,
    plan: CollectionRunPlan,
    run_id: str,
    state: CollectionRunState,
    destination: Path,
) -> bool:
    root = marker_path(collector, run_id).parent
    required_path = root / REQUIRED_NAME
    completed_path = root / COMPLETED_NAME
    required_present = _entry_present(required_path)
    completed_present = _entry_present(completed_path)
    if not required_present and not completed_present:
        return False
    if not required_present:
        raise ValueError("universe recovery completion exists without its required token")
    required_raw, required = _decode(required_path, _COMMON_FIELDS)
    expected = _base_payload(collector, plan, run_id, destination, domain=_REQUIRED_DOMAIN)
    if required != expected or state.state is not RunEventType.RUN_SUCCEEDED or not state.terminal:
        raise ValueError("universe recovery token conflicts with plan, run, or lifecycle state")
    if not completed_present:
        return True
    completed_fields = _COMMON_FIELDS | {"required_token_sha256"}
    _completed_raw, completed = _decode(completed_path, completed_fields)
    completed_expected = _base_payload(
        collector, plan, run_id, destination, domain=_COMPLETED_DOMAIN
    )
    completed_expected["required_token_sha256"] = hashlib.sha256(required_raw).hexdigest()
    if completed != completed_expected:
        raise ValueError("completed universe recovery token conflicts with required token")
    return False


def publish_completed(
    collector: FmpCollector, plan: CollectionRunPlan, run_id: str, destination: Path
) -> None:
    root = marker_path(collector, run_id).parent
    required_raw, _required = _decode(root / REQUIRED_NAME, _COMMON_FIELDS)
    payload = _base_payload(collector, plan, run_id, destination, domain=_COMPLETED_DOMAIN)
    payload["required_token_sha256"] = hashlib.sha256(required_raw).hexdigest()
    collector.require_bound_approval()
    publish_bundle([(root / COMPLETED_NAME, _encode(payload))])
