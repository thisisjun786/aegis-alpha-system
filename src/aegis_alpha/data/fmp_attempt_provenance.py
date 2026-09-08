"""Load and structurally validate durable response provenance records."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Iterator
from pathlib import Path
from typing import cast

from aegis_alpha.data.fmp_symbol_observation import DURABLE_RESPONSE_KEYS
from aegis_alpha.data.fmp_windows import CollectorContractError
from aegis_alpha.data.serialization import canonical_json_bytes


def provenance_reference(
    raw_root: Path,
    run_identity: str,
    attempt_seq: int,
    request_fingerprint: str,
    provenance: bytes,
) -> tuple[Path, bytes]:
    digest = hashlib.sha256(provenance).hexdigest()
    payload = canonical_json_bytes(
        {
            "attempt_seq": attempt_seq,
            "provenance_sha256": digest,
            "request_fingerprint": request_fingerprint,
            "run_identity": run_identity,
            "schema_version": 1,
        }
    )
    path = (
        raw_root
        / "fmp"
        / "runs"
        / run_identity
        / "provenance"
        / f"{attempt_seq:08d}.provenance.json"
    )
    return path, payload


def load_attempt_provenance_record(
    raw_root: Path,
    run_identity: str,
    sequence: int,
) -> tuple[bytes, dict[str, object]]:
    reference_path = (
        raw_root / "fmp" / "runs" / run_identity / "provenance" / f"{sequence:08d}.provenance.json"
    )
    try:
        reference_payload = reference_path.read_bytes()
        reference = cast("dict[str, object]", json.loads(reference_payload))
        digest = str(reference["provenance_sha256"])
        provenance_path = raw_root / "fmp" / "provenance" / "sha256" / digest[:2] / f"{digest}.json"
        payload = provenance_path.read_bytes()
        record = cast("dict[str, object]", json.loads(payload))
        if (
            reference_payload != canonical_json_bytes(reference)
            or reference.get("schema_version") != 1
            or reference.get("run_identity") != run_identity
            or reference.get("attempt_seq") != sequence
            or reference.get("request_fingerprint") != record.get("request_fingerprint")
            or hashlib.sha256(payload).hexdigest() != digest
            or payload != canonical_json_bytes(record)
            or set(record) != DURABLE_RESPONSE_KEYS
            or record.get("run_identity") != run_identity
            or record.get("attempt_seq") != sequence
        ):
            raise ValueError  # noqa: TRY301 - normalized at this trust boundary
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError):
        raise CollectorContractError("durable run provenance is invalid") from None
    return payload, record


def load_attempt_provenance(
    raw_root: Path, run_identity: str
) -> dict[int, tuple[bytes, dict[str, object]]]:
    """Return this run's unique canonical provenance indexed by attempt sequence."""

    return {
        sequence: (payload, record)
        for sequence, payload, record in iter_attempt_provenance(raw_root, run_identity)
    }


def iter_attempt_provenance(
    raw_root: Path, run_identity: str
) -> Iterator[tuple[int, bytes, dict[str, object]]]:
    """Validate this run's provenance index incrementally."""

    root = raw_root / "fmp" / "runs" / run_identity / "provenance"
    entries = os.scandir(root) if root.is_dir() else ()
    for entry in entries:
        path = Path(entry.path)
        if not path.name.endswith(".provenance.json"):
            continue
        try:
            sequence = int(path.name.removesuffix(".provenance.json"))
            if path != root / f"{sequence:08d}.provenance.json":
                raise ValueError  # noqa: TRY301 - normalized at this trust boundary
            payload, record = load_attempt_provenance_record(raw_root, run_identity, sequence)
        except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError):
            raise CollectorContractError("durable run provenance is invalid") from None
        yield sequence, payload, record


def count_attempt_provenance_references(raw_root: Path, run_identity: str) -> int:
    root = raw_root / "fmp" / "runs" / run_identity / "provenance"
    if not root.is_dir():
        return 0
    with os.scandir(root) as entries:
        return sum(entry.name.endswith(".provenance.json") for entry in entries)
