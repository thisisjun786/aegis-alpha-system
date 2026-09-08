"""Repository-owned SEC policy gate, independent of historical installations."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from aegis_alpha.data.sec_collector import CollectorError
from aegis_alpha.data.sec_evidence import read_bytes

TRUSTED_REGISTRY_SHA256 = "f2b97a3ca024d0fec3f4351608667b98deaffccdf5989ec7175852e5e01def38"
SEC_POLICY_ID = "sec-official-verifier-v1"
INSTALLED_REGISTRY_PATH = Path("/app/config/data_authority_registry.json")


def trusted_registry_path() -> Path:
    source_root = Path(__file__).absolute().parents[3]
    if (source_root / "pyproject.toml").is_file() and (
        source_root / "src/aegis_alpha/data/sec_policy.py"
    ).is_file():
        return source_root / "config/data_authority_registry.json"
    return INSTALLED_REGISTRY_PATH


def require_live_policy(path: Path | None = None) -> Path:
    # The reviewed public policy starts disabled; edits require an explicit new pin.
    trusted = trusted_registry_path()
    if path is not None and path != trusted:
        raise CollectorError("live SEC requires the trusted SEC registry")
    try:
        payload = read_bytes(trusted)
    except (ValueError, OSError) as error:
        raise CollectorError("trusted SEC registry is unavailable") from error
    if hashlib.sha256(payload).hexdigest() != TRUSTED_REGISTRY_SHA256:
        raise CollectorError("trusted SEC registry hash mismatch")
    document = json.loads(payload)
    policies = [item for item in document["policies"] if item["policy_id"] == SEC_POLICY_ID]
    if len(policies) != 1 or policies[0]["provider"] != "sec":
        raise CollectorError("SEC data-authority policy is invalid")
    policy = policies[0]
    if (
        policy["scheduled_collection_allowed"] is not True
        or "PENDING" in policy["license_classification"]
    ):
        raise CollectorError(
            "SEC policy does not permit scheduled collection; "
            "reviewed current authority is required"
        )
    return trusted
