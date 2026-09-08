"""Data-authority registry loading for the AAS-DATA-012 FRED/ALFRED policy."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from aegis_alpha.data.fred_alfred_series import PENDING_LICENSE, POLICY_ID


class PolicyError(ValueError):
    """The data-authority registry is missing, malformed, or incomplete."""


@dataclass(frozen=True, slots=True)
class FredAlfredPolicy:
    policy_id: str
    license_classification: str
    scheduled_collection_allowed: bool


def _read_json(label: str, path: Path) -> tuple[object, str]:
    try:
        payload = path.read_bytes()
    except OSError:
        raise PolicyError(f"{label} artifact is missing: {path}") from None
    try:
        document = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise PolicyError(f"{label} artifact is not valid JSON: {path}") from None
    return (document, hashlib.sha256(payload).hexdigest())


def load_fred_alfred_policy(registry_path: Path, *, policy_id: str = POLICY_ID) -> FredAlfredPolicy:
    raw_document, _digest = _read_json("data-authority registry", registry_path)
    if not isinstance(raw_document, Mapping):
        raise PolicyError("data-authority registry must be a JSON object")
    document = cast("Mapping[str, object]", raw_document)
    policies = document.get("policies")
    if not isinstance(policies, Sequence):
        raise PolicyError("data-authority registry has no policies array")
    for policy in policies:
        if isinstance(policy, Mapping) and policy.get("policy_id") == policy_id:
            classification = policy.get("license_classification")
            scheduled = policy.get("scheduled_collection_allowed")
            if not isinstance(classification, str):
                raise PolicyError("policy has no license_classification")
            if not isinstance(scheduled, bool):
                raise PolicyError("policy has no scheduled_collection_allowed")
            return FredAlfredPolicy(
                policy_id=policy_id,
                license_classification=classification,
                scheduled_collection_allowed=scheduled,
            )
    raise PolicyError(f"data-authority registry has no policy {policy_id!r}")


def policy_blocks_live(policy: FredAlfredPolicy) -> bool:
    return (
        policy.license_classification == PENDING_LICENSE
        or policy.scheduled_collection_allowed is False
    )
