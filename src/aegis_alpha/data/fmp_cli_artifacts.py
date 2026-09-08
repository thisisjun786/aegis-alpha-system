"""Local operator-intent and storage-notification artifact parsing."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Final, cast

from aegis_alpha.data.serialization import canonical_json_bytes

#: Exit code for every fail-closed precondition: zero provider calls happened.
PRECONDITION_EXIT: Final = 2
PENDING_CONTRACT_REVIEW: Final = "PENDING_CONTRACT_REVIEW"
FMP_POLICY_ID: Final = "fmp-operational-candidate-v1"
AUTHORIZED_MAX_CALLS: Final = 25

_APPROVAL_FIELDS: Final = (
    "approver",
    "contract_revision",
    "max_calls",
    "run_identity",
    "expires_at_utc",
)
_NOTIFICATION_FIELDS: Final = ("notified_at_utc", "channel", "storage_aliases")


class PreconditionError(RuntimeError):
    """A hard gate failed, so the run performs zero provider calls."""


@dataclass(frozen=True, slots=True)
class ApprovalArtifact:
    """P1 contract-review *intent evidence*, not an authorization boundary.

    Codex, the collector, and the owner currently share one OS principal, so a
    local file cannot prove that a distinct owner approved this run. This
    artifact is operator-intent audit evidence only and must never be described
    as blocking an agent holding that same OS authority.
    """

    approver: str
    contract_revision: str
    max_calls: int
    run_identity: str
    expires_at_utc: datetime
    sha256: str

    def require_unexpired(self, now: datetime) -> None:
        if self.expires_at_utc <= now:
            raise PreconditionError(
                "contract-review artifact expired; the run is cancelled with zero further calls"
            )


@dataclass(frozen=True, slots=True)
class FmpPolicy:
    """Exact selected registry policy used by the manual FMP executable."""

    policy_id: str
    license_classification: str
    scheduled_collection_allowed: bool
    sha256: str


@dataclass(frozen=True, slots=True)
class NotificationArtifact:
    """P2 storage-location notification: a claim, never proof of delivery.

    It records an operator's claimed notification only. It does not prove
    delivery to FMP, provider receipt, or owner approval.
    """

    notified_at_utc: datetime
    channel: str
    storage_aliases: tuple[str, ...]
    sha256: str


def _read_json(label: str, path: Path) -> tuple[object, str]:
    try:
        payload = path.read_bytes()
    except OSError:
        raise PreconditionError(f"{label} artifact is missing: {path}") from None

    def strict_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
        document: dict[str, object] = {}
        for key, value in pairs:
            if key in document:
                raise PreconditionError(f"{label} artifact contains duplicate object keys")
            document[key] = value
        return document

    try:
        document = json.loads(payload, object_pairs_hook=strict_object)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise PreconditionError(f"{label} artifact is not valid JSON: {path}") from None
    return (document, hashlib.sha256(payload).hexdigest())


def _parse_utc(label: str, field_name: str, value: object) -> datetime:
    if not isinstance(value, str):
        raise PreconditionError(f"{label} artifact field {field_name!r} must be an RFC 3339 string")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        raise PreconditionError(
            f"{label} artifact field {field_name!r} must be an RFC 3339 timestamp"
        ) from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise PreconditionError(f"{label} artifact field {field_name!r} must be timezone-aware UTC")
    return parsed.astimezone(UTC)


def load_approval_artifact(path: Path) -> ApprovalArtifact:
    raw_document, digest = _read_json("contract-review", path)
    if not isinstance(raw_document, Mapping):
        raise PreconditionError("contract-review artifact must be a JSON object")
    document = cast("Mapping[str, object]", raw_document)
    missing = [name for name in _APPROVAL_FIELDS if name not in document]
    if missing:
        raise PreconditionError(
            f"contract-review artifact is missing required fields: {', '.join(sorted(missing))}"
        )
    unknown = set(document).difference(_APPROVAL_FIELDS)
    if unknown:
        raise PreconditionError("contract-review artifact contains unknown fields")
    for name in ("approver", "contract_revision", "run_identity"):
        value = document[name]
        if not isinstance(value, str) or not value.strip():
            raise PreconditionError(
                f"contract-review artifact field {name!r} must be a nonempty string"
            )
    max_calls = document["max_calls"]
    if not isinstance(max_calls, int) or isinstance(max_calls, bool) or max_calls < 1:
        raise PreconditionError(
            "contract-review artifact field 'max_calls' must be a positive integer"
        )
    if max_calls > AUTHORIZED_MAX_CALLS:
        raise PreconditionError(
            "contract-review artifact field 'max_calls' exceeds the authorized ceiling of 25"
        )
    return ApprovalArtifact(
        approver=str(document["approver"]),
        contract_revision=str(document["contract_revision"]),
        max_calls=max_calls,
        run_identity=str(document["run_identity"]),
        expires_at_utc=_parse_utc("contract-review", "expires_at_utc", document["expires_at_utc"]),
        sha256=digest,
    )


def load_notification_artifact(path: Path) -> NotificationArtifact:
    raw_document, digest = _read_json("storage-notification", path)
    if not isinstance(raw_document, Mapping):
        raise PreconditionError("storage-notification artifact must be a JSON object")
    document = cast("Mapping[str, object]", raw_document)
    missing = [name for name in _NOTIFICATION_FIELDS if name not in document]
    if missing:
        raise PreconditionError(
            "storage-notification artifact is missing required fields: "
            f"{', '.join(sorted(missing))}"
        )
    unknown = set(document).difference(_NOTIFICATION_FIELDS)
    if unknown:
        raise PreconditionError("storage-notification artifact contains unknown fields")
    channel = document["channel"]
    if not isinstance(channel, str) or not channel.strip():
        raise PreconditionError(
            "storage-notification artifact field 'channel' must be a nonempty string"
        )
    aliases = document["storage_aliases"]
    if (
        not isinstance(aliases, Sequence)
        or isinstance(aliases, (str, bytes))
        or not aliases
        or any(not isinstance(alias, str) or not alias.strip() for alias in aliases)
    ):
        raise PreconditionError(
            "storage-notification artifact field 'storage_aliases' must be a nonempty string list"
        )
    return NotificationArtifact(
        notified_at_utc=_parse_utc(
            "storage-notification", "notified_at_utc", document["notified_at_utc"]
        ),
        channel=channel,
        storage_aliases=tuple(str(alias) for alias in aliases),
        sha256=digest,
    )


def load_fmp_policy(registry_path: Path, *, policy_id: str = FMP_POLICY_ID) -> FmpPolicy:
    """Parse the exact selected policy fields that constrain manual execution."""

    raw_document, _digest = _read_json("data-authority registry", registry_path)
    if not isinstance(raw_document, Mapping):
        raise PreconditionError("data-authority registry must be a JSON object")
    document = cast("Mapping[str, object]", raw_document)
    policies = document.get("policies")
    if not isinstance(policies, Sequence):
        raise PreconditionError("data-authority registry has no policies array")
    matches = [
        cast("Mapping[str, object]", policy)
        for policy in policies
        if isinstance(policy, Mapping) and policy.get("policy_id") == policy_id
    ]
    if len(matches) != 1:
        raise PreconditionError(f"data-authority registry must contain one policy {policy_id!r}")
    policy = matches[0]
    classification = policy.get("license_classification")
    scheduled = policy.get("scheduled_collection_allowed")
    if not isinstance(classification, str) or type(scheduled) is not bool:
        raise PreconditionError("FMP policy classification or scheduling field is malformed")
    return FmpPolicy(
        policy_id=policy_id,
        license_classification=classification,
        scheduled_collection_allowed=scheduled,
        sha256=hashlib.sha256(canonical_json_bytes(policy)).hexdigest(),
    )


def license_classification(registry_path: Path, *, policy_id: str = FMP_POLICY_ID) -> str:
    return load_fmp_policy(registry_path, policy_id=policy_id).license_classification
