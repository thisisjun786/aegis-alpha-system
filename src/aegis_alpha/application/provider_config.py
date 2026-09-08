from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from types import MappingProxyType

from aegis_alpha.application.data_config import _pairs, _read

_PROVIDER_KEYS = {
    "fmp": frozenset(
        {"FMP_API_KEY", "AAS_FMP_OWNER_APPROVAL_AUTHORITY_PATH", "AAS_FMP_USAGE_AUTHORITY_PATH"}
    ),
    "fred_alfred": frozenset({"FRED_API_KEY", "AAS_FRED_OWNER_APPROVAL_AUTHORITY_PATH"}),
    "sec": frozenset({"SEC_USER_AGENT"}),
    "finimpulse": frozenset(
        {"FINIMPULSE_API_TOKEN", "AAS_FINIMPULSE_OWNER_APPROVAL_AUTHORITY_PATH"}
    ),
    "qveris": frozenset(),
    "norgate": frozenset(),
}
_REQUIRED_OPTIONS = {
    "fmp": frozenset(
        {
            "registry",
            "storage_notification",
            "tier",
            "recurring_authority",
            "recurring_authority_signature",
        }
    ),
    "fred_alfred": frozenset(
        {
            "registry",
            "raw_store_root",
            "dataset_root",
            "receipt_path",
            "recurring_authority",
            "recurring_authority_signature",
        }
    ),
    "sec": frozenset(
        {"registry", "identity_snapshot", "raw_store_root", "dataset_root", "receipt_path", "as_of"}
    ),
    "finimpulse": frozenset(
        {
            "universe_file",
            "identity_export",
            "gate_evidence",
            "raw_store_root",
            "dataset_root",
            "receipt_path",
            "recurring_authority",
            "recurring_authority_signature",
            "snapshot_id",
            "budget_usd",
        }
    ),
    "qveris": frozenset(
        {"jobs", "jobs_sha256", "raw_store_root", "max_credits", "timeout_seconds"}
    ),
    "norgate": frozenset({"dataset_id", "dataset_version"}),
}
_OPTIONAL_OPTIONS = {
    "fmp": frozenset({"raw_store_root", "dataset_root"}),
    "fred_alfred": frozenset({"series", "observation_start"}),
    "sec": frozenset({"instrument_ids"}),
    "finimpulse": frozenset(),
    "qveris": frozenset(),
    "norgate": frozenset(),
}
_TEXT_OPTIONS = frozenset(
    {
        "snapshot_id",
        "budget_usd",
        "dataset_id",
        "dataset_version",
        "observation_start",
        "as_of",
        "max_credits",
        "timeout_seconds",
        "jobs_sha256",
    }
)
_LIST_OPTIONS = frozenset({"series", "instrument_ids"})
_QVERIS_MAX_TIMEOUT = 600
_KEY = re.compile(r"[A-Z][A-Z0-9_]*")


@dataclass(frozen=True, slots=True)
class ProviderProfile:
    provider: str
    enabled: bool
    credential_file: Path | None
    max_calls: int
    mode: str
    options: Mapping[str, str | tuple[str, ...]] = field(repr=False)

    def __post_init__(self) -> None:
        if self.provider not in _PROVIDER_KEYS or type(self.enabled) is not bool:
            raise ValueError("unknown provider or invalid enabled flag")
        if type(self.max_calls) is not int or self.max_calls < 1:
            raise ValueError("provider max_calls must be a positive integer")
        modes = (
            {"daily", "universe"}
            if self.provider == "fmp"
            else {"probe", "incremental", "backfill"}
        )
        if self.provider == "qveris":
            modes = {"daily", "backfill"}
        if self.provider == "norgate":
            modes = {"inspect"}
        if self.mode not in modes:
            raise ValueError("unsupported provider mode")
        if self.credential_file is not None and (
            not isinstance(self.credential_file, Path) or not self.credential_file.is_absolute()
        ):
            raise ValueError("credential_file must be an absolute private file path")
        required = _REQUIRED_OPTIONS[self.provider]
        allowed = required | _OPTIONAL_OPTIONS[self.provider]
        if not isinstance(self.options, Mapping) or set(self.options) - allowed:
            raise ValueError("provider options contain unknown fields")
        _validate_options(self.options)
        if self.provider == "qveris":
            _validate_qveris_options(self.options)
        object.__setattr__(self, "options", MappingProxyType(dict(self.options)))

    def missing_options(self) -> tuple[str, ...]:
        return tuple(sorted(_REQUIRED_OPTIONS[self.provider] - self.options.keys()))


def _validate_qveris_options(options: Mapping[str, str | tuple[str, ...]]) -> None:
    from decimal import Decimal, InvalidOperation  # noqa: PLC0415 -- provider-specific validation

    if (
        "jobs_sha256" in options
        and re.fullmatch(r"[0-9a-f]{64}", str(options["jobs_sha256"])) is None
    ):
        raise ValueError("Qveris jobs_sha256 must pin the exact job manifest")
    try:
        if "max_credits" in options:
            credit_limit = Decimal(str(options["max_credits"]))
            if not credit_limit.is_finite() or credit_limit < 0:
                raise ValueError("invalid finite Qveris credit ceiling")
        if "timeout_seconds" in options:
            timeout = Decimal(str(options["timeout_seconds"]))
            if not timeout.is_finite() or not 0 < timeout <= _QVERIS_MAX_TIMEOUT:
                raise ValueError("Qveris timeout must be positive and at most 600 seconds")
    except InvalidOperation:
        raise ValueError("Qveris numeric options must be decimal values") from None


def _validate_options(options: Mapping[str, str | tuple[str, ...]]) -> None:
    for key, value in options.items():
        if key == "as_of" and (
            not isinstance(value, str) or datetime.fromisoformat(value).utcoffset() is None
        ):
            raise ValueError("provider as_of must be timezone-aware")
        if key in _LIST_OPTIONS:
            if (
                not isinstance(value, tuple)
                or not value
                or any(
                    not isinstance(item, str) or not item.strip() or item != item.strip()
                    for item in value
                )
            ):
                raise ValueError("provider selection must be a nonempty string tuple")
            if len(value) != len(set(value)):
                raise ValueError("provider selection contains duplicates")
        elif not isinstance(value, str) or not value.strip() or value != value.strip():
            raise ValueError("provider options must be nonempty strings")
        elif key not in _TEXT_OPTIONS and (
            not Path(value).is_absolute() or ".." in Path(value).parts
        ):
            raise ValueError(
                "provider artifact and output paths must be absolute without traversal"
            )


@dataclass(frozen=True, slots=True)
class CollectionConfig:
    data_config: Path
    providers: tuple[ProviderProfile, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.data_config, Path) or not self.data_config.is_absolute():
            raise ValueError("data_config must be absolute")
        if not isinstance(self.providers, tuple) or any(
            not isinstance(item, ProviderProfile) for item in self.providers
        ):
            raise TypeError("providers must be immutable validated profiles")
        names = [profile.provider for profile in self.providers]
        if len(names) != len(set(names)):
            raise ValueError("duplicate provider profile")

    def select(self, name: str) -> ProviderProfile:
        for profile in self.providers:
            if profile.provider == name:
                return profile
        raise ValueError("provider is not configured")


def load_collection_config(path: Path) -> CollectionConfig:
    value = json.loads(_read(path, 1024 * 1024, private=False), object_pairs_hook=_pairs)
    if not isinstance(value, dict) or set(value) != {"version", "data_config", "providers"}:
        raise ValueError("collection config has missing or unknown fields")
    if type(value["version"]) is not int or value["version"] != 1:
        raise ValueError("unsupported collection config version")
    if not isinstance(value["data_config"], str) or not isinstance(value["providers"], list):
        raise TypeError("invalid collection config field types")
    profiles = []
    for row in value["providers"]:
        if not isinstance(row, dict) or set(row) != {
            "provider",
            "enabled",
            "credential_file",
            "max_calls",
            "mode",
            "options",
        }:
            raise ValueError("provider profile has missing or unknown fields")
        if (
            not isinstance(row["provider"], str)
            or not isinstance(row["mode"], str)
            or not isinstance(row["options"], dict)
        ):
            raise TypeError("invalid provider profile field types")
        credential = row["credential_file"]
        if credential is not None and not isinstance(credential, str):
            raise TypeError("credential_file must be a path or null")
        options = {
            key: tuple(item) if isinstance(item, list) else item
            for key, item in row["options"].items()
        }
        profiles.append(
            ProviderProfile(
                row["provider"],
                row["enabled"],
                None if credential is None else Path(credential),
                row["max_calls"],
                row["mode"],
                options,
            )
        )
    return CollectionConfig(Path(value["data_config"]), tuple(profiles))


def load_provider_environment(profile: ProviderProfile) -> dict[str, str]:
    """Read selected assignments as literal data; never source a shell or inherit old DB URLs."""
    if profile.credential_file is None:
        return {}
    body = _read(profile.credential_file, 64 * 1024, private=True).decode("utf-8")
    result: dict[str, str] = {}
    seen = set()
    for raw in body.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        key, delimiter, value = line.partition("=")
        if not delimiter or _KEY.fullmatch(key) is None or key in seen:
            raise ValueError("credential file needs unique KEY=value assignments")
        seen.add(key)
        if value.startswith(('"', "'")):
            if len(value) == 1 or value[-1] != value[0]:
                raise ValueError("credential assignment has unmatched quotes")
            value = value[1:-1]
        if any(character in value for character in "\x00\r\n"):
            raise ValueError("credential assignment contains control characters")
        if key in _PROVIDER_KEYS[profile.provider]:
            result[key] = value
    return result
