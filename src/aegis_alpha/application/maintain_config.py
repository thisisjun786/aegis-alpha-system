"""The ``providers`` and ``jobs`` of ``runtime.json`` that ``aas maintain`` reads.

``jobs.enabled`` is the installation's grant for scheduled provider calls: ``aas maintain
run`` asks a provider only while it is ``true`` and the provider's own section says
``"enabled": true``. Every other stage (recovery, calendars, imports, identity, promotion,
the report) runs either way. A provider section names its credential file (relative paths
resolve inside the installation's ``secrets`` directory) and its per-run caps; the file is
read only when the run asks that provider. Unknown fields, wrong types and missing
required fields are refused before any stage runs, so a typo never silently disables a
cap.

```json
{
  "jobs": {"enabled": true},
  "providers": {
    "qveris": {"enabled": true, "key_file": "qveris-api-key", "raw_root": "/abs/raw/qveris",
               "identity": "/abs/qveris-bulk-identity.json",
               "exchanges": ["US", "KO", "KQ"], "datasets": ["prices", "splits", "dividends"],
               "since": {"US": "2026-09-01", "KO": "2026-09-05", "KQ": "2026-09-05"},
               "extra_sessions": {"US": ["2026-07-28"]},
               "symbol_lists": ["KO", "KQ"], "symbol_list_days": 7,
               "forex": ["USDKRW"], "forex_lookback_days": 10,
               "max_calls": 30, "max_credits": "100"},
    "dart": {"enabled": true, "key_file": "opendart-api-key", "max_calls": 2000,
             "daily_quota": 19000},
    "kind": {"enabled": true},
    "sec": {"enabled": true, "user_agent_file": "sec-user-agent", "max_calls": 2000,
            "issuers": "registered", "since": "2026-08-31"},
    "fred": {"enabled": true, "key_file": "fred-api-key", "max_calls": 500}
  }
}
```
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Final, cast

from aegis_alpha.application.maintain_qveris import QverisCaps, QverisPolicy
from aegis_alpha.storage.paths import read_json

if TYPE_CHECKING:
    from collections.abc import Mapping

    from aegis_alpha.storage.paths import StoragePaths

PROVIDERS: Final = ("kind", "dart", "sec", "fred", "qveris")
_QVERIS: Final = {
    "enabled", "key_file", "raw_root", "identity", "exchanges", "datasets", "since",
    "extra_sessions",
    "symbol_lists", "symbol_list_days", "forex", "forex_lookback_days", "max_calls",
    "max_credits", "max_http_requests", "time_limit_seconds", "request_interval",
    "timeout_seconds",
}  # fmt: skip
_FIELDS: Final = {
    "kind": {"enabled"},
    "dart": {"enabled", "key_file", "max_calls", "daily_quota"},
    "sec": {"enabled", "user_agent_file", "max_calls", "issuers", "since"},
    "fred": {"enabled", "key_file", "max_calls"},
    "qveris": _QVERIS,
}
_REQUIRED: Final = {
    "kind": set(),
    "dart": {"key_file"},
    "sec": {"user_agent_file"},
    "fred": {"key_file"},
    "qveris": {"key_file", "raw_root", "since", "max_calls", "max_credits"},
}


@dataclass(frozen=True, slots=True)
class DartSettings:
    key_file: Path
    max_calls: int = 2_000
    daily_quota: int = 19_000


@dataclass(frozen=True, slots=True)
class SecSettings:
    user_agent_file: Path
    max_calls: int = 2_000
    issuers: str = "registered"
    since: date | None = None


@dataclass(frozen=True, slots=True)
class FredSettings:
    key_file: Path
    max_calls: int = 500


@dataclass(frozen=True, slots=True)
class QverisSettings:
    key_file: Path
    raw_root: Path
    policy: QverisPolicy
    caps: QverisCaps
    identity: Path | None = None
    timeout_seconds: float = 45.0


@dataclass(frozen=True, slots=True)
class MaintainConfig:
    jobs_enabled: bool = False
    kind: bool = False
    dart: DartSettings | None = None
    sec: SecSettings | None = None
    fred: FredSettings | None = None
    qveris: QverisSettings | None = None

    def enabled(self) -> list[str]:
        return [name for name in PROVIDERS if getattr(self, name)]


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be a JSON object")
    return cast("dict[str, object]", value)


def _integer(section: Mapping[str, object], key: str, default: int, name: str) -> int:
    value = section.get(key, default)
    if type(value) is not int or value < 0:
        raise ValueError(f"{name}.{key} must be a nonnegative integer")
    return value


def _seconds(section: Mapping[str, object], key: str, default: float | None, name: str) -> float:
    value = section.get(key, default)
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f"{name}.{key} must be a finite number of seconds")
    if value < 0:
        raise ValueError(f"{name}.{key} must not be negative")
    return float(value)


def _text(section: Mapping[str, object], key: str, name: str) -> str:
    value = section.get(key)
    if not isinstance(value, str) or not value or "\x00" in value:
        raise ValueError(f"{name}.{key} must be nonempty text")
    return value


def _day(value: object, name: str) -> date:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a YYYY-MM-DD date")
    parsed = date.fromisoformat(value)
    if parsed.isoformat() != value:
        raise ValueError(f"{name} must be a YYYY-MM-DD date")
    return parsed


def _texts(
    section: Mapping[str, object], key: str, default: tuple[str, ...], name: str
) -> tuple[str, ...]:
    value = section.get(key, list(default))
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(f"{name}.{key} must be a list of text")
    return tuple(cast("list[str]", value))


def _secret(paths: StoragePaths, value: str) -> Path:
    path = Path(value).expanduser()
    return path if path.is_absolute() else paths.secrets / path


def _absolute(value: str, name: str) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        raise ValueError(f"{name} must be an absolute path")
    return path


def _qveris(paths: StoragePaths, section: Mapping[str, object]) -> QverisSettings:
    name = "providers.qveris"
    since = _object(section["since"], f"{name}.since")
    exchanges = _texts(section, "exchanges", ("US", "KO", "KQ"), name)
    extra = _object(section.get("extra_sessions", {}), f"{name}.extra_sessions")
    sessions: dict[str, tuple[date, ...]] = {}
    for key, value in extra.items():
        if not isinstance(value, list):
            raise TypeError(f"{name}.extra_sessions.{key} must be a list of dates")
        sessions[key] = tuple(_day(item, f"{name}.extra_sessions.{key}") for item in value)
    policy = QverisPolicy(
        exchanges=exchanges,
        datasets=_texts(section, "datasets", ("prices", "splits", "dividends"), name),
        since={key: _day(value, f"{name}.since.{key}") for key, value in since.items()},
        extra_sessions=sessions,
        symbol_lists=_texts(section, "symbol_lists", (), name),
        symbol_list_days=_integer(section, "symbol_list_days", 7, name),
        forex=_texts(section, "forex", (), name),
        forex_lookback_days=_integer(section, "forex_lookback_days", 10, name),
    )
    max_credits = section["max_credits"]
    if not isinstance(max_credits, str):
        raise TypeError(f"{name}.max_credits must be decimal text")
    http = section.get("max_http_requests")
    if http is not None and (type(http) is not int or http < 1):
        raise ValueError(f"{name}.max_http_requests must be a positive integer")
    interval = section.get("request_interval")
    caps = QverisCaps(
        max_calls=_integer(section, "max_calls", 0, name),
        max_credits=max_credits,
        max_http_requests=http,
        time_limit_seconds=_seconds(section, "time_limit_seconds", 3600.0, name),
        request_interval=None if interval is None else _seconds(section, "request_interval",
                                                                None, name),
    )  # fmt: skip
    identity = section.get("identity")
    return QverisSettings(
        key_file=_secret(paths, _text(section, "key_file", name)),
        raw_root=_absolute(_text(section, "raw_root", name), f"{name}.raw_root"),
        policy=policy,
        caps=caps,
        identity=None
        if identity is None
        else _absolute(_text(section, "identity", name), f"{name}.identity"),
        timeout_seconds=_seconds(section, "timeout_seconds", 45.0, name),
    )


def _section(
    paths: StoragePaths, provider: str, section: Mapping[str, object], name: str
) -> object:
    if provider == "qveris":
        return _qveris(paths, section)
    if provider == "dart":
        return DartSettings(
            _secret(paths, _text(section, "key_file", name)),
            _integer(section, "max_calls", 2_000, name),
            _integer(section, "daily_quota", 19_000, name),
        )
    if provider == "sec":
        issuers = section.get("issuers", "registered")
        if issuers not in {"registered", "all"}:
            raise ValueError(f"{name}.issuers is registered or all")
        since = section.get("since")
        return SecSettings(
            _secret(paths, _text(section, "user_agent_file", name)),
            _integer(section, "max_calls", 2_000, name),
            cast("str", issuers),
            None if since is None else _day(since, f"{name}.since"),
        )
    if provider == "fred":
        return FredSettings(
            _secret(paths, _text(section, "key_file", name)),
            _integer(section, "max_calls", 500, name),
        )
    return True


def parse_config(paths: StoragePaths, runtime: Mapping[str, object]) -> MaintainConfig:
    """The maintenance settings of one ``runtime.json`` document; reads no credential."""
    jobs = _object(runtime.get("jobs", {"enabled": False}), "jobs")
    if set(jobs) - {"enabled"} or type(jobs.get("enabled", False)) is not bool:
        raise ValueError("jobs takes only a boolean enabled")
    providers = _object(runtime.get("providers", {}), "providers")
    unknown = set(providers) - set(PROVIDERS)
    if unknown:
        raise ValueError(f"providers has unknown sections {sorted(unknown)}")
    settings: dict[str, object] = {"jobs_enabled": bool(jobs.get("enabled", False))}
    for provider, value in providers.items():
        name = f"providers.{provider}"
        section = _object(value, name)
        extra = set(section) - _FIELDS[provider]
        missing = _REQUIRED[provider] - set(section)
        if extra or missing or type(section.get("enabled")) is not bool:
            raise ValueError(
                f"{name} needs a boolean enabled and {sorted(_REQUIRED[provider])}; "
                f"unknown {sorted(extra)}, missing {sorted(missing)}"
            )
        parsed = _section(paths, provider, section, name)
        if section["enabled"]:
            settings[provider] = parsed
    return MaintainConfig(**settings)  # ty: ignore[invalid-argument-type]


def load_config(paths: StoragePaths) -> MaintainConfig:
    return parse_config(paths, read_json(paths.root / "runtime.json"))
