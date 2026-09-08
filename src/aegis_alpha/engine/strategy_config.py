"""Strict nested strategy-config validation for the public engine contract."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from types import MappingProxyType
from typing import Final

from aegis_alpha.engine.errors import ContractDefinitionError

_CANARY_MODES: Final = frozenset({"AND", "OR"})
_STRATEGY_TYPES: Final = frozenset(
    {
        "equal_weight",
        "relative",
        "absolute",
        "absolute_defensive",
        "relative_absolute",
        "relative_absolute_defensive",
        "relative_absolute_cash",
    }
)
_SCORING_METHODS: Final = frozenset({"moving_average", "return_rate", "momentum_score"})
_OFFENSIVE_CONFIG_KEYS: Final = frozenset(
    {"strategy_type", "assets", "top_n", "scoring", "reference_asset"}
)
_DEFENSIVE_CONFIG_KEYS: Final = frozenset({"assets"})
_CANARY_CONFIG_KEYS: Final = frozenset({"canary_mode", "assets", "enabled", "scoring"})
_SCORING_KEYS: Final = frozenset({"method", "horizon", "score_name"})
_THRESHOLD_MODE_KEYS: Final = frozenset({"kind", "mode"})
_NEGATIVE_ABS_KEYS: Final = frozenset({"kind", "enabled", "threshold", "scoring"})
_VARIANT_SPEC_KEYS: Final = frozenset({"axis"})


def freeze_value(value: object) -> object:
    if isinstance(value, Mapping):
        frozen: dict[str, object] = {}
        for key, child in value.items():
            if not isinstance(key, str):
                raise ContractDefinitionError("strategy config keys must be strings")
            frozen[key] = freeze_value(child)
        return MappingProxyType(frozen)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(freeze_value(item) for item in value)
    if not isinstance(value, (str, int, float, bool, type(None))):
        raise ContractDefinitionError(
            "strategy config values must be JSON-compatible scalars, arrays, or objects; "
            f"got {type(value).__name__}"
        )
    if isinstance(value, float) and not math.isfinite(value):
        raise ContractDefinitionError("strategy config numbers must be finite")
    return value


def freeze_config(value: Mapping[str, object]) -> Mapping[str, object]:
    return MappingProxyType({key: freeze_value(child) for key, child in value.items()})


def require_nonempty(value: str, field_name: str) -> str:
    if not value.strip():
        raise ContractDefinitionError(f"{field_name} must be a nonempty string")
    return value


def _mapping(value: object, field: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise ContractDefinitionError(f"{field} must be an object")
    result: dict[str, object] = {}
    for key, child in value.items():
        if not isinstance(key, str):
            raise ContractDefinitionError(f"{field} keys must be strings")
        result[key] = child
    return result


def require_known_mapping_keys(
    mapping: Mapping[str, object],
    *,
    field_name: str,
    allowed: frozenset[str],
) -> None:
    unknown = sorted(set(mapping) - allowed)
    if unknown:
        raise ContractDefinitionError(f"{field_name} unknown keys: {unknown}")


def require_asset_ids(
    raw: object,
    *,
    field_name: str,
    allow_empty: bool = False,
) -> tuple[str, ...]:
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes, bytearray)):
        raise ContractDefinitionError(f"{field_name} must be an array of strings")
    names: list[str] = []
    seen: set[str] = set()
    for item in raw:
        if not isinstance(item, str):
            raise ContractDefinitionError(f"{field_name} must contain only strings")
        if not item.strip():
            raise ContractDefinitionError(f"{field_name} must contain nonempty strings")
        if item in seen:
            raise ContractDefinitionError(f"{field_name} values must be unique")
        seen.add(item)
        names.append(item)
    if not names and not allow_empty:
        raise ContractDefinitionError(f"{field_name} must contain at least one asset")
    return tuple(names)


def require_boolean_tuple(raw: object, *, field_name: str) -> tuple[bool, ...]:
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes, bytearray)):
        raise ContractDefinitionError(f"{field_name} must be an array of booleans")
    flags: list[bool] = []
    for item in raw:
        if item is not True and item is not False:
            raise ContractDefinitionError(f"{field_name} must contain only booleans")
        flags.append(item)
    return tuple(flags)


def validate_strategy_config(  # noqa: C901, PLR0912 -- discriminated strategy boundary
    *,
    offensive_config: Mapping[str, object],
    defensive_config: Mapping[str, object],
    canary_config: Mapping[str, object],
    signals_config: Mapping[str, object],
    variant_spec: Mapping[str, object] | None,
) -> None:
    require_known_mapping_keys(
        offensive_config,
        field_name="offensive_config",
        allowed=_OFFENSIVE_CONFIG_KEYS,
    )
    require_known_mapping_keys(
        defensive_config,
        field_name="defensive_config",
        allowed=_DEFENSIVE_CONFIG_KEYS,
    )
    require_known_mapping_keys(
        canary_config,
        field_name="canary_config",
        allowed=_CANARY_CONFIG_KEYS,
    )
    if variant_spec is not None:
        require_known_mapping_keys(
            variant_spec,
            field_name="variant_spec",
            allowed=_VARIANT_SPEC_KEYS,
        )
    if "canary_mode" not in canary_config:
        raise ContractDefinitionError("canary_config requires explicit canary_mode")
    canary_mode = canary_config["canary_mode"]
    if canary_mode not in _CANARY_MODES:
        raise ContractDefinitionError("canary_mode must be the string AND or OR")
    if "strategy_type" not in offensive_config:
        raise ContractDefinitionError("offensive_config requires explicit strategy_type")
    strategy_type = offensive_config["strategy_type"]
    if not isinstance(strategy_type, str):
        raise ContractDefinitionError("strategy_type must be a string")
    if strategy_type not in _STRATEGY_TYPES:
        raise ContractDefinitionError(
            "strategy_type must be one of " + ", ".join(sorted(_STRATEGY_TYPES))
        )
    if "assets" not in offensive_config:
        raise ContractDefinitionError("offensive_config requires explicit assets")
    require_asset_ids(offensive_config["assets"], field_name="offensive_config.assets")
    if "top_n" not in offensive_config:
        raise ContractDefinitionError("offensive_config requires explicit top_n")
    top_n = offensive_config["top_n"]
    if not isinstance(top_n, int) or isinstance(top_n, bool) or top_n <= 0:
        raise ContractDefinitionError("top_n must be a positive integer")
    if "scoring" not in offensive_config:
        raise ContractDefinitionError("offensive_config requires explicit scoring")
    _require_scoring(offensive_config["scoring"], field_name="offensive_config.scoring")
    if "reference_asset" not in offensive_config:
        raise ContractDefinitionError("offensive_config requires explicit reference_asset")
    reference_asset = offensive_config["reference_asset"]
    if not isinstance(reference_asset, str):
        raise ContractDefinitionError("reference_asset must be a nonempty string")
    require_nonempty(reference_asset, "reference_asset")
    if "assets" not in defensive_config:
        raise ContractDefinitionError("defensive_config requires explicit assets")
    require_asset_ids(
        defensive_config["assets"],
        field_name="defensive_config.assets",
        allow_empty=True,
    )
    _require_canary_scoring(canary_config)
    _require_signals_config(signals_config)


def _require_scoring(value: object, *, field_name: str) -> None:
    raw = _mapping(value, field_name)
    require_known_mapping_keys(raw, field_name=field_name, allowed=_SCORING_KEYS)
    if "method" not in raw:
        raise ContractDefinitionError(f"{field_name} requires explicit method")
    method = raw["method"]
    if not isinstance(method, str):
        raise ContractDefinitionError(f"{field_name}.method must be a string")
    if method not in _SCORING_METHODS:
        raise ContractDefinitionError(
            f"{field_name}.method must be one of " + ", ".join(sorted(_SCORING_METHODS))
        )
    if "horizon" not in raw:
        raise ContractDefinitionError(f"{field_name} requires explicit horizon")
    horizon = raw["horizon"]
    if not isinstance(horizon, int) or isinstance(horizon, bool) or horizon <= 0:
        raise ContractDefinitionError(f"{field_name}.horizon must be a positive integer")
    if method == "momentum_score":
        if "score_name" not in raw:
            raise ContractDefinitionError(f"{field_name} requires explicit score_name")
        score_name = raw["score_name"]
        if not isinstance(score_name, str):
            raise ContractDefinitionError(f"{field_name}.score_name must be a nonempty string")
        require_nonempty(score_name, f"{field_name}.score_name")
    elif "score_name" in raw:
        raise ContractDefinitionError(f"{field_name}.score_name is only valid for momentum_score")


def _require_canary_scoring(config: Mapping[str, object]) -> None:
    if "assets" not in config:
        raise ContractDefinitionError("canary_config requires explicit assets")
    if "enabled" not in config:
        raise ContractDefinitionError("canary_config requires explicit enabled")
    assets = require_asset_ids(
        config["assets"],
        field_name="canary_config.assets",
        allow_empty=True,
    )
    enabled = require_boolean_tuple(config["enabled"], field_name="canary_config.enabled")
    if len(assets) != len(enabled):
        raise ContractDefinitionError("canary_config.enabled must match assets length")
    if any(enabled):
        if "scoring" not in config:
            raise ContractDefinitionError("canary_config requires explicit scoring when enabled")
        _require_scoring(config["scoring"], field_name="canary_config.scoring")
    elif "scoring" in config:
        raise ContractDefinitionError(
            "canary_config.scoring is only valid when a canary is enabled"
        )


def _require_signals_config(config: Mapping[str, object]) -> None:  # noqa: C901, PLR0912 -- signal variants
    for key, value in config.items():
        require_nonempty(key, "signal identifier")
        raw = _mapping(value, f"signals_config.{key}")
        if "kind" not in raw:
            raise ContractDefinitionError(f"signals_config.{key} requires explicit kind")
        kind = raw["kind"]
        if not isinstance(kind, str):
            raise ContractDefinitionError(f"signals_config.{key}.kind must be a string")
        if kind == "threshold_mode":
            require_known_mapping_keys(
                raw,
                field_name=f"signals_config.{key}",
                allowed=_THRESHOLD_MODE_KEYS,
            )
            if "mode" not in raw:
                raise ContractDefinitionError(f"signals_config.{key} requires explicit mode")
            if raw["mode"] not in ("SOFT", "HARD"):
                raise ContractDefinitionError(f"signals_config.{key}.mode must be SOFT or HARD")
        elif kind == "negative_abs_momentum":
            require_known_mapping_keys(
                raw,
                field_name=f"signals_config.{key}",
                allowed=_NEGATIVE_ABS_KEYS,
            )
            if "enabled" not in raw:
                raise ContractDefinitionError(f"signals_config.{key} requires explicit enabled")
            if raw["enabled"] is not True and raw["enabled"] is not False:
                raise ContractDefinitionError(f"signals_config.{key}.enabled must be a boolean")
            if raw["enabled"] is True:
                if "threshold" not in raw:
                    raise ContractDefinitionError(
                        f"signals_config.{key} requires explicit threshold"
                    )
                threshold = raw["threshold"]
                if not isinstance(threshold, int) or isinstance(threshold, bool) or threshold <= 0:
                    raise ContractDefinitionError(
                        f"signals_config.{key}.threshold must be a positive integer"
                    )
                if "scoring" not in raw:
                    raise ContractDefinitionError(f"signals_config.{key} requires explicit scoring")
                _require_scoring(raw["scoring"], field_name=f"signals_config.{key}.scoring")
            elif "threshold" in raw or "scoring" in raw:
                raise ContractDefinitionError(
                    f"signals_config.{key} threshold and scoring are only valid when enabled"
                )
        else:
            raise ContractDefinitionError(
                f"signals_config.{key}.kind must be threshold_mode or negative_abs_momentum"
            )
