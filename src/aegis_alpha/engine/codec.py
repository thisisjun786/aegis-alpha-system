"""Boundary parser for canonical engine contract and bundle JSON."""

from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from typing import Final

from aegis_alpha.engine.errors import (
    BundleParseError,
    BundleVersionError,
    ContractParseError,
    ContractVersionError,
)
from aegis_alpha.engine.models import (
    ENGINE_CONTRACT_VERSION_V1,
    CalendarConventions,
    DerivedInputBinding,
    DerivedSeriesSpec,
    EngineContract,
    FeatureMatrixSpec,
    MacroSignalSpec,
    MomentumScoreSpec,
    StaleGateSpec,
    StrategyRecord,
)

ENGINE_BUNDLE_SCHEMA_V1: Final = "aas-engine-bundle-v1"
_ALLOWED_BUNDLE_KEYS: Final = frozenset(
    {"schema_version", "bundle_id", "bundle_version", "contract"}
)
_ALLOWED_ROOT_KEYS: Final = frozenset(
    {
        "contract_version",
        "pack",
        "feature_matrix",
        "macro_signals",
        "calendar",
        "stale_gates",
        "ensemble_membership_reference",
        "derived_series",
    }
)
_ALLOWED_STRATEGY_KEYS: Final = frozenset(
    {
        "name",
        "description",
        "cash_asset",
        "role",
        "similarity_group",
        "offensive_config",
        "defensive_config",
        "canary_config",
        "signals_config",
        "variant_of",
        "variant_spec",
    }
)
_ALLOWED_FEATURE_MATRIX_KEYS: Final = frozenset(
    {
        "momentum_scores",
        "moving_average_months",
        "ma_window_includes_current_month",
        "return_months",
        "includes_latest_price",
    }
)
_ALLOWED_MOMENTUM_SCORE_KEYS: Final = frozenset({"name", "return_months", "weights", "divisor"})
_ALLOWED_MACRO_SIGNAL_KEYS: Final = frozenset(
    {
        "series_id",
        "lag_months",
        "lag_combination",
        "comparison",
        "thresholds",
        "threshold_mode_key",
    }
)
_ALLOWED_CALENDAR_KEYS: Final = frozenset(
    {
        "evaluation_snap",
        "signal_date",
        "current_month_drop_before_day",
        "history_observations",
        "canonical_selection_authority",
        "evaluation_authority",
        "signal_authority",
        "history_authority",
    }
)
_ALLOWED_STALE_GATE_KEYS: Final = frozenset({"price_stale_after_days", "macro_stale_after_days"})
_ALLOWED_DERIVED_KEYS: Final = frozenset(
    {
        "series_id",
        "operation",
        "trailing_months",
        "consumes_capital",
        "consumes_totalreturn",
        "input_bindings",
        "signal_lag_months",
        "signal_thresholds",
        "reference_provenance",
        "canonical_sha256",
    }
)
_ALLOWED_INPUT_BINDING_KEYS: Final = frozenset({"dataset_id", "dataset_version", "series", "field"})


def _reject_non_finite(token: str) -> float:
    raise ContractParseError("document", f"non-finite JSON constant {token} is forbidden")


def _require_known_keys(row: Mapping[str, object], field: str, allowed: frozenset[str]) -> None:
    unknown = sorted(set(row) - allowed)
    if unknown:
        raise ContractParseError(field, f"unknown keys: {unknown}")


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ContractParseError("document", f"duplicate JSON object key {key!r}")
        result[key] = value
    return result


def decode_json(raw: bytes) -> object:
    try:
        return json.loads(
            raw,
            parse_constant=_reject_non_finite,
            object_pairs_hook=_reject_duplicate_keys,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ContractParseError("document", "not valid UTF-8 JSON") from error


def parse_contract(raw: bytes) -> EngineContract:
    return parse_contract_mapping(_mapping(decode_json(raw), "document"))


def parse_contract_mapping(decoded_root: Mapping[str, object]) -> EngineContract:
    _require_known_keys(decoded_root, "document", _ALLOWED_ROOT_KEYS)
    version = _string(decoded_root, "contract_version")
    contract = EngineContract(
        contract_version=version,
        pack=tuple(
            _strategy(item, index) for index, item in enumerate(_sequence(decoded_root, "pack"))
        ),
        feature_matrix=_feature_matrix(_mapping_field(decoded_root, "feature_matrix")),
        macro_signals=tuple(
            _macro_signal(item, index)
            for index, item in enumerate(_sequence(decoded_root, "macro_signals"))
        ),
        calendar=_calendar(_mapping_field(decoded_root, "calendar")),
        stale_gates=_stale_gates(_mapping_field(decoded_root, "stale_gates")),
        ensemble_membership_reference=_string(decoded_root, "ensemble_membership_reference"),
        derived_series=tuple(
            _derived_series(item, index)
            for index, item in enumerate(_sequence(decoded_root, "derived_series"))
        ),
    )
    if contract.contract_version != ENGINE_CONTRACT_VERSION_V1:
        raise ContractVersionError(version)
    return contract


def parse_bundle_mapping(
    decoded_root: Mapping[str, object],
) -> tuple[str, str, str, EngineContract]:
    _require_known_keys(decoded_root, "document", _ALLOWED_BUNDLE_KEYS)
    schema_version = _string(decoded_root, "schema_version")
    if schema_version != ENGINE_BUNDLE_SCHEMA_V1:
        raise BundleVersionError(schema_version)
    bundle_id = _string(decoded_root, "bundle_id")
    bundle_version = _string(decoded_root, "bundle_version")
    if not bundle_id.strip() or not bundle_version.strip():
        raise BundleParseError("bundle_id", "bundle identity fields must be nonempty")
    contract = parse_contract_mapping(_mapping_field(decoded_root, "contract"))
    return schema_version, bundle_id, bundle_version, contract


def _strategy(value: object, index: int) -> StrategyRecord:
    row = _mapping(value, f"pack[{index}]")
    _require_known_keys(row, f"pack[{index}]", _ALLOWED_STRATEGY_KEYS)
    variant = row.get("variant_of")
    similarity = row.get("similarity_group")
    variant_spec = row.get("variant_spec")
    return StrategyRecord(
        name=_string(row, "name"),
        description=_string(row, "description"),
        cash_asset=_string(row, "cash_asset"),
        role=_string(row, "role"),
        similarity_group=_optional_string(similarity, "similarity_group"),
        offensive_config=dict(_mapping_field(row, "offensive_config")),
        defensive_config=dict(_mapping_field(row, "defensive_config")),
        canary_config=dict(_mapping_field(row, "canary_config")),
        signals_config=dict(_mapping_field(row, "signals_config")),
        variant_of=_optional_string(variant, "variant_of"),
        variant_spec=(
            None if variant_spec is None else dict(_mapping(variant_spec, "variant_spec"))
        ),
    )


def _feature_matrix(row: Mapping[str, object]) -> FeatureMatrixSpec:
    _require_known_keys(row, "feature_matrix", _ALLOWED_FEATURE_MATRIX_KEYS)
    scores = tuple(
        _momentum_score(item, index) for index, item in enumerate(_sequence(row, "momentum_scores"))
    )
    return FeatureMatrixSpec(
        momentum_scores=scores,
        moving_average_months=_integer_tuple(row, "moving_average_months"),
        ma_window_includes_current_month=_boolean(row, "ma_window_includes_current_month"),
        return_months=_integer_tuple(row, "return_months"),
        includes_latest_price=_boolean(row, "includes_latest_price"),
    )


def _momentum_score(value: object, index: int) -> MomentumScoreSpec:
    row = _mapping(value, f"momentum_scores[{index}]")
    _require_known_keys(row, f"momentum_scores[{index}]", _ALLOWED_MOMENTUM_SCORE_KEYS)
    return MomentumScoreSpec(
        name=_string(row, "name"),
        return_months=_integer_tuple(row, "return_months"),
        weights=_number_tuple(row, "weights"),
        divisor=_number(row, "divisor"),
    )


def _macro_signal(value: object, index: int) -> MacroSignalSpec:
    row = _mapping(value, f"macro_signals[{index}]")
    _require_known_keys(row, f"macro_signals[{index}]", _ALLOWED_MACRO_SIGNAL_KEYS)
    mode_key = row.get("threshold_mode_key")
    return MacroSignalSpec(
        series_id=_string(row, "series_id"),
        lag_months=_integer_tuple(row, "lag_months"),
        lag_combination=_string(row, "lag_combination"),
        comparison=_string(row, "comparison"),
        thresholds=_number_tuple(row, "thresholds"),
        threshold_mode_key=_optional_string(mode_key, "threshold_mode_key"),
    )


def _calendar(row: Mapping[str, object]) -> CalendarConventions:
    _require_known_keys(row, "calendar", _ALLOWED_CALENDAR_KEYS)
    return CalendarConventions(
        evaluation_snap=_string(row, "evaluation_snap"),
        signal_date=_string(row, "signal_date"),
        current_month_drop_before_day=_integer(row, "current_month_drop_before_day"),
        history_observations=_integer(row, "history_observations"),
        canonical_selection_authority=_string(row, "canonical_selection_authority"),
        evaluation_authority=_string(row, "evaluation_authority"),
        signal_authority=_string(row, "signal_authority"),
        history_authority=_string(row, "history_authority"),
    )


def _stale_gates(row: Mapping[str, object]) -> StaleGateSpec:
    _require_known_keys(row, "stale_gates", _ALLOWED_STALE_GATE_KEYS)
    return StaleGateSpec(
        price_stale_after_days=_integer(row, "price_stale_after_days"),
        macro_stale_after_days=_integer(row, "macro_stale_after_days"),
    )


def _derived_series(value: object, index: int) -> DerivedSeriesSpec:
    row = _mapping(value, f"derived_series[{index}]")
    _require_known_keys(row, f"derived_series[{index}]", _ALLOWED_DERIVED_KEYS)
    bindings = tuple(
        _derived_binding(item, binding_index)
        for binding_index, item in enumerate(_sequence(row, "input_bindings"))
    )
    spec = DerivedSeriesSpec(
        series_id=_string(row, "series_id"),
        operation=_string(row, "operation"),
        trailing_months=_integer(row, "trailing_months"),
        consumes_capital=_boolean(row, "consumes_capital"),
        consumes_totalreturn=_boolean(row, "consumes_totalreturn"),
        input_bindings=bindings,
        signal_lag_months=_integer_tuple(row, "signal_lag_months"),
        signal_thresholds=_number_tuple(row, "signal_thresholds"),
        reference_provenance=_string(row, "reference_provenance"),
    )
    if _string(row, "canonical_sha256") != spec.canonical_sha256:
        raise ContractParseError("canonical_sha256", "derived series digest mismatch")
    return spec


def _derived_binding(value: object, index: int) -> DerivedInputBinding:
    row = _mapping(value, f"input_bindings[{index}]")
    _require_known_keys(row, f"input_bindings[{index}]", _ALLOWED_INPUT_BINDING_KEYS)
    return DerivedInputBinding(
        dataset_id=_string(row, "dataset_id"),
        dataset_version=_string(row, "dataset_version"),
        series=_string(row, "series"),
        field=_string(row, "field"),
    )


def _mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ContractParseError(field, "must be an object with string keys")
    result: dict[str, object] = {}
    for key, child in value.items():
        if not isinstance(key, str):
            raise ContractParseError(field, "must be an object with string keys")
        result[key] = child
    return result


def _mapping_field(row: Mapping[str, object], field: str) -> Mapping[str, object]:
    return _mapping(_required(row, field), field)


def _sequence(row: Mapping[str, object], field: str) -> Sequence[object]:
    value = _required(row, field)
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ContractParseError(field, "must be an array")
    return value


def _required(row: Mapping[str, object], field: str) -> object:
    if field not in row:
        raise ContractParseError(field, "is required")
    return row[field]


def _string(row: Mapping[str, object], field: str) -> str:
    value = _required(row, field)
    if not isinstance(value, str):
        raise ContractParseError(field, "must be a string")
    return value


def _optional_string(value: object, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ContractParseError(field, "must be a string or null")
    return value


def _integer(row: Mapping[str, object], field: str) -> int:
    value = _required(row, field)
    if not isinstance(value, int) or isinstance(value, bool):
        raise ContractParseError(field, "must be an integer")
    return value


def _boolean(row: Mapping[str, object], field: str) -> bool:
    value = _required(row, field)
    if not isinstance(value, bool):
        raise ContractParseError(field, "must be a boolean")
    return value


def _number(row: Mapping[str, object], field: str) -> float:
    value = _required(row, field)
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ContractParseError(field, "must be a number")
    try:
        converted = float(value)
    except OverflowError as error:
        raise ContractParseError(field, "number is outside finite range") from error
    if not math.isfinite(converted):
        raise ContractParseError(field, "non-finite numbers are forbidden")
    return converted


def _integer_tuple(row: Mapping[str, object], field: str) -> tuple[int, ...]:
    return tuple(_integer({field: value}, field) for value in _sequence(row, field))


def _number_tuple(row: Mapping[str, object], field: str) -> tuple[float, ...]:
    return tuple(_number({field: value}, field) for value in _sequence(row, field))
