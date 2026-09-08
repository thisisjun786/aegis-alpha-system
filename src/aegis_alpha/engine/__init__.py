"""Public reusable engine: explicit contracts, no private strategy recipes."""

from __future__ import annotations

from aegis_alpha.engine.allocation import StrategyType, allocate
from aegis_alpha.engine.bundle import (
    EngineBundle,
    canonical_contract_hash,
    load_bundle,
    serialize_bundle,
    sha256_bytes,
)
from aegis_alpha.engine.codec import ENGINE_BUNDLE_SCHEMA_V1, parse_contract
from aegis_alpha.engine.derived import DerivedCashflows, derive_history, derive_series
from aegis_alpha.engine.ensemble import EnsembleMembership, combine_ensemble
from aegis_alpha.engine.errors import (
    BlockReason,
    BundleIdentityError,
    BundleParseError,
    BundleVersionError,
    ContractDefinitionError,
    ContractParseError,
    ContractVersionError,
    EnsembleMembershipError,
    ReplayBlockedError,
    StrategyPackLineageError,
)
from aegis_alpha.engine.features import (
    AssetFeatures,
    FeatureBuildRequest,
    PricePoint,
    build_feature_matrix,
)
from aegis_alpha.engine.membership import MembershipRow, membership_hash
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
from aegis_alpha.engine.replay import ReplayReceipt, ReplayRequest, replay
from aegis_alpha.engine.signals import (
    MacroPoint,
    ScoringSpec,
    SignalSnapshot,
    evaluate_signals,
)

__all__ = [
    "ENGINE_BUNDLE_SCHEMA_V1",
    "ENGINE_CONTRACT_VERSION_V1",
    "AssetFeatures",
    "BlockReason",
    "BundleIdentityError",
    "BundleParseError",
    "BundleVersionError",
    "CalendarConventions",
    "ContractDefinitionError",
    "ContractParseError",
    "ContractVersionError",
    "DerivedCashflows",
    "DerivedInputBinding",
    "DerivedSeriesSpec",
    "EngineBundle",
    "EngineContract",
    "EnsembleMembership",
    "EnsembleMembershipError",
    "FeatureBuildRequest",
    "FeatureMatrixSpec",
    "MacroPoint",
    "MacroSignalSpec",
    "MembershipRow",
    "MomentumScoreSpec",
    "PricePoint",
    "ReplayBlockedError",
    "ReplayReceipt",
    "ReplayRequest",
    "ScoringSpec",
    "SignalSnapshot",
    "StaleGateSpec",
    "StrategyPackLineageError",
    "StrategyRecord",
    "StrategyType",
    "allocate",
    "build_feature_matrix",
    "canonical_contract_hash",
    "combine_ensemble",
    "derive_history",
    "derive_series",
    "evaluate_signals",
    "load_bundle",
    "membership_hash",
    "parse_contract",
    "replay",
    "serialize_bundle",
    "sha256_bytes",
]
