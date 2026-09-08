"""Fabricated inputs for public engine contracts; no private strategy records."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.engine import (
    ENGINE_BUNDLE_SCHEMA_V1,
    ENGINE_CONTRACT_VERSION_V1,
    CalendarConventions,
    EngineBundle,
    EngineContract,
    EnsembleMembership,
    FeatureMatrixSpec,
    MembershipRow,
    PricePoint,
    ReplayRequest,
    StaleGateSpec,
    StrategyRecord,
    load_bundle,
    membership_hash,
    sha256_bytes,
)

DAY = date(2026, 3, 31)
GATES = StaleGateSpec(50, 50)


def strategy() -> StrategyRecord:
    return StrategyRecord(
        name="synthetic-choice",
        description="fabricated arithmetic",
        cash_asset="CASH_X",
        role="research",
        similarity_group=None,
        offensive_config={
            "strategy_type": "relative",
            "assets": ["ASSET_A", "ASSET_B"],
            "top_n": 1,
            "scoring": {"method": "return_rate", "horizon": 2},
            "reference_asset": "REF_X",
        },
        defensive_config={"assets": []},
        canary_config={"canary_mode": "OR", "assets": [], "enabled": []},
        signals_config={},
        variant_of=None,
        variant_spec=None,
    )


def membership() -> EnsembleMembership:
    rows = (MembershipRow("synthetic-choice", Decimal(1)),)
    return EnsembleMembership(rows, membership_hash(rows))


def contract() -> EngineContract:
    return EngineContract(
        contract_version=ENGINE_CONTRACT_VERSION_V1,
        pack=(strategy(),),
        feature_matrix=FeatureMatrixSpec(
            momentum_scores=(),
            moving_average_months=(),
            ma_window_includes_current_month=True,
            return_months=(2,),
            includes_latest_price=True,
        ),
        macro_signals=(),
        calendar=CalendarConventions(
            "calendar_month_end",
            "prior_calendar_month_end",
            1,
            3,
            "synthetic",
            "synthetic",
            "synthetic",
            "synthetic",
        ),
        stale_gates=GATES,
        ensemble_membership_reference="ensemble:" + membership().membership_sha256,
        derived_series=(),
    )


def raw_bundle(value: EngineContract) -> bytes:
    return canonical_json_bytes(
        {
            "schema_version": ENGINE_BUNDLE_SCHEMA_V1,
            "bundle_id": "synthetic-probe",
            "bundle_version": "1",
            "contract": value,
        }
    )


def bundle(value: EngineContract) -> EngineBundle:
    raw = raw_bundle(value)
    return load_bundle(raw, sha256_bytes(raw), "synthetic-probe", "1")


def request() -> ReplayRequest:
    dates = (date(2026, 1, 31), date(2026, 2, 28), DAY)
    prices = {
        key: tuple(PricePoint(when, close, when) for when, close in zip(dates, values, strict=True))
        for key, values in {
            "ASSET_A": (10, 11, 14),
            "ASSET_B": (10, 12, 12),
            "REF_X": (10, 10, 10),
        }.items()
    }
    return ReplayRequest(DAY, DAY, prices, {}, {}, membership())
