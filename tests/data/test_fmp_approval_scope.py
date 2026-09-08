from __future__ import annotations

from dataclasses import replace
from datetime import date
from pathlib import Path

from aegis_alpha.data.fmp_approval_scope import (
    CollectScope,
    CommonScope,
    UniverseScope,
    collect_scope_sha256,
    universe_scope_sha256,
)

RUN_ID = "fmp-run-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"


def _common(tmp_path: Path) -> CommonScope:
    return CommonScope(
        run_identity=RUN_ID,
        policy_sha256="a" * 64,
        tier_sha256="b" * 64,
        notification_sha256="c" * 64,
        raw_store_root=(tmp_path / "raw").resolve(),
        dataset_root=(tmp_path / "datasets").resolve(),
        max_calls=6,
    )


def test_collect_scope_binds_every_immutable_selection(tmp_path: Path) -> None:
    scope = CollectScope(
        common=_common(tmp_path),
        manifest_sha256="d" * 64,
        dataset_selection="fmp_dividends",
        datasets=("fmp_dividends", "fmp_profile"),
        mode="backfill",
        as_of=date(2026, 8, 19),
        operator_from=date(2026, 7, 27),
        receipt_path=(tmp_path / "receipt.json").resolve(),
    )
    digest = collect_scope_sha256(scope)
    mutations = (
        replace(scope, manifest_sha256="e" * 64),
        replace(scope, dataset_selection="fmp_splits", datasets=("fmp_splits", "fmp_profile")),
        replace(scope, mode="incremental"),
        replace(scope, as_of=date(2026, 8, 20)),
        replace(scope, operator_from=None),
        replace(scope, receipt_path=(tmp_path / "other.json").resolve()),
        replace(scope, common=replace(scope.common, max_calls=5)),
        replace(scope, common=replace(scope.common, policy_sha256="f" * 64)),
        replace(scope, common=replace(scope.common, tier_sha256="f" * 64)),
        replace(scope, common=replace(scope.common, notification_sha256="f" * 64)),
    )
    assert all(collect_scope_sha256(changed) != digest for changed in mutations)


def test_universe_scope_binds_output_and_fixed_semantics(tmp_path: Path) -> None:
    scope = UniverseScope(
        common=_common(tmp_path),
        destination=(tmp_path / "universe.json").resolve(),
    )
    digest = universe_scope_sha256(scope)
    assert (
        universe_scope_sha256(replace(scope, destination=(tmp_path / "other.json").resolve()))
        != digest
    )
