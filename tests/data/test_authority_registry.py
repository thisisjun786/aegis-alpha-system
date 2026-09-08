from __future__ import annotations

import json
from pathlib import Path

from aegis_alpha.data.contracts import SourcePolicy, SourceRole


def test_registry_is_valid_and_keeps_unapproved_sources_disabled() -> None:
    document = json.loads(Path("config/data_authority_registry.json").read_text())
    policies = tuple(
        SourcePolicy(
            policy_id=entry["policy_id"],
            provider=entry["provider"],
            role=SourceRole(entry["role"]),
            domains=tuple(entry["domains"]),
            fields=tuple(entry["fields"]),
            semantic_compatibility=entry["semantic_compatibility"],
            scheduled_collection_allowed=entry["scheduled_collection_allowed"],
            canonical_write_allowed=entry["canonical_write_allowed"],
            backtest_eligible=entry["backtest_eligible"],
            paper_eligible=entry["paper_eligible"],
            order_eligible=entry["order_eligible"],
            license_classification=entry["license_classification"],
            retention_classification=entry["retention_classification"],
        )
        for entry in document["policies"]
    )

    assert policies
    assert all(not policy.order_eligible for policy in policies)
    assert all(
        not policy.scheduled_collection_allowed
        for policy in policies
        if policy.provider in {"fmp", "finimpulse"}
    )
    norgate = next(policy for policy in policies if policy.provider == "norgate")
    assert "historical_universe" in norgate.domains
    assert norgate.role is SourceRole.HISTORICAL_BACKTEST_REFERENCE
    assert not norgate.scheduled_collection_allowed
    assert not norgate.backtest_eligible
    assert not norgate.paper_eligible
    assert not norgate.order_eligible
