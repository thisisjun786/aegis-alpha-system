from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest

from aegis_alpha.data.contracts import SourceSnapshot, ValidationStatus
from aegis_alpha.identity.registry import IdentityRegistry
from aegis_alpha.metadata.records import (
    SourceSnapshotFile,
    SourceSnapshotRegistration,
    source_tree_digest,
)
from aegis_alpha.metadata.registry import MetadataRegistry

if TYPE_CHECKING:
    from sqlalchemy import Engine


@pytest.fixture
def identity_registry(clean_postgres: Engine) -> IdentityRegistry:
    return IdentityRegistry(clean_postgres)


@pytest.fixture
def register_source_snapshot(
    clean_postgres: Engine,
) -> Callable[..., None]:
    def register(
        snapshot_id: str,
        provider: str = "norgate",
        validation_status: ValidationStatus = ValidationStatus.PASS,
    ) -> None:
        captured_at = datetime(2026, 7, 31, 9, 0, tzinfo=UTC)
        snapshot = SourceSnapshot(
            snapshot_id=snapshot_id,
            schema_version=1,
            provider=provider,
            dataset="security-identity",
            source_uri=f"https://provider.invalid/identity/{snapshot_id}",
            request_fingerprint="sha256:" + "b" * 64,
            parameters={"fixture": "identity"},
            requested_at_utc=captured_at,
            retrieved_at_utc=captured_at,
            content_type="application/json",
            encoding="utf-8",
            compression=None,
            raw_byte_length=100,
            content_sha256="a" * 64,
            parser_name="identity-fixture",
            parser_version="1.0.0",
            validation_status=validation_status,
        )
        files = (SourceSnapshotFile("payload.json", 100, "1" * 64),)
        MetadataRegistry(clean_postgres).register_source_snapshot(
            SourceSnapshotRegistration(
                snapshot=snapshot,
                tree_sha256=source_tree_digest(files),
                files=files,
                manifest={"fixture": "identity"},
            )
        )

    return register
