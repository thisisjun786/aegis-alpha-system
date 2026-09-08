from __future__ import annotations

import hashlib
from datetime import UTC, datetime

import pytest

from aegis_alpha.data.contracts import SourceSnapshot, ValidationStatus


def make_snapshot(
    *,
    source_uri: str,
    parameters: dict[str, str],
    request_fingerprint: str = "sha256:" + "a" * 64,
) -> SourceSnapshot:
    payload = b"{}"
    captured_at = datetime(2026, 7, 29, tzinfo=UTC)
    return SourceSnapshot(
        snapshot_id="snapshot-credential-check",
        schema_version=1,
        provider="synthetic",
        dataset="daily_prices",
        source_uri=source_uri,
        request_fingerprint=request_fingerprint,
        parameters=parameters,
        requested_at_utc=captured_at,
        retrieved_at_utc=captured_at,
        content_type="application/json",
        encoding="utf-8",
        compression=None,
        raw_byte_length=len(payload),
        content_sha256=hashlib.sha256(payload).hexdigest(),
        parser_name="synthetic-json",
        parser_version="1.0.0",
        validation_status=ValidationStatus.PASS,
    )


@pytest.mark.parametrize(
    ("source_uri", "parameters"),
    [
        ("https://provider.test/prices", {"api-key": "placeholder-secret"}),
        ("https://provider.test/prices", {"X-API-Key": "placeholder-secret"}),
        ("https://provider.test/prices", {"access_token": "placeholder-secret"}),
        ("https://provider.test/prices", {"ACCESSKey": "placeholder-secret"}),
        (
            "https://provider.test/prices",
            {"\uff21\uff23\uff23\uff25\uff33\uff33\uff2b\uff45\uff59": "placeholder-secret"},
        ),
        ("https://provider.test/prices?apikey=placeholder-secret", {}),
        ("https://provider.test/prices?access.key=placeholder-secret", {}),
        ("https://provider.test/prices#access.key=placeholder-secret", {}),
        ("//provider.test/prices?access.key=placeholder-secret", {}),
        ("https://user:placeholder-secret@provider.test/prices", {}),
    ],
)
def test_snapshot_rejects_credentials_in_parameters_or_source_uri(
    source_uri: str, parameters: dict[str, str]
) -> None:
    with pytest.raises(ValueError, match="credential"):
        make_snapshot(source_uri=source_uri, parameters=parameters)


def test_snapshot_rejects_raw_request_fingerprint() -> None:
    with pytest.raises(ValueError, match="request_fingerprint"):
        make_snapshot(
            source_uri="https://provider.test/prices",
            parameters={},
            request_fingerprint="placeholder-secret",
        )
