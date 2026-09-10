"""Synthetic audit evidence survives malformed provider responses."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path

import pytest

from aegis_alpha.data.descriptor_tree import DescriptorTreeError
from aegis_alpha.data.qveris_billing import audit_request
from aegis_alpha.data.qveris_client import QverisDocumentError, QverisResponse
from aegis_alpha.data.qveris_store import QverisStore
from aegis_alpha.data.serialization import canonical_json_bytes

_NOW = datetime(2026, 1, 1, tzinfo=UTC)
_SENTINEL = "synthetic-omitted-value"
_AUDIT_CALLS = 2


class AuditClient:
    account_key = "synthetic-account"

    def __init__(self, response: QverisResponse) -> None:
        self.response = response
        self.calls = 0

    def request(
        self,
        path: str,
        *,
        body: dict[str, object] | None = None,
        query: dict[str, str | int] | None = None,
    ) -> QverisResponse:
        del path, body, query
        self.calls += 1
        return self.response


@pytest.mark.parametrize(
    "raw",
    [
        b"\xffsynthetic-omitted-value",
        b"<html>synthetic-omitted-value</html>",
        b"",
        b'["synthetic-omitted-value"]',
        b'{"synthetic-omitted-value":',
        b'{"value":NaN}',
        b'{"value":1,"value":2}',
    ],
)
def test_malformed_response_retains_only_safe_audit_metadata(tmp_path: Path, raw: bytes) -> None:
    response = QverisResponse(502, (("content-type", "text/plain"),), raw, _NOW, _NOW)
    client = AuditClient(response)
    with QverisStore(tmp_path, client.account_key) as store:
        with pytest.raises(QverisDocumentError):
            audit_request(
                client,
                store,
                "/auth/credits",
                body={"nested": [{"api_key": _SENTINEL, "keep": 1}]},
                query={"authorization": _SENTINEL, "page": 1},
            )
        files = list((tmp_path / "audits").glob("*.json"))
        assert len(files) == 1
        saved = store.document(files[0].relative_to(tmp_path).as_posix())
        assert saved == {
            "path": "/auth/credits",
            "body": {"nested": [{"keep": 1}]},
            "query": {"page": 1},
            "status": 502,
            "headers": [["content-type", "text/plain"]],
            "requested_at_utc": "2026-01-01T00:00:00Z",
            "retrieved_at_utc": "2026-01-01T00:00:00Z",
            "response": None,
            "representation": "undecodable response omitted",
            "body_sha256": hashlib.sha256(raw).hexdigest(),
            "body_bytes": len(raw),
            "error_type": "QverisDocumentError",
        }
        assert _SENTINEL.encode() not in files[0].read_bytes()
    assert client.calls == 1


def test_successful_audit_projection_preserves_bytes(tmp_path: Path) -> None:
    document = {"status": "success", "data": {"remaining_credits": "5"}}
    response = QverisResponse(200, (), canonical_json_bytes(document), _NOW, _NOW)
    client = AuditClient(response)
    expected = canonical_json_bytes(
        {
            "path": "/auth/credits",
            "body": None,
            "query": {"page": 1},
            "status": 200,
            "headers": (),
            "requested_at_utc": _NOW,
            "retrieved_at_utc": _NOW,
            "response": document,
            "representation": "credential-attribution-redacted JSON projection",
        }
    )
    with QverisStore(tmp_path, client.account_key) as store:
        assert audit_request(client, store, "/auth/credits", query={"page": 1}) == document
        (first,) = (tmp_path / "audits").glob("*.json")
        assert first.read_bytes() == expected
        assert audit_request(client, store, "/auth/credits", query={"page": 1}) == document
        assert first.read_bytes() == expected
        assert len(list((tmp_path / "audits").glob("*.json"))) == _AUDIT_CALLS


def test_existing_evidence_longer_than_new_payload_is_rejected(tmp_path: Path) -> None:
    with QverisStore(tmp_path, "synthetic") as store:
        store.publish("existing", b"abcdef")
        with pytest.raises(DescriptorTreeError, match="size cap"):
            store.publish("existing", b"abc")
        assert store.read("existing") == b"abcdef"
