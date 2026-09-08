"""Validated bundle envelope: identity, source hash, and nested contract."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Final, cast

from aegis_alpha.data.serialization import canonical_json_bytes, content_sha256
from aegis_alpha.engine.codec import (
    ENGINE_BUNDLE_SCHEMA_V1,
    decode_json,
    parse_bundle_mapping,
)
from aegis_alpha.engine.errors import BundleIdentityError, BundleParseError
from aegis_alpha.engine.models import EngineContract

_SHA256_HEX: Final = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True, slots=True)
class EngineBundle:
    schema_version: str
    bundle_id: str
    bundle_version: str
    contract: EngineContract
    source_sha256: str

    @property
    def contract_sha256(self) -> str:
        return content_sha256(self.contract)


def sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def serialize_bundle(bundle: EngineBundle) -> bytes:
    return canonical_json_bytes(
        {
            "schema_version": bundle.schema_version,
            "bundle_id": bundle.bundle_id,
            "bundle_version": bundle.bundle_version,
            "contract": bundle.contract,
        }
    )


def load_bundle(
    raw: bytes,
    expected_sha256: str,
    expected_id: str,
    expected_version: str,
) -> EngineBundle:
    if not isinstance(raw, (bytes, bytearray)):
        raise BundleParseError("document", "bundle payload must be bytes")
    payload = bytes(raw)
    if not isinstance(expected_sha256, str) or not _SHA256_HEX.fullmatch(expected_sha256):
        raise BundleIdentityError("expected_sha256 must be a 64-character lowercase hex digest")
    if any(
        not isinstance(value, str) or not value.strip() for value in (expected_id, expected_version)
    ):
        raise BundleIdentityError("expected bundle identity fields must be nonempty")
    observed = sha256_bytes(payload)
    if observed != expected_sha256:
        raise BundleIdentityError("raw payload SHA-256 does not match expected_sha256")
    decoded = decode_json(payload)
    if not isinstance(decoded, dict):
        raise BundleParseError("document", "bundle must be an object")
    # JSON object decoding guarantees string keys after the container check.
    schema_version, bundle_id, bundle_version, contract = parse_bundle_mapping(
        cast("dict[str, object]", decoded)
    )
    if bundle_id != expected_id:
        raise BundleIdentityError("bundle_id does not match expected_id")
    if bundle_version != expected_version:
        raise BundleIdentityError("bundle_version does not match expected_version")
    if schema_version != ENGINE_BUNDLE_SCHEMA_V1:
        raise BundleIdentityError("schema_version is not the supported engine bundle schema")
    return EngineBundle(
        schema_version=schema_version,
        bundle_id=bundle_id,
        bundle_version=bundle_version,
        contract=contract,
        source_sha256=observed,
    )


def canonical_contract_hash(contract: EngineContract) -> str:
    return content_sha256(contract)
