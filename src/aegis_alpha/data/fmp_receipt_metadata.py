"""Memory-bounded durable metadata chunks for uncapped FMP receipts."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterator, Mapping, Sequence
from pathlib import Path
from typing import Final

from aegis_alpha.data.fmp_symbol_observation import DURABLE_RESPONSE_KEYS
from aegis_alpha.data.serialization import canonical_json_bytes

RECEIPT_METADATA_CHUNK_SIZE: Final = 256
_SCHEMA_VERSION: Final = 1

Publish = Callable[[Sequence[tuple[Path, bytes]]], tuple[Path, ...]]
Validate = Callable[[bytes], None]


class ReceiptMetadataStream:
    def __init__(
        self,
        *,
        raw_store_root: Path,
        run_identity: str,
        publish: Publish,
        validate: Validate,
    ) -> None:
        self._raw_store_root: Path = raw_store_root
        self._run_identity: str = run_identity
        self._publish: Publish = publish
        self._validate: Validate = validate
        self._requests: list[Mapping[str, object]] = []
        self._provenance_addresses: list[str] = []
        self._addresses: list[str] = []
        self._paths: list[Path] = []

    def record_request(self, request: Mapping[str, object]) -> None:
        self._requests.append(request)
        if len(self._requests) >= RECEIPT_METADATA_CHUNK_SIZE:
            self._flush()

    def record_provenance(self, provenance: bytes) -> None:
        self._provenance_addresses.append(f"sha256:{hashlib.sha256(provenance).hexdigest()}")
        if len(self._provenance_addresses) >= RECEIPT_METADATA_CHUNK_SIZE:
            self._flush()

    def addresses(self) -> tuple[str, ...]:
        self._flush()
        return tuple(self._addresses)

    @property
    def paths(self) -> tuple[Path, ...]:
        return tuple(self._paths)

    def provenance_records(self) -> Iterator[bytes]:
        self._flush()
        for chunk_path in self._paths:
            document = json.loads(chunk_path.read_bytes())
            for address in document["provenance_addresses"]:
                digest = str(address).removeprefix("sha256:")
                yield (
                    self._raw_store_root
                    / "fmp"
                    / "provenance"
                    / "sha256"
                    / digest[:2]
                    / f"{digest}.json"
                ).read_bytes()

    def _flush(self) -> None:
        if not self._requests and not self._provenance_addresses:
            return
        payload = canonical_json_bytes(
            {
                "chunk_index": len(self._addresses),
                "provenance_addresses": self._provenance_addresses,
                "requests": self._requests,
                "run_identity": self._run_identity,
                "schema_version": _SCHEMA_VERSION,
            }
        )
        self._validate(payload)
        digest = hashlib.sha256(payload).hexdigest()
        path = (
            self._raw_store_root
            / "fmp"
            / "receipt-metadata"
            / "sha256"
            / digest[:2]
            / f"{digest}.json"
        )
        self._paths.extend(self._publish(((path, payload),)))
        self._addresses.append(f"sha256:{digest}")
        self._requests.clear()
        self._provenance_addresses.clear()


def validate_receipt_metadata_chunks(
    raw_store_root: Path,
    run_identity: str,
    addresses: object,
) -> None:
    if not isinstance(addresses, list):
        raise ValueError(  # noqa: TRY004 - persisted artifact validation
            "receipt metadata chunk addresses are invalid"
        )
    for address in addresses:
        if not isinstance(address, str) or not address.startswith("sha256:"):
            raise ValueError("receipt metadata chunk address is invalid")
        digest = address.removeprefix("sha256:")
        path = (
            raw_store_root / "fmp" / "receipt-metadata" / "sha256" / digest[:2] / f"{digest}.json"
        )
        try:
            payload = path.read_bytes()
            document = json.loads(payload)
        except (OSError, json.JSONDecodeError):
            raise ValueError("receipt metadata chunk is unavailable") from None
        if (
            hashlib.sha256(payload).hexdigest() != digest
            or payload != canonical_json_bytes(document)
            or document.get("schema_version") != _SCHEMA_VERSION
            or document.get("run_identity") != run_identity
            or not isinstance(document.get("requests"), list)
            or not isinstance(document.get("provenance_addresses"), list)
        ):
            raise ValueError("receipt metadata chunk is invalid")
        for provenance_address in document["provenance_addresses"]:
            _validate_provenance_address(raw_store_root, run_identity, provenance_address)


def _validate_provenance_address(
    raw_store_root: Path,
    run_identity: str,
    address: object,
) -> None:
    if not isinstance(address, str) or not address.startswith("sha256:"):
        raise ValueError("receipt provenance address is invalid")
    digest = address.removeprefix("sha256:")
    path = raw_store_root / "fmp" / "provenance" / "sha256" / digest[:2] / f"{digest}.json"
    try:
        payload = path.read_bytes()
        document = json.loads(payload)
    except (OSError, json.JSONDecodeError):
        raise ValueError("receipt provenance is unavailable") from None
    if (
        hashlib.sha256(payload).hexdigest() != digest
        or payload != canonical_json_bytes(document)
        or set(document) != DURABLE_RESPONSE_KEYS
        or document.get("run_identity") != run_identity
    ):
        raise ValueError("receipt provenance is invalid")
