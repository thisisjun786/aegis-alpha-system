"""Fail-closed local revocation for signed standing FMP authority."""

from __future__ import annotations

import json
import os
import stat
from datetime import UTC, datetime
from pathlib import Path
from typing import Final, Protocol, cast

from aegis_alpha.data.fmp_collector import publish_bundle
from aegis_alpha.data.fmp_recurring_errors import RecurringAuthorityError
from aegis_alpha.data.serialization import canonical_json_bytes

REVOCATION_CONTRACT: Final = "aegis-alpha/fmp-recurring-revocation/v1"
_MAX_REVOCATION_BYTES = 4096
_FIELDS = frozenset(
    {
        "authority_id",
        "contract",
        "payload_sha256",
        "revoked_at_utc",
        "version",
    }
)


class RevocableAuthority(Protocol):
    @property
    def authority_id(self) -> str: ...

    @property
    def raw_store_root(self) -> Path: ...

    @property
    def payload_sha256(self) -> str: ...

    def require_request(self, now: datetime) -> None: ...


def revocation_path(authority: RevocableAuthority) -> Path:
    return (
        authority.raw_store_root
        / "fmp"
        / "authority-revocations"
        / f"{authority.payload_sha256}.json"
    )


def _read_regular(path: Path) -> bytes | None:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except FileNotFoundError:
        return None
    except OSError as error:
        raise RecurringAuthorityError("recurring authority revocation is unreadable") from error
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > _MAX_REVOCATION_BYTES:
            raise RecurringAuthorityError("recurring authority revocation is invalid")
        return os.read(descriptor, _MAX_REVOCATION_BYTES + 1)
    finally:
        os.close(descriptor)


def require_not_revoked(
    authority: RevocableAuthority,
    now: datetime,
) -> None:
    payload = _read_regular(revocation_path(authority))
    if payload is None:
        return
    try:
        document = cast("dict[str, object]", json.loads(payload))
        revoked_at = datetime.fromisoformat(str(document["revoked_at_utc"]))
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        raise RecurringAuthorityError("recurring authority revocation is invalid") from None
    if (
        set(document) != _FIELDS
        or payload != canonical_json_bytes(document)
        or document["contract"] != REVOCATION_CONTRACT
        or document["version"] != 1
        or document["authority_id"] != authority.authority_id
        or document["payload_sha256"] != authority.payload_sha256
        or revoked_at.utcoffset() != UTC.utcoffset(None)
        or revoked_at > now
    ):
        raise RecurringAuthorityError("recurring authority revocation is invalid")
    raise RecurringAuthorityError("recurring authority was revoked")


def publish_recurring_revocation(
    authority: RevocableAuthority,
    now: datetime,
) -> Path:
    authority.require_request(now)
    destination = revocation_path(authority)
    payload = canonical_json_bytes(
        {
            "authority_id": authority.authority_id,
            "contract": REVOCATION_CONTRACT,
            "payload_sha256": authority.payload_sha256,
            "revoked_at_utc": now.astimezone(UTC).isoformat().replace("+00:00", "Z"),
            "version": 1,
        }
    )
    _ = publish_bundle([(destination, payload)])
    return destination
