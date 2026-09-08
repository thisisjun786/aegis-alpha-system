from __future__ import annotations

import hashlib
import os
import re
import tempfile
from pathlib import Path

from aegis_alpha.data.contracts import SourceSnapshot
from aegis_alpha.data.serialization import canonical_json_bytes

_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


class ContentAddressedRawStore:
    """Append-only raw byte store addressed by SHA-256."""

    def __init__(self, root: Path) -> None:
        self._root = root

    def capture(self, snapshot: SourceSnapshot, payload: bytes) -> Path:
        actual_length = len(payload)
        if actual_length != snapshot.raw_byte_length:
            raise ValueError(
                "raw byte length mismatch: "
                f"declared={snapshot.raw_byte_length}, actual={actual_length}"
            )

        actual_sha256 = hashlib.sha256(payload).hexdigest()
        if actual_sha256 != snapshot.content_sha256:
            raise ValueError("content SHA-256 mismatch")

        if _SAFE_ID.fullmatch(snapshot.snapshot_id) is None:
            raise ValueError("snapshot_id contains unsafe path characters")
        provenance = canonical_json_bytes(snapshot)
        provenance_path = self._root / "snapshots" / f"{snapshot.snapshot_id}.json"
        if provenance_path.exists() and provenance_path.read_bytes() != provenance:
            raise ValueError("snapshot ID already has different provenance")

        destination = self.capture_payload(payload)
        self._write_provenance(provenance_path, provenance)
        return destination

    def capture_payload(self, payload: bytes) -> Path:
        """Durably retain bytes before parser-dependent provenance is known."""

        actual_sha256 = hashlib.sha256(payload).hexdigest()
        destination = self._root / "blobs" / "sha256" / actual_sha256[:2] / f"{actual_sha256}.raw"
        self._publish_immutable(
            destination,
            payload,
            mismatch_message="existing content-addressed blob does not match its digest",
        )
        return destination

    @staticmethod
    def _write_provenance(destination: Path, provenance: bytes) -> None:
        ContentAddressedRawStore._publish_immutable(
            destination,
            provenance,
            mismatch_message="snapshot ID already has different provenance",
        )

    @staticmethod
    def _publish_immutable(destination: Path, content: bytes, *, mismatch_message: str) -> None:
        if destination.exists():
            if destination.read_bytes() != content:
                raise ValueError(mismatch_message)
            return

        destination.parent.mkdir(parents=True, exist_ok=True)
        file_descriptor, temporary_name = tempfile.mkstemp(
            dir=destination.parent,
            prefix=f".{destination.name}.",
            suffix=".tmp",
        )
        temporary_path = Path(temporary_name)
        published = False
        try:
            with os.fdopen(file_descriptor, "wb") as temporary_file:
                temporary_file.write(content)
                temporary_file.flush()
                os.fsync(temporary_file.fileno())
            temporary_path.chmod(0o444)
            try:
                os.link(temporary_path, destination)
                published = True
            except FileExistsError:
                if destination.read_bytes() != content:
                    raise ValueError(mismatch_message) from None
        finally:
            temporary_path.unlink(missing_ok=True)

        if published:
            directory_descriptor = os.open(destination.parent, os.O_RDONLY)
            try:
                os.fsync(directory_descriptor)
            finally:
                os.close(directory_descriptor)
