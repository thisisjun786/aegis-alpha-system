"""The install receipt of the running ``aas``: interpreter, package revision, environment.

The installed tool (``uv tool install --python <exact CPython> 'aegis-alpha-system[legacy]
@ git+<repository>@<tag>'``) records ``aas-install-receipt-v1`` once after installation:

- ``interpreter``: the resolved executable path, ``sys.version``, the version tuple and the
  implementation, so a floating minor-version link cannot hide which Python ran;
- ``package``: the distribution's version, its PEP 610 ``direct_url.json`` (the repository,
  the requested tag and the resolved commit of a VCS install) and the SHA-256 of its
  ``RECORD`` (every installed file's hash);
- ``environment``: the SHA-256 of the sorted ``name==version`` list of every distribution
  the interpreter imports from, and its length;
- ``lock_sha256``: the SHA-256 of the ``uv.lock`` the operator installed from, when given.

The receipt is written to ``<runtime>/install-receipt.json`` and its exact bytes to
``raw/``. Each ``aas maintain`` run records the running interpreter and package beside the
receipt's and lists the fields that differ. A difference is reported, never refused: the
run is the record of what ran.
"""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
import platform
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Final, cast

from aegis_alpha.data.serialization import canonical_json_bytes

if TYPE_CHECKING:
    from aegis_alpha.storage.paths import StoragePaths

RECEIPT_SCHEMA: Final = "aas-install-receipt-v1"
RECEIPT_NAME: Final = "install-receipt.json"
DISTRIBUTION: Final = "aegis-alpha-system"
_MAX_LOCK_BYTES: Final = 64 * 1024 * 1024


def _distribution_record() -> dict[str, object]:
    try:
        found = importlib.metadata.distribution(DISTRIBUTION)
    except importlib.metadata.PackageNotFoundError:
        return {"version": None, "direct_url": None, "record_sha256": None}
    direct = found.read_text("direct_url.json")
    record = found.read_text("RECORD")
    return {
        "version": found.version,
        "direct_url": None if direct is None else json.loads(direct),
        "record_sha256": None if record is None else hashlib.sha256(record.encode()).hexdigest(),
    }


def _environment() -> dict[str, object]:
    names = sorted(
        {
            f"{(item.metadata['Name'] or '').lower()}=={item.version}"
            for item in importlib.metadata.distributions()
        }
    )
    return {"distributions": len(names), "sha256": hashlib.sha256("\n".join(names).encode())
            .hexdigest()}  # fmt: skip


def current_runtime() -> dict[str, object]:
    """What is running now, in the receipt's shape (without the lock)."""
    return {
        "interpreter": {
            "executable": os.path.realpath(sys.executable),
            "version": sys.version,
            "version_info": list(sys.version_info[:3]),
            "implementation": platform.python_implementation(),
        },
        "package": _distribution_record(),
        "environment": _environment(),
    }


def build_receipt(*, lock: Path | None, now: datetime | None = None) -> bytes:
    from aegis_alpha.data.sec_evidence import read_bytes  # noqa: PLC0415

    moment = now or datetime.now(UTC)
    document: dict[str, object] = {
        "schema": RECEIPT_SCHEMA,
        **current_runtime(),
        "lock_sha256": None
        if lock is None
        else hashlib.sha256(read_bytes(lock.absolute(), maximum=_MAX_LOCK_BYTES)).hexdigest(),
        "recorded_at_utc": moment.astimezone(UTC).isoformat(),
    }
    return canonical_json_bytes(document)


def write_receipt(paths: StoragePaths, *, lock: Path | None) -> dict[str, object]:
    """Record the running installation's receipt in ``runtime/`` and ``raw/``."""
    from aegis_alpha.data.descriptor_tree import DescriptorTree  # noqa: PLC0415
    from aegis_alpha.storage.locks import private_directory  # noqa: PLC0415
    from aegis_alpha.storage.raw import put_raw  # noqa: PLC0415

    raw = build_receipt(lock=lock)
    private_directory(paths.runtime, create=True)
    with DescriptorTree.open_path(paths.runtime) as tree:
        tree.atomic_write_bytes(RECEIPT_NAME, raw)
    _, digest, _ = put_raw(paths.raw, raw)
    return {"receipt_sha256": digest, "path": str(paths.runtime / RECEIPT_NAME),
            **json.loads(raw)}  # fmt: skip


def read_receipt(paths: StoragePaths) -> tuple[str, dict[str, object]] | None:
    from aegis_alpha.data.descriptor_tree import DescriptorTree  # noqa: PLC0415

    if not (paths.runtime / RECEIPT_NAME).exists():
        return None
    with DescriptorTree.open_path(paths.runtime) as tree:
        raw = tree.read_bytes(RECEIPT_NAME, max_bytes=1024 * 1024)
    document = json.loads(raw)
    if not isinstance(document, dict) or document.get("schema") != RECEIPT_SCHEMA:
        raise ValueError("the install receipt is not an aas-install-receipt-v1 document")
    return hashlib.sha256(raw).hexdigest(), cast("dict[str, object]", document)


def _differences(recorded: object, running: object, prefix: str) -> list[str]:
    if isinstance(recorded, dict) and isinstance(running, dict):
        keys = sorted(set(recorded) | set(running))
        return [
            item
            for key in keys
            for item in _differences(recorded.get(key), running.get(key), f"{prefix}{key}.")
        ]
    return [] if recorded == running else [prefix.rstrip(".")]


def runtime_report(paths: StoragePaths) -> dict[str, object]:
    """The running interpreter and package, and how they differ from the install receipt."""
    running = current_runtime()
    found = read_receipt(paths)
    if found is None:
        return {"receipt_sha256": None, "running": running, "differences": None}
    digest, recorded = found
    compared = {key: recorded.get(key) for key in ("interpreter", "package", "environment")}
    return {
        "receipt_sha256": digest,
        "running": running,
        "differences": _differences(compared, running, ""),
    }
