"""``aas-legacy-import-v1``: the hashed manifest that names legacy originals to import.

The manifest is strict UTF-8 JSON of at most 1 MiB with no unknown, missing or duplicate
keys::

    {
      "schema_version": "aas-legacy-import-v1",
      "entries": [
        {
          "name": "norgate-equity-none",
          "loader": "norgate.history_export@1",
          "path": "/absolute/path/to/an/export",
          "args": {},
          "expect": {"records": 35833, "rows": 75636899}
        }
      ]
    }

``name`` is a unique lowercase label that only orders and reports entries; it never enters
a source ID. ``loader`` is a registered ``name@major`` and fixes how ``path`` is read, which
original files form one unit and the output columns. ``args`` holds only the arguments that
loader defines. ``expect`` maps the loader's reconciliation metrics to the counts the
operator expects; the report states each observed value beside it. A path is absolute and
names the original bytes where they lie; it is never copied into an ID or a stored value.
"""

from __future__ import annotations

# ruff: noqa: TRY004 -- untrusted legacy bytes raise one ingress error type, ValueError.
import hashlib
import re
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Final

from aegis_alpha.engine.codec import decode_json

MANIFEST_SCHEMA: Final = "aas-legacy-import-v1"
MAX_MANIFEST_BYTES: Final = 1024 * 1024
_ROOT: Final = frozenset({"schema_version", "entries"})
_ENTRY: Final = frozenset({"name", "loader", "path", "args", "expect"})
_NAME: Final = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*")
_METRIC: Final = re.compile(r"[a-z][a-z0-9_]*")
_MAX_NAME: Final = 120


@dataclass(frozen=True, slots=True)
class Entry:
    """One manifest entry: a loader applied to one original location."""

    name: str
    loader: str
    path: Path
    args: dict[str, object]
    expect: dict[str, int]


@dataclass(frozen=True, slots=True)
class Manifest:
    sha256: str
    raw: bytes
    entries: tuple[Entry, ...]


def relative_name(value: object, name: str) -> PurePosixPath:
    """A relative path below an entry's root: no absolute part, ``..``, or empty segment."""
    if not isinstance(value, str) or not value or "\x00" in value or "\\" in value:
        raise ValueError(f"legacy manifest {name} must be a relative path")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in value.split("/")):
        raise ValueError(f"legacy manifest {name} must be a relative path below the entry")
    return path


def _entry(value: object, index: int) -> Entry:  # noqa: C901 -- one check per manifest field
    label = f"entry {index}"
    if not isinstance(value, dict) or set(value) != _ENTRY:
        raise ValueError(f"legacy manifest {label} needs exactly {sorted(_ENTRY)}")
    name = value["name"]
    if not isinstance(name, str) or len(name) > _MAX_NAME or _NAME.fullmatch(name) is None:
        raise ValueError(f"legacy manifest {label} name must be lowercase words and hyphens")
    loader = value["loader"]
    if not isinstance(loader, str) or not loader:
        raise ValueError(f"legacy manifest {label} loader must be name@major")
    path = value["path"]
    if not isinstance(path, str) or "\x00" in path or not path.startswith("/"):
        raise ValueError(f"legacy manifest {label} path must be absolute")
    if Path(path).as_posix() != path or any(part in {".", ".."} for part in path.split("/")):
        raise ValueError(f"legacy manifest {label} path must be normalized")
    args = value["args"]
    if not isinstance(args, dict):
        raise ValueError(f"legacy manifest {label} args must be an object")
    expect = value["expect"]
    if not isinstance(expect, dict):
        raise ValueError(f"legacy manifest {label} expect must be an object")
    for metric, count in expect.items():
        if _METRIC.fullmatch(metric) is None:
            raise ValueError(f"legacy manifest {label} expect has an invalid metric name")
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise ValueError(f"legacy manifest {label} expected counts are nonnegative integers")
    return Entry(name, loader, Path(path), dict(args), dict(expect))


def parse_manifest(raw: bytes, sha256: str) -> Manifest:
    """Parse exact manifest bytes whose SHA-256 the caller states."""
    if len(raw) > MAX_MANIFEST_BYTES:
        raise ValueError("legacy manifest exceeds 1 MiB")
    if hashlib.sha256(raw).hexdigest() != sha256:
        raise ValueError("legacy manifest bytes do not match the expected SHA-256")
    if raw.startswith(b"\xef\xbb\xbf") or b"\x00" in raw:
        raise ValueError("legacy manifest must be UTF-8 JSON without BOM or NUL")
    try:
        raw.decode("utf-8", errors="strict")
        document = decode_json(raw)
    except (UnicodeDecodeError, ValueError, RecursionError) as error:
        raise ValueError(f"legacy manifest is not strict JSON: {error}") from None
    if not isinstance(document, dict) or set(document) != _ROOT:
        raise ValueError(f"legacy manifest needs exactly {sorted(_ROOT)}")
    if document["schema_version"] != MANIFEST_SCHEMA:
        raise ValueError("unsupported legacy manifest schema")
    values = document["entries"]
    if not isinstance(values, list) or not values:
        raise ValueError("legacy manifest needs at least one entry")
    entries = tuple(_entry(value, index) for index, value in enumerate(values))
    if len({entry.name for entry in entries}) != len(entries):
        raise ValueError("legacy manifest entry names must be unique")
    return Manifest(sha256, raw, entries)
