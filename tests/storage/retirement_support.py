"""Synthetic source tables, retirement documents and backups for retirement tests.

Every row is made up. ``commit`` stores rows as one content-addressed, linked source
table named ``bars``; a different ``tag`` gives a different original file, so the same
rows under two tags are two sources holding the same content, the shape the retirement
proof compares.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Final

import pyarrow as pa

from aegis_alpha.storage import source_library, source_retirement
from aegis_alpha.storage.raw import put_raw
from aegis_alpha.storage.source_identity import SourceContent, SourceFile

if TYPE_CHECKING:
    from collections.abc import Sequence

    import pytest

    from aegis_alpha.storage.workspace import Workspace

COLUMNS: Final = ("symbol", "day", "close", "volume", "seen", "ok")
SCHEMA: Final = pa.schema(
    [
        ("symbol", pa.string()),
        ("day", pa.date32()),
        ("close", pa.float64()),
        ("volume", pa.int64()),
        ("seen", pa.timestamp("us", tz="UTC")),
        ("ok", pa.bool_()),
        ("path", pa.string()),
    ]
)
SEEN: Final = datetime(2025, 1, 10, 8, 30, tzinfo=UTC)
ROWS: Final = (
    ("AAA", date(2025, 1, 2), 101.25, 1000, SEEN, True),
    ("AAA", date(2025, 1, 3), -0.0, 0, SEEN, False),
    ("BBB", date(2025, 1, 2), 7.5, -42, None, None),
    ("가나", date(2025, 1, 3), None, None, SEEN, True),
    ("AAA", date(2025, 1, 2), 101.25, 1000, SEEN, True),
)


def commit(
    workspace: Workspace,
    tag: str,
    rows: Sequence[tuple[object, ...]] = ROWS,
    *,
    path: str = "export",
    schema: pa.Schema = SCHEMA,
) -> str:
    """Commit ``rows`` (plus a ``path`` column) as source table ``bars``; return its ID."""
    _, digest, size = put_raw(workspace.paths.raw, f"synthetic-original-{tag}".encode())
    content = SourceContent("synthetic", "bars", 1, (SourceFile(digest, size),))
    names = schema.names
    columns: dict[str, list[object]] = {name: [] for name in names}
    for number, row in enumerate(rows):
        values = (*row, f"{path}/{number}") if "path" in names else row
        for name, value in zip(names, values, strict=True):
            columns[name].append(value)
    table = pa.table(columns, schema=schema)
    source_library.import_content_arrow(workspace, content, "bars", table.to_reader())
    return content.source_id


def group(
    retire: Sequence[str],
    equivalent: Sequence[str],
    *,
    columns: Sequence[str] = COLUMNS,
    equivalent_columns: Sequence[str] | None = None,
    reason: str = "a copy of the export",
) -> dict[str, object]:
    return {
        "reason": reason,
        "retire": {"sources": list(retire), "table": "bars", "columns": list(columns)},
        "equivalent": {
            "sources": list(equivalent),
            "table": "bars",
            "columns": list(equivalent_columns or columns),
        },
    }


def document(*groups: dict[str, object]) -> tuple[bytes, str]:
    raw = json.dumps(
        {"schema_version": "aas-source-retirement-v1", "groups": list(groups)}, sort_keys=True
    ).encode()
    return raw, hashlib.sha256(raw).hexdigest()


def spec(*groups: dict[str, object]) -> source_retirement.RetirementSpec:
    raw, digest = document(*groups)
    return source_retirement.parse_spec(raw, digest)


def other_device(monkeypatch: pytest.MonkeyPatch, backup_root: Path) -> None:
    """Report ``backup_root`` as another device than every installation path."""
    original = source_retirement.device_of

    def device(path: Path) -> int:
        return -1 if Path(path).is_relative_to(backup_root) else original(path)

    monkeypatch.setattr(source_retirement, "device_of", device)
