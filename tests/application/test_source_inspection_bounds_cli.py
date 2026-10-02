"""The actual CLI emits recursive scalar representations or bounded errors."""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest

from aegis_alpha.storage.source_library import import_arrow, list_tables, verify_sources
from aegis_alpha.storage.workspace import initialize, open_workspace
from tests.application.test_storage_cli import run_cli
from tests.storage.test_source_inspection_bounds import import_large_cell

_MAX_ERROR_BYTES = 1024


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        (pa.array([[b"a"]]), '[{"base64":"YQ=="}]'),
        (pa.array([[Decimal("1.20")]]), '["1.20"]'),
        (pa.array([[0]], type=pa.list_(pa.date32())), '["1970-01-01"]'),
        (pa.array([[float("nan")]]), '[{"float":"nan"}]'),
    ],
)
def test_emits_json_when_list_leaves_need_scalar_conversion(
    tmp_path: Path,
    values: pa.Array,
    expected: str,
) -> None:
    # Given an admitted list whose leaf is not a strict JSON scalar.
    home = tmp_path / "native"
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        import_arrow(
            workspace,
            "synthetic",
            "0" * 64,
            "cells",
            pa.table({"value": values}).to_reader(),
        )
    # When the real CLI serializes the source-read response.
    result = run_cli(
        "db",
        "source-read",
        "--source",
        "synthetic",
        "--table",
        "cells",
        home=home,
    )
    # Then strict JSON contains the same scalar representations inside the list.
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["rows"] == [{"value": json.loads(expected)}]


def test_serializes_admitted_lists_when_cells_contain_special_scalars(tmp_path: Path) -> None:
    # Given admitted lists (including nested/large lists) with non-JSON scalar leaves.
    home = tmp_path / "native"
    initialize(home)
    table = pa.table(
        {
            "binary": pa.array([[b"\x00\xff", None]], type=pa.list_(pa.binary())),
            "decimal": pa.array([[Decimal("1.20"), None]], type=pa.list_(pa.decimal128(4, 2))),
            "date": pa.array([[0, None]], type=pa.list_(pa.date32())),
            "time": pa.array([[123, None]], type=pa.list_(pa.time64("ns"))),
            "timestamp": pa.array([[123, None]], type=pa.list_(pa.timestamp("ns", "UTC"))),
            "float": pa.array([[float("inf"), float("-inf"), float("nan"), 1.5, None]]),
            "nested": pa.array([[[b"a"], None, []]], type=pa.large_list(pa.list_(pa.binary()))),
        }
    )
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        import_arrow(workspace, "synthetic", "0" * 64, "cells", table.to_reader())
        before = list_tables(workspace, "synthetic")
    # When source-read executes through its real process/JSON surface.
    result = run_cli("db", "source-read", "--source", "synthetic", "--table", "cells", home=home)
    # Then recursive representations preserve scalar precision, nulls and list structure.
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["rows"] == [
        {
            "binary": [{"base64": "AP8="}, None],
            "decimal": ["1.20", None],
            "date": ["1970-01-01", None],
            "time": ["00:00:00.000000123", None],
            "timestamp": ["1970-01-01 00:00:00.000000123Z", None],
            "float": [{"float": "inf"}, {"float": "-inf"}, {"float": "nan"}, 1.5, None],
            "nested": [[{"base64": "YQ=="}], None, []],
        }
    ]
    with open_workspace(home, require_strategies=True) as workspace:
        assert list_tables(workspace, "synthetic") == before
        assert verify_sources(workspace) == {"sources": 1, "tables": 1, "rows": 1}


@pytest.mark.parametrize("kind", ["sqlite", "binary", "string", "list", "nested"])
def test_returns_bounded_error_when_one_cell_exceeds_budget(tmp_path: Path, kind: str) -> None:
    # Given a real imported source, not a mocked reader or serializer.
    home = tmp_path / "native"
    import_large_cell(home, kind)
    # When a row-count-bounded inspection encounters a byte-oversized cell.
    result = run_cli(
        "db",
        "source-read",
        "--source",
        "synthetic",
        "--table",
        "cells",
        "--limit",
        "1",
        home=home,
    )
    # Then no partial/giant stdout is emitted, and the CLI returns a small JSON error.
    assert result.returncode == 1, result.stderr
    assert result.stdout == ""
    assert len(result.stderr.encode()) < _MAX_ERROR_BYTES
    assert "byte budget" in json.loads(result.stderr)["error"]
