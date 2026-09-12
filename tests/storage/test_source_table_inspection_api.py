from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pyarrow as pa
import pytest

from aegis_alpha.storage.source_library import import_arrow, read_table
from aegis_alpha.storage.workspace import initialize, open_workspace
from tests.storage.test_source_inspection_bounds import import_large_cell

if TYPE_CHECKING:
    from pathlib import Path


def test_table_api_returns_json_safe_list_values(tmp_path: Path) -> None:
    home = tmp_path / "native"
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        import_arrow(
            workspace,
            "synthetic",
            "0" * 64,
            "cells",
            pa.table({"value": [[b"a", None]]}).to_reader(),
        )
        result = read_table(workspace, "synthetic", "cells", 1)
        assert json.loads(json.dumps(result, allow_nan=False))["rows"] == [
            {"value": [{"base64": "YQ=="}, None]}
        ]


@pytest.mark.parametrize("kind", ["sqlite", "binary", "list"])
def test_table_api_rejects_oversized_inspection(tmp_path: Path, kind: str) -> None:
    home = tmp_path / "native"
    import_large_cell(home, kind)
    with (
        open_workspace(home, require_strategies=True) as workspace,
        pytest.raises(ValueError, match="byte budget"),
    ):
        read_table(workspace, "synthetic", "cells", 1)
