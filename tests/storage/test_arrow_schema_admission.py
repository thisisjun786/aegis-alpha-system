"""Arrow source schema admission precedes durable publication intent."""

from __future__ import annotations

import base64
import json
import subprocess
import sys
from decimal import Decimal
from pathlib import Path
from typing import Final

import pyarrow as pa
import pytest

from aegis_alpha.storage import source_library
from aegis_alpha.storage.source_library_digest import BATCH_ROWS, arrow_digest
from aegis_alpha.storage.workspace import initialize, open_workspace

_SUPPORTED_DIGEST: Final = "e0e3e36e2f8e6c4f0be525d7f8fb29422f76b0a773f6be8c31a34915d5540e27"
_BOUNDARY_DIGEST: Final = "570fc8cdb02871aa2ba9a0382f06c35a64380eac7a03196f2184f3c51fad01af"


@pytest.mark.parametrize("empty", [False, True])
@pytest.mark.parametrize(
    "array",
    [
        pytest.param(pa.array([None], type=pa.null()), id="null"),
        pytest.param(pa.array([{"n": 7}], type=pa.struct([("n", pa.int64())])), id="struct-valid"),
        pytest.param(pa.array([[("n", 7)]], type=pa.map_(pa.string(), pa.int64())), id="map-valid"),
        pytest.param(pa.array(["alpha"]).dictionary_encode(), id="dictionary-string-valid"),
        pytest.param(pa.array([7]).dictionary_encode(), id="dictionary-integer-valid"),
        pytest.param(pa.array([[7, 8]], type=pa.list_(pa.int64(), 2)), id="fixed-list-valid"),
        pytest.param(pa.array([b"ab"], type=pa.binary(2)), id="fixed-binary-valid"),
        pytest.param(pa.array([1.0], type=pa.float16()), id="half-float-valid"),
        pytest.param(pa.array([7], type=pa.duration("us")), id="duration-valid"),
        pytest.param(pa.array([7], type=pa.decimal256(10, 2)), id="decimal256-valid"),
        pytest.param(
            pa.array([["alpha"]], type=pa.list_(pa.dictionary(pa.int8(), pa.string()))),
            id="list-dictionary-valid",
        ),
    ],
)
def test_rejects_unsupported_schema_before_intent_when_values_are_valid(
    tmp_path: Path, array: pa.Array, *, empty: bool
) -> None:
    # Given real Arrow values and a newly admitted synthetic installation.
    home = tmp_path / "native"
    initialize(home)
    table = pa.table({"value": array.slice(0, 0) if empty else array})
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        before = workspace.market.execute("SHOW TABLES").fetchall()
        # When an unsupported schema crosses the public import boundary.
        with pytest.raises(ValueError, match="unsupported Arrow source type"):
            source_library.import_arrow(
                workspace, "unsupported", "a" * 64, "values", table.to_reader()
            )
        # Then neither durable intent nor source catalog/target artifacts exist.
        assert workspace.state.execute("SELECT * FROM storage_operations").fetchall() == []
        assert workspace.market.execute("SHOW TABLES").fetchall() == before
        assert source_library.verify_sources(workspace) is None
        assert source_library.list_sources(workspace) == []


def supported_table() -> pa.Table:
    """Synthetic retained scalar families, including nulls and original row order."""
    return pa.table(
        {
            "n": pa.array([7, 2, 9, None], type=pa.int32()),
            "flag": [True, False, None, True],
            "x": pa.array([1.25, -0.0, None, float("nan")], type=pa.float32()),
            "text": ["alpha", "", None, "omega"],
            "large_text": pa.array(["a", None, "c", "d"], type=pa.large_string()),
            "raw": pa.array([b"a", None, b"", b"z"], type=pa.binary()),
            "large_raw": pa.array([b"a", None, b"", b"z"], type=pa.large_binary()),
            "day": pa.array([1, None, 3, 4], type=pa.date32()),
            "day_ms": pa.array([86400000, None, 0, 172800000], type=pa.date64()),
            "clock": pa.array([123, None, 456, 789], type=pa.time64("ns")),
            "stamp": pa.array([123, None, 456, 789], type=pa.timestamp("ns")),
            "zoned": pa.array([123, None, 456, 789], type=pa.timestamp("ns", "UTC")),
            "price": pa.array(
                [Decimal("1.25"), None, Decimal("-2.50"), Decimal("0.00")],
                type=pa.decimal128(10, 2),
            ),
            "tags": pa.array([["a", None], None, [], ["b"]], type=pa.list_(pa.string())),
            "large_tags": pa.array([[7], None, [], [2]], type=pa.large_list(pa.int32())),
        }
    )


@pytest.mark.parametrize("partition", [1, 2, 100])
def test_retains_old_digest_when_supported_families_are_partitioned(partition: int) -> None:
    # Given the exact synthetic fixture hashed before schema admission changed.
    table = supported_table()
    # When the caller changes only input batch boundaries.
    observed = arrow_digest(table.to_reader(max_chunksize=partition))
    # Then the old canonical digest is byte-for-byte unchanged.
    assert observed == (table.num_rows, _SUPPORTED_DIGEST)


@pytest.mark.parametrize("partition", [BATCH_ROWS - 1, BATCH_ROWS + 1])
def test_retains_old_digest_when_input_crosses_canonical_boundary(partition: int) -> None:
    # Given ordered, non-null values spanning the canonical batch boundary.
    table = pa.table({"value": pa.array(range(BATCH_ROWS + 3), type=pa.int32())})
    # When incoming partitions straddle that boundary differently.
    observed = arrow_digest(table.to_reader(max_chunksize=partition))
    # Then both runs retain the digest recorded from the unmodified base.
    assert observed == (table.num_rows, _BOUNDARY_DIGEST)


@pytest.mark.parametrize("partition", [1, 2, 100])
def test_retains_manifest_when_supported_source_is_admitted(tmp_path: Path, partition: int) -> None:
    # Given supported scalar, nullable, temporal, decimal and variable-list values.
    home = tmp_path / "native"
    initialize(home)
    table = supported_table()
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        # When the real public importer publishes them without casts or replacements.
        source_library.import_arrow(
            workspace,
            "supported",
            "b" * 64,
            "values",
            table.to_reader(max_chunksize=partition),
        )
        # Then the original schema and old canonical digest remain the source identity.
        manifest = source_library.list_tables(workspace, "supported")[0]
        assert manifest["digest"] == _SUPPORTED_DIGEST
        assert (
            manifest["arrow_schema"]
            == base64.b64encode(table.schema.serialize().to_pybytes()).decode()
        )


def test_reads_original_order_and_values_when_opened_in_fresh_process(tmp_path: Path) -> None:
    # Given a supported source published through an admitted workspace and then closed.
    home = tmp_path / "native"
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        source_library.import_arrow(
            workspace, "supported", "b" * 64, "values", supported_table().to_reader()
        )
    # When a fresh interpreter opens the installation through its real read boundary.
    result = subprocess.run(  # noqa: S603 -- fixed interpreter/code, synthetic private home
        [
            sys.executable,
            "-c",
            (
                "import json,sys; from pathlib import Path; "
                "from aegis_alpha.storage import source_library as s; "
                "from aegis_alpha.storage.workspace import open_workspace; "
                "\nwith open_workspace(Path(sys.argv[1])) as w:\n"
                " print(json.dumps({'verification':s.verify_sources(w),"
                "'manifest':s.list_tables(w,'supported')[0],"
                "'data':s.read_table(w,'supported','values')}))"
            ),
            str(home),
        ],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    # Then persisted values, ordering and independently pinned digest survive readback.
    report = json.loads(result.stdout)
    assert report["verification"] == {"sources": 1, "tables": 1, "rows": 4}
    assert report["manifest"]["digest"] == _SUPPORTED_DIGEST
    rows = report["data"]["rows"]
    assert [row["n"] for row in rows] == [7, 2, 9, None]
    assert [row["price"] for row in rows] == ["1.25", None, "-2.50", "0.00"]
    assert rows[0]["stamp"] == "1970-01-01 00:00:00.000000123"
    assert rows[0]["clock"] == "00:00:00.000000123"
    assert rows[0]["tags"] == ["a", None]
    assert rows[0]["raw"] == {"base64": "YQ=="}
