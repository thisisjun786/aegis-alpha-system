"""``aas data datasets`` lists each committed dataset under the catalog entry it belongs to."""

from __future__ import annotations

import json
from pathlib import Path

from aegis_alpha.storage import publication
from aegis_alpha.storage.dataset_catalog import list_datasets
from aegis_alpha.storage.import_document import parse_import
from aegis_alpha.storage.workspace import initialize, open_workspace


def _document(dataset_id: str, generation: str, parent: tuple[str, str] | None = None) -> bytes:
    """One ``ASSET_A`` bar; with ``parent`` (its version, generation) the next version."""
    version = "1" if parent is None else str(int(parent[0]) + 1)
    row = {
        "instrument_id": "ASSET_A",
        "session_date": "2026-01-02",
        "interval": "1d",
        "bar_end_us": 20,
        "basis": "unadjusted",
        "currency": "KRW",
        "open": "10",
        "high": "12",
        "low": "9",
        "close": "11",
        "volume": "100",
        "price_role": "canonical",
        "value_state": "present",
        "revision_id": "r-" + generation,
        "supersedes_revision_id": None if parent is None else "r-" + parent[1],
        "op": "ASSERT" if parent is None else "SUPERSEDE",
        "available_at_us": 20 * int(version),
        "revision_known_at_us": 20 * int(version),
        "ingested_at_us": 30,
    }
    return json.dumps(
        {
            "schema_version": "aas-market-import-v1",
            "dataset_id": dataset_id,
            "version": version,
            "generation_id": generation,
            "operation_id": "import-" + generation,
            "parent_id": None if parent is None else parent[1],
            "domain": "prices",
            "provider": "synthetic",
            "publication_at_us": 10,
            "normalizer_version": "synthetic-v1",
            "transform_sha256": "a" * 64,
            "instruments": [
                {"instrument_id": "ASSET_A", "asset_type": "equity", "venue": "SYNTHETIC"}
            ],
            "rows": [row],
        }
    ).encode()


def test_published_datasets_are_listed_under_their_entry(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    initialize(home)
    with open_workspace(home, writable=True) as workspace:
        for dataset_id, generation, parent in (
            ("prices.kr.eodhd", "g-kr", None),
            ("prices.kr.eodhd", "g-kr-2", ("1", "g-kr")),
            ("prices.kr.eodhd.r2", "g-kr-r2", None),
            ("synthetic-prices", "g-synthetic", None),
        ):
            published = publication.publish_document(
                workspace, parse_import(_document(dataset_id, generation, parent))
            )
            assert published["published"] is True
    with open_workspace(home) as workspace:
        # Through JSON, as the command prints it.
        listed = json.loads(json.dumps(list_datasets(workspace.state)))
    assert [(row["dataset_id"], row["version"]) for row in listed["datasets"]] == [
        ("prices.kr.eodhd", "1"),
        ("prices.kr.eodhd", "2"),
        ("prices.kr.eodhd.r2", "1"),
        ("synthetic-prices", "1"),
    ]
    # A name outside the catalog is still published and read; it is only named.
    assert listed["uncataloged"] == ["synthetic-prices"]
    by_id = {item["dataset_id"]: item for item in listed["catalog"]}
    kr = by_id["prices.kr.eodhd"]["published"]
    assert [item["dataset_id"] for item in kr] == ["prices.kr.eodhd", "prices.kr.eodhd.r2"]
    # The head is the newest committed version of the chain.
    assert [item["versions"] for item in kr] == [2, 1]
    assert kr[0]["head"] == {
        "version": "2",
        "generation_id": "g-kr-2",
        "chain_hash": listed["datasets"][1]["chain_hash"],
        "row_count": 1,
    }
    assert listed["datasets"][1]["chain_hash"] != listed["datasets"][0]["chain_hash"]
    assert kr[1]["head"]["generation_id"] == "g-kr-r2"
    assert all(
        item["published"] == []
        for dataset_id, item in by_id.items()
        if dataset_id != "prices.kr.eodhd"
    )
