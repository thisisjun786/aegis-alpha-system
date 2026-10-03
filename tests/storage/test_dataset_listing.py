"""``aas data datasets`` lists each committed dataset under the catalog entry it belongs to."""

from __future__ import annotations

import json
from pathlib import Path

from aegis_alpha.storage import publication
from aegis_alpha.storage.dataset_catalog import list_datasets
from aegis_alpha.storage.import_document import parse_import
from aegis_alpha.storage.workspace import initialize, open_workspace


def _document(dataset_id: str, generation: str) -> bytes:
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
        "supersedes_revision_id": None,
        "op": "ASSERT",
        "available_at_us": 20,
        "revision_known_at_us": 20,
        "ingested_at_us": 30,
    }
    return json.dumps(
        {
            "schema_version": "aas-market-import-v1",
            "dataset_id": dataset_id,
            "version": "1",
            "generation_id": generation,
            "operation_id": "import-" + generation,
            "parent_id": None,
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
        for dataset_id, generation in (
            ("prices.kr.eodhd", "g-kr"),
            ("prices.kr.eodhd.r2", "g-kr-r2"),
            ("synthetic-prices", "g-synthetic"),
        ):
            published = publication.publish_document(
                workspace, parse_import(_document(dataset_id, generation))
            )
            assert published["published"] is True
    with open_workspace(home) as workspace:
        # Through JSON, as the command prints it.
        listed = json.loads(json.dumps(list_datasets(workspace.state)))
    assert [row["dataset_id"] for row in listed["datasets"]] == [
        "prices.kr.eodhd",
        "prices.kr.eodhd.r2",
        "synthetic-prices",
    ]
    # A name outside the catalog is still published and read; it is only named.
    assert listed["uncataloged"] == ["synthetic-prices"]
    by_id = {item["dataset_id"]: item for item in listed["catalog"]}
    kr = by_id["prices.kr.eodhd"]["published"]
    assert [item["dataset_id"] for item in kr] == ["prices.kr.eodhd", "prices.kr.eodhd.r2"]
    assert kr[0]["versions"] == 1
    assert kr[0]["head"] == {
        "version": "1",
        "generation_id": "g-kr",
        "chain_hash": listed["datasets"][0]["chain_hash"],
        "row_count": 1,
    }
    assert all(
        item["published"] == []
        for dataset_id, item in by_id.items()
        if dataset_id != "prices.kr.eodhd"
    )
