from __future__ import annotations

import json
from decimal import Decimal
from fractions import Fraction
from pathlib import Path

import pytest

from aegis_alpha.compute_resources import ComputeBudget
from aegis_alpha.storage import market, publication
from aegis_alpha.storage.import_document import parse_import
from aegis_alpha.storage.workspace import initialize, open_workspace


def document() -> bytes:
    return json.dumps(
        {
            "schema_version": "aas-market-import-v1",
            "dataset_id": "synthetic-prices",
            "version": "1",
            "generation_id": "synthetic-generation",
            "operation_id": "synthetic-import",
            "parent_id": None,
            "domain": "prices",
            "provider": "synthetic",
            "publication_at_us": 10,
            "normalizer_version": "synthetic-v1",
            "transform_sha256": "a" * 64,
            "instruments": [
                {"instrument_id": "ASSET_A", "asset_type": "equity", "venue": "SYNTHETIC"}
            ],
            "rows": [
                {
                    "instrument_id": "ASSET_A",
                    "session_date": "2026-01-02",
                    "interval": "1d",
                    "bar_end_us": 20,
                    "basis": "unadjusted",
                    "currency": "USD",
                    "open": "10",
                    "high": "12",
                    "low": "9",
                    "close": "11",
                    "volume": "100",
                    "price_role": "canonical",
                    "value_state": "present",
                    "revision_id": "r1",
                    "supersedes_revision_id": None,
                    "op": "ASSERT",
                    "available_at_us": 20,
                    "revision_known_at_us": 20,
                    "ingested_at_us": 30,
                }
            ],
        }
    ).encode()


def test_publish_restart_and_duplicate(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    initialize(home)
    parsed = parse_import(document())
    with open_workspace(home, writable=True) as workspace:
        result = publication.publish_document(workspace, parsed)
        assert result["published"] is True
        assert publication.publish_document(workspace, parsed) == result
    with open_workspace(home) as workspace:
        dataset = publication.read_dataset(workspace, "synthetic-prices", "1")
        assert dataset["generation_id"] == "synthetic-generation"
        assert dataset["row_count"] == 1


def test_crash_after_market_commit_not_visible_then_recovered(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    home = tmp_path / "aas"
    initialize(home)
    complete = publication._complete_publication  # noqa: SLF001 -- inject failure at cross-DB boundary

    def crash(*_args: object) -> None:
        raise RuntimeError("synthetic publication crash")

    with open_workspace(home, writable=True) as workspace:
        monkeypatch.setattr(publication, "_complete_publication", crash)
        with pytest.raises(RuntimeError, match="synthetic"):
            publication.publish_document(workspace, parse_import(document()))
        with pytest.raises(ValueError, match="not published"):
            publication.read_dataset(workspace, "synthetic-prices", "1")
    monkeypatch.setattr(publication, "_complete_publication", complete)
    with open_workspace(home, writable=True) as workspace:
        report = publication.recover_operations(workspace)
        assert report["recovered"] == ["synthetic-import"]
        assert report["provider_calls"] == 0
        assert publication.recover_operations(workspace)["recovered"] == []
        assert publication.read_dataset(workspace, "synthetic-prices", "1")["row_count"] == 1


def test_invalid_revision_rejected_before_intent_and_source(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    initialize(home)
    with open_workspace(home, writable=True) as workspace:
        publication.publish_document(workspace, parse_import(document()))
        body = json.loads(document())
        body.update(
            version="2", generation_id="g2", operation_id="op2", parent_id="synthetic-generation"
        )
        body["rows"][0]["revision_id"] = "r2"
        with pytest.raises(ValueError, match="ASSERT"):
            publication.publish_document(workspace, parse_import(json.dumps(body).encode()))
        assert workspace.state.execute("SELECT count(*) FROM storage_operations").fetchone()[0] == 1
        assert workspace.state.execute("SELECT count(*) FROM source_snapshots").fetchone()[0] == 1
        body["rows"][0].update(
            op="SUPERSEDE", supersedes_revision_id="r1", available_at_us=40, revision_known_at_us=40
        )
        assert publication.publish_document(workspace, parse_import(json.dumps(body).encode()))[
            "published"
        ]


def test_actual_ingestion_timestamp_cannot_be_backdated(tmp_path: Path) -> None:
    import time  # noqa: PLC0415 -- independently observe time before import

    from aegis_alpha.storage.market import read_generation  # noqa: PLC0415

    home = tmp_path / "aas"
    initialize(home)
    with open_workspace(home, writable=True) as workspace:
        before = time.time_ns() // 1000
        publication.publish_document(workspace, parse_import(document()))
        row = read_generation(workspace.market, "synthetic-generation")[0]
        assert int(str(row["ingested_at_us"])) >= before
        assert (
            read_generation(workspace.market, "synthetic-generation", ingestion_cutoff_us=35) == []
        )


def test_markerless_operation_can_be_quarantined_without_delete(tmp_path: Path) -> None:
    from aegis_alpha.storage.state import prepare_operation  # noqa: PLC0415

    home = tmp_path / "aas"
    initialize(home)
    with open_workspace(home, writable=True) as workspace:
        prepare_operation(
            workspace.state,
            operation_id="interrupted",
            kind="market_publish",
            request_hash="a" * 64,
            target_id="g",
            expected_parent=None,
            payload_hash="a" * 64,
        )
        assert publication.recover_operations(workspace)["pending"] == ["interrupted"]
        assert (
            publication.quarantine(workspace, "interrupted", "source unavailable")["deleted"]
            is False
        )
        assert publication.recover_operations(workspace)["pending"] == []
        assert (
            workspace.state.execute("SELECT phase FROM storage_operations").fetchone()[0]
            == "QUARANTINED"
        )


def test_full_chain_count_is_not_head_catalog_delta_count(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    initialize(home)
    base = json.loads(document())
    base["rows"].append({**base["rows"][0], "session_date": "2026-01-03", "close": "21"})
    child = {
        **base,
        "version": "2",
        "generation_id": "g2",
        "operation_id": "op2",
        "parent_id": "synthetic-generation",
        "rows": [
            {
                **base["rows"][0],
                "revision_id": "r2",
                "op": "SUPERSEDE",
                "supersedes_revision_id": "r1",
                "close": "12",
                "available_at_us": 40,
                "revision_known_at_us": 40,
            }
        ],
    }
    with open_workspace(home, writable=True) as workspace:
        publication.publish_document(workspace, parse_import(json.dumps(base).encode()))
        publication.publish_document(workspace, parse_import(json.dumps(child).encode()))
    with open_workspace(home) as workspace:
        assert {
            version: publication.read_dataset(workspace, "synthetic-prices", version)["row_count"]
            for version in ("1", "2")
        } == {"1": 2, "2": 1}
        rows = market.read_chain_rows(
            workspace.market, "g2", budget=ComputeBudget(Fraction(1), 64 * 1024 * 1024)
        )
    # The detached immutable history can be projected after workspace admission closes.
    expected_chain_count = 3
    assert len(rows) == expected_chain_count
    assert {row["close"] for row in market.project_heads(rows, cutoff_us=30)} == {
        Decimal(11),
        Decimal(21),
    }
    assert {row["close"] for row in market.project_heads(rows, cutoff_us=40)} == {
        Decimal(12),
        Decimal(21),
    }
