from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Never, cast

import pytest

from aegis_alpha.console.catalog import Catalog
from aegis_alpha.storage.source_library import import_sqlite
from aegis_alpha.storage.workspace import initialize, open_workspace

_OBSERVATION_ROWS = 2


def _seed(home: Path, source: Path) -> None:
    with sqlite3.connect(source) as connection:
        connection.execute(
            "CREATE TABLE strategy_original (id TEXT, title TEXT, country TEXT, request_json TEXT, "
            "start_date TEXT, finish_date TEXT, source_data_basis TEXT, quality_status TEXT)"
        )
        connection.execute(
            "INSERT INTO strategy_original VALUES (?,?,?,?,?,?,?,?)",
            (
                "s-1",
                "Original strategy",
                "US",
                json.dumps(
                    {"offensive": ["SYNTH-A"], "defensive_rule": {"defensive": ["SYNTH-B"]}}
                ),
                "2020-01-01",
                "2024-01-01",
                "report",
                "stored",
            ),
        )
        connection.execute(
            "CREATE TABLE bars (instrument_id TEXT, venue TEXT, instrument_type TEXT, "
            "provider_symbol TEXT, date TEXT, close REAL)"
        )
        connection.executemany(
            "INSERT INTO bars VALUES (?,?,?,?,?,?)",
            [
                ("us-1", "NASDAQ", "Equity", "SYN", "2020-01-01", 1.0),
                ("us-1", "NASDAQ", "Equity", "SYN", "2020-01-02", 2.0),
            ],
        )
    source.chmod(0o600)
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        import_sqlite(
            workspace, source, "synthetic-research", hashlib.sha256(source.read_bytes()).hexdigest()
        )


def test_research_catalog_exposes_original_request_and_observation_ranges(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    source = tmp_path / "research.sqlite3"
    _seed(home, source)
    catalog = Catalog(home)

    strategies = catalog.strategies(collection=None, query="Original", country="US", offset=0)
    assert strategies["collection_id"] == "synthetic-research:strategy_original"
    assert strategies["total"] == 1
    record = cast("list[dict[str, object]]", strategies["records"])[0]
    assert record["assets"] == ["SYNTH-A"]
    assert record["defensive_assets"] == ["SYNTH-B"]
    assert record["data_basis"] == "report"

    coverage = catalog.coverage(query="SYN", category="us_equity", offset=0)
    assert coverage["total"] == 1
    assert coverage["items"] == [
        {
            "id": "us-1",
            "symbol": "SYN",
            "name": None,
            "category": "us_equity",
            "first_date": "2020-01-01",
            "last_date": "2020-01-02",
            "rows": _OBSERVATION_ROWS,
            "source_count": 1,
        }
    ]
    assert (
        next(
            group
            for group in cast("list[dict[str, object]]", coverage["groups"])
            if group["category"] == "us_equity"
        )["rows"]
        == _OBSERVATION_ROWS
    )


def test_coverage_joins_split_identity_dates_and_rejects_invalid_values(tmp_path: Path) -> None:
    import pyarrow as pa  # noqa: PLC0415

    from aegis_alpha.storage.source_library import import_arrow  # noqa: PLC0415

    home = tmp_path / "aas"
    initialize(home)
    identities = pa.table(
        {
            "assetid": [1, 2],
            "symbol": ["SYN-A", "SYN-B"],
            "security_name": ["Synthetic equity", "Synthetic ETF"],
            "database_or_watchlist": ["US Equities", "US Equities"],
            "exchange": ["NYSE", "NASDAQ"],
            "base_type": ["Stock Market"] * 2,
            "subtype1": ["Equity", "Exchange Traded Product"],
            "is_etf": [False, True],
            "source": ["norgate", "norgate"],
        }
    )
    raw = pa.table(
        {
            "assetid": [1, 2],
            "symbol": ["SYN-A", "SYN-B"],
            "date": [datetime(2020, 1, 1), datetime(2020, 1, 3)],  # noqa: DTZ001 -- exchange-local observation dates.
            "close": [1.0, 2.0],
            "source": ["norgate"] * 2,
        }
    )
    canonical = pa.table(
        {
            "instrument_id": ["norgate-instrument-1"] * 4,
            "observation_date": ["2020-01-05", "2020-02-30", "2020-01-09", "bad-date"],
            "close": ["3", "4", "NaN", "bad-price"],
            "source_provider": ["norgate"] * 4,
        }
    )
    with open_workspace(home, writable=True, strategy_write=True) as w:
        for i, (name, data) in enumerate(
            [("identity", identities), ("raw", raw), ("canonical", canonical), ("copy", raw)]
        ):
            import_arrow(w, name, str(i) * 64, "observations", data.to_reader())
    result = Catalog(home).coverage(query="SYN", category="", offset=0)
    rows = cast("list[dict[str, object]]", result["items"])
    assert len(rows) == _OBSERVATION_ROWS
    a, b = rows
    assert (a["symbol"], a["category"], a["first_date"], a["last_date"], a["rows"]) == (
        "SYN-A",
        "us_equity",
        "2020-01-01",
        "2020-01-05",
        2,
    )
    assert (b["category"], b["last_date"]) == ("us_etf", "2020-01-03")
    assert result["invalid_dates"] == _OBSERVATION_ROWS
    assert result["invalid_values"] == _OBSERVATION_ROWS
    assert result["truncated"] is False


def test_strategy_pagination_search_and_derived_collection(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    source = tmp_path / "research.sqlite3"
    _seed(home, source)
    extra = tmp_path / "extra.sqlite3"
    with sqlite3.connect(extra) as conn:
        conn.execute(
            "CREATE TABLE strategy (id TEXT,title TEXT,country TEXT,status TEXT,payload_json TEXT)"
        )
        conn.executemany(
            "INSERT INTO strategy VALUES (?,?,?,?,?)",
            [
                (
                    f"item-{i:03}",
                    f"Synthetic {i:03}",
                    "KR",
                    "stored",
                    json.dumps({"economic_config": {"offensive": ["SYNTH-C"]}}),
                )
                for i in range(105)
            ],
        )
    extra.chmod(0o600)
    with open_workspace(home, writable=True, strategy_write=True) as w:
        import_sqlite(w, extra, "derived", hashlib.sha256(extra.read_bytes()).hexdigest())
    catalog = Catalog(home)
    initial = catalog.strategies(collection=None, query="", country="", offset=0)
    assert str(initial["collection_id"]).startswith("synthetic-research")
    page = catalog.strategies(collection="derived:strategy", query="", country="", offset=100)
    assert page["total"] == 105  # noqa: PLR2004 -- two pages of synthetic records.
    assert len(cast("list[object]", page["records"])) == 5  # noqa: PLR2004
    result = catalog.strategies(
        collection="derived:strategy", query="Synthetic 104", country="KR", offset=0
    )
    assert result["total"] == 1
    record = cast("list[dict[str, object]]", result["records"])[0]
    assert record["assets"] == ["SYNTH-C"]
    assert (
        catalog.strategies(collection="derived:strategy", query="SYNTH-C", country="", offset=0)[
            "total"
        ]
        == 105  # noqa: PLR2004
    )


def test_same_bars_identity_merges_dates_and_metadata_conflict_is_unknown(tmp_path: Path) -> None:
    import pyarrow as pa  # noqa: PLC0415

    from aegis_alpha.storage.source_library import import_arrow  # noqa: PLC0415

    home = tmp_path / "aas"
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as w:
        for i, day in enumerate(["2020-01-03", "2020-01-05"]):
            t = pa.table(
                {
                    "instrument_id": ["provider:SYN"],
                    "venue": ["KO" if i == 0 else "US"],
                    "instrument_type": ["ETF"],
                    "provider_symbol": ["SYN"],
                    "date": [day],
                    "close": [1.0],
                }
            )
            import_arrow(w, f"bars-{i}", str(i) * 64, "bars", t.to_reader())
    result = Catalog(home).coverage(query="SYN", category="", offset=0)
    record = cast("list[dict[str, object]]", result["items"])[0]
    assert result["total"] == 1
    assert record["category"] == "unknown"
    assert record["last_date"] == "2020-01-05"


def test_strategy_request_does_not_wait_for_coverage_and_cache_expires(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from aegis_alpha.console import catalog as module  # noqa: PLC0415
    from aegis_alpha.storage.locks import file_lock  # noqa: PLC0415

    home = tmp_path / "aas"
    _seed(home, tmp_path / "source.sqlite3")

    def fail_coverage(_workspace: object) -> Never:
        raise AssertionError("strategy request invoked coverage")

    monkeypatch.setattr(module, "coverage", fail_coverage)
    catalog = Catalog(home)
    first = catalog.strategies(collection=None, query="", country="", offset=0)
    with file_lock(home / ".storage.lock"):
        assert catalog.strategies(collection=None, query="", country="", offset=0) == first
        monkeypatch.setattr(module, "_CACHE_SECONDS", -1)

        with pytest.raises(RuntimeError, match="busy"):
            catalog.strategies(collection=None, query="", country="", offset=0)


def test_source_budget_marks_partial_without_claiming_empty_complete(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from aegis_alpha.storage import research_inspection  # noqa: PLC0415

    home = tmp_path / "aas"
    _seed(home, tmp_path / "source.sqlite3")
    monkeypatch.setattr(research_inspection, "CATALOG_BYTES", 1)
    result = Catalog(home).strategies(collection=None, query="", country="", offset=0)
    assert result["status"] == "partial"
    assert result["truncated"] is True
    assert result["records"] == []


def test_read_deadline_interrupts_query_and_releases_handlers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from aegis_alpha.storage import research_coverage  # noqa: PLC0415

    home = tmp_path / "aas"
    initialize(home)
    monkeypatch.setattr(research_coverage, "READ_SECONDS", 0)
    with open_workspace(home) as workspace:
        with (
            pytest.raises(RuntimeError, match="deadline"),
            research_coverage.read_deadline(workspace),
        ):
            workspace.state.execute(
                "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL "
                "SELECT x+1 FROM n WHERE x<1000000) SELECT sum(x) FROM n"
            ).fetchone()
        assert workspace.state.execute("SELECT 1").fetchone()[0] == 1


def test_native_limit_is_applied_in_sql_and_multiple_versions_do_not_duplicate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from aegis_alpha.storage import research_inspection  # noqa: PLC0415
    from aegis_alpha.storage.strategies import import_strategy  # noqa: PLC0415
    from tests.engine.engine_support import contract, raw_bundle  # noqa: PLC0415

    home = tmp_path / "aas"
    initialize(home)
    base = json.loads(raw_bundle(contract()))
    with open_workspace(home, writable=True, strategy_write=True) as w:
        assert w.strategies is not None
        for identifier in ("native-a", "native-b"):
            for version in ("1", "2"):
                base.update(bundle_id=identifier, bundle_version=version)
                raw = json.dumps(base).encode()
                import_strategy(
                    w.strategies,
                    raw,
                    hashlib.sha256(raw).hexdigest(),
                    identifier,
                    version,
                    identifier + version,
                )
    monkeypatch.setattr(research_inspection, "RECORD_LIMIT", 1)
    result = Catalog(home).strategies(collection="native", query="", country="", offset=0)
    assert result["total"] == 1
    assert result["truncated"] is True
    assert cast("list[dict[str, object]]", result["records"])[0]["name"] == "native-a"


def test_identity_name_conflict_in_same_category_stays_unknown(tmp_path: Path) -> None:
    import pyarrow as pa  # noqa: PLC0415

    from aegis_alpha.storage.source_library import import_arrow  # noqa: PLC0415

    home = tmp_path / "aas"
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as w:
        for i, name in enumerate(("Synthetic A", "Synthetic B", "Synthetic A")):
            t = pa.table(
                {
                    "assetid": [1],
                    "symbol": ["SYN"],
                    "security_name": [name],
                    "database_or_watchlist": ["US Equities"],
                    "exchange": ["NYSE"],
                    "base_type": ["Stock Market"],
                    "subtype1": ["Equity"],
                    "is_etf": [False],
                    "source": ["norgate"],
                }
            )
            import_arrow(w, f"identity-{i}", str(i) * 64, "identity", t.to_reader())
        t = pa.table(
            {
                "assetid": [1],
                "symbol": ["SYN"],
                "date": ["2020-01-01"],
                "close": [1.0],
                "source": ["norgate"],
            }
        )
        import_arrow(w, "prices", "a" * 64, "observations", t.to_reader())
    result = Catalog(home).coverage(query="", category="", offset=0)
    record = cast("list[dict[str,object]]", result["items"])[0]
    assert record["category"] == "unknown"
    assert record["name"] is None
