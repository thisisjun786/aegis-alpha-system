"""A failed v2 COMMIT, scaled down: a v2 key index outgrows a fixed DuckDB share.

On a large v2 store a promotion can fail to commit because DuckDB keeps the prices
table's key indexes in memory whole, and they grow with every row the table has stored.
v3 has no such index, so the same delta fits a share sized for the delta alone. This
file builds a store of a few hundred thousand rows, which takes seconds, so it runs in
every lane; it is its own file so the file-granular scheduler runs it beside the other
v3 cases.
"""

from __future__ import annotations

from fractions import Fraction
from pathlib import Path
from typing import Final

import duckdb
import pytest

from aegis_alpha.compute_resources import ComputeBudget, ComputeResourceError
from aegis_alpha.storage import market
from aegis_alpha.storage.bulk_generation import (
    plan_generation_bulk,
    publish_generation_bulk,
    verify_generation_bulk,
)
from tests.storage.test_bulk_generation import _request, _staged_prices

# Only building the stored history uses a roomy share; it is not what the case measures.
_BUILD: Final = ComputeBudget(Fraction(2), 1024 * 1024 * 1024)


def _keyed_prices(path: Path, version: int, base: int) -> None:
    """A store of ``version`` holding ``base`` prices rows and a staged next delta."""
    connection = duckdb.connect(str(path), config={"threads": 2, "memory_limit": "1GB"})
    market.initialize_market(connection, "synthetic", version=version)
    _staged_prices(connection, base)
    publish_generation_bulk(connection, _request("1", parent=None, domain="prices"), budget=_BUILD)
    connection.execute("DROP TABLE staged")
    _staged_prices(connection, 10_000, first=base)
    connection.execute("CHECKPOINT")
    connection.close()


def test_the_rehearsal_failure_shape_publishes_once_the_store_is_v3(tmp_path: Path) -> None:
    """A delta that a large keyed table cannot take within a small share, v3 takes.

    At 300,000 stored rows the v2 key indexes alone outgrow a 48 MiB DuckDB share, so a
    10,000-row publication is refused as a budget error (the same failure a much larger
    store meets, scaled down). The same store migrated to v3 within the same lease
    publishes the same generation, and its marker is the planned one.
    """
    path = tmp_path / "market.duckdb"
    _keyed_prices(path, 2, 300_000)
    small = ComputeBudget(Fraction(1), 64 * 1024 * 1024)
    request = _request("2", parent="g1", domain="prices")
    connection = duckdb.connect(str(path))
    planned = plan_generation_bulk(connection, request, budget=small)
    with pytest.raises(ComputeResourceError, match="within admitted memory") as caught:
        publish_generation_bulk(connection, request, budget=small, plan=planned)
    assert isinstance(caught.value.__cause__, duckdb.Error)
    assert connection.execute(
        "SELECT count(*) FROM market_generations WHERE generation_id='g2'"
    ).fetchone() == (0,)
    connection.close()
    # The migration itself runs within the same lease.
    connection = duckdb.connect(str(path))
    with market.budgeted(connection, small, "the core schema migration"):
        market.upgrade_market(connection, "synthetic", 3)
    connection.close()
    connection = duckdb.connect(str(path))
    marker = publish_generation_bulk(connection, request, budget=small, plan=planned)
    assert marker == planned.marker
    # Verified within the same lease: every chain link, and the new delta rehashed.
    assert verify_generation_bulk(connection, "g2", budget=small) == marker
    assert connection.execute("SELECT count(*) FROM prices").fetchone() == (310_000,)
    connection.close()
