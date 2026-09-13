from __future__ import annotations

import hashlib
from dataclasses import replace
from pathlib import Path

import pytest

from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.engine import replay
from aegis_alpha.engine.models import EngineContract
from aegis_alpha.storage.strategies import load_strategy
from aegis_alpha.storage.strategy_import import register_strategy
from aegis_alpha.storage.workspace import initialize, open_workspace
from tests.engine.engine_support import contract, raw_bundle, request
from tests.engine.test_requirements import rich_contract

MACRO_ROWS = [
    (
        "macro",
        1,
        "engine-macro-v1",
        "MACRO_Y",
        "macro_observations",
        2,
        "not_applicable",
        "calendar_month_end",
    ),
    (
        "macro",
        2,
        "engine-macro-v1",
        "YIELD_Y",
        "macro_observations",
        1,
        "not_applicable",
        "calendar_month_end",
    ),
]


@pytest.mark.parametrize(
    ("value", "warmup", "macro_rows"),
    [
        pytest.param(contract(), 3, [], id="prices-only"),
        pytest.param(rich_contract(), 8, MACRO_ROWS, id="derived-macro-retained"),
        pytest.param(
            replace(
                rich_contract(), calendar=replace(rich_contract().calendar, history_observations=1)
            ),
            1,
            MACRO_ROWS,
            id="history-cap-not-feature-minimum",
        ),
    ],
)
def test_registered_requirements_preserve_complete_v1_rows(
    tmp_path: Path,
    value: EngineContract,
    warmup: int,
    macro_rows: list[tuple[str | int, ...]],
) -> None:
    # Given independent historical v1 rows, including derived signals as macro rows.
    expected = [
        ("synthetic-probe", "1", *row)
        for row in [
            *macro_rows,
            (
                "prices",
                1,
                "engine-price-v1",
                "close",
                "prices",
                warmup,
                "explicit-input",
                "calendar_month_end",
            ),
        ]
    ]
    payload = raw_bundle(value)
    digest = hashlib.sha256(payload).hexdigest()
    contract_digest = hashlib.sha256(canonical_json_bytes(value)).hexdigest()
    home = tmp_path / "aas"
    initialize(home)
    source = tmp_path / "synthetic.json"
    source.write_bytes(payload)

    # When registration (including idempotent reimport) is reloaded without the source.
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        first = register_strategy(workspace, source, digest, "synthetic-probe", "1")
        assert register_strategy(workspace, source, digest, "synthetic-probe", "1") == first
    source.unlink()
    with open_workspace(home) as workspace:
        assert workspace.strategies is not None
        loaded = load_strategy(workspace.strategies, "synthetic-probe", "1", digest)
        rows = workspace.strategies.execute(
            "SELECT * FROM strategy_requirements ORDER BY role,ordinal"
        ).fetchall()

    # Then all ten stored columns and both identity hashes retain their v1 values.
    assert [tuple(row) for row in rows] == expected
    assert (loaded.source_sha256, loaded.contract_sha256) == (digest, contract_digest)
    assert loaded.contract == value


def test_registered_bundle_replays_after_original_file_removed(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    initialize(home)
    payload = raw_bundle(contract())
    digest = hashlib.sha256(payload).hexdigest()
    source = tmp_path / "synthetic.json"
    source.write_bytes(payload)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        register_strategy(workspace, source, digest, "synthetic-probe", "1")
    source.unlink()
    with open_workspace(home) as workspace:
        assert workspace.strategies is not None
        bundle = load_strategy(workspace.strategies, "synthetic-probe", "1", digest)
        result = replay(bundle, request())
        assert dict(result.ensemble) == {"ASSET_A": 1.0}
        assert result.source_sha256 == digest
        operations = workspace.state.execute("SELECT phase FROM storage_operations").fetchall()
        assert [row[0] for row in operations] == ["COMPLETED"]
