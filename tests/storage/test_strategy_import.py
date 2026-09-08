from __future__ import annotations

import hashlib
from pathlib import Path

from aegis_alpha.engine import replay
from aegis_alpha.storage.strategies import load_strategy
from aegis_alpha.storage.strategy_import import register_strategy
from aegis_alpha.storage.workspace import initialize, open_workspace
from tests.engine.engine_support import contract, raw_bundle, request


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
