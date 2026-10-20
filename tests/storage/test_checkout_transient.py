"""An unrelated empty Git mount target may come and go during persistent writes."""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event

from aegis_alpha.data import sec_bulk
from aegis_alpha.storage.locks import private_directory
from tests.application.test_storage_cli import run_cli
from tests.data.test_sec_bulk import CAP, UA, Reply, Transport, payload
from tests.storage.test_backup import seed_workspace


def test_writes_and_deep_backup_survive_transient_empty_marker(tmp_path: Path) -> None:
    home = tmp_path / "home"
    seed_workspace(home)
    stop, started = Event(), Event()

    def churn() -> int:
        marker = tmp_path / ".git"
        cycles = 0
        while not stop.is_set():
            marker.mkdir()
            started.set()
            marker.rmdir()
            cycles += 1
        return cycles

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(churn)
        try:
            assert started.wait(timeout=5)
            for index in range(20):
                private_directory(tmp_path / "private" / str(index), create=True)
            transport = Transport(Reply(payload()))
            collected = sec_bulk.acquire_archive(
                "companyfacts",
                tmp_path / "bulk",
                user_agent=UA,
                max_bytes=CAP,
                transport=transport,
            )
            assert collected["members"] == 1
            assert transport.calls == 1
            archive = tmp_path / "backup"
            result = run_cli("db", "backup", "--deep", "--output", str(archive), home=home)
            assert result.returncode == 0, result.stderr
            assert json.loads(result.stdout)["backed_up"] is True
            manifest = json.loads((archive / "backup.json").read_bytes())
            assert manifest["logical"]["dataset_versions"] == 1
            assert manifest["logical"]["strategy_versions"] == 1
        finally:
            stop.set()
        assert future.result(timeout=5) > 0
    assert not (tmp_path / ".git").exists()
