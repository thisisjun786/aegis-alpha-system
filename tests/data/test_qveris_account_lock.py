from __future__ import annotations

import hashlib
import selectors
import subprocess
import sys
from typing import TYPE_CHECKING

import pytest

from aegis_alpha.data.qveris import InvocationBudget
from aegis_alpha.data.qveris_acquisition import acquire_jobs
from aegis_alpha.data.qveris_store import QverisStore
from tests.data.test_qveris_acquisition import FakeQveris, eod_job

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize(
    ("peer_root", "peer_account", "expected"),
    [("first", "same", "BLOCKED"), ("second", "same", "BLOCKED"), ("second", "other", "OPEN")],
)
def test_account_exclusion_across_processes_and_roots(
    tmp_path: Path, peer_root: str, peer_account: str, expected: str
) -> None:
    account = hashlib.sha256(str(tmp_path).encode()).hexdigest()
    key = account if peer_account == "same" else account + "-other"
    child = """
import sys
from pathlib import Path
from aegis_alpha.data.qveris_store import QverisStore
try:
    with QverisStore(Path(sys.argv[1]), sys.argv[2]):
        print("OPEN")
except BlockingIOError:
    print("BLOCKED")
"""
    with QverisStore(tmp_path / "first", account):
        result = subprocess.run(  # noqa: S603 -- fixed probe, synthetic paths and account IDs
            [sys.executable, "-B", "-c", child, str(tmp_path / peer_root), key],
            capture_output=True,
            text=True,
            timeout=10,
            check=True,
        )
        assert result.stdout.strip() == expected
        assert not result.stderr
    with QverisStore(tmp_path / peer_root, key):
        pass


def test_competing_root_cannot_reach_provider_preflight(tmp_path: Path) -> None:
    client = FakeQveris()
    client.account_key = hashlib.sha256(str(tmp_path).encode()).hexdigest()
    root = tmp_path / "second"
    with QverisStore(tmp_path / "first", client.account_key):
        with pytest.raises(BlockingIOError):
            acquire_jobs((eod_job(),), root, client, budget=InvocationBudget(1, client.price))
        assert not client.calls
        assert not root.exists()
    result = acquire_jobs((eod_job(),), root, client, budget=InvocationBudget(1, client.price))
    assert result["status"] == "RAW_ACQUIRED"
    assert client.execute_count == 1


def test_failed_root_admission_releases_account_lease(tmp_path: Path) -> None:
    account = hashlib.sha256(str(tmp_path).encode()).hexdigest()
    root = tmp_path / "first"
    with QverisStore(root, account):
        pass
    other = account + "-other"
    with (
        pytest.raises(ValueError, match="immutable Qveris evidence conflicts"),
        QverisStore(root, other),
    ):
        pytest.fail("conflicting credential binding admitted")
    with QverisStore(tmp_path / "second", other):
        pass


def test_process_death_releases_account_lease(tmp_path: Path) -> None:
    account = hashlib.sha256(str(tmp_path).encode()).hexdigest()
    child = """
import sys
from pathlib import Path
from aegis_alpha.data.qveris_store import QverisStore
with QverisStore(Path(sys.argv[1]), sys.argv[2]):
    print("HELD", flush=True)
    sys.stdin.read(1)
"""
    with subprocess.Popen(  # noqa: S603 -- fixed probe, synthetic paths and account ID
        [sys.executable, "-B", "-c", child, str(tmp_path / "first"), account],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ) as process:
        try:
            assert process.stdout is not None
            with selectors.DefaultSelector() as ready:
                ready.register(process.stdout, selectors.EVENT_READ)
                assert ready.select(timeout=10)
                assert process.stdout.readline().strip() == "HELD"
            with pytest.raises(BlockingIOError), QverisStore(tmp_path / "second", account):
                pytest.fail("competing account admitted")
        finally:
            process.terminate()
            process.communicate(timeout=10)
    with QverisStore(tmp_path / "second", account):
        pass
