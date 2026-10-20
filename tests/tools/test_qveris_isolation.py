"""Independent pytest invocations can hold their synthetic account leases together."""

from __future__ import annotations

import os
import selectors
import subprocess
import sys
from contextlib import ExitStack
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_PROBE = """
from contextlib import ExitStack
import sys
import pytest
from aegis_alpha.data.qveris_store import QverisStore
from tests.data.test_qveris_acquisition import FakeQveris
from tests.data.test_qveris_billing import AuditClient

pytestmark = pytest.mark.xdist_group("qveris-account-lease")

def test_hold(tmp_path):
    with ExitStack() as stack:
        for index, account in enumerate((FakeQveris.account_key, AuditClient.account_key)):
            stack.enter_context(QverisStore(tmp_path / str(index), account))
        print("HELD", flush=True)
        assert sys.stdin.readline().strip() == "RELEASE"
"""


def test_concurrent_pytest_invocations_hold_distinct_synthetic_accounts(tmp_path: Path) -> None:
    probe = tmp_path / "probe"
    probe.mkdir()
    (probe / "pytest.ini").write_text("[pytest]\n")
    (probe / "test_hold.py").write_text(_PROBE)
    environment = {key: value for key, value in os.environ.items() if key != "PYTEST_ADDOPTS"}
    environment["PYTHONPATH"] = os.pathsep.join(
        filter(None, (str(_ROOT), environment.get("PYTHONPATH")))
    )
    with ExitStack() as stack:
        processes = [
            stack.enter_context(
                subprocess.Popen(
                    [sys.executable, "-m", "pytest", "-q", "-s", "-p", "tests.conftest"],
                    cwd=probe,
                    env=environment,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
            )
            for _ in range(4)
        ]
        try:
            # No timing race or sleeps: all four leases must be held before any is released.
            for process in processes:
                assert process.stdout is not None
                with selectors.DefaultSelector() as ready:
                    ready.register(process.stdout, selectors.EVENT_READ)
                    assert ready.select(timeout=30), "lease probe did not respond"
                    line = process.stdout.readline().strip()
                    if line != "HELD":
                        out, err = process.communicate(timeout=30)
                        raise AssertionError(line + out + err)
            for process in processes:
                out, err = process.communicate("RELEASE\n", timeout=30)
                assert process.returncode == 0, out + err
        finally:
            for process in processes:
                if process.poll() is None:
                    process.terminate()
                process.communicate(timeout=10)
