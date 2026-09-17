"""Unit checks for the installed-scenario driver's own helpers.

These exercise the driver, never an installed wheel. The lane owns that evidence; what
these own is the failure mode where a helper quietly stops doing its job and the lane
still reports pass.
"""

from __future__ import annotations

import fcntl
import json
import os
import subprocess
from importlib import import_module
from pathlib import Path

import pytest

from scripts.verify_installed_scenario import (
    InstalledCli,
    cleanup_receipt,
    command_environment,
    forbidden_code_objects,
    request_for,
    second_strategy,
    sha256_file,
    tree_fingerprint,
)

# Written out rather than derived from FORBIDDEN_DURING_RECOVERY: generating the
# expectation from the constant would still pass after a target was deleted.
_ARMED = {
    "aegis_alpha.application.backtest_cli.run_document",
    "aegis_alpha.engine.execution.replay_next_open",
    "aegis_alpha.engine.execution.replay_next_open_cashflows",
    "aegis_alpha.engine.execution._replay",
    "aegis_alpha.engine.replay.replay",
}
_WIDENED_TOP_N = 2
_BUNDLE = {
    "schema_version": "aas-engine-bundle-v1",
    "bundle_id": "synthetic-probe",
    "bundle_version": "1",
    "contract": {
        "contract_version": "aas-engine-v1",
        "pack": [{"name": "choice", "offensive_config": {"top_n": 1, "assets": ["A", "B"]}}],
    },
}
_REQUEST = {
    "schema": "aas-prepare-request-v1",
    "strategy": {
        "strategy_id": "synthetic-probe",
        "version": "1",
        "raw_sha256": "old-raw",
        "contract_sha256": "old-contract",
        "strategy_store_id": "keep-me",
    },
    "bindings": [{"role": "sessions"}],
    "period": {"end": "2026-03-30"},
}


def test_every_forbidden_target_still_resolves_in_the_package() -> None:
    """If a target is renamed the observer silently disarms, so resolution must fail loudly."""
    armed = forbidden_code_objects()
    assert set(armed.values()) == _ARMED
    replay = import_module("aegis_alpha.engine.replay")
    assert armed[replay.replay.__code__] == "aegis_alpha.engine.replay.replay"
    execution = import_module("aegis_alpha.engine.execution")
    assert armed[execution.replay_next_open.__code__] == (
        "aegis_alpha.engine.execution.replay_next_open"
    )
    # Every entry is a distinct code object; a duplicate would shrink the armed set.
    assert len(armed) == len(_ARMED)


def test_a_renamed_target_is_not_silently_skipped(monkeypatch: pytest.MonkeyPatch) -> None:
    replay = import_module("aegis_alpha.engine.replay")
    monkeypatch.delattr(replay, "replay")
    with pytest.raises(AttributeError):
        forbidden_code_objects()


def test_second_strategy_changes_the_contract_not_just_the_version(tmp_path: Path) -> None:
    """A re-versioned identical contract is one strategy registered twice, not two."""
    source = tmp_path / "strategy.json"
    source.write_text(json.dumps(_BUNDLE), encoding="utf-8")
    original = source.read_bytes()
    target = tmp_path / "wide.json"
    spec = second_strategy(source, target)
    assert spec["strategy_id"] == "synthetic-probe-wide"
    assert spec["sha256"] == sha256_file(target)
    derived = json.loads(target.read_bytes())
    assert derived["bundle_id"] != _BUNDLE["bundle_id"]
    assert derived["contract"]["pack"][0]["offensive_config"]["top_n"] == _WIDENED_TOP_N
    assert derived["contract"] != _BUNDLE["contract"]
    assert source.read_bytes() == original, "the seeded bundle must not be rewritten"


def test_request_for_replaces_only_the_strategy_pin(tmp_path: Path) -> None:
    source = tmp_path / "request.json"
    source.write_text(json.dumps(_REQUEST), encoding="utf-8")
    target = request_for(
        source,
        tmp_path / "request-b.json",
        {"strategy_id": "synthetic-probe-wide", "version": "1"},
        {"raw_sha256": "new-raw", "contract_sha256": "new-contract"},
    )
    body = json.loads(target.read_bytes())
    assert body["strategy"]["strategy_id"] == "synthetic-probe-wide"
    assert body["strategy"]["raw_sha256"] == "new-raw"
    assert body["strategy"]["contract_sha256"] == "new-contract"
    # The store id and everything outside the strategy block are carried through.
    assert body["strategy"]["strategy_store_id"] == "keep-me"
    assert body["bindings"] == _REQUEST["bindings"]
    assert body["period"] == _REQUEST["period"]


def test_tree_fingerprint_notices_one_changed_byte(tmp_path: Path) -> None:
    root = tmp_path / "home"
    (root / "nested").mkdir(parents=True)
    (root / "a.txt").write_bytes(b"one")
    (root / "nested/b.txt").write_bytes(b"two")
    before = tree_fingerprint(root)
    assert sorted(before) == ["a.txt", "nested/b.txt"]
    assert tree_fingerprint(root) == before
    (root / "nested/b.txt").write_bytes(b"twO")
    assert tree_fingerprint(root) != before


def test_command_environment_cannot_leak_the_checkout(tmp_path: Path) -> None:
    """The scenario's whole claim rests on the installed process never seeing the checkout."""
    home = tmp_path / "synthetic-home"
    lock = tmp_path / "c.lock"
    environment = command_environment(home, lock)
    for leak in ("PYTHONPATH", "PYTHONHOME", "VIRTUAL_ENV"):
        assert leak not in environment
    assert environment["AAS_HOME"] == str(home)
    assert environment["AAS_COMPUTE_LOCK_FILE"] == str(lock)
    # The compute budget has no silent default, so all five must be present.
    for name in (
        "AAS_HOST_CPU_LIMIT",
        "AAS_HOST_MEMORY_LIMIT_BYTES",
        "AAS_CPU_LIMIT",
        "AAS_MEMORY_LIMIT_BYTES",
        "AAS_COMPUTE_LOCK_FILE",
    ):
        assert environment[name]


def test_cleanup_receipt_refuses_while_a_lock_is_held(tmp_path: Path) -> None:
    """A held lock must stop the receipt, and probing must happen before deletion.

    The real locks live inside the tree cleanup removes, so deleting first would report
    every one of them absent and the receipt would certify nothing.
    """
    owned = tmp_path / "work"
    (owned / "deep").mkdir(parents=True)
    (owned / "deep/file").write_bytes(b"x")
    lock = owned / "storage.lock"
    lock.write_bytes(b"")
    absent = tmp_path / "never-created.lock"

    holder = os.open(lock, os.O_RDWR)
    try:
        fcntl.flock(holder, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(SystemExit, match="locks still held at cleanup"):
            cleanup_receipt([owned], [lock, absent])
        # Refusing must not delete the evidence it refused over.
        assert lock.exists()
        assert (owned / "deep/file").exists()
    finally:
        os.close(holder)

    receipt = cleanup_receipt([owned], [lock, absent])
    assert receipt["removed"] == [{"path": str(owned), "remaining": False}]
    assert not owned.exists()
    assert receipt["lock_probes"] == [
        {"path": str(lock), "state": "free"},
        {"path": str(absent), "state": "absent"},
    ]


def test_refused_rejects_a_command_that_unexpectedly_succeeded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A refusal check that accepts success would turn every negative case into a pass."""
    cli = InstalledCli(tmp_path / "aas", tmp_path / "home", tmp_path / "c.lock", tmp_path)

    def completed(returncode: int, stderr: str) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(["aas"], returncode, "", stderr)

    monkeypatch.setattr(
        InstalledCli, "run", lambda *_a, **_k: completed(1, json.dumps({"error": "refused: why"}))
    )
    assert cli.refused("db", "restore") == {
        "returncode": 1,
        "stdout": "",
        "error": "refused: why",
    }
    monkeypatch.setattr(InstalledCli, "run", lambda *_a, **_k: completed(0, ""))
    with pytest.raises(SystemExit, match="expected a refusal"):
        cli.refused("db", "restore")
