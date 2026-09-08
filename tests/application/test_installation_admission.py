from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event

import pytest

from aegis_alpha.application import container_install as installer
from aegis_alpha.application import container_runtime as runtime
from aegis_alpha.application.installation_lock import installation_admission
from tests.application.test_container_runtime import _installation


def test_installer_rejects_custom_command_path_before_changes(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="default"):
        installer.install(tmp_path / "missing.json", tmp_path, tmp_path / "other-command")
    assert not (tmp_path / "other-command").exists()


def test_launcher_waits_for_installation_then_holds_admission_for_entire_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _installation(tmp_path)
    loaded, running, finish = Event(), Event(), Event()
    launched = []

    def load(_path: Path) -> runtime.ContainerInstallation:
        loaded.set()
        return config

    def execute(arguments: list[str], _environment: dict[str, str]) -> int:
        launched.append(arguments)
        running.set()
        assert finish.wait(timeout=5)
        return 0

    monkeypatch.setattr(runtime, "load_installation", load)
    monkeypatch.setattr(runtime, "require_local_host", lambda _env: None)
    monkeypatch.setattr(runtime, "host_environment", lambda _config: {})
    monkeypatch.setattr(runtime, "_run_attached", execute)
    monkeypatch.setattr(installer, "load_installation", load)
    monkeypatch.setattr(installer, "_install_locked", lambda *_args, **_kwargs: {"installed": True})
    with ThreadPoolExecutor(max_workers=1) as pool:
        try:
            with installation_admission(tmp_path, update=True):
                task = pool.submit(runtime.main, ["status"])
                assert loaded.wait(timeout=5)
                assert not task.done()
                assert not launched
            assert running.wait(timeout=5)
            with pytest.raises(RuntimeError, match="active"):
                installer.install(tmp_path / "manifest", tmp_path, Path.home() / ".local/bin/aas")
        finally:
            finish.set()
        assert task.result(timeout=5) == 0
    assert installer.install(tmp_path / "manifest", tmp_path, Path.home() / ".local/bin/aas") == {
        "installed": True
    }


def test_cleanup_uses_inspected_id_even_if_name_is_reused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _installation(tmp_path)
    invocation, old_id = "e" * 32, "f" * 64
    commands = []

    def docker(args: list[str], **_kwargs: object) -> str:
        commands.append(args)
        if args[0] == "ps" and args[3].startswith("name="):
            return old_id
        return ""

    monkeypatch.setattr(runtime, "_docker", docker)
    monkeypatch.setattr(
        runtime,
        "_inspect",
        lambda _name: {
            "Id": old_id,
            "Config": {
                "Labels": {
                    "aas.installation": config.installation_id,
                    "aas.invocation": invocation,
                    "aas.role": "collector",
                }
            },
        },
    )
    assert runtime.stop_managed(config, {"INVOCATION_ID": invocation})["reason"] == "already_absent"
    stop = [args for args in commands if args[0] == "stop"]
    assert stop == [["stop", "--time", "30", old_id]]
