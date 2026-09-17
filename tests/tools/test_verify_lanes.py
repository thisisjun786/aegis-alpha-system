from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import sys
import time
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import TypedDict

import pytest

_ROOT = Path(__file__).resolve().parents[2]
_FAILURE = 23
_SIGTERM_EXIT = 143
_STATUS = {
    "application": "aegis-alpha-system",
    "mode": "standalone-cli",
    "capabilities": {"allocation_preview": True, "live_orders": False},
    "runtime_dependencies": {"database_for_preview": False},
}
_PREVIEW = {
    "schema_version": 1,
    "as_of": "2026-09-05",
    "execution_enabled": False,
    "positions": [
        {"instrument_id": "asset:equity-basket", "weight": 0.58},
        {"instrument_id": "asset:protective-basket", "weight": 0.1},
        {"instrument_id": "asset:stock-b", "weight": 0.15},
    ],
    "cash_weight": 0.17,
    "modules": [{"module": name} for name in ("aegis", "alpha", "hedge")],
}
# Only process boundaries are faked: lane shell, fingerprint Python, venv
# interpreter, JSON validation and installed-package import checks all execute.
_FAKE_SCENARIO = r"""
import json
import os
import sys
from pathlib import Path

mode = sys.argv[1]
log = os.getenv("FAKE_SCENARIO_LOG")
if log:
    with Path(log).open("a", encoding="utf-8") as handle:
        handle.write(
            json.dumps(
                {
                    "mode": mode,
                    "argv": sys.argv[1:],
                    "cwd": os.getcwd(),
                    "executable": sys.executable,
                    "pythonpath": os.environ.get("PYTHONPATH"),
                }
            )
            + "\n"
        )
if os.getenv("FAKE_SCENARIO_FAIL") == mode:
    sys.exit("fake scenario refused: " + mode)
override = os.getenv("FAKE_SEED" if mode == "seed" else "FAKE_SCENARIO")
print(override if override else json.dumps(json.loads(os.environ["FAKE_SCENARIO_DEFAULT"])[mode]))
"""

_RECOVERY = {
    "observers_armed": True,
    "observers_fired": [],
    "armed_calculation_targets": ["aegis_alpha.engine.replay.replay"],
    "armed_network_targets": ["socket.create_connection"],
}
_SCENARIO_DEFAULT = {
    "seed": {"seeded": True, "any_call_saw_pythonpath": False},
    "scenario": {
        "scenario": "complete",
        "any_call_saw_pythonpath": False,
        "provenance": {"import_root_inside_prefix": True, "legacy_extra": "not_exercised"},
        "strategies": [
            {"strategy_id": "synthetic-probe", "contract_sha256": "a"},
            {"strategy_id": "synthetic-probe-wide", "contract_sha256": "b"},
        ],
        "cases": {
            "a_cli": {"result_hash": "one"},
            "b_cli": {"result_hash": "two"},
            "a_api": {"result_hash": "one"},
        },
        "reexecuted_without_seed": {"case_directory_present": False},
        "restored_runs": [{"run_id": "run-x", "identical": True}],
        "restore_refusals": {
            "existing_home_unchanged": True,
            "nothing_written_before_verification": True,
            "tampered_home_is_marked_incomplete": True,
        },
        "recovery_without_marker": {**_RECOVERY, "status": "INTERRUPTED"},
        "recovery_after_marker": {
            **_RECOVERY,
            "status": "SUCCESS",
            "sealed_files_unchanged": True,
        },
        "cleanup": {
            "removed": [{"path": "owned-work-root", "remaining": False}],
            "lock_probes": [{"path": "owned-storage-lock", "state": "free"}],
        },
    },
}


_FAKE_TOOL = r"""
import json
import os
import signal
import sys
import sysconfig
import tarfile
import venv
import zipfile
import io
from pathlib import Path

tool, args = Path(sys.argv[0]).name, sys.argv[1:]
with Path(os.environ["FAKE_LOG"]).open("a") as log:
    log.write(json.dumps({"tool": tool, "args": args, "cwd": os.getcwd(),
        "token": os.getenv("AAS_VERIFY_PREPARED", ""),
        "pythonpath": os.getenv("PYTHONPATH", ""),
        "database_url": os.getenv("AAS_TEST_DATABASE_URL", "")}) + "\n")

def value(flag):
    return args[args.index(flag) + 1]

def outcome():
    command = " ".join(args)
    if os.getenv("FAKE_SIGNAL") and os.environ["FAKE_SIGNAL"] in command:
        os.kill(os.getppid(), signal.SIGTERM)
        sys.exit(23)
    if os.getenv("FAKE_FAIL") and os.environ["FAKE_FAIL"] in command:
        sys.exit(23)

def cli_output():
    if "doctor" in args:
        print(json.dumps({"ready": True, "strategy_versions": 0,
            "docker_required": False, "database_server_required": False}))
    elif "restore" in args:
        print(json.dumps({"restored": True}))
    elif "init" in args or "backup" in args:
        print("{}")
    else:
        print(os.environ["FAKE_PREVIEW" if "preview" in args else "FAKE_STATUS"])

if tool == "uv":
    outcome()
    if args == ["--version"]:
        print("uv " + os.environ["FAKE_UV_VERSION"])
    elif args[0] == "sync":
        venv.EnvBuilder(with_pip=False).create(os.environ["UV_PROJECT_ENVIRONMENT"])
    elif args[0] == "build" and not os.getenv("FAKE_NO_WHEEL"):
        dist = Path(value("--out-dir"))
        dist.mkdir()
        with zipfile.ZipFile(dist / "aegis_alpha_system-0.0.1-py3-none-any.whl", "w") as archive:
            for name in ("LICENSE", "THIRD_PARTY_NOTICES.md"):
                archive.writestr("aegis_alpha_system.dist-info/licenses/" + name, "fixture")
            if os.getenv("FAKE_PRIVATE_PACKAGE"):
                archive.writestr("aegis_alpha/strategies/recipe.py", "private = True")
        with tarfile.open(dist / "aegis_alpha_system-0.0.1.tar.gz", "w:gz") as archive:
            for name in ("LICENSE", "THIRD_PARTY_NOTICES.md"):
                member = tarfile.TarInfo("aegis_alpha_system-0.0.1/" + name)
                member.size = 7
                archive.addfile(member, io.BytesIO(b"fixture"))
    elif args[0] == "export":
        Path(value("--output-file")).touch()
    elif args[0] == "venv":
        venv.EnvBuilder(with_pip=False).create(args[-1])
    elif args[:2] == ["pip", "install"] and "--no-deps" in args:
        environment = Path(value("--python")).parents[1]
        packages = Path(sysconfig.get_path("purelib", vars={"base": str(environment)}))
        package = packages / "aegis_alpha"
        package.mkdir(parents=True)
        source = ""
        if os.getenv("FAKE_SOURCE_LEAK"):
            source = "__file__ = " + repr(os.environ["FAKE_ROOT"] + "/src/aegis_alpha.py")
        (package / "__init__.py").write_text(source)
        (package / "engine").mkdir()
        (package / "engine/__init__.py").write_text("")
        if os.getenv("FAKE_RETIRED_PATH"):
            retired = package / os.environ["FAKE_RETIRED_PATH"]
            retired.parent.mkdir(parents=True, exist_ok=True)
            retired.write_text("")
        cli = environment / "bin/aas"
        cli.write_text("#!" + str(environment / "bin/python") + "\n" + Path(__file__).read_text())
        cli.chmod(0o755)
elif tool == "aas":
    outcome()
    cli_output()
elif tool == "docker":
    state_path = Path(os.environ["FAKE_STATE"])
    state = json.loads(state_path.read_text()) if state_path.exists() else {}
    if args[0] == "build":
        state[value("--tag")] = value("--label").split("=", 1)[1]
    elif args[0] == "run":
        state[value("--name")] = value("--label").split("=", 1)[1]
    elif args[0] == "inspect":
        if not os.getenv("FAKE_NO_ADDRESS"):
            print("192.0.2.1")
    elif args[1] == "inspect":
        if args[-1] not in state:
            sys.exit(1)
        observed = "foreign" if os.getenv("FAKE_FOREIGN_OWNER") else state[args[-1]]
        print("" if os.getenv("FAKE_EMPTY_OWNER") else observed)
    elif args[1] == "rm":
        outcome()
        del state[args[-1]]
    state_path.write_text(json.dumps(state))
    outcome()
    if args[0] == "run":
        cli_output()
"""


class Call(TypedDict):
    tool: str
    args: list[str]
    cwd: str
    token: str
    pythonpath: str
    database_url: str


@dataclass
class Harness:
    root: Path
    environment: dict[str, str]

    def run(self, body: str, **environment: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(  # noqa: S603 -- fixed test commands in an isolated fixture
            ["/bin/bash", "-euc", body],
            cwd=self.root.parent,
            env={**self.environment, **environment},
            check=False,
            capture_output=True,
            text=True,
            timeout=15,
        )

    def calls(self, tool: str = "uv") -> list[Call]:
        path = Path(self.environment["FAKE_LOG"])
        calls: list[Call] = [json.loads(line) for line in path.read_text().splitlines()]
        return [call for call in calls if call["tool"] == tool]

    def prepare(self, profile: str = "full") -> str:
        result = self.run(
            'source "$FAKE_ROOT/scripts/verify-lib.sh"; ensure_verify_toolchain; '
            'printf "TOKEN=%s\\n" "$AAS_VERIFY_PREPARED"',
            AAS_VERIFY_PROFILE=profile,
        )
        assert result.returncode == 0, result.stderr
        return next(line.removeprefix("TOKEN=") for line in result.stdout.splitlines())


@pytest.fixture
def harness(tmp_path: Path) -> Harness:
    root = tmp_path / "checkout with spaces"
    scripts = root / "scripts"
    scripts.mkdir(parents=True)
    for name in (
        "verify",
        "verify-lib.sh",
        "ci_public.py",
        *(path.name for path in (_ROOT / "scripts").glob("verify-lane-*")),
    ):
        shutil.copy2(_ROOT / "scripts" / name, scripts / name)
    # The real installed-scenario driver needs a real wheel and a real installation, which
    # this harness fakes at the process boundary. Renaming the driver must break loudly
    # here rather than silently skipping the lane's newest step.
    assert (_ROOT / "scripts/verify_installed_scenario.py").is_file()
    (scripts / "verify_installed_scenario.py").write_text(_FAKE_SCENARIO)
    for name in ("pyproject.toml", "uv.lock"):
        shutil.copy2(_ROOT / name, root / name)
    (root / ".python-version").write_text(f"{sys.version_info.major}.{sys.version_info.minor}\n")
    (root / "examples").mkdir()
    shutil.copy2(
        _ROOT / "examples/portfolio-preview.json", root / "examples/portfolio-preview.json"
    )
    binaries = tmp_path / "bin"
    binaries.mkdir()
    for name in ("uv", "docker"):
        path = binaries / name
        path.write_text(f"#!{sys.executable}\n{_FAKE_TOOL}")
        path.chmod(0o755)
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("AAS_", "UV_", "PYTHON")) and key != "VIRTUAL_ENV"
    }
    environment.update(
        PATH=f"{binaries}:{environment['PATH']}",
        FAKE_LOG=str(tmp_path / "calls.jsonl"),
        FAKE_STATE=str(tmp_path / "docker.json"),
        FAKE_ROOT=str(root),
        FAKE_STATUS=json.dumps(_STATUS),
        FAKE_PREVIEW=json.dumps(_PREVIEW),
        FAKE_SCENARIO_DEFAULT=json.dumps(_SCENARIO_DEFAULT),
        FAKE_SCENARIO_LOG=str(tmp_path / "scenario.jsonl"),
        FAKE_UV_VERSION="0.11.32",
        AAS_TEST_DATABASE_URL="postgresql+psycopg://synthetic.invalid/disposable_test",
    )
    return Harness(root, environment)


@pytest.mark.parametrize("profile", ["full", "style"])
def test_standalone_setup_exports_reusable_token_and_syncs_once(
    harness: Harness, profile: str
) -> None:
    token = harness.prepare(profile)
    result = harness.run(
        'source "$FAKE_ROOT/scripts/verify-lib.sh"; ensure_verify_toolchain; '
        '"$FAKE_ROOT/scripts/verify-lane-style"',
        AAS_VERIFY_PREPARED=token,
        AAS_VERIFY_PROFILE=profile,
    )
    assert result.returncode == 0, result.stderr
    syncs = [call for call in harness.calls() if call["args"][0] == "sync"]
    assert len(syncs) == 1
    assert "--locked" in syncs[0]["args"]
    if profile == "style":
        assert syncs[0]["args"][-3:] == ["--only-group", "dev", "--no-install-project"]
        assert "--dev" not in syncs[0]["args"]
    else:
        assert syncs[0]["args"][-1] == "--dev"
    runs = [call for call in harness.calls() if call["args"][0] == "run"]
    assert [call["args"] for call in runs] == [
        ["run", "--no-sync", "ruff", "format", "--check", "."],
        ["run", "--no-sync", "ruff", "check", "."],
    ]
    assert all(call["token"] == token for call in runs)


@pytest.mark.parametrize("token", ["", "1", "v1:full:missing"])
def test_requested_preparation_never_falls_back_to_sync(harness: Harness, token: str) -> None:
    result = harness.run('"$FAKE_ROOT/scripts/verify-lane-test"', AAS_VERIFY_PREPARED=token)
    assert result.returncode != 0
    assert "prepared interpreter is missing" in result.stderr
    assert [call["args"] for call in harness.calls()] == [["--version"]]


@pytest.mark.parametrize(
    "changed", ["pyproject.toml", "uv.lock", ".python-version", "scripts/verify-lib.sh"]
)
def test_prepared_mode_rejects_changed_inputs_without_setup(harness: Harness, changed: str) -> None:
    token = harness.prepare()
    path = harness.root / changed
    path.write_text("0.0\n" if changed == ".python-version" else path.read_text() + "\n")
    result = harness.run('"$FAKE_ROOT/scripts/verify-lane-test"', AAS_VERIFY_PREPARED=token)
    assert result.returncode != 0
    assert "stale" in result.stderr or "interpreter version" in result.stderr
    assert len([call for call in harness.calls() if call["args"][0] == "sync"]) == 1
    assert not [call for call in harness.calls() if call["args"][0] == "run"]


@pytest.mark.parametrize("change", ["root", "interpreter", "profile", "token"])
def test_prepared_mode_binds_checkout_interpreter_and_profile(
    harness: Harness, change: str
) -> None:
    token = harness.prepare()
    environment = {"AAS_VERIFY_PREPARED": token}
    if change == "root":
        moved = harness.root.with_name("moved")
        harness.root.rename(moved)
        harness.environment["FAKE_ROOT"] = str(moved)
    elif change == "interpreter":
        executable = harness.root / ".venv/bin/python"
        executable.unlink()
        executable.symlink_to("/usr/bin/python3")
    elif change == "profile":
        environment["AAS_VERIFY_PROFILE"] = "style"
    else:
        environment["AAS_VERIFY_PREPARED"] = ""
    result = harness.run('"$FAKE_ROOT/scripts/verify-lane-test"', **environment)
    assert result.returncode != 0
    assert len([call for call in harness.calls() if call["args"][0] == "sync"]) == 1


@pytest.mark.parametrize("step", ["python install", "sync", "run --no-sync ruff format"])
def test_setup_and_style_failures_propagate(harness: Harness, step: str) -> None:
    result = harness.run('"$FAKE_ROOT/scripts/verify-lane-style"', FAKE_FAIL=step)
    assert result.returncode == _FAILURE
    assert not [
        call
        for call in harness.calls()
        if call["args"][:4] == ["run", "--no-sync", "ruff", "check"]
    ]
    if step == "python install":
        assert not [call for call in harness.calls() if call["args"][0] == "sync"]


@pytest.mark.parametrize("setting", [{"AAS_VERIFY_PROFILE": ""}, {"FAKE_UV_VERSION": "0.0.0"}])
def test_invalid_profile_or_uv_fails_before_setup(
    harness: Harness, setting: dict[str, str]
) -> None:
    result = harness.run('"$FAKE_ROOT/scripts/verify-lane-style"', **setting)
    assert result.returncode != 0
    assert not (harness.root / ".venv").exists()


@pytest.mark.parametrize("topology", ["serial", "parallel"])
def test_dispatcher_prepares_once_before_all_children(harness: Harness, topology: str) -> None:
    result = harness.run('"$FAKE_ROOT/scripts/verify"', AAS_VERIFY_TOPOLOGY=topology)
    assert result.returncode == 0, result.stderr
    calls = harness.calls()
    assert len([call for call in calls if call["args"][0] == "sync"]) == 1
    for name in ("style", "types", "test", "database", "build"):
        assert f"lane-start {name}" in result.stdout
        assert f"lane-end {name} rc=0 secs=" in result.stdout
    assert len({call["token"] for call in calls if call["args"][0] in {"run", "build"}}) == 1
    assert f"repository verification: pass (topology={topology})" in result.stdout


@pytest.mark.parametrize("topology", ["serial", "parallel"])
def test_dispatcher_reports_failed_lane_and_stops(harness: Harness, topology: str) -> None:
    result = harness.run(
        '"$FAKE_ROOT/scripts/verify"', AAS_VERIFY_TOPOLOGY=topology, FAKE_FAIL="ty check"
    )
    assert result.returncode == _FAILURE
    assert f"lane-end types rc={_FAILURE}" in result.stdout
    assert "lane-start test" not in result.stdout
    assert "verification: pass" not in result.stdout


def test_setup_refuses_another_preparation_lock(harness: Harness) -> None:
    result = harness.run('exec 9<"$FAKE_ROOT"; flock -n 9; "$FAKE_ROOT/scripts/verify-lane-style"')
    assert result.returncode != 0
    assert "another setup owns" in result.stderr
    assert not [call for call in harness.calls() if call["args"][0] == "sync"]


def test_cleanup_escalates_and_reaps_a_child_ignoring_term(harness: Harness) -> None:
    child = harness.root / "stubborn.py"
    pidfile = harness.root / "stubborn.pid"
    child.write_text(
        "import os, signal\n"
        "from pathlib import Path\n"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        "Path(os.environ['FAKE_ROOT'], 'stubborn.pid').write_text(str(os.getpid()))\n"
        "os.kill(os.getppid(), signal.SIGTERM)\n"
        "signal.pause()\n"
    )
    started = time.monotonic()
    max_stop_seconds = 10  # five-second grace plus scheduling/launch margin
    try:
        result = harness.run(
            'source "$FAKE_ROOT/scripts/verify-lib.sh"; '
            "trap 'rc=$?; verify_stop_child; exit $rc' EXIT; trap 'exit 143' TERM; "
            'verify_run "$FAKE_PYTHON" "$FAKE_ROOT/stubborn.py"',
            FAKE_PYTHON=sys.executable,
        )
        assert result.returncode == _SIGTERM_EXIT, result.stderr
        assert time.monotonic() - started < max_stop_seconds
        with pytest.raises(ProcessLookupError):
            os.kill(int(pidfile.read_text()), 0)
    finally:
        if pidfile.exists():
            with suppress(ProcessLookupError):
                os.killpg(int(pidfile.read_text()), signal.SIGKILL)


def test_wheel_smoke_runs_installed_cli_outside_checkout(harness: Harness) -> None:
    result = harness.run('"$FAKE_ROOT/scripts/verify-lane-build"', PYTHONPATH=str(harness.root))
    assert result.returncode == 0, result.stderr
    calls = harness.calls("aas")
    assert [call["args"][0] for call in calls[:2]] == ["status", "preview"]
    assert [call["args"][2] for call in calls[2:]] == ["init", "doctor", "db", "db"]
    assert calls[-1]["args"][3] == "restore"
    for call in calls:
        assert not Path(call["cwd"]).is_relative_to(harness.root)
        assert not Path(call["cwd"]).exists()  # owned install/build/smoke files cleaned
        assert call["pythonpath"] == ""
    assert "aas status/preview JSON: pass" in result.stdout


@pytest.mark.parametrize(
    "setting",
    [
        {"FAKE_FAIL": "build"},
        {"FAKE_FAIL": "pip install"},
        {"FAKE_FAIL": "status"},
        {"FAKE_PREVIEW": json.dumps({**_PREVIEW, "cash_weight": 0.5})},
        {"FAKE_STATUS": "not JSON"},
        {"FAKE_NO_WHEEL": "1"},
        {"FAKE_SOURCE_LEAK": "1"},
        {"FAKE_PRIVATE_PACKAGE": "1"},
        {"FAKE_SIGNAL": "build"},
    ],
)
def test_package_errors_reject_smoke_and_clean_scratch(
    harness: Harness, setting: dict[str, str]
) -> None:
    result = harness.run('"$FAKE_ROOT/scripts/verify-lane-build"', **setting)
    assert result.returncode != 0
    build = next(call for call in harness.calls() if call["args"][0] == "build")
    assert not Path(build["args"][-1]).parent.exists()
    assert "aas status/preview JSON: pass" not in result.stdout


def test_container_isolated_runtime_commands_and_owned_cleanup(harness: Harness) -> None:
    result = harness.run('"$FAKE_ROOT/scripts/verify-lane-container" aas')
    assert result.returncode == 0, result.stderr
    assert not harness.calls()  # host container lane never invokes uv or prepares a venv
    calls = harness.calls("docker")
    build = next(call["args"] for call in calls if call["args"][0] == "build")
    assert build[build.index("--file") + 1] == "Dockerfile"
    runs = [call["args"] for call in calls if call["args"][0] == "run"]
    for args in runs:
        assert {"--network=none", "--read-only", "--no-healthcheck", "--cap-drop=ALL"} <= set(args)
        assert not {"--mount", "--volume", "-v", "--privileged", "serve", "db", "collect"} & set(
            args
        )
    imports, status, preview, native = runs
    assert imports[imports.index("--entrypoint") + 1] == "/opt/venv/bin/python"
    assert native[native.index("--entrypoint") + 1] == "/opt/venv/bin/python"
    assert status[-1] == "status"
    assert preview[-3:] == ["preview", "--input", "/app/examples/portfolio-preview.json"]
    assert "import aegis_alpha.engine" in imports[-1]
    assert json.loads(Path(harness.environment["FAKE_STATE"]).read_text()) == {}
    assert not (harness.root / ".venv").exists()


@pytest.mark.parametrize("arguments", ["", "vt", "all", "aas vt", "--help"])
def test_container_requires_exactly_one_known_target(harness: Harness, arguments: str) -> None:
    result = harness.run(f'"$FAKE_ROOT/scripts/verify-lane-container" {arguments}')
    assert result.returncode == 2  # noqa: PLR2004 -- CLI usage status
    assert not Path(harness.environment["FAKE_LOG"]).exists()


@pytest.mark.parametrize(
    "setting",
    [
        {"FAKE_FAIL": "build"},
        {"FAKE_FAIL": "status"},
        {"FAKE_STATUS": "{}"},
        {"FAKE_PREVIEW": json.dumps({**_PREVIEW, "execution_enabled": True})},
        {"FAKE_SIGNAL": "preview"},
    ],
)
def test_container_failure_and_signal_clean_owned_resources(
    harness: Harness, setting: dict[str, str]
) -> None:
    result = harness.run('"$FAKE_ROOT/scripts/verify-lane-container" aas', **setting)
    assert result.returncode != 0
    if "FAKE_SIGNAL" in setting:
        assert result.returncode == _SIGTERM_EXIT
    assert json.loads(Path(harness.environment["FAKE_STATE"]).read_text()) == {}


def test_container_cleanup_refuses_foreign_resource_labels(harness: Harness) -> None:
    result = harness.run(
        '"$FAKE_ROOT/scripts/verify-lane-container" aas', FAKE_FAIL="status", FAKE_FOREIGN_OWNER="1"
    )
    assert result.returncode == _FAILURE
    assert not [call for call in harness.calls("docker") if call["args"][1] == "rm"]


@pytest.mark.parametrize("setting", [{"FAKE_FAIL": "image rm"}, {"FAKE_FOREIGN_OWNER": "1"}])
def test_container_cleanup_errors_cannot_report_success(
    harness: Harness, setting: dict[str, str]
) -> None:
    result = harness.run('"$FAKE_ROOT/scripts/verify-lane-container" aas', **setting)
    assert result.returncode != 0


def test_container_runs_get_unique_image_names(harness: Harness) -> None:
    for _ in range(2):
        result = harness.run('"$FAKE_ROOT/scripts/verify-lane-container" aas')
        assert result.returncode == 0, result.stderr
    builds = [call["args"] for call in harness.calls("docker") if call["args"][0] == "build"]
    assert builds[0][builds[0].index("--tag") + 1] != builds[1][builds[1].index("--tag") + 1]


@pytest.mark.parametrize(("lane", "marker"), [("test", "not database"), ("database", "database")])
def test_pytest_lanes_select_marker_and_isolate_database_url(
    harness: Harness, lane: str, marker: str
) -> None:
    token = harness.prepare()
    result = harness.run(f'"$FAKE_ROOT/scripts/verify-lane-{lane}"', AAS_VERIFY_PREPARED=token)
    assert result.returncode == 0, result.stderr
    runs = [call for call in harness.calls() if call["args"][0] == "run"]
    assert [call["args"] for call in runs] == [["run", "--no-sync", "pytest", "-m", marker]]
    assert runs[0]["database_url"] == (
        harness.environment["AAS_TEST_DATABASE_URL"] if lane == "database" else ""
    )
    assert len([call for call in harness.calls() if call["args"][0] == "sync"]) == 1
    assert not harness.calls("docker")  # Supplied disposable DB is not provisioned or removed.


def test_database_lane_propagates_pytest_failure(harness: Harness) -> None:
    result = harness.run(
        '"$FAKE_ROOT/scripts/verify-lane-database"', FAKE_FAIL="pytest -m database"
    )
    assert result.returncode == _FAILURE


def test_database_container_is_owned_unpublished_and_removed(harness: Harness) -> None:
    result = harness.run('"$FAKE_ROOT/scripts/verify-lane-database"', AAS_TEST_DATABASE_URL="")
    assert result.returncode == 0, result.stderr
    calls = harness.calls("docker")
    run = next(call["args"] for call in calls if call["args"][0] == "run")
    name = run[run.index("--name") + 1]
    label = run[run.index("--label") + 1]
    assert label == f"org.aegis-alpha.verify={name}"
    assert {"-p", "--publish", "--publish-all", "--privileged", "-v", "--volume"}.isdisjoint(run)
    assert "@sha256:" in run[-1]
    removals = [call["args"] for call in calls if call["args"][:2] == ["container", "rm"]]
    assert removals == [["container", "rm", "--force", "--volumes", name]]
    assert json.loads(Path(harness.environment["FAKE_STATE"]).read_text()) == {}
    pytest_run = next(call for call in harness.calls() if call["args"][0] == "run")
    assert pytest_run["args"] == ["run", "--no-sync", "pytest", "-m", "database"]
    assert pytest_run["database_url"].endswith("@192.0.2.1:5432/aas_test")


@pytest.mark.parametrize(
    "setting",
    [{"FAKE_FOREIGN_OWNER": "1"}, {"FAKE_EMPTY_OWNER": "1"}, {"FAKE_FAIL": "container inspect"}],
)
def test_database_cleanup_refuses_unverified_ownership(
    harness: Harness, setting: dict[str, str]
) -> None:
    result = harness.run(
        '"$FAKE_ROOT/scripts/verify-lane-database"', AAS_TEST_DATABASE_URL="", **setting
    )
    assert result.returncode != 0
    assert "ownership" in result.stderr
    assert not [call for call in harness.calls("docker") if call["args"][:2] == ["container", "rm"]]
    assert json.loads(Path(harness.environment["FAKE_STATE"]).read_text())


def test_database_cleanup_failure_cannot_report_success(harness: Harness) -> None:
    result = harness.run(
        '"$FAKE_ROOT/scripts/verify-lane-database"',
        AAS_TEST_DATABASE_URL="",
        FAKE_FAIL="container rm",
    )
    assert result.returncode != 0
    assert "cleanup failed" in result.stderr
    assert json.loads(Path(harness.environment["FAKE_STATE"]).read_text())


@pytest.mark.parametrize(
    "setting", [{"FAKE_SIGNAL": "pytest -m database"}, {"FAKE_NO_ADDRESS": "1"}]
)
def test_database_signal_and_setup_failure_remove_owned_container(
    harness: Harness, setting: dict[str, str]
) -> None:
    result = harness.run(
        '"$FAKE_ROOT/scripts/verify-lane-database"', AAS_TEST_DATABASE_URL="", **setting
    )
    assert result.returncode != 0
    if "FAKE_SIGNAL" in setting:
        assert result.returncode == _SIGTERM_EXIT
    assert json.loads(Path(harness.environment["FAKE_STATE"]).read_text()) == {}


@pytest.mark.parametrize(
    "retired",
    [
        "persistence/__init__.py",
        "data/canonical_production.py",
        "data/canonical_publish.py",
        "data/postgres_rebind.py",
    ],
)
def test_installed_package_rejects_retired_runtime(harness: Harness, retired: str) -> None:
    result = harness.run('"$FAKE_ROOT/scripts/verify-lane-build"', FAKE_RETIRED_PATH=retired)
    assert result.returncode != 0
    assert "retired/private code was installed" in result.stderr
    assert not harness.calls("aas")
    build = next(call for call in harness.calls() if call["args"][0] == "build")
    assert not Path(build["args"][-1]).parent.exists()


def test_package_lane_runs_the_installed_scenario(harness: Harness) -> None:
    """The lane must drive the scenario, and the scenario must not see the checkout."""
    result = harness.run('"$FAKE_ROOT/scripts/verify-lane-build"', PYTHONPATH=str(harness.root))
    assert result.returncode == 0, result.stderr
    assert "installed candidate provenance:" in result.stdout
    assert (
        "registration, run, re-read, backup, restore, refusal and recovery: pass" in result.stdout
    )
    calls = [
        json.loads(line)
        for line in Path(harness.environment["FAKE_SCENARIO_LOG"]).read_text().splitlines()
    ]
    assert [call["mode"] for call in calls] == ["seed", "scenario"]
    seeded, ran = calls
    # seed needs the development interpreter because the generator lives under tests/.
    assert seeded["executable"].startswith(str(harness.root))
    # The scenario must not: it runs the installed interpreter with no checkout on the path.
    assert not ran["executable"].startswith(str(harness.root))
    assert ran["pythonpath"] is None
    assert not ran["cwd"].startswith(str(harness.root))
    for call in calls:
        assert "--aas" in call["argv"]


@pytest.mark.parametrize(
    "setting",
    [
        {"FAKE_SCENARIO_FAIL": "seed"},
        {"FAKE_SCENARIO_FAIL": "scenario"},
        {"FAKE_SCENARIO": json.dumps({**_SCENARIO_DEFAULT["scenario"], "scenario": "partial"})},
        {
            "FAKE_SCENARIO": json.dumps(
                {
                    **_SCENARIO_DEFAULT["scenario"],
                    "recovery_without_marker": {
                        **_SCENARIO_DEFAULT["scenario"]["recovery_without_marker"],
                        "status": "SUCCESS",
                    },
                }
            )
        },
        {
            "FAKE_SCENARIO": json.dumps(
                {
                    **_SCENARIO_DEFAULT["scenario"],
                    "restored_runs": [{"run_id": "run-x", "identical": False}],
                }
            )
        },
        {
            "FAKE_SCENARIO": json.dumps(
                {
                    **_SCENARIO_DEFAULT["scenario"],
                    "cases": {
                        "a_cli": {"result_hash": "same"},
                        "b_cli": {"result_hash": "same"},
                        "a_api": {"result_hash": "same"},
                    },
                }
            )
        },
        {
            "FAKE_SCENARIO": json.dumps(
                {
                    **_SCENARIO_DEFAULT["scenario"],
                    "recovery_after_marker": {
                        **_SCENARIO_DEFAULT["scenario"]["recovery_after_marker"],
                        "observers_fired": ["aegis_alpha.engine.replay.replay"],
                    },
                }
            )
        },
    ],
)
def test_package_lane_rejects_a_scenario_that_did_not_prove_itself(
    harness: Harness, setting: dict[str, str]
) -> None:
    """A green exit code is not the evidence; the lane reads the document and refuses."""
    result = harness.run('"$FAKE_ROOT/scripts/verify-lane-build"', **setting)
    assert result.returncode != 0
    assert "recovery: pass" not in result.stdout
