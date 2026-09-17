"""Prove the Aegis product path from a clean wheel install, with synthetic inputs only.

The lane runs this twice. ``seed`` runs under the development interpreter, because the
synthetic input documents come from the repository's own generator under ``tests/``, which
is deliberately not shipped in the wheel. It writes plain files and registers them only by
invoking the installed ``aas`` binary in a scrubbed environment. ``scenario`` then runs
under the INSTALLED interpreter with ``-I`` and imports nothing from the checkout; it is
the part whose evidence answers "does the installed product work".

Nothing here is an application command. It is a verification probe, like the heredocs it
replaces in ``scripts/verify-lane-build``, kept in a file so it can be linted, type
checked and unit tested.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import io
import json
import os
import subprocess
import sys
from pathlib import Path
from types import FrameType
from typing import Any, cast

Document = dict[str, Any]
# Two strategies with two contracts is the floor; one strategy registered twice is not two.
MINIMUM_STRATEGIES = 2

# The compute budget has no silent default; every command below needs it explicitly.
COMPUTE_ENVIRONMENT = {
    "AAS_HOST_CPU_LIMIT": "1",
    "AAS_HOST_MEMORY_LIMIT_BYTES": str(1024 * 1024 * 1024),
    "AAS_CPU_LIMIT": "1",
    "AAS_MEMORY_LIMIT_BYTES": str(512 * 1024 * 1024),
}
# Recovery may re-derive and verify sealed results. It may not evaluate a strategy or
# replay accounting. These are the entry points that would mean it did.
FORBIDDEN_DURING_RECOVERY = (
    ("aegis_alpha.application.backtest_cli", "run_document"),
    ("aegis_alpha.engine.execution", "replay_next_open"),
    ("aegis_alpha.engine.execution", "replay_next_open_cashflows"),
    ("aegis_alpha.engine.execution", "_replay"),
    ("aegis_alpha.engine.replay", "replay"),
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def private_directory(path: Path) -> Path:
    """Create a directory the compute lock guard will accept as owned and private."""
    path.mkdir(parents=True, exist_ok=True)
    path.chmod(0o700)
    return path


def command_environment(home: Path, lock: Path) -> dict[str, str]:
    """The allowlist an installed command runs under: no checkout, no inherited venv.

    PYTHONPATH, PYTHONHOME and VIRTUAL_ENV are absent rather than empty, so an installed
    process cannot reach the development tree even by accident.
    """
    return {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": os.environ.get("HOME", str(Path.home())),
        "LANG": os.environ.get("LANG", "C.UTF-8"),
        "AAS_HOME": str(home),
        "AAS_COMPUTE_LOCK_FILE": str(lock),
        "PYTHONNOUSERSITE": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
        **COMPUTE_ENVIRONMENT,
    }


class InstalledCli:
    """Invoke the installed console script and record every invocation as evidence."""

    def __init__(self, binary: Path, home: Path, lock: Path, cwd: Path) -> None:
        self.binary = binary
        self.home = home
        self.lock = lock
        self.cwd = cwd
        self.calls: list[Document] = []

    def run(self, *arguments: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        environment = command_environment(self.home, self.lock)
        result = subprocess.run(  # noqa: S603 -- fixed installed binary, synthetic home
            [str(self.binary), *arguments],
            env=environment,
            cwd=str(self.cwd),
            text=True,
            capture_output=True,
            check=False,
            timeout=600,
        )
        self.calls.append(
            {
                "argv": [self.binary.name, *arguments],
                "cwd": str(self.cwd),
                "environment": sorted(environment),
                "pythonpath_present": "PYTHONPATH" in environment,
                "returncode": result.returncode,
            }
        )
        if check and result.returncode != 0:
            raise SystemExit(
                "installed command failed: " + " ".join(arguments) + chr(10) + result.stderr
            )
        return result

    def json(self, *arguments: str) -> Document:
        return json.loads(self.run(*arguments).stdout)

    def refused(self, *arguments: str) -> Document:
        """Run a command that must fail, and return what the operator would be told."""
        result = self.run(*arguments, check=False)
        if result.returncode == 0:
            raise SystemExit("expected a refusal but it succeeded: " + " ".join(arguments))
        return {
            "returncode": result.returncode,
            "stdout": result.stdout,
            "error": json.loads(result.stderr)["error"] if result.stderr.strip() else None,
        }


def seed(arguments: argparse.Namespace) -> Document:
    """Register one synthetic installation, writing only through the installed binary."""
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from tests.application.test_prepare_cli import register_fixture  # noqa: PLC0415

    root = Path(arguments.root).absolute()
    root.mkdir(parents=True, exist_ok=True)
    case = Path(arguments.case).absolute()
    lock = private_directory(root / "lock") / "compute.lock"
    cli = InstalledCli(Path(arguments.aas).absolute(), root / "home", lock, root)
    # The generator reads freshly published snapshot headers back through the same
    # installed binary, so nothing in the tested home is written by this interpreter.
    os.environ.update(command_environment(root / "home", lock))
    home, request = register_fixture(root, cli.json, keep_incoming=case)
    (case / "request.json").write_bytes(request.read_bytes())
    return {
        "seeded": True,
        "home": str(home),
        "request": str(case / "request.json"),
        "documents": sorted(path.name for path in case.iterdir()),
        "installed_calls": len(cli.calls),
        "any_call_saw_pythonpath": any(call["pythonpath_present"] for call in cli.calls),
    }


# Crash a real run at an exact point in the commit sequence. sys.setprofile lives in this
# child, never in the product: the profiler fires on the CALL of the marker write (nothing
# is committed yet) or on its RETURN (the DuckDB marker is committed, the SQLite result
# receipts are not). SIGKILL leaves no finalizers, which is what a lost process looks like.
_INTERRUPT_CHILD = """import os, signal, sys
from pathlib import Path
from aegis_alpha.application.run_backtest import RunBacktestRequest, run_backtest
from aegis_alpha.storage import runs

MODE, REQUEST, DIGEST, HOME, RUN_ID = sys.argv[1:6]
TARGET = runs._write_marker.__code__
WANTED = "call" if MODE == "before-marker" else "return"


def hook(frame, event, arg):
    if frame.f_code is TARGET and event == WANTED:
        sys.stdout.write("KILLED " + event + chr(10))
        sys.stdout.flush()
        os.kill(os.getpid(), signal.SIGKILL)


sys.setprofile(hook)
run_backtest(
    RunBacktestRequest(
        request=Path(REQUEST), request_sha256=DIGEST, home=Path(HOME), run_id=RUN_ID
    )
)
sys.exit("the run completed; the interruption point was never reached")
"""

_KILLED_BY_SIGNAL = -9


def installed_provenance(binary: Path, *, allow_checkout: bool) -> Document:
    """Record who is actually executing, from inside the installed interpreter."""
    import aegis_alpha  # noqa: PLC0415

    import_root = Path(aegis_alpha.__file__).resolve()
    inside_prefix = import_root.is_relative_to(Path(sys.prefix).resolve())
    if not inside_prefix and not allow_checkout:
        raise SystemExit("the scenario imported the checkout instead of the installed wheel")
    shebang = ""
    with binary.open("rb") as handle:
        first = handle.readline()
    if first.startswith(b"#!"):
        shebang = first[2:].decode("utf-8", "replace").strip()
    from importlib.metadata import version  # noqa: PLC0415

    return {
        "python_version": sys.version,
        "sys_executable": sys.executable,
        "sys_prefix": sys.prefix,
        "installed_import_root": str(import_root),
        "import_root_inside_prefix": inside_prefix,
        "console_script": str(binary.resolve()),
        "console_script_interpreter": shebang,
        "distribution_version": version("aegis-alpha-system"),
        "legacy_extra": "not_exercised",
    }


def forbidden_code_objects() -> dict[object, str]:
    """Look the forbidden entry points up by code-object identity, never by name."""
    from importlib import import_module  # noqa: PLC0415

    armed: dict[object, str] = {}
    for module_name, attribute in FORBIDDEN_DURING_RECOVERY:
        function = getattr(import_module(module_name), attribute)
        armed[function.__code__] = module_name + "." + attribute
    return armed


def recover_in_process(home: Path, lock: Path) -> Document:
    """Run db recover with calculation and network observers armed around it.

    Re-deriving and verifying an already sealed result is exactly what recovery is for, so
    storage projection is allowed. What must never happen is a strategy evaluation or an
    accounting replay, and no socket may be opened at all.
    """
    import socket  # noqa: PLC0415

    from aegis_alpha.application.cli import main as cli_main  # noqa: PLC0415

    armed = forbidden_code_objects()
    fired: list[str] = []

    def observer(frame: FrameType, event: str, _arg: object) -> None:
        if event == "call":
            name = armed.get(frame.f_code)
            if name is not None:
                fired.append(name)
                raise AssertionError("recovery entered a calculation path: " + name)

    def refuse(*_args: object, **_kwargs: object) -> object:
        fired.append("network")
        raise AssertionError("recovery attempted a network call")

    os.environ.update(command_environment(home, lock))
    saved = (socket.create_connection, socket.getaddrinfo, socket.socket.connect)
    socket.create_connection = refuse  # ty: ignore[invalid-assignment]
    socket.getaddrinfo = refuse  # ty: ignore[invalid-assignment]
    socket.socket.connect = refuse  # ty: ignore[invalid-assignment]
    sys.setprofile(observer)
    # cli_main prints its receipt, and this scenario's own stdout is a single JSON
    # document, so the receipt is captured and reported rather than interleaved.
    captured = io.StringIO()
    try:
        with contextlib.redirect_stdout(captured):
            code = cli_main(["--home", str(home), "db", "recover"])
    finally:
        sys.setprofile(None)
        socket.create_connection = saved[0]
        socket.getaddrinfo = saved[1]
        socket.socket.connect = saved[2]
    return {
        "exit_code": code,
        "receipt": json.loads(captured.getvalue()) if captured.getvalue().strip() else None,
        "armed_calculation_targets": sorted(armed.values()),
        "armed_network_targets": [
            "socket.create_connection",
            "socket.getaddrinfo",
            "socket.socket.connect",
        ],
        "observers_armed": True,
        "observers_fired": fired,
        # publication.recover_operations returns provider_calls 0 as a literal. It is
        # reported, never treated as a measurement; the observers above are the evidence.
        "provider_calls_is_a_measurement": False,
    }


def interrupt_run(  # noqa: PLR0913, PLR0917 -- one crash point needs every coordinate
    python: Path, home: Path, lock: Path, request: Path, run_id: str, mode: str, cwd: Path
) -> Document:
    """Crash one real run at an exact point and report what survived."""
    child = cwd / ("interrupt_" + mode + ".py")
    child.write_text(_INTERRUPT_CHILD, encoding="utf-8")
    result = subprocess.run(  # noqa: S603 -- fixed installed interpreter, synthetic home
        [
            str(python),
            "-I",
            str(child),
            mode,
            str(request),
            sha256_file(request),
            str(home),
            run_id,
        ],
        env=command_environment(home, lock),
        cwd=str(cwd),
        text=True,
        capture_output=True,
        check=False,
        timeout=600,
    )
    if result.returncode != _KILLED_BY_SIGNAL:
        raise SystemExit("the interruption child was not killed: " + str(result.returncode))
    return {
        "mode": mode,
        "run_id": run_id,
        "returncode": result.returncode,
        "stdout": result.stdout,
    }


RESULT_TABLES = ("equity_points", "positions", "signals", "simulated_trades", "target_weights")
# Everything two reads of one recorded run must agree on, whichever home holds it.
RUN_IDENTITY_KEYS = (
    "status",
    "bundle_id",
    "prior_run_id",
    "request_hash",
    "engine_hash",
    "environment_hash",
    "reason",
    "created_at_us",
    "completed_at_us",
    "result_hash",
    "module",
    "artifacts",
    "artifact_sizes",
    "table_hashes",
    "table_counts",
    "metrics",
    "strategy_pins",
)


def second_strategy(source: Path, target: Path) -> Document:
    """Derive a genuinely different strategy from the seeded bundle.

    A re-versioned identical contract would register a second row without being a second
    strategy. Widening the offensive selection changes the contract hash and the decisions
    it produces, which is what makes the two cases independent evidence.
    """
    from aegis_alpha.data.serialization import canonical_json_bytes  # noqa: PLC0415

    body = json.loads(source.read_bytes())
    body["bundle_id"] = "synthetic-probe-wide"
    body["bundle_version"] = "1"
    body["contract"]["pack"][0]["offensive_config"]["top_n"] = 2
    target.write_bytes(canonical_json_bytes(body))
    return {"strategy_id": "synthetic-probe-wide", "version": "1", "sha256": sha256_file(target)}


def request_for(source: Path, target: Path, strategy: Document, imported: Document) -> Path:
    from aegis_alpha.data.serialization import canonical_json_bytes  # noqa: PLC0415

    body = json.loads(source.read_bytes())
    body["strategy"]["strategy_id"] = strategy["strategy_id"]
    body["strategy"]["version"] = strategy["version"]
    body["strategy"]["raw_sha256"] = imported["raw_sha256"]
    body["strategy"]["contract_sha256"] = imported["contract_sha256"]
    target.write_bytes(canonical_json_bytes(body))
    return target


def stored_table_counts(home: Path, run_id: str) -> dict[str, int]:
    """Count the stored result rows directly, without going through the run reader."""
    from aegis_alpha.storage.workspace import open_workspace  # noqa: PLC0415

    with open_workspace(home) as workspace:
        counts: dict[str, int] = {}
        for table in RESULT_TABLES:
            row = workspace.market.execute(
                "SELECT count(*) FROM " + table + " WHERE run_id=?",  # noqa: S608 -- fixed names
                [run_id],
            ).fetchone()
            counts[table] = 0 if row is None else int(row[0])
        return counts


def identity_of(shown: Document) -> Document:
    return {key: shown[key] for key in RUN_IDENTITY_KEYS}


def tree_fingerprint(root: Path) -> dict[str, str]:
    return {
        str(path.relative_to(root)): sha256_file(path)
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def restore_refusals(cli: InstalledCli, backup_root: Path, work: Path, existing: Path) -> Document:
    """Five ways a restore must refuse, including one that rewrites the manifest to match."""
    import shutil  # noqa: PLC0415

    before = tree_fingerprint(existing)
    refusals = {
        "existing_home": cli.refused(
            "--home", str(existing), "db", "restore", "--backup", str(backup_root)
        )
    }
    refusals["existing_home_unchanged"] = tree_fingerprint(existing) == before

    def copy(name: str) -> Path:
        target = work / ("backup_" + name)
        shutil.copytree(backup_root, target)
        return target

    corrupt = copy("corrupt")
    raw = bytearray((corrupt / "state.sqlite3").read_bytes())
    raw[len(raw) // 2] ^= 0xFF
    (corrupt / "state.sqlite3").write_bytes(bytes(raw))
    refusals["corrupt_store"] = cli.refused(
        "--home", str(work / "restored_corrupt"), "db", "restore", "--backup", str(corrupt)
    )

    missing = copy("missing")
    artifact = min(path for path in (missing / "runs").rglob("*") if path.is_file())
    artifact.unlink()
    refusals["missing_artifact"] = cli.refused(
        "--home", str(work / "restored_missing"), "db", "restore", "--backup", str(missing)
    )

    truncated = copy("truncated")
    min(path for path in (truncated / "runs").rglob("*") if path.is_file()).write_bytes(b"")
    refusals["truncated_artifact"] = cli.refused(
        "--home", str(work / "restored_truncated"), "db", "restore", "--backup", str(truncated)
    )

    # The interesting one: tamper with an artifact AND rewrite its manifest entry, so byte
    # accounting alone would accept it. Only re-deriving the run can still refuse.
    tampered = copy("tampered")
    target = min(path for path in (tampered / "runs").rglob("*") if path.is_file())
    payload = bytearray(target.read_bytes())
    payload[0] ^= 0xFF
    target.write_bytes(bytes(payload))
    manifest = json.loads((tampered / "backup.json").read_text())
    manifest["files"][str(target.relative_to(tampered))] = {
        "size_bytes": len(payload),
        "sha256": hashlib.sha256(bytes(payload)).hexdigest(),
    }
    (tampered / "backup.json").write_text(json.dumps(manifest))
    refusals["tampered_artifact_and_manifest"] = cli.refused(
        "--home", str(work / "restored_tampered"), "db", "restore", "--backup", str(tampered)
    )
    # The first three fail byte accounting before anything is written, so no home appears.
    # The tampered one passes byte accounting on purpose and is only caught when the
    # restored installation is re-verified, so a home does exist. What matters is that it
    # is never reported as restored: the receipt says restore-incomplete and the command
    # still fails.
    refusals["nothing_written_before_verification"] = not any(
        (work / ("restored_" + name)).exists() for name in ("corrupt", "missing", "truncated")
    )
    incomplete = work / "restored_tampered"
    refusals["tampered_home_is_marked_incomplete"] = (
        incomplete.is_dir()
        and json.loads((incomplete / "installation.json").read_text())["phase"]
        == "restore-incomplete"
    )
    refusals["tampered_home_is_unusable"] = cli.refused("--home", str(incomplete), "db", "verify")
    return refusals


def run_state(home: Path) -> Document:
    """Statuses and committed result markers, read straight from the two stores."""
    from aegis_alpha.storage.workspace import open_workspace  # noqa: PLC0415

    with open_workspace(home) as workspace:
        return {
            "statuses": {
                str(row[0]): str(row[1])
                for row in workspace.state.execute("SELECT run_id, status FROM runs")
            },
            "markers": sorted(
                str(row[0])
                for row in workspace.market.execute("SELECT run_id FROM result_commits").fetchall()
            ),
        }


def require(condition: bool, message: str) -> None:  # noqa: FBT001 -- an assertion helper
    if not condition:
        raise SystemExit("installed scenario: " + message)


def cleanup_receipt(owned: list[Path], locks: list[Path]) -> Document:
    """Remove only what this scenario created and prove no lock is still held."""
    import fcntl  # noqa: PLC0415
    import shutil  # noqa: PLC0415

    removed = []
    for path in owned:
        if path.exists():
            shutil.rmtree(path, ignore_errors=True)
        removed.append({"path": str(path), "remaining": path.exists()})
    probes = []
    for path in locks:
        if not path.exists():
            probes.append({"path": str(path), "state": "absent"})
            continue
        handle = os.open(path, os.O_RDWR)
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(handle, fcntl.LOCK_UN)
            probes.append({"path": str(path), "state": "free"})
        except OSError:
            probes.append({"path": str(path), "state": "STILL HELD"})
        finally:
            os.close(handle)
    return {"removed": removed, "lock_probes": probes}


def scenario(arguments: argparse.Namespace) -> Document:
    import shutil  # noqa: PLC0415

    from aegis_alpha.application.run_backtest import (  # noqa: PLC0415
        RunBacktestRequest,
        run_backtest,
    )

    binary = Path(arguments.aas).absolute()
    case = Path(arguments.case).absolute()
    home = Path(arguments.home).absolute()
    work = Path(arguments.work).absolute()
    work.mkdir(parents=True, exist_ok=True)
    lock = private_directory(work / "lock") / "compute.lock"
    cli = InstalledCli(binary, home, lock, work)
    evidence: Document = {
        "provenance": installed_provenance(binary, allow_checkout=arguments.allow_checkout)
    }

    request_a = case / "request.json"
    digest_a = sha256_file(request_a)
    # The add-on is a separate explicit install, so the command must say so and compute
    # nothing before it exists.
    evidence["add_on_required"] = cli.refused(
        "run", "execute", "--request", str(request_a), "--sha256", digest_a
    )
    require(
        "run-install" in str(evidence["add_on_required"]["error"]),
        "refusal must name the add-on install",
    )
    require(not run_state(home)["markers"], "a refused run must leave no marker")
    evidence["run_install"] = cli.json("db", "run-install")

    spec = second_strategy(case / "strategy.json", work / "strategy-wide.json")
    imported = cli.json(
        "strategy",
        "import",
        str(work / "strategy-wide.json"),
        "--id",
        spec["strategy_id"],
        "--version",
        spec["version"],
        "--sha256",
        spec["sha256"],
    )
    request_b = request_for(request_a, work / "request-wide.json", spec, imported)
    digest_b = sha256_file(request_b)
    registered = cli.json("strategy", "list")["strategies"]
    evidence["strategies"] = [
        {key: entry[key] for key in ("strategy_id", "version", "contract_sha256")}
        for entry in registered
    ]
    require(
        len({entry["strategy_id"] for entry in registered}) >= MINIMUM_STRATEGIES,
        "two strategies must be registered",
    )
    require(
        len({entry["contract_sha256"] for entry in registered}) >= MINIMUM_STRATEGIES,
        "the second strategy must be a different contract, not a re-version",
    )

    receipt_a = cli.json(
        "run",
        "execute",
        "--request",
        str(request_a),
        "--sha256",
        digest_a,
        "--envelope-output",
        str(work / "envelope-a.json"),
    )
    receipt_b = cli.json("run", "execute", "--request", str(request_b), "--sha256", digest_b)
    os.environ.update(command_environment(home, lock))
    api = cast(
        "Document",
        run_backtest(RunBacktestRequest(request=request_a, request_sha256=digest_a, home=home)),
    )
    run_a = str(receipt_a["run"]["run_id"])
    evidence["cases"] = {
        "a_cli": {
            "run_id": run_a,
            "result_hash": receipt_a["run"]["result_hash"],
            "target_weights": receipt_a["target_weights"],
        },
        "b_cli": {
            "run_id": receipt_b["run"]["run_id"],
            "result_hash": receipt_b["run"]["result_hash"],
            "target_weights": receipt_b["target_weights"],
        },
        "a_api": {"run_id": api["run"]["run_id"], "result_hash": api["run"]["result_hash"]},
    }
    require(
        receipt_a["run"]["result_hash"] != receipt_b["run"]["result_hash"],
        "two different strategies must not produce one result",
    )
    require(
        api["run"]["result_hash"] == receipt_a["run"]["result_hash"]
        and api["run"]["run_id"] != run_a,
        "the API and the CLI must be one execution path with separate run identity",
    )

    # Drop every seeded import document and every earlier working path, then prove the
    # registered installation alone still runs and reads.
    detached = work / "detached"
    detached.mkdir()
    moved = detached / "request-a.json"
    moved.write_bytes(request_a.read_bytes())
    shutil.rmtree(case)
    (work / "strategy-wide.json").unlink()
    cli.cwd = detached
    repeated = cli.json("run", "execute", "--request", str(moved), "--sha256", digest_a)
    require(
        repeated["run"]["result_hash"] == receipt_a["run"]["result_hash"],
        "re-execution without the import documents must reproduce the result",
    )
    evidence["reexecuted_without_seed"] = {
        "case_directory_present": case.exists(),
        "run_id": repeated["run"]["run_id"],
        "result_hash": repeated["run"]["result_hash"],
    }

    shown = cli.json("run", "show", "--run-id", run_a)["run"]
    listed = cli.json("run", "list")["runs"]
    evidence["fresh_process_read"] = {
        "run_id": run_a,
        "verified": cli.json("db", "verify"),
        "recorded_runs": len(listed),
        "stored_table_counts": stored_table_counts(home, run_a),
        "research_only": shown["research_only"],
    }
    require(
        evidence["fresh_process_read"]["stored_table_counts"] == shown["table_counts"],
        "the stored rows must match the counts the run reader re-derives",
    )
    return finish_scenario(
        evidence, cli, binary, home, work, lock, detached, moved, digest_a, listed
    )


def finish_scenario(  # noqa: PLR0913, PLR0915, PLR0917 -- the rest of one linear scenario
    evidence: Document,
    cli: InstalledCli,
    binary: Path,
    home: Path,
    work: Path,
    lock: Path,
    detached: Path,
    request: Path,
    digest: str,
    listed: list[Document],
) -> Document:
    """Back up a run-bearing installation, restore it elsewhere, then interrupt and recover.

    The order matters and is a constraint, not a preference. The backup is taken while every
    run is SUCCESS, because an interrupted run leaves sealed files with no artifact rows;
    verification never walks those, while the backup copies the whole runs tree. Keeping the
    interruptions on the RESTORED home keeps them out of the clean backup evidence and also
    proves the restored installation is operational rather than merely readable.
    """
    backup_root = work / "backup"
    evidence["backup"] = cli.json("db", "backup", "--output", str(backup_root))
    require(evidence["backup"]["secrets_included"] is False, "a backup must exclude secrets")
    require(
        not (backup_root / "secrets").exists()
        and json.loads((backup_root / "runtime.json").read_text())["providers"] == {},
        "no secret material may reach the backup directory",
    )

    restored_home = work / "restored"
    evidence["restore"] = cli.json(
        "--home", str(restored_home), "db", "restore", "--backup", str(backup_root)
    )
    restored = InstalledCli(binary, restored_home, lock, detached)
    comparisons = []
    for row in listed:
        run_id = str(row["run_id"])
        before = identity_of(cli.json("run", "show", "--run-id", run_id)["run"])
        after = identity_of(restored.json("run", "show", "--run-id", run_id)["run"])
        counts = stored_table_counts(restored_home, run_id)
        require(before == after, "restored run " + run_id + " differs from the original")
        require(counts == after["table_counts"], "restored rows differ for " + run_id)
        comparisons.append({"run_id": run_id, "identical": True, "stored_table_counts": counts})
    evidence["restored_runs"] = comparisons
    require(bool(comparisons), "the backup under test must contain recorded runs")
    evidence["restored_verify"] = restored.json("db", "verify")

    evidence["restore_refusals"] = restore_refusals(cli, backup_root, work, restored_home)
    require(
        evidence["restore_refusals"]["existing_home_unchanged"] is True
        and evidence["restore_refusals"]["nothing_written_before_verification"] is True
        and evidence["restore_refusals"]["tampered_home_is_marked_incomplete"] is True,
        "a refused restore must leave the target untouched and never claim success",
    )

    python = Path(sys.executable)
    interrupted_id = "run-interrupted-probe"
    evidence["interrupted_without_marker"] = interrupt_run(
        python, restored_home, lock, request, interrupted_id, "before-marker", detached
    )
    state = run_state(restored_home)
    require(state["statuses"][interrupted_id] == "RUNNING", "a lost run stays RUNNING")
    require(interrupted_id not in state["markers"], "no marker may exist before the marker write")
    evidence["recovery_without_marker"] = recover_in_process(restored_home, lock)
    require(
        run_state(restored_home)["statuses"][interrupted_id] == "INTERRUPTED",
        "a run lost before its marker must end INTERRUPTED",
    )
    evidence["recovery_without_marker"]["status"] = "INTERRUPTED"
    recomputed = restored.json(
        "run",
        "execute",
        "--request",
        str(request),
        "--sha256",
        digest,
        "--prior-run-id",
        interrupted_id,
    )
    linked = restored.json("run", "show", "--run-id", str(recomputed["run"]["run_id"]))["run"]
    require(
        linked["prior_run_id"] == interrupted_id and linked["run_id"] != interrupted_id,
        "a recomputation must be a new run linked to its predecessor",
    )
    evidence["recomputation"] = {"run_id": linked["run_id"], "prior_run_id": linked["prior_run_id"]}

    resumed_id = "run-resumed-probe"
    evidence["interrupted_after_marker"] = interrupt_run(
        python, restored_home, lock, request, resumed_id, "after-marker", detached
    )
    state = run_state(restored_home)
    require(state["statuses"][resumed_id] == "RUNNING", "a lost run stays RUNNING")
    # The profile return event also fires when a function unwinds with an exception, so the
    # marker has to be observed rather than assumed before recovery is asked to resume.
    require(resumed_id in state["markers"], "the marker must be committed before recovery resumes")
    sealed = sorted(
        (path.name, sha256_file(path))
        for path in (restored_home / "runs" / resumed_id).iterdir()
        if path.is_file()
    )
    evidence["recovery_after_marker"] = recover_in_process(restored_home, lock)
    require(
        run_state(restored_home)["statuses"][resumed_id] == "SUCCESS",
        "a run lost after its marker must be finished, not abandoned",
    )
    require(
        sealed
        == sorted(
            (path.name, sha256_file(path))
            for path in (restored_home / "runs" / resumed_id).iterdir()
            if path.is_file()
        ),
        "resuming the commit must not rewrite the sealed files",
    )
    evidence["recovery_after_marker"]["status"] = "SUCCESS"
    evidence["recovery_after_marker"]["sealed_files_unchanged"] = True
    resumed = restored.json("run", "show", "--run-id", resumed_id)["run"]
    evidence["recovery_after_marker"]["result_hash"] = resumed["result_hash"]

    evidence["installed_calls"] = cli.calls + restored.calls
    evidence["any_call_saw_pythonpath"] = any(
        call["pythonpath_present"] for call in evidence["installed_calls"]
    )
    require(evidence["any_call_saw_pythonpath"] is False, "no installed call may see PYTHONPATH")
    evidence["cleanup"] = cleanup_receipt(
        [work],
        [home / ".storage.lock", restored_home / ".storage.lock", lock],
    )
    evidence["scenario"] = "complete"
    return evidence


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_subparsers(dest="mode", required=True)
    seeding = modes.add_parser("seed", help="Register one synthetic installation")
    seeding.add_argument("--aas", required=True, help="Installed aas console script")
    seeding.add_argument("--root", required=True, help="Scratch root for the seed")
    seeding.add_argument("--case", required=True, help="Where the input documents are kept")
    running = modes.add_parser("scenario", help="Run the installed end-to-end scenario")
    running.add_argument("--aas", required=True, help="Installed aas console script")
    running.add_argument("--home", required=True, help="The seeded installation")
    running.add_argument("--case", required=True, help="The seeded input documents")
    running.add_argument("--work", required=True, help="Scratch root this scenario owns")
    running.add_argument(
        "--allow-checkout",
        action="store_true",
        help="Permit a checkout import; for the unit test only, never for the lane",
    )
    arguments = parser.parse_args(argv)
    document = seed(arguments) if arguments.mode == "seed" else scenario(arguments)
    print(json.dumps(document, indent=2, sort_keys=True))  # noqa: T201 -- the evidence itself
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
