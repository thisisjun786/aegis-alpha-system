"""Synthetic preparation CLI and exclusive output sealing regressions."""

from __future__ import annotations

import hashlib
import json
import os
import sys
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

from aegis_alpha.application import prepare_cli
from aegis_alpha.application.backtest_prepare import (
    PreparedBacktest,
    PrepareRequest,
    prepare_backtest,
)
from aegis_alpha.application.cli import main
from aegis_alpha.application.compute_cli import price_compute
from aegis_alpha.compute_resources import ComputeBudget
from aegis_alpha.data.descriptor_tree import DescriptorTree
from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.storage.input_pins import (
    ConventionPin,
    DefinitionPin,
    read_convention,
    read_definition,
)
from aegis_alpha.storage.membership_pins import IdentityPin, UniversePin, read_membership_pins
from aegis_alpha.storage.workspace import Workspace, initialize, open_workspace
from tests.application.test_backtest_prepare import (
    BUDGET,
    reject_accounting,
    select_only,
    stored_request,
)
from tests.application.test_storage_cli import run_cli
from tests.storage.test_research_inputs import registration_state

type Document = dict[str, Any]
type CLI = Callable[..., Document]
_USAGE_ERROR = 2


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def arguments(home: Path, request: Path, output: Path) -> list[str]:
    return [
        "--home",
        str(home),
        "prepare",
        "--request",
        str(request),
        "--sha256",
        sha(request.read_bytes()),
        "--output",
        str(output),
    ]


@pytest.fixture(autouse=True)
def compute_environment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AAS_HOST_CPU_LIMIT", "1")
    monkeypatch.setenv("AAS_HOST_MEMORY_LIMIT_BYTES", str(1024 * 1024 * 1024))
    monkeypatch.setenv("AAS_CPU_LIMIT", "1")
    monkeypatch.setenv("AAS_MEMORY_LIMIT_BYTES", str(512 * 1024 * 1024))
    monkeypatch.setenv("AAS_COMPUTE_LOCK_FILE", str(tmp_path / "compute.lock"))


@pytest.fixture(scope="module")
def case(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, Path]:
    root = tmp_path_factory.mktemp("prepare-case")
    incoming = root / "incoming"
    incoming.mkdir()
    home = root / "home"
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        body = stored_request(workspace, incoming)
    request = root / "request.json"
    request.write_bytes(canonical_json_bytes(body))
    for path in incoming.iterdir():
        path.unlink()
    incoming.rmdir()
    return home, request


def test_wrong_hash_is_controlled_before_workspace(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    request = tmp_path / "request.json"
    request.write_bytes(b"{}\n")
    output = tmp_path / "envelope.json"
    args = arguments(tmp_path / "absent", request, output)
    args[args.index("--sha256") + 1] = sha(b"{}")
    assert main(args) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert json.loads(captured.err).keys() == {"error"}
    assert not output.exists()
    assert not Path(str(output) + ".preparation.json").exists()


@pytest.mark.parametrize(
    "raw", [b"{}", b"{", b'{"schema":1,"schema":2}', b"\xff", b"x" * (1024 * 1024 + 1)]
)
def test_malformed_request_has_no_outputs(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], raw: bytes
) -> None:
    request = tmp_path / "request.json"
    request.write_bytes(raw)
    output = tmp_path / "envelope.json"
    assert main(arguments(tmp_path / "absent", request, output)) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert json.loads(captured.err).keys() == {"error"}
    assert sorted(path.name for path in tmp_path.iterdir()) == [request.name]


@pytest.mark.parametrize("missing", ["--request", "--sha256", "--output"])
def test_required_arguments(tmp_path: Path, missing: str) -> None:
    request = tmp_path / "request.json"
    request.write_bytes(b"{}")
    args = arguments(tmp_path / "home", request, tmp_path / "out")
    index = args.index(missing)
    del args[index : index + 2]
    with pytest.raises(SystemExit) as error:
        main(args)
    assert error.value.code == _USAGE_ERROR


@pytest.mark.parametrize("leaf", ["envelope.json", "envelope.json.preparation.json"])
@pytest.mark.parametrize("kind", ["file", "symlink", "dangling", "hardlink", "directory"])
def test_existing_outputs_never_overwritten(
    case: tuple[Path, Path],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    leaf: str,
    kind: str,
) -> None:
    home, request = case
    foreign = tmp_path / "foreign"
    foreign.write_bytes(b"foreign content")
    existing = tmp_path / leaf
    if kind == "file":
        existing.write_bytes(b"existing output")
    elif kind == "symlink":
        existing.symlink_to(foreign)
    elif kind == "dangling":
        existing.symlink_to(tmp_path / "missing")
    elif kind == "hardlink":
        existing.hardlink_to(foreign)
    else:
        existing.mkdir()
    before = existing.lstat()
    assert main(arguments(home, request, tmp_path / "envelope.json")) == 1
    assert capsys.readouterr().out == ""
    assert os.path.samestat(before, existing.lstat())
    assert foreign.read_bytes() == b"foreign content"
    assert sorted(path.name for path in tmp_path.iterdir()) == sorted([foreign.name, leaf])


@pytest.mark.parametrize("kind", ["symlink", "fifo", "directory", "parent"])
def test_descriptor_request_admission(
    case: tuple[Path, Path], tmp_path: Path, capsys: pytest.CaptureFixture[str], kind: str
) -> None:
    home, request = case
    candidate = tmp_path / "request"
    if kind == "symlink":
        candidate.symlink_to(request)
    elif kind == "fifo":
        os.mkfifo(candidate)
    elif kind == "directory":
        candidate.mkdir()
    else:
        candidate.symlink_to(request.parent, target_is_directory=True)
        candidate /= request.name
    args = arguments(home, request, tmp_path / "envelope.json")
    args[args.index("--request") + 1] = str(candidate)
    assert main(args) == 1
    assert capsys.readouterr().out == ""
    assert not (tmp_path / "envelope.json").exists()


def test_no_implicit_compute_budget(
    case: tuple[Path, Path],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in (
        "AAS_HOST_CPU_LIMIT",
        "AAS_HOST_MEMORY_LIMIT_BYTES",
        "AAS_CPU_LIMIT",
        "AAS_MEMORY_LIMIT_BYTES",
        "AAS_COMPUTE_LOCK_FILE",
    ):
        monkeypatch.delenv(name)
    home, request = case
    assert main(arguments(home, request, tmp_path / "envelope.json")) == 1
    assert capsys.readouterr().out == ""
    assert not (tmp_path / "envelope.json").exists()


def test_compute_lock_alias_rejected(
    case: tuple[Path, Path],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    home, request = case
    monkeypatch.setenv("AAS_COMPUTE_LOCK_FILE", str(home / ".storage.lock"))
    assert main(arguments(home, request, tmp_path / "envelope.json")) == 1
    assert capsys.readouterr().out == ""
    assert not (tmp_path / "envelope.json").exists()


def inventory(home: Path) -> dict[str, str]:
    return {
        str(path.relative_to(home)): sha(path.read_bytes())
        for path in home.rglob("*")
        if path.is_file()
    }


def test_readonly_preparation_under_lease_and_exact_sidecar(
    case: tuple[Path, Path],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    home, request = case
    active = []
    observed: list[PreparedBacktest] = []

    @contextmanager
    def lease(*, excluded_locks: tuple[Path, ...]) -> Iterator[ComputeBudget | None]:
        with price_compute(excluded_locks=excluded_locks) as budget:
            active.append("lease")
            try:
                yield budget
            finally:
                active.pop()

    @contextmanager
    def admitted(path: Path) -> Iterator[Workspace]:
        assert active == ["lease"]
        with open_workspace(path) as workspace:
            assert workspace.state.execute("PRAGMA query_only").fetchone()[0] == 1
            yield workspace

    def prepare(
        workspace: Workspace, request: PrepareRequest, *, budget: ComputeBudget
    ) -> PreparedBacktest:
        before = inventory(home)
        registered = registration_state(workspace)
        assert workspace.strategies is not None
        connections = (workspace.state, workspace.strategies)
        for connection in connections:
            connection.set_authorizer(select_only)
        sys.setprofile(reject_accounting)
        try:
            result = prepare_backtest(workspace, request, budget=budget)
        finally:
            sys.setprofile(None)
            for connection in connections:
                connection.set_authorizer(None)
        assert inventory(home) == before
        assert registration_state(workspace) == registered
        observed.append(result)
        return result

    monkeypatch.setattr(prepare_cli, "price_compute", lease)
    monkeypatch.setattr(prepare_cli, "open_workspace", admitted)
    monkeypatch.setattr(prepare_cli, "prepare_backtest", prepare)
    outputs = []
    for name in ("first.json", "second.json"):
        assert main(arguments(home, request, tmp_path / name)) == 0
        receipt = json.loads(capsys.readouterr().out)
        assert receipt["prepared"] is True
        assert receipt["certified"] is False
        assert receipt["request_hash"] == observed[-1].request_hash
        assert receipt["request_sha256"] == sha(request.read_bytes())
        for key, payload in (
            ("envelope", observed[-1].envelope.canonical_bytes),
            ("preparation", observed[-1].provenance),
        ):
            path = Path(receipt[key]["path"])
            assert path.read_bytes() == payload
            assert receipt[key]["sha256"] == sha(path.read_bytes())
        assert receipt["preparation"]["path"] == str(tmp_path / name) + ".preparation.json"
        outputs.append(receipt)
    assert outputs[0]["preparation"]["sha256"] == outputs[1]["preparation"]["sha256"]
    assert outputs[0]["envelope"]["sha256"] == outputs[1]["envelope"]["sha256"]
    assert active == []


@pytest.mark.parametrize(
    "fault",
    ["file_fsync", "directory_fsync", "foreign_sidecar", "replacement", "parent_replacement"],
)
def test_partial_sealing_never_claims_success(
    case: tuple[Path, Path],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    home, request = case
    parent = tmp_path / "outputs"
    parent.mkdir()
    output = parent / "envelope.json"
    sidecar = parent / "envelope.json.preparation.json"
    sync = os.fsync
    writes = []

    def fsync(descriptor: int) -> None:
        # Trigger at exact descriptor events, never elapsed time or polling.
        if output.exists():
            writes.append(descriptor)
            if fault == "file_fsync":
                raise OSError("injected output fsync failure")
            if fault == "foreign_sidecar" and not sidecar.exists():
                sidecar.write_bytes(b"foreign sidecar")
            if fault == "replacement" and sidecar.exists():
                output.unlink()
                output.write_bytes(b"foreign replacement")
            if fault == "parent_replacement" and sidecar.exists():
                parent.rename(tmp_path / "detached")
                parent.mkdir()
                (parent / "foreign").write_bytes(b"foreign parent")
        sync(descriptor)

    def fail_directory(_tree: DescriptorTree, _relative: str = ".") -> None:
        raise OSError("injected directory fsync failure")

    monkeypatch.setattr(os, "fsync", fsync)
    if fault == "directory_fsync":
        monkeypatch.setattr(DescriptorTree, "fsync_directory", fail_directory)
    assert main(arguments(home, request, output)) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert json.loads(captured.err).keys() == {"error"}
    assert writes
    foreign_files = {
        "foreign_sidecar": {sidecar: b"foreign sidecar"},
        "replacement": {output: b"foreign replacement"},
        "parent_replacement": {parent / "foreign": b"foreign parent"},
    }
    for path, raw in foreign_files.get(fault, {}).items():
        assert path.read_bytes() == raw


def fixture_documents(root: Path) -> Document:
    """Reuse T18's synthetic generator; this seed is not the tested installation."""
    incoming = root / "incoming"
    incoming.mkdir()
    seed = root / "seed"
    initialize(seed)
    with open_workspace(seed, writable=True, strategy_write=True) as workspace:
        body = stored_request(workspace, incoming)
        for ref in body["refs"]:
            pin = ref["pin"]
            if ref["ref_kind"].startswith("convention:"):
                raw = read_convention(workspace.state, ConventionPin(**pin))
            elif ref["ref_kind"] == "membership":
                raw = read_definition(workspace, DefinitionPin(**pin), budget=BUDGET)
            elif ref["ref_kind"] in ("identity", "universe"):
                pair = read_membership_pins(
                    workspace.state,
                    IdentityPin(**pin) if ref["ref_kind"] == "identity" else None,
                    UniversePin(**pin) if ref["ref_kind"] == "universe" else None,
                    max_materialization_bytes=BUDGET.memory_limit_bytes,
                )
                member = pair.identity if ref["ref_kind"] == "identity" else pair.universe
                assert member is not None
                raw = member.canonical_bytes
            else:
                continue
            (incoming / (ref["ref_kind"].replace(":", "-") + ".json")).write_bytes(raw)
    return body


def register_fixture(root: Path, cli: CLI) -> tuple[Path, Path]:
    """All writes to the tested home go through init/registration CLI commands."""
    body = fixture_documents(root)
    incoming = root / "incoming"
    initialized = cli("init")
    home = Path(initialized["home"])
    strategy = body["strategy"]
    path = incoming / "strategy.json"
    imported = cli(
        "strategy",
        "import",
        str(path),
        "--id",
        strategy["strategy_id"],
        "--version",
        strategy["version"],
        "--sha256",
        sha(path.read_bytes()),
    )
    strategy.update(raw_sha256=imported["raw_sha256"], contract_sha256=imported["contract_sha256"])
    strategy["strategy_store_id"] = json.loads((home / "installation.json").read_bytes())["stores"][
        "strategies"
    ]["store_id"]
    for name in ("signal", "outcomes", "sessions"):
        source = incoming / (name + ".sqlite3")
        imported = cli(
            "db", "source-import", str(source), "--id", name, "--sha256", sha(source.read_bytes())
        )
        assert imported["source_id"] == name
        spec = incoming / (name + ".json")
        cli(
            "data",
            "register-sessions" if name == "sessions" else "register-prices",
            "--spec",
            str(spec),
            "--sha256",
            sha(spec.read_bytes()),
        )
    for binding in body["bindings"]:
        ref = next(ref for ref in body["refs"] if ref["ref_id"] == binding["ref_id"])
        kind = ref["ref_kind"]
        if kind == "generation":
            record = cli(
                "data", "inspect", "--dataset", ref["ref_id"], "--version", ref["ref_version"]
            )
            pin = {
                key: record[key]
                for key in ("dataset_id", "version", "generation_id", "chain_hash", "manifest_hash")
            }
        else:
            spec = incoming / (kind.replace(":", "-") + ".json")
            if kind in ("identity", "universe"):
                document = json.loads(spec.read_bytes())
                # Registration times belong to the actual fresh publication, not the seed.
                with open_workspace(home) as workspace:
                    document["sources"] = [
                        {
                            **dict(
                                workspace.state.execute(
                                    "SELECT * FROM source_snapshots WHERE snapshot_id=?",
                                    (source["snapshot_id"],),
                                ).fetchone()
                            ),
                            "files": source["files"],
                        }
                        for source in document["sources"]
                    ]
                spec.write_bytes(canonical_json_bytes(document))
            result = cli(
                "data",
                "convention-import" if kind.startswith("convention:") else "binding-import",
                "--spec",
                str(spec),
                "--sha256",
                sha(spec.read_bytes()),
            )
            pin = result["pin"]
        ref["pin"] = pin
        ref["hash"] = binding["hash"] = pin.get(
            "chain_hash", pin.get("content_hash", pin.get("hash"))
        )
    request = root / "request.json"
    request.write_bytes(canonical_json_bytes(body))
    for path in incoming.iterdir():
        path.unlink()
    incoming.rmdir()
    return home, request


def assert_accounting(response: Document) -> None:
    result = response["result"]
    assert [row["date"] for row in result["nav"]] == [
        "2026-01-29",
        "2026-02-02",
        "2026-02-26",
        "2026-03-02",
        "2026-03-30",
    ]
    assert [row["equity"] for row in result["nav"]] == pytest.approx([100, 100, 80, 80, 80])
    assert [
        (row["decision_date"], row["execution_date"], row["symbol"]) for row in result["fills"]
    ] == [
        ("2026-01-29", "2026-02-02", "ASSET_A"),
        ("2026-02-26", "2026-03-02", "ASSET_A"),
        ("2026-02-26", "2026-03-02", "ASSET_B"),
    ]
    assert [row["shares"] for row in result["fills"]] == pytest.approx([100 / 15, -100 / 15, 4])
    assert [row["fee"] for row in result["nav"]] == [0] * 5
    assert result["nav"][-1]["cash"] == 0
    assert response["source_pins_verified"] is False
    assert response["point_in_time_verified"] is False
    assert response["live_orders"] is False


def test_fresh_cli_registration_prepare_backtest(tmp_path: Path) -> None:
    home = tmp_path / "home"

    def cli(*args: str) -> Document:
        result = run_cli(*args, home=home)
        assert result.returncode == 0, result.stdout + result.stderr
        return json.loads(result.stdout)

    home, request = register_fixture(tmp_path, cli)
    assert not (tmp_path / "incoming").exists()
    with open_workspace(home) as workspace:
        before = registration_state(workspace)
    output = tmp_path / "envelope.json"
    receipt = cli(*arguments(home, request, output))
    for key in ("envelope", "preparation"):
        assert sha(Path(receipt[key]["path"]).read_bytes()) == receipt[key]["sha256"]
    response = cli(
        "backtest",
        "--input",
        receipt["envelope"]["path"],
        "--sha256",
        receipt["envelope"]["sha256"],
    )
    assert_accounting(response)
    with open_workspace(home) as workspace:
        assert registration_state(workspace) == before


def test_prepare_accepts_home_after_the_subcommand(case: tuple[Path, Path], tmp_path: Path) -> None:
    """Every other command takes --home after its name; prepare used to reject it."""
    home, request = case
    output = tmp_path / "envelope.json"
    assert (
        main(
            [
                "prepare",
                "--request",
                str(request),
                "--sha256",
                sha(request.read_bytes()),
                "--output",
                str(output),
                "--home",
                str(home),
            ]
        )
        == 0
    )
    assert output.is_file()
