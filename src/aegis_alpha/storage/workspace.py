"""Create and admit one local installation without activating providers or strategies."""

from __future__ import annotations

import json
import re
import sqlite3
import uuid
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, cast

from aegis_alpha.data.descriptor_tree import DescriptorTree
from aegis_alpha.storage import sqlite
from aegis_alpha.storage.locks import file_lock, private_directory, private_file, storage_locks
from aegis_alpha.storage.paths import (
    DEFAULT_PATHS,
    StoragePaths,
    load_paths,
    read_json,
    resolve_home,
)

if TYPE_CHECKING:
    import duckdb

_MAX_THREADS = 256


def write_json(path: Path, value: object) -> None:
    with DescriptorTree.open_path(path.parent) as tree:
        tree.atomic_write_bytes(
            path.name, (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()
        )


def _receipt(root: Path) -> dict[str, object]:
    receipt = read_json(root / "installation.json")
    if (
        type(receipt.get("format_version")) is not int
        or receipt.get("format_version") != 1
        or receipt.get("phase") not in {"initializing", "ready", "restoring", "restore-incomplete"}
        or not isinstance(receipt.get("installation_id"), str)
        or not isinstance(receipt.get("stores"), dict)
    ):
        raise ValueError("unsupported installation receipt; no database was changed")
    return receipt


def _prepare(root: Path) -> dict[str, object]:
    if (root / "installation.json").is_symlink():
        raise ValueError("installation receipt cannot be a symlink")
    if (root / "installation.json").exists():
        return _receipt(root)
    if any((root / DEFAULT_PATHS[key]).exists() for key in ("state", "strategies", "market")):
        raise ValueError("unowned database exists; automatic adoption is forbidden")
    receipt: dict[str, object] = {
        "format_version": 1,
        "installation_id": uuid.uuid4().hex,
        "deployment_id": uuid.uuid4().hex,
        "phase": "initializing",
        "stores": {},
    }
    write_json(root / "installation.json", receipt)
    return receipt


def _configure(root: Path) -> StoragePaths:
    if (root / "runtime.json").is_symlink():
        raise ValueError("runtime configuration cannot be a symlink")
    if not (root / "runtime.json").exists():
        write_json(
            root / "runtime.json",
            {
                "format_version": 1,
                "paths": DEFAULT_PATHS,
                "resources": {"threads": 2, "memory_limit": "512MB"},
                "providers": {},
                "jobs": {"enabled": False},
            },
        )
    paths = load_paths(root)
    for key, path in paths.to_dict().items():
        private_directory(
            Path(path).parent if key in {"state", "strategies", "market"} else Path(path),
            create=True,
        )
    return paths


def market_connect(
    path: Path, *, read_only: bool = False, resources: dict[str, object] | None = None
) -> duckdb.DuckDBPyConnection:
    import duckdb  # noqa: PLC0415 -- DB-independent help/preview

    if read_only or path.exists():
        private_file(path)
    # Parent admission protects initial creation; DuckDB will reject an existing empty/foreign file.
    resources = resources or {"threads": 2, "memory_limit": "512MB"}
    threads, memory = resources.get("threads"), resources.get("memory_limit")
    if type(threads) is not int or not 1 <= threads <= _MAX_THREADS:
        raise ValueError("resources.threads must be an integer from 1 to 256")
    if not isinstance(memory, str) or not re.fullmatch(r"[1-9][0-9]*(?:MB|GB)", memory):
        raise ValueError("resources.memory_limit must be a positive MB or GB value")
    connection = duckdb.connect(
        str(path),
        read_only=read_only,
        config={
            "threads": threads,
            "memory_limit": memory,
            "enable_external_access": False,
        },
    )
    try:
        if not read_only:
            path.chmod(0o600)
        private_file(path)
    except BaseException:
        connection.close()
        raise
    return connection


def _store_info(connection: sqlite3.Connection | duckdb.DuckDBPyConnection) -> dict[str, object]:
    rows = connection.execute(
        "SELECT store_id, installation_id, schema_version, kind FROM store_info"
    ).fetchall()
    if len(rows) != 1:
        raise ValueError("store must have exactly one identity")
    return dict(
        zip(("store_id", "installation_id", "schema_version", "kind"), rows[0], strict=True)
    )


def initialize(home: Path | None = None) -> dict[str, object]:
    from aegis_alpha.storage.market import initialize_market  # noqa: PLC0415
    from aegis_alpha.storage.state import initialize_state  # noqa: PLC0415
    from aegis_alpha.storage.strategies import initialize_strategies  # noqa: PLC0415

    root = resolve_home(home)
    private_directory(root, create=True)
    with file_lock(root / ".storage.lock"):
        receipt = _prepare(root)
        if receipt["phase"] in {"restoring", "restore-incomplete"}:
            raise ValueError(
                "incomplete restore cannot be adopted by init; restore into a new home"
            )
        paths = _configure(root)
        installation_id = str(receipt["installation_id"])
        stores = cast("dict[str, object]", receipt["stores"])
        with ExitStack() as admission:
            for path in sorted(paths.stores()):
                admission.enter_context(file_lock(path.with_name(path.name + ".lock")))
            for kind in ("state", "strategies", "market"):
                path = getattr(paths, kind)
                if receipt["phase"] == "ready" and not path.exists():
                    raise ValueError("installed database is missing; restore into a new home")
                connection = market_connect(path) if kind == "market" else sqlite.connect(path)
                try:
                    if kind == "market":
                        initialize_market(
                            cast("duckdb.DuckDBPyConnection", connection), installation_id
                        )
                    elif kind == "state":
                        initialize_state(cast("sqlite3.Connection", connection), installation_id)
                    else:
                        initialize_strategies(
                            cast("sqlite3.Connection", connection), installation_id
                        )
                    info = _store_info(connection)
                    if kind in stores and stores[kind] != info:
                        raise ValueError("store identity differs from installation receipt")
                    stores[kind] = info
                    write_json(root / "installation.json", receipt)
                finally:
                    connection.close()
        receipt["phase"] = "ready"
        write_json(root / "installation.json", receipt)
    return {
        "initialized": True,
        "home": str(root),
        "paths": paths.to_dict(),
        "strategies_seeded": False,
        "scheduler_started": False,
    }


@dataclass(slots=True)
class Workspace:
    paths: StoragePaths
    state: sqlite3.Connection
    strategies: sqlite3.Connection | None
    market: duckdb.DuckDBPyConnection
    installation_id: str

    def doctor(self) -> dict[str, object]:
        return {
            "ready": True,
            "home": str(self.paths.root),
            "paths": self.paths.to_dict(),
            "storage": {"state": "sqlite", "strategies": "sqlite", "market": "duckdb"},
            "strategy_versions": self.strategies.execute(
                "SELECT count(*) FROM strategy_versions"
            ).fetchone()[0]
            if self.strategies
            else None,
            "market_generations": self.market.execute(
                "SELECT count(*) FROM market_generations"
            ).fetchall()[0][0],
            "database_server_required": False,
            "docker_required": False,
            "scheduler_started": False,
            "provider_live_verification": False,
        }


@contextmanager
def open_workspace(  # noqa: C901 -- lifecycle of all three owned stores
    home: Path | None = None,
    *,
    writable: bool = False,
    strategy_write: bool = False,
    require_strategies: bool = True,
    validating_restore: bool = False,
) -> Iterator[Workspace]:
    root = resolve_home(home)
    private_directory(root)
    paths = load_paths(root)
    resources = read_json(root / "runtime.json").get(
        "resources", {"threads": 2, "memory_limit": "512MB"}
    )
    if not isinstance(resources, dict):
        raise TypeError("resources must be an object")
    with storage_locks(root, paths.stores()), ExitStack() as stack:
        receipt = _receipt(root)
        if receipt["phase"] != "ready" and not (
            validating_restore and receipt["phase"] == "restoring"
        ):
            raise ValueError("installation is incomplete; rerun aas init")
        expected = receipt["stores"]
        if not isinstance(expected, dict):
            raise TypeError("missing store receipts")
        for path in (paths.state, paths.market):
            if not path.exists():
                raise ValueError("installed database is missing; restore into a new home")
        state = sqlite.connect(paths.state, read_only=not writable)
        stack.callback(state.close)
        strategies = None
        if paths.strategies.exists():
            strategies = sqlite.connect(paths.strategies, read_only=not strategy_write)
            stack.callback(strategies.close)
        elif require_strategies or strategy_write:
            raise ValueError("strategy database is missing")
        market = market_connect(
            paths.market, read_only=not writable, resources=cast("dict[str, object]", resources)
        )
        stack.callback(market.close)
        for kind, connection in (("state", state), ("strategies", strategies), ("market", market)):
            if connection is None:
                continue
            info = _store_info(connection)
            if info != expected.get(kind) or info["schema_version"] != 1:
                raise ValueError("store identity/schema mismatch")
        from aegis_alpha.storage.market import validate_market  # noqa: PLC0415
        from aegis_alpha.storage.state import initialize_state  # noqa: PLC0415
        from aegis_alpha.storage.strategies import initialize_strategies  # noqa: PLC0415

        installation_id = str(receipt["installation_id"])
        initialize_state(state, installation_id)
        if strategies is not None:
            initialize_strategies(strategies, installation_id)
        validate_market(market, installation_id)
        yield Workspace(paths, state, strategies, market, installation_id)
