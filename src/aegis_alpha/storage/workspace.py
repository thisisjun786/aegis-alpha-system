"""Create and admit one local installation without activating providers or strategies."""

from __future__ import annotations

import json
import os
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
from aegis_alpha.storage.market_schema import MIGRATIONS as MARKET_MIGRATIONS
from aegis_alpha.storage.paths import (
    DEFAULT_PATHS,
    StoragePaths,
    load_paths,
    read_json,
    resolve_home,
    same_private_file,
)

if TYPE_CHECKING:
    import duckdb

_MAX_THREADS = 256
# The core schema version aas init gives a new state and market store; None is the newest.
# A store is never upgraded here: an existing one is validated at the version it records.
_INSTALL_VERSION: int | None = None
# Core schema versions an admitted state or market store may record. The strategy store
# has its own schema and stays at version 1.
CORE_VERSIONS = tuple(range(1, len(MARKET_MIGRATIONS) + 1))


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


def store_info(connection: sqlite3.Connection | duckdb.DuckDBPyConnection) -> dict[str, object]:
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
                            cast("duckdb.DuckDBPyConnection", connection),
                            installation_id,
                            version=_INSTALL_VERSION,
                        )
                    elif kind == "state":
                        initialize_state(
                            cast("sqlite3.Connection", connection),
                            installation_id,
                            version=_INSTALL_VERSION,
                        )
                    else:
                        initialize_strategies(
                            cast("sqlite3.Connection", connection), installation_id
                        )
                    info = store_info(connection)
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
    market_resources: dict[str, object]
    _market_identity: tuple[Path, int, int]
    _market_info: dict[str, object]

    def _market_file(self) -> os.stat_result:
        observed = private_file(self.paths.market)
        if (self.paths.market, observed.st_dev, observed.st_ino) != self._market_identity:
            raise ValueError("market file changed during admitted maintenance")
        return observed

    @contextmanager
    def checkpointed_market(self) -> Iterator[None]:
        """Close/copy/reopen the same verified market file under retained admission."""
        _ = self._market_file()
        if store_info(self.market) != self._market_info:
            raise ValueError("market identity changed during admitted maintenance")
        self.market.execute("CHECKPOINT")
        # Checkpoint legitimately changes metadata, but never the admitted file identity.
        admitted = self._market_file()
        self.market.close()
        # In particular, do not bless a replacement installed by the real close boundary.
        if not same_private_file(admitted, self._market_file()):
            raise ValueError("market file changed during admitted maintenance")
        try:
            yield
        finally:
            if not same_private_file(admitted, self._market_file()):
                raise ValueError("market file changed during admitted maintenance")
            connection = market_connect(self.paths.market, resources=self.market_resources)
            try:
                observed = self._market_file()
                _verify_reopened_market(
                    connection,
                    self._market_info,
                    (admitted.st_dev, admitted.st_ino),
                    (observed.st_dev, observed.st_ino),
                )
            except BaseException:
                connection.close()
                raise
            self.market = connection

    def close_market(self) -> None:
        """Close the current handle, including one reopened under maintenance."""
        self.market.close()

    def doctor(self) -> dict[str, object]:
        return {
            "ready": True,
            "home": str(self.paths.root),
            "paths": self.paths.to_dict(),
            "storage": {"state": "sqlite", "strategies": "sqlite", "market": "duckdb"},
            # A prepared backtest request has to name the strategy store this installation
            # actually holds, so the identifier has to be readable without opening
            # installation.json or the database by hand.
            "stores": {
                "state": {"store_id": store_info(self.state)["store_id"]},
                "strategies": {"store_id": store_info(self.strategies)["store_id"]}
                if self.strategies
                else None,
                "market": {"store_id": store_info(self.market)["store_id"]},
            },
            "strategy_versions": self.strategies.execute(
                "SELECT count(*) FROM strategy_versions"
            ).fetchone()[0]
            if self.strategies
            else None,
            "schema_versions": {
                "state": store_info(self.state)["schema_version"],
                "market": store_info(self.market)["schema_version"],
            },
            "market_generations": self.market.execute(
                "SELECT count(*) FROM market_generations"
            ).fetchall()[0][0],
            "database_server_required": False,
            "docker_required": False,
            "scheduler_started": False,
            "provider_live_verification": False,
        }


def _admit_store(kind: str, info: dict[str, object], recorded: object, *, migrating: bool) -> None:
    """Match one store to the installation receipt, allowing only a migration's own lag.

    Identity always matches exactly. The schema version matches the receipt, except while
    a core migration is being finished: the stores are upgraded before the receipt is
    rewritten, so a store may then be ahead of what the receipt names, never behind.
    """
    if not isinstance(recorded, dict) or recorded.keys() != info.keys():
        raise ValueError("store identity/schema mismatch")
    if any(info[key] != recorded[key] for key in info if key != "schema_version"):
        raise ValueError("store identity/schema mismatch")
    versions = (1,) if kind == "strategies" else CORE_VERSIONS
    actual, named = info["schema_version"], recorded["schema_version"]
    if actual not in versions or named not in versions:
        raise ValueError("store identity/schema mismatch")
    if actual == named:
        return
    if not migrating and cast("int", actual) > cast("int", named):
        raise ValueError(
            "core schema migration is incomplete; repeat aas db migrate --to " + str(actual)
        )
    if cast("int", actual) < cast("int", named):
        raise ValueError("store identity/schema mismatch")


def _verify_reopened_market(
    connection: duckdb.DuckDBPyConnection,
    expected: dict[str, object],
    admitted: tuple[int, int],
    observed: tuple[int, int],
) -> None:
    if admitted != observed or store_info(connection) != expected:
        raise ValueError("market identity changed during admitted maintenance")


@contextmanager
def open_workspace(  # noqa: C901, PLR0913 -- lifecycle of all three owned stores
    home: Path | None = None,
    *,
    writable: bool = False,
    strategy_write: bool = False,
    require_strategies: bool = True,
    validating_restore: bool = False,
    migrating: bool = False,
) -> Iterator[Workspace]:
    """Admit the installation's stores under its locks for one connection lifetime.

    A core schema migration that has not finished is refused, so nothing reads or writes
    a store whose state and market halves may disagree. ``migrating`` admits such an
    installation for the migration command alone, which finishes it.
    """
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
        admitted_market = private_file(paths.market)
        market = market_connect(
            paths.market, read_only=not writable, resources=cast("dict[str, object]", resources)
        )
        stack.callback(market.close)
        observed_market = private_file(paths.market)
        if not os.path.samestat(admitted_market, observed_market):
            raise ValueError("market file changed during workspace admission")
        for kind, connection in (("state", state), ("strategies", strategies), ("market", market)):
            if connection is None:
                continue
            _admit_store(kind, store_info(connection), expected.get(kind), migrating=migrating)
        from aegis_alpha.storage.market import validate_market  # noqa: PLC0415
        from aegis_alpha.storage.migration import require_core_migration_finished  # noqa: PLC0415
        from aegis_alpha.storage.state import state_version  # noqa: PLC0415
        from aegis_alpha.storage.strategies import initialize_strategies  # noqa: PLC0415

        installation_id = str(receipt["installation_id"])
        state_version(state, installation_id)
        if strategies is not None:
            initialize_strategies(strategies, installation_id)
        validate_market(market, installation_id)
        if not migrating:
            require_core_migration_finished(state)
        workspace = Workspace(
            paths,
            state,
            strategies,
            market,
            installation_id,
            cast("dict[str, object]", resources),
            (paths.market, admitted_market.st_dev, admitted_market.st_ino),
            cast("dict[str, object]", expected["market"]),
        )
        # Maintenance may replace the closed handle, never its admission or file identity.
        stack.callback(workspace.close_market)
        yield workspace
