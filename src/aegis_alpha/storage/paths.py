"""Resolve the single user configuration independently of the working directory."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from aegis_alpha.data.descriptor_tree import DescriptorTree
from aegis_alpha.storage.locks import private_file, require_outside_checkout

STORE_NAMES = ("state", "strategies", "market")
DEFAULT_PATHS = {
    "state": "state.sqlite3",
    "strategies": "strategies.sqlite3",
    "market": "market.duckdb",
    "raw": "raw",
    "runs": "runs",
    "secrets": "secrets",
    "backups": "backups",
    "runtime": "runtime",
}


def resolve_home(home: Path | None = None) -> Path:
    chosen = home if home is not None else Path(os.environ.get("AAS_HOME") or "~/.aas")
    return Path(os.path.abspath(chosen.expanduser()))  # noqa: PTH100 -- normalize without following aliases


def _unique_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate configuration key")
        result[key] = value
    return result


def read_json(path: Path) -> dict[str, object]:
    private_file(path)
    with DescriptorTree.open_path(path.parent) as tree:
        value = json.loads(
            tree.read_bytes(path.name, max_bytes=1024 * 1024), object_pairs_hook=_unique_pairs
        )
    if not isinstance(value, dict):
        raise TypeError("configuration must be a JSON object")
    return cast("dict[str, object]", value)


@dataclass(frozen=True, slots=True)
class StoragePaths:
    root: Path
    state: Path
    strategies: Path
    market: Path
    raw: Path
    runs: Path
    secrets: Path
    backups: Path
    runtime: Path

    def stores(self) -> tuple[Path, ...]:
        return self.state, self.strategies, self.market

    def to_dict(self) -> dict[str, str]:
        return {key: str(getattr(self, key)) for key in DEFAULT_PATHS}


def load_paths(root: Path) -> StoragePaths:  # noqa: C901 -- one bounded config boundary
    config = read_json(root / "runtime.json")
    if (
        type(config.get("format_version")) is not int
        or config.get("format_version") != 1
        or set(config)
        - {
            "format_version",
            "paths",
            "resources",
            "providers",
            "jobs",
        }
    ):
        raise ValueError("unsupported runtime configuration")
    overrides = config.get("paths", {})
    if not isinstance(overrides, dict) or set(overrides) - DEFAULT_PATHS.keys():
        raise ValueError("runtime paths contain unknown fields")
    paths: dict[str, Path] = {}
    for key, fallback in DEFAULT_PATHS.items():
        value = overrides.get(key, fallback)
        if not isinstance(value, str) or not value or "\x00" in value:
            raise ValueError("runtime paths must be nonempty strings")
        path = Path(value).expanduser()
        paths[key] = Path(os.path.abspath(path if path.is_absolute() else root / path))  # noqa: PTH100 -- refuse symlinks later
    if len(set(paths.values())) != len(paths):
        raise ValueError("runtime paths must be distinct")
    for key, path in paths.items():
        require_outside_checkout(path)
        if path == root:
            raise ValueError("a storage path cannot replace the installation root")
        if key in STORE_NAMES and path.suffix not in {".sqlite3", ".duckdb"}:
            raise ValueError("database path must use .sqlite3 or .duckdb")
    for key, path in paths.items():
        if any(
            path.is_relative_to(other) for other_key, other in paths.items() if other_key != key
        ):
            raise ValueError("runtime storage paths cannot contain one another")
    return StoragePaths(root=root, **paths)
