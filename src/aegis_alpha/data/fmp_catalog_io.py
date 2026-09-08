"""Read-only, descriptor-bound evidence admission for FMP catalog registration."""

from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Mapping
from contextlib import ExitStack
from datetime import datetime
from pathlib import Path
from typing import Self, cast

from aegis_alpha.data.descriptor_tree import DescriptorTree
from aegis_alpha.data.serialization import canonical_json_bytes

MAX_JSON_BYTES = 16 * 1024 * 1024


class FmpCatalogError(ValueError):
    """Catalog evidence is incomplete or conflicts with immutable run identity."""


def require(condition: bool, message: str) -> None:  # noqa: FBT001 -- assertion-style trust boundary
    if not condition:
        raise FmpCatalogError(message)


def object_value(value: object) -> dict[str, object]:
    require(isinstance(value, Mapping), "FMP catalog expected a JSON object")
    return dict(cast("Mapping[str, object]", value))


def list_value(value: object) -> list[object]:
    require(isinstance(value, list), "FMP catalog expected a JSON array")
    return list(cast("list[object]", value))


def digest(value: object) -> str:
    require(
        isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None,
        "FMP catalog digest is invalid",
    )
    return str(value)


def instant(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value))
    require(parsed.utcoffset() is not None, "FMP catalog timestamp must be timezone aware")
    return parsed


def identifier(value: str) -> str:
    require(
        re.fullmatch(r"[a-z0-9][a-z0-9._:-]{0,254}", value) is not None,
        "FMP catalog run identity is invalid",
    )
    return value


def _pairs(items: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in items:
        require(key not in result, "FMP catalog JSON contains duplicate keys")
        result[key] = value
    return result


def json_object(payload: bytes) -> dict[str, object]:
    document = object_value(json.loads(payload, object_pairs_hook=_pairs))
    require(payload == canonical_json_bytes(document), "FMP catalog evidence is not canonical")
    return document


class CatalogFiles:
    """Keep admitted root descriptors alive and detect changes before catalog commit."""

    def __init__(self, raw_root: Path, dataset_root: Path) -> None:
        for root in (raw_root, dataset_root):
            require(
                root.is_absolute() and ".." not in root.parts,
                "FMP catalog roots must be absolute without traversal",
            )
        require(
            raw_root != dataset_root and raw_root.parent == dataset_root.parent,
            "FMP catalog roots must be distinct siblings in one data home",
        )
        require(raw_root.parent != Path("/"), "FMP catalog data home cannot be filesystem root")
        self.home = raw_root.parent
        self.raw_root = raw_root
        self.dataset_root = dataset_root
        self._stack = ExitStack()
        self._trees: dict[Path, DescriptorTree] = {}
        self._observed: dict[Path, tuple[int, int, int, int, int]] = {}

    def __enter__(self) -> Self:
        try:
            for root in (self.home, self.raw_root, self.dataset_root):
                self._trees[root] = self._stack.enter_context(DescriptorTree.open_path(root))
        except BaseException:
            self._stack.close()
            raise
        return self

    def __exit__(self, *_args: object) -> None:
        self._stack.close()

    def tree(self, root: Path) -> DescriptorTree:
        return self._trees[root]

    def relative(self, root: Path, path: Path) -> str:
        require(
            path.is_absolute() and ".." not in path.parts and path.is_relative_to(root),
            "FMP catalog file escapes its configured root",
        )
        return str(path.relative_to(root))

    def verify(self, root: Path, path: Path, expected: str | None = None) -> tuple[int, str]:
        relative = self.relative(root, path)
        with self.tree(root).binary_reader(relative) as handle:
            before = os.fstat(handle.fileno())
            hasher = hashlib.sha256()
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                hasher.update(chunk)
            hashed = hasher.hexdigest()
            after = os.fstat(handle.fileno())
        stamp = self._stamp(before)
        require(stamp == self._stamp(after), "FMP catalog file changed during verification")
        require(expected is None or hashed == digest(expected), "FMP catalog file hash mismatch")
        prior = self._observed.setdefault(path, stamp)
        require(prior == stamp, "FMP catalog evidence changed between reads")
        return after.st_size, hashed

    def read(self, root: Path, path: Path, expected: str | None = None) -> bytes:
        size, hashed = self.verify(root, path, expected)
        require(size <= MAX_JSON_BYTES, "FMP catalog JSON exceeds the 16 MiB limit")
        payload = self.tree(root).read_bytes(self.relative(root, path), max_bytes=MAX_JSON_BYTES)
        require(hashlib.sha256(payload).hexdigest() == hashed, "FMP catalog evidence changed")
        return payload

    def revalidate(self) -> None:
        for root, tree in self._trees.items():
            with DescriptorTree.open_path(root) as visible:
                require(visible.identity == tree.identity, "FMP catalog root identity changed")
        for path, stamp in self._observed.items():
            require(
                self._stamp(self.tree(self.home).stat(self.relative(self.home, path))) == stamp,
                "FMP catalog file changed before commit",
            )

    @staticmethod
    def _stamp(info: os.stat_result) -> tuple[int, int, int, int, int]:
        return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns
